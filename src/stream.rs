//! Одно WS-соединение к KuCoin: подписка на топики свечей, keepalive,
//! разбор обновлений и вывод в stdout.

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::kucoin::BulletInfo;
use crate::stats::Stats;

/// Конкретный тип WS-потока, возвращаемый `connect_async` с TLS.
type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Отладочная печать keepalive-событий (KCS_DEBUG_PING=1).
fn dbg_enabled() -> bool {
    static E: OnceLock<bool> = OnceLock::new();
    *E.get_or_init(|| {
        std::env::var("KCS_DEBUG_PING")
            .is_ok_and(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
    })
}

fn dbg_log(msg: &str) {
    if dbg_enabled() {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        eprintln!("[dbg {ms}] {msg}");
    }
}

/// Порог тишины на соединении: сервер шлёт ping каждые `pingInterval` мс,
/// поэтому отсутствие любых сообщений дольше бюджета = мёртвое соединение.
fn idle_budget(info: &BulletInfo) -> Duration {
    Duration::from_millis(info.ping_interval_ms + info.ping_timeout_ms + 10_000)
}

/// Бесконечный цикл соединения с переподключением; завершается по сигналу
/// остановки (`shutdown`), предварительно отписавшись от всех топиков.
///
/// Публичный WS-токен KuCoin действителен ограниченное время (около суток),
/// поэтому свежий токен запрашивается перед каждой попыткой подключения.
pub async fn run_connection(
    api_base: &str,
    topics: Vec<String>,
    conn_no: usize,
    stats: std::sync::Arc<Stats>,
    emit_candles: bool,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut attempt: u32 = 0;
    loop {
        if *shutdown.borrow() {
            eprintln!("[conn {conn_no}] остановка по сигналу");
            return;
        }
        // Свежий токен/эндпоинт перед коннектом (переживает 24h-лимит токена).
        let info = match crate::kucoin::fetch_bullet(api_base).await {
            Ok(info) => info,
            Err(e) => {
                eprintln!("[conn {conn_no}] не удалось получить ws-токен: {e:#}");
                let delay = Duration::from_secs(1u64 << attempt.min(5));
                attempt = attempt.saturating_add(1);
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown.changed() => {}
                }
                continue;
            }
        };
        let result =
            connect_once(&info, &topics, conn_no, &stats, emit_candles, &mut shutdown).await;
        // Сигнал остановки мог прийти во время connect_once — не переподключаемся.
        if *shutdown.borrow() {
            return;
        }
        match result {
            Ok(()) => eprintln!("[conn {conn_no}] соединение закрыто, переподключаюсь"),
            Err(e) => eprintln!("[conn {conn_no}] ошибка: {e:#}"),
        }
        let delay = Duration::from_secs(1u64 << attempt.min(5));
        attempt = attempt.saturating_add(1);
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown.changed() => {}
        }
    }
}

/// Шлёт unsubscribe по всем топикам соединения и закрывает WS.
async fn unsubscribe_and_close(ws: &mut WsStream, topics: &[String], conn_no: usize) {
    let mut unsubscribed = 0usize;
    for (i, topic) in topics.iter().enumerate() {
        let msg = json!({
            "id": format!("u{conn_no}-{i}"),
            "type": "unsubscribe",
            "topic": topic,
            "privateChannel": false,
            "response": true,
        })
        .to_string();
        // Соединение могло уже умереть — тогда и отписываться не от чего.
        if ws.send(Message::Text(msg.into())).await.is_err() {
            break;
        }
        unsubscribed += 1;
    }
    let _ = ws.close(None).await;
    eprintln!("[conn {conn_no}] отписался от {unsubscribed} топиков");
}

/// Одно подключение: коннект, подписки, чтение до разрыва/ошибки/сигнала.
async fn connect_once(
    info: &BulletInfo,
    topics: &[String],
    conn_no: usize,
    stats: &Stats,
    emit_candles: bool,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let connect_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let url = format!(
        "{}/?token={}&connectId={}",
        info.endpoint.trim_end_matches('/'),
        info.token,
        connect_id
    );

    let (mut ws, _) = connect_async(&url)
        .await
        .with_context(|| format!("WS connect: {url}"))?;

    // Подписка на все топики свечей этой порции.
    for (i, topic) in topics.iter().enumerate() {
        let msg = json!({
            "id": format!("{conn_no}-{i}"),
            "type": "subscribe",
            "topic": topic,
            "privateChannel": false,
            "response": true,
        })
        .to_string();
        ws.send(Message::Text(msg.into()))
            .await
            .context("отправка подписки")?;
    }
    eprintln!("[conn {conn_no}] подключён, {} подписок", topics.len());

    let budget = idle_budget(info);
    // KuCoin ожидает от клиента регулярные JSON-ping независимо от трафика
    // (иначе закрывает соединение с "ping timeout"). Шлём каждые pingInterval.
    let ping_period = Duration::from_millis(info.ping_interval_ms.max(1000));
    let mut ping_ticker =
        tokio::time::interval_at(tokio::time::Instant::now() + ping_period, ping_period);
    ping_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ping_seq: u64 = 0;
    loop {
        // Получили сигнал остановки — отписываемся и выходим.
        if *shutdown.borrow() {
            unsubscribe_and_close(&mut ws, topics, conn_no).await;
            return Ok(());
        }
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                unsubscribe_and_close(&mut ws, topics, conn_no).await;
                return Ok(());
            }
            // Регулярный клиентский ping для поддержания соединения.
            _ = ping_ticker.tick() => {
                ping_seq = ping_seq.wrapping_add(1);
                let ping = json!({
                    "id": format!("{conn_no}-p{connect_id}-{ping_seq}"),
                    "type": "ping",
                })
                .to_string();
                dbg_log(&format!("[conn {conn_no}] шлю клиентский ping #{ping_seq}"));
                ws.send(Message::Text(ping.into())).await.ok();
            }
            next = tokio::time::timeout(budget, ws.next()) => {
                let next = next.context("таймаут чтения: соединение молчит")?;
                let msg = match next {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => return Err(e).context("ошибка чтения WS"),
                    None => bail!("WS закрыт сервером"),
                };
                match msg {
                    Message::Text(text) => {
                        handle_text(&mut ws, text.to_string(), conn_no, stats, emit_candles).await?
                    }
                    Message::Ping(payload) => {
                        dbg_log(&format!(
                            "[conn {conn_no}] фрейм-ping от сервера ({} байт) -> pong",
                            payload.len()
                        ));
                        ws.send(Message::Pong(payload)).await.ok();
                    }
                    Message::Pong(payload) => {
                        dbg_log(&format!("[conn {conn_no}] фрейм-pong от сервера ({payload:?})"));
                    }
                    Message::Close(_) => bail!("WS закрыт сервером"),
                    _ => {}
                }
            }
        }
    }
}

/// Обрабатывает текстовое сообщение KuCoin.
async fn handle_text(
    ws: &mut WsStream,
    text: String,
    conn_no: usize,
    stats: &Stats,
    emit_candles: bool,
) -> Result<()> {
    let v: serde_json::Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => {
            eprintln!("[ws] не-JSON сообщение: {text}");
            return Ok(());
        }
    };
    let msg_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("?");

    match msg_type {
        "welcome" => eprintln!("[ws] welcome получено"),
        "pong" => {
            dbg_log(&format!(
                "[conn {conn_no}] pong от сервера на наш ping (id={:?})",
                v.get("id")
            ));
        }
        "ack" => {}
        // Сервер шлёт ping — отвечаем pong с тем же id.
        "ping" => {
            dbg_log(&format!(
                "[conn {conn_no}] ping от сервера (id={:?}) -> pong",
                v.get("id")
            ));
            let reply =
                json!({ "type": "pong", "id": v.get("id").cloned().unwrap_or(json!(null)) });
            ws.send(Message::Text(reply.to_string().into()))
                .await
                .context("отправка pong")?;
            dbg_log(&format!("[conn {conn_no}] pong отправлен"));
        }
        "message" => {
            let topic = v.get("topic").and_then(|t| t.as_str()).unwrap_or("");
            // Считаем пришедшие сообщения канала свечей по интервалу.
            if let Some(interval) = topic
                .strip_prefix("/market/candles:")
                .and_then(|rest| rest.split_once('_'))
                .map(|(_, interval)| interval)
            {
                stats.record(interval);
                if emit_candles {
                    // Построчный вывод свечей — только по запросу (KCS_EMIT_CANDLES=1).
                    if let Some(line) = candle_line(&v, topic) {
                        emit(&line);
                    } else {
                        stats.record_parse_error();
                    }
                }
            }
        }
        "error" => eprintln!("[ws] error: {text}"),
        "notice" => eprintln!("[ws] notice: {text}"),
        other => eprintln!("[ws] неизвестный тип '{other}': {text}"),
    }
    Ok(())
}

/// Извлекает (символ, интервал) из топика `/market/candles:SYM_1min`.
fn symbol_interval_from_topic(topic: &str) -> Option<(String, String)> {
    let rest = topic.strip_prefix("/market/candles:")?;
    let (symbol, interval) = rest.split_once('_')?;
    Some((symbol.to_string(), interval.to_string()))
}

/// Строит строку лога из сообщения канала свечей (новый и старый форматы).
fn candle_line(v: &serde_json::Value, topic: &str) -> Option<String> {
    let data = v.get("data")?;
    let (topic_symbol, topic_interval) = symbol_interval_from_topic(topic)?;

    let (symbol, candles, update_ms) = match data {
        // Новый формат: {"symbol": "...", "candles": [...], "time": ...}
        serde_json::Value::Object(map) => {
            let candles = map.get("candles")?.as_array()?;
            let symbol = map
                .get("symbol")
                .and_then(|s| s.as_str())
                .map(|s| s.to_string())
                .unwrap_or(topic_symbol.clone());
            let update_ms = map.get("time").and_then(|t| t.as_i64());
            (symbol, candles, update_ms)
        }
        // Старый формат: data — сразу массив свечи.
        serde_json::Value::Array(candles) => (topic_symbol.clone(), candles, None),
        _ => return None,
    };

    if candles.len() != 7 {
        return None;
    }
    let num = |i: usize| candles[i].as_str().and_then(|s| s.parse::<f64>().ok());
    let start = candles[0].as_str()?.parse::<i64>().ok()?;
    let open = num(1)?;
    let close = num(2)?;
    let high = num(3)?;
    let low = num(4)?;
    let volume = num(5)?;
    let turnover = num(6)?;

    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    // Поле time у KuCoin в новых сообщениях — в наносекундах; нормализуем в мс.
    let update_ms = update_ms.map(|t| {
        if t >= 100_000_000_000_000 {
            t / 1_000_000
        } else {
            t
        }
    });

    let rec = json!({
        "type": "candle",
        "exchange": "kucoin",
        "ts": ts,
        "symbol": symbol,
        "interval": topic_interval,
        "start": start,
        "update": update_ms,
        "open": open,
        "close": close,
        "high": high,
        "low": low,
        "volume": volume,
        "turnover": turnover,
    });
    Some(rec.to_string())
}

/// Атомарно печатает строку в stdout.
fn emit(line: &str) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let _ = writeln!(out, "{line}");
}
