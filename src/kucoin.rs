//! KuCoin REST: список торгуемых символов и публичный WS-токен.

use anyhow::{Context, Result, bail};
use serde::Deserialize;

pub const DEFAULT_API_BASE: &str = "https://api.kucoin.com";
const OK_CODE: &str = "200000";

/// Допустимые типы свечей KuCoin (spot): используются в топиках подписки
/// и в параметре `type` REST-запроса истории.
pub const VALID_KLINE_INTERVALS: &[&str] = &[
    "1min", "3min", "5min", "15min", "30min", "1hour", "2hour", "4hour", "6hour", "8hour",
    "12hour", "1day", "1week",
];

/// Длина интервала в секундах (для расчёта границ закрытия свечей).
pub fn interval_seconds(interval: &str) -> Option<u64> {
    Some(match interval {
        "1min" => 60,
        "3min" => 180,
        "5min" => 300,
        "15min" => 900,
        "30min" => 1800,
        "1hour" => 3600,
        "2hour" => 7200,
        "4hour" => 14_400,
        "6hour" => 21_600,
        "8hour" => 28_800,
        "12hour" => 43_200,
        "1day" => 86_400,
        "1week" => 604_800,
        _ => return None,
    })
}

/// Начало текущего «открытого» бара интервала по времени `now` (unix-сек).
/// Все бары со start меньше этого значения — закрыты. Неделя у KuCoin
/// начинается в понедельник 00:00 UTC, поэтому нужна поправка
/// (эпоха Unix стартовала в четверг).
pub fn forming_bucket_start(interval: &str, now: i64) -> Option<i64> {
    let period = interval_seconds(interval)? as i64;
    if interval == "1week" {
        const MONDAY_OFFSET: i64 = 3 * 86_400;
        Some((now + MONDAY_OFFSET) / period * period - MONDAY_OFFSET)
    } else {
        Some(now / period * period)
    }
}

/// Одна строка свечи из REST `/api/v1/market/candles`:
/// [start, open, close, high, low, volume, turnover].
#[derive(Debug, Clone)]
pub struct RestCandle {
    pub start_ts: i64,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64,
    pub turnover: f64,
}

/// Запрашивает страницу свечей (новые сверху, до ~100 строк) за окно
/// [start_at, end_at] (unix-сек; None = без соответствующей границы).
pub async fn fetch_kline_page(
    api_base: &str,
    symbol: &str,
    interval: &str,
    start_at: Option<i64>,
    end_at: Option<i64>,
) -> Result<Vec<RestCandle>> {
    let mut params = vec![
        ("type", interval.to_string()),
        ("symbol", symbol.to_string()),
    ];
    if let Some(s) = start_at {
        params.push(("startAt", s.to_string()));
    }
    if let Some(e) = end_at {
        params.push(("endAt", e.to_string()));
    }
    let path = format!(
        "/api/v1/market/candles?{}",
        params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&")
    );
    let v = crate::http::get_json(api_base, &path).await?;
    ensure_ok(&v)?;

    let rows: Vec<Vec<String>> = serde_json::from_value(
        v.get("data")
            .cloned()
            .context("нет data в ответе candles")?,
    )
    .context("не удалось разобрать строки свечей")?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if row.len() != 7 {
            bail!("свеча не из 7 полей: {row:?}");
        }
        let num = |i: usize| -> Result<f64> {
            row[i].parse().with_context(|| format!("число в {row:?}"))
        };
        out.push(RestCandle {
            start_ts: row[0].parse().with_context(|| format!("start в {row:?}"))?,
            open: num(1)?,
            close: num(2)?,
            high: num(3)?,
            low: num(4)?,
            volume: num(5)?,
            turnover: num(6)?,
        });
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct BulletInfo {
    /// Токен для подключения к публичному WebSocket.
    pub token: String,
    /// Адрес WS-шлюза (берём из ответа bullet-public, а не хардкодим).
    pub endpoint: String,
    pub ping_interval_ms: u64,
    pub ping_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
struct Symbol {
    #[serde(rename = "symbol")]
    symbol: String,
    #[serde(rename = "enableTrading", default)]
    enable_trading: bool,
}

/// Проверяет код ответа KuCoin API.
fn ensure_ok(v: &serde_json::Value) -> Result<()> {
    match v.get("code").and_then(|c| c.as_str()) {
        Some(OK_CODE) => Ok(()),
        other => bail!(
            "KuCoin API code={:?} msg={:?}",
            other,
            v.get("msg").and_then(|m| m.as_str())
        ),
    }
}

/// Возвращает отсортированный список торгуемых пар (или отфильтрованный по
/// `symbols_override`, если он задан).
pub async fn fetch_symbols(
    api_base: &str,
    symbols_override: &Option<String>,
) -> Result<Vec<String>> {
    let v = crate::http::get_json(api_base, "/api/v1/symbols").await?;
    ensure_ok(&v)?;
    let symbols: Vec<Symbol> = serde_json::from_value(
        v.get("data")
            .cloned()
            .context("нет data в ответе symbols")?,
    )
    .context("не удалось разобрать список символов")?;

    let mut list: Vec<String> = symbols
        .into_iter()
        .filter(|s| s.enable_trading)
        .map(|s| s.symbol)
        .collect();

    if let Some(filter) = symbols_override {
        let wanted: std::collections::HashSet<String> =
            filter.split(',').map(|s| s.trim().to_string()).collect();
        list.retain(|s| wanted.contains(s));
    }
    list.sort();
    list.dedup();
    Ok(list)
}

/// Получает публичный WS-токен и адрес шлюза.
pub async fn fetch_bullet(api_base: &str) -> Result<BulletInfo> {
    let v =
        crate::http::post_json(api_base, "/api/v1/bullet-public", &serde_json::Value::Null).await?;
    ensure_ok(&v)?;
    let data = v.get("data").context("нет data в ответе bullet-public")?;
    let token = data
        .get("token")
        .and_then(|t| t.as_str())
        .context("нет token")?
        .to_string();
    let server = data
        .get("instanceServers")
        .and_then(|s| s.as_array())
        .and_then(|arr| arr.first())
        .context("нет instanceServers")?;
    let endpoint = server
        .get("endpoint")
        .and_then(|e| e.as_str())
        .context("нет endpoint")?
        .trim_end_matches('/')
        .to_string();
    let ping_interval_ms = server
        .get("pingInterval")
        .and_then(|p| p.as_u64())
        .unwrap_or(18_000);
    let ping_timeout_ms = server
        .get("pingTimeout")
        .and_then(|p| p.as_u64())
        .unwrap_or(10_000);

    Ok(BulletInfo {
        token,
        endpoint,
        ping_interval_ms,
        ping_timeout_ms,
    })
}
