//! kcs-monitor: поток свечей (kline) KuCoin по всем торгуемым парам.
//!
//! Схема работы:
//!   1. REST /api/v1/symbols — список торгуемых пар;
//!   2. подписка `/market/candles:<SYM>_<interval>` по всем парам,
//!      распределённым по нескольким WS-соединениям (лимит KuCoin —
//!      100 подписок на соединение);
//!   3. supervisor каждые `KCS_RESYNC_SECONDS` пересматривает список пар:
//!      если появились новые листинги, соединения плавно переподключаются
//!      на обновлённый набор топиков (старые корректно отписываются);
//!   4. свежий WS-токен запрашивается перед каждым подключением
//!      (токен KuCoin живёт ~24 ч);
//!   5. ведётся статистика принятых сообщений по интервалам; раз в
//!      `KCS_STATS_SECONDS` секунд в stdout печатается JSON-строка со
//!      счётчиками. Построчный вывод самих свечей выключен и включается
//!      через `KCS_EMIT_CANDLES=1`.

mod backfill;
mod config;
mod http;
mod kucoin;
mod stats;
mod stream;

use std::sync::Arc;
use std::time::Duration;

use config::Config;
use tokio::sync::watch;

use stats::Stats;

/// Поколение соединений: полный набор топиков и запущенные задачи.
struct Generation {
    topics: Vec<String>,
    tasks: tokio::task::JoinSet<()>,
    shutdown_tx: watch::Sender<bool>,
}

impl Generation {
    /// Плавно останавливает все соединения (отписка + закрытие), ждёт их
    /// до 10 секунд, остальные абортит. Возвращает число не успевших.
    async fn stop(mut self) -> usize {
        let _ = self.shutdown_tx.send(true);
        let mut deadline = std::pin::pin!(tokio::time::sleep(Duration::from_secs(10)));
        while !self.tasks.is_empty() {
            tokio::select! {
                _ = &mut deadline => break,
                _ = self.tasks.join_next() => {}
            }
        }
        let unfinished = self.tasks.len();
        self.tasks.abort_all();
        unfinished
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::from_env();
    let emit_candles = cfg.emit_candles;

    // Проверяем интервалы, чтобы опечатка не упала молча на подписке.
    for interval in &cfg.kline_intervals {
        if !kucoin::VALID_KLINE_INTERVALS.contains(&interval.as_str()) {
            anyhow::bail!(
                "неизвестный интервал '{interval}'; допустимые: {}",
                kucoin::VALID_KLINE_INTERVALS.join(", ")
            );
        }
    }
    eprintln!(
        "kcs-monitor: intervals={} subs/conn={} api={}",
        cfg.kline_intervals.join(","),
        cfg.subs_per_connection,
        cfg.api_base
    );

    // Диагностика: текущий WS-шлюз (сами соединения берут свежий токен сами).
    match kucoin::fetch_bullet(&cfg.api_base).await {
        Ok(b) => eprintln!(
            "kcs-monitor: ws endpoint={} (ping {}ms / timeout {}ms)",
            b.endpoint, b.ping_interval_ms, b.ping_timeout_ms
        ),
        Err(e) => eprintln!("kcs-monitor: не удалось получить ws-токен при старте: {e:#}"),
    }
    eprintln!(
        "kcs-monitor: вывод свечей: {}; статистика: каждые {}s; ресинк пар: {}",
        if emit_candles {
            "вкл (KCS_EMIT_CANDLES)"
        } else {
            "выкл"
        },
        cfg.stats_period_secs,
        if cfg.resync_secs == 0 {
            "выкл".to_string()
        } else {
            format!("каждые {}s", cfg.resync_secs)
        }
    );

    // Статистика и её reporter — живут всё время работы процесса.
    let stats = Arc::new(Stats::default());
    let stats_for_reporter = stats.clone();
    let reporter_period = Duration::from_secs(cfg.stats_period_secs);
    let reporter = tokio::spawn(async move {
        stats_reporter(stats_for_reporter, reporter_period).await;
    });

    // Ресинк: None = выключен (ждём вечно), иначе период проверки.
    let resync_period = (cfg.resync_secs > 0).then(|| Duration::from_secs(cfg.resync_secs));

    let mut generation: Option<Generation> = None;
    let exit_code = loop {
        // Свежий список пар.
        let symbols = match kucoin::fetch_symbols(&cfg.api_base, &cfg.symbols_override).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("kcs-monitor: не удалось обновить список пар: {e:#}");
                if shutdown_requested(Duration::from_secs(30)).await {
                    break 0;
                }
                continue;
            }
        };
        if symbols.is_empty() {
            eprintln!("kcs-monitor: список символов пуст — жду следующей проверки");
            if shutdown_requested(Duration::from_secs(60)).await {
                break 0;
            }
            continue;
        }

        // Топики = каждая пара × каждый интервал.
        let mut topics: Vec<String> = Vec::with_capacity(symbols.len() * cfg.kline_intervals.len());
        for symbol in &symbols {
            for interval in &cfg.kline_intervals {
                topics.push(format!("/market/candles:{symbol}_{interval}"));
            }
        }
        topics.sort();

        // REST-бэкфилл закрытых свечей — один раз, при первом старте.
        if cfg.backfill_bars > 0 && generation.is_none() {
            eprintln!(
                "kcs-monitor: бэкфилл закрытых свечей (bars={}, пар={}, интервалов={})",
                cfg.backfill_bars,
                symbols.len(),
                cfg.kline_intervals.len()
            );
            let summary = backfill::run(
                &cfg.api_base,
                &symbols,
                &cfg.kline_intervals,
                cfg.backfill_bars,
                cfg.backfill_concurrency,
            )
            .await;
            eprintln!(
                "kcs-monitor: бэкфилл завершён: пар ок {}, ошибок {}, строк {}",
                summary.pairs_ok, summary.pairs_err, summary.lines
            );
        }

        // Ресинк нужен, если набор топиков изменился (или первый запуск,
        // или включён принудительный режим для отладки).
        let changed = generation.as_ref().is_none_or(|g| g.topics != topics) || cfg.resync_force;
        if changed {
            if let Some(old) = generation.take() {
                let n = old.stop().await;
                if n > 0 {
                    eprintln!("kcs-monitor: {n} соединений не успели отписаться при ресинке");
                }
            }
            generation = Some(spawn_generation(&cfg, topics.clone(), &stats, emit_candles).await);
            let conns = generation.as_ref().expect("generation").tasks.len();
            eprintln!(
                "kcs-monitor: поколение активно: топиков {}, соединений {}",
                topics.len(),
                conns
            );
        }

        // Ждём: сигнал остановки / панику соединения / следующий тик ресинка.
        let wait_signal = wait_for_shutdown_signal();
        let tick = resync_tick(resync_period);
        tokio::pin!(wait_signal);
        tokio::pin!(tick);
        tokio::select! {
            _ = &mut wait_signal => break 0,
            finished = generation.as_mut().expect("generation").tasks.join_next() => {
                if let Some(Err(e)) = finished {
                    eprintln!("kcs-monitor: аварийное завершение соединения: {e}");
                    break 1;
                }
            }
            _ = &mut tick => {
                // Время проверить листинги заново.
            }
        }
    };

    // Финальная остановка.
    if let Some(generation) = generation.take() {
        let n = generation.stop().await;
        if n > 0 {
            eprintln!("kcs-monitor: {n} соединений не успели отписаться за отведённое время");
        }
    }
    reporter.abort();
    print_stats(&stats);
    eprintln!("kcs-monitor: остановлен");
    std::process::exit(exit_code);
}

/// Ждёт тика ресинка (или никогда, если ресинк выключен).
async fn resync_tick(period: Option<Duration>) {
    match period {
        Some(p) => tokio::time::sleep(p).await,
        None => std::future::pending().await,
    }
}

/// Ждёт сигнал остановки либо таймаут (используется в циклах ошибок).
async fn shutdown_requested(wait: Duration) -> bool {
    tokio::select! {
        _ = wait_for_shutdown_signal() => true,
        _ = tokio::time::sleep(wait) => false,
    }
}

/// Запускает соединения для одного набора топиков (одно поколение).
async fn spawn_generation(
    cfg: &Config,
    topics: Vec<String>,
    stats: &Arc<Stats>,
    emit_candles: bool,
) -> Generation {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();
    let api_base = cfg.api_base.clone();
    for (conn_no, chunk) in topics.chunks(cfg.subs_per_connection).enumerate() {
        let api_base = api_base.clone();
        let chunk = chunk.to_vec();
        let stats = stats.clone();
        let shutdown = shutdown_rx.clone();
        tasks.spawn(async move {
            stream::run_connection(&api_base, chunk, conn_no, stats, emit_candles, shutdown).await
        });
    }
    Generation {
        topics,
        tasks,
        shutdown_tx,
    }
}

/// Периодически печатает статистику принятых сообщений в stdout.
async fn stats_reporter(stats: Arc<Stats>, period: Duration) {
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut prev_total: u64 = 0;
    let mut prev_at = std::time::Instant::now();
    loop {
        ticker.tick().await;
        let (total, by_interval, errors) = stats.snapshot();
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(prev_at).as_secs_f64().max(0.001);
        let rate = (total - prev_total) as f64 / elapsed;
        prev_total = total;
        prev_at = now;

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let by_interval: serde_json::Map<String, serde_json::Value> = by_interval
            .into_iter()
            .map(|(k, v)| (k, serde_json::json!(v)))
            .collect();
        let line = serde_json::json!({
            "type": "stats",
            "ts": ts,
            "total": total,
            "rate_ps": (rate * 1000.0).round() / 1000.0,
            "parse_errors": errors,
            "by_interval": by_interval,
        });
        println!("{line}");
    }
}

/// Печатает разовый снимок статистики в stdout.
fn print_stats(stats: &Arc<Stats>) {
    let (total, by_interval, errors) = stats.snapshot();
    let by_interval: serde_json::Map<String, serde_json::Value> = by_interval
        .into_iter()
        .map(|(k, v)| (k, serde_json::json!(v)))
        .collect();
    let line = serde_json::json!({
        "type": "stats",
        "final": true,
        "total": total,
        "parse_errors": errors,
        "by_interval": by_interval,
    });
    println!("{line}");
}

/// Ждёт SIGINT (ctrl-c) или SIGTERM (docker stop и т.п.).
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("не удалось подписаться на SIGTERM");
        let mut sigint = signal(SignalKind::interrupt()).expect("не удалось подписаться на SIGINT");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
