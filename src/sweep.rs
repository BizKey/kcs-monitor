//! Свип закрытых свечей через REST: по каждой паре × интервалу забираем
//! последние закрытые бары и пишем их в БД (или печатаем, если БД не задана).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use tokio::sync::Semaphore;

use crate::candle::{candle_to_json_line, from_rest_candle};
use crate::config::Config;
use crate::db::CandleSender;
use crate::kucoin::{RestCandle, fetch_kline_page, forming_bucket_start};

/// Максимум баров, которые REST отдаёт за один запрос (страница).
const PAGE_MAX: usize = 100;
/// Ограничение «глубины» свипа на пару×интервал (страховка).
const BARS_CAP: usize = 1500;

/// Итоги свипа.
#[derive(Debug, Default)]
pub struct Summary {
    pub pairs_ok: usize,
    pub pairs_err: usize,
    pub candles: u64,
}

/// Печатает свечу в stdout.
fn emit_line(c: &crate::candle::CandleUpdate) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let _ = writeln!(out, "{}", candle_to_json_line(c));
}

/// Забирает с REST до `bars` последних ЗАКРЫТЫХ свечей (новые сверху).
async fn fetch_closed(
    api_base: &str,
    symbol: &str,
    interval: &str,
    bars: usize,
) -> Result<Vec<RestCandle>> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let forming = forming_bucket_start(interval, now)
        .ok_or_else(|| anyhow::anyhow!("неизвестный интервал {interval}"))?;

    let cap = bars.min(BARS_CAP);
    let mut closed: Vec<RestCandle> = Vec::with_capacity(cap);
    let mut end_at: Option<i64> = Some(now);
    let mut pages = 0usize;

    while closed.len() < cap && pages < BARS_CAP / PAGE_MAX + 2 {
        let page = fetch_kline_page(api_base, symbol, interval, None, end_at).await?;
        pages += 1;
        if page.is_empty() {
            break;
        }
        let oldest = page.last().expect("not empty").start_ts;
        // Первая строка может быть текущей (незакрытой) свечой — пропускаем.
        for c in page {
            if c.start_ts < forming {
                closed.push(c);
            }
        }
        if closed.len() >= cap {
            break;
        }
        // Следующая страница — строго старее самой старой строки.
        let next_end = oldest - 1;
        if Some(next_end) >= end_at {
            break; // защита от зацикливания
        }
        end_at = Some(next_end);
    }

    closed.truncate(cap);
    Ok(closed)
}

/// Один свип по всем парам × интервалам с ограниченной конкурентностью.
pub async fn run(cfg: &Config, symbols: &[String], db_tx: Option<&CandleSender>) -> Summary {
    let mut summary = Summary::default();
    if symbols.is_empty() || cfg.kline_intervals.is_empty() {
        return summary;
    }
    let sem = Arc::new(Semaphore::new(cfg.concurrency));
    let mut tasks = tokio::task::JoinSet::new();

    for symbol in symbols {
        for interval in &cfg.kline_intervals {
            let api_base = cfg.api_base.clone();
            let exchange = cfg.exchange.clone();
            let symbol = symbol.clone();
            let interval = interval.clone();
            let bars = cfg.bars;
            let sem = sem.clone();
            let db_tx = db_tx.cloned();
            tasks.spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore");
                let closed = fetch_closed(&api_base, &symbol, &interval, bars).await?;
                let mut n = 0u64;
                for c in closed {
                    let update = from_rest_candle(&exchange, &symbol, &interval, c);
                    match &db_tx {
                        Some(tx) => {
                            tx.send(update).await.ok();
                        }
                        None => emit_line(&update),
                    }
                    n += 1;
                }
                Ok::<u64, anyhow::Error>(n)
            });
        }
    }

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(n)) => {
                summary.pairs_ok += 1;
                summary.candles += n;
            }
            Ok(Err(e)) => {
                summary.pairs_err += 1;
                eprintln!("[sweep] ошибка: {e:#}");
            }
            Err(e) => {
                summary.pairs_err += 1;
                eprintln!("[sweep] задача упала: {e}");
            }
        }
    }
    summary
}
