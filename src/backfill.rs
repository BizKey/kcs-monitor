//! REST-бэкфилл закрытых свечей.
//!
//! При запуске (KCS_BACKFILL_BARS=N) для каждой пары × интервала через REST
//! `/api/v1/market/candles` запрашиваются последние закрытые бары и
//! отправляются в БД (если подключена) и/или выводятся в stdout
//! JSON-строками с `"final": true`. Это «долечивает» последнюю строку в БД,
//! если процесс падал и в ней застряла нефинальная (формирующаяся) свеча.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use tokio::sync::Semaphore;

use crate::candle::{CandleUpdate, candle_to_json_line, from_rest_candle};
use crate::kucoin::{RestCandle, fetch_kline_page, forming_bucket_start};
use crate::stream::ConnSettings;

/// Максимум баров, которые REST отдаёт за один запрос (страница).
const PAGE_MAX: usize = 100;
/// Ограничение «глубины» бэкфилла на пару×интервал (страховка).
const BARS_CAP: usize = 1500;

/// Итоги прохода бэкфилла.
#[derive(Debug, Default)]
pub struct Summary {
    pub pairs_ok: usize,
    pub pairs_err: usize,
    pub lines: u64,
}

/// Печатает свечу в stdout.
fn emit_line(c: &CandleUpdate) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let _ = writeln!(out, "{}", candle_to_json_line(c));
}

/// Забирает с REST до `bars` последних закрытых свечей (новые сверху).
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

    let mut closed: Vec<RestCandle> = Vec::with_capacity(bars.min(BARS_CAP));
    let mut end_at: Option<i64> = Some(now);
    let mut pages = 0usize;

    while closed.len() < bars.min(BARS_CAP) && pages < BARS_CAP / PAGE_MAX + 2 {
        let page = fetch_kline_page(api_base, symbol, interval, None, end_at).await?;
        pages += 1;
        if page.is_empty() {
            break;
        }
        let oldest = page.last().expect("not empty").start_ts;
        // Первые строки могут быть текущей (формирующейся) свечой — пропускаем.
        for c in page {
            if c.start_ts < forming {
                closed.push(c);
            }
        }
        if closed.len() >= bars.min(BARS_CAP) {
            break;
        }
        // Двигаемся в прошлое: следующая страница — строго старее самой старой.
        let next_end = oldest - 1;
        if Some(next_end) >= end_at {
            break; // защита от зацикливания
        }
        end_at = Some(next_end);
    }

    closed.truncate(bars.min(BARS_CAP));
    Ok(closed)
}

/// Запускает бэкфилл для всех пар × интервалов c ограниченной конкурентностью.
/// Каждая свеча отправляется в БД (если `db_tx` есть) и/или печатается.
pub async fn run(
    settings: &ConnSettings,
    symbols: &[String],
    intervals: &[String],
    bars: usize,
    concurrency: usize,
) -> Summary {
    let mut summary = Summary::default();
    if bars == 0 || symbols.is_empty() || intervals.is_empty() {
        return summary;
    }
    let cap = bars.min(BARS_CAP);
    let sem = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut tasks = tokio::task::JoinSet::new();
    let print_if_no_db = settings.db_tx.is_none() || settings.emit_candles;

    for symbol in symbols {
        for interval in intervals {
            let exchange = settings.exchange.clone();
            let api_base = settings.api_base.clone();
            let symbol = symbol.clone();
            let interval = interval.clone();
            let sem = sem.clone();
            let db_tx = settings.db_tx.clone();
            let print = print_if_no_db;
            tasks.spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore");
                let closed = fetch_closed(&api_base, &symbol, &interval, cap).await?;
                let mut n = 0u64;
                // От старых к новым.
                for c in closed.iter().rev() {
                    let update = from_rest_candle(&exchange, &symbol, &interval, c.clone());
                    if let Some(tx) = &db_tx {
                        tx.send(update.clone()).await.ok();
                    }
                    if print {
                        emit_line(&update);
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
                summary.lines += n;
            }
            Ok(Err(e)) => {
                summary.pairs_err += 1;
                eprintln!("[backfill] ошибка: {e:#}");
            }
            Err(e) => {
                summary.pairs_err += 1;
                eprintln!("[backfill] задача упала: {e}");
            }
        }
    }
    summary
}
