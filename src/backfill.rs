//! REST-бэкфилл закрытых свечей.
//!
//! При запуске (KCS_BACKFILL_BARS=N) для каждой пары × интервала через REST
//! `/api/v1/market/candles` запрашиваются последние закрытые бары и выводятся
//! в stdout JSON-строками с `"final": true`. Это позволяет «долечить»
//! последнюю строку в БД, если процесс падал и в ней застряла нефинальная
//! (формирующаяся) свеча. Запись в БД пока не производится — только вывод.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde_json::json;
use tokio::sync::Semaphore;

use crate::kucoin::{RestCandle, fetch_kline_page, forming_bucket_start};

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

/// Печатает одну закрытую свечу в stdout (JSON-строка).
fn emit_closed(symbol: &str, interval: &str, c: &RestCandle) {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let rec = json!({
        "type": "candle",
        "source": "backfill",
        "final": true,
        "exchange": "kucoin",
        "ts": ts,
        "symbol": symbol,
        "interval": interval,
        "start": c.start_ts,
        "open": c.open,
        "close": c.close,
        "high": c.high,
        "low": c.low,
        "volume": c.volume,
        "turnover": c.turnover,
    });
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let _ = writeln!(out, "{rec}");
}

/// Забирает с REST до `bars` последних закрытых свечей (новые сверху) и
/// печатает их в хронологическом порядке. Возвращает число выведенных.
async fn backfill_pair_interval(
    api_base: &str,
    symbol: &str,
    interval: &str,
    bars: usize,
) -> Result<u64> {
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
    let n = closed.len();
    // Печатаем от старых к новым.
    for c in closed.iter().rev() {
        emit_closed(symbol, interval, c);
    }
    Ok(n as u64)
}

/// Запускает бэкфилл для всех пар × интервалов c ограниченной конкурентностью.
pub async fn run(
    api_base: &str,
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

    for symbol in symbols {
        for interval in intervals {
            let api_base = api_base.to_string();
            let symbol = symbol.clone();
            let interval = interval.clone();
            let sem = sem.clone();
            tasks.spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore");
                backfill_pair_interval(&api_base, &symbol, &interval, cap).await
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
