//! Общее представление свечи (WS и REST) и преобразования.

use serde_json::Value;

use crate::kucoin::RestCandle;

/// Свеча, готовая к записи в БД (и к печати в stdout).
#[derive(Debug, Clone)]
pub struct CandleUpdate {
    /// Биржа-источник (kucoin, ...).
    pub exchange: String,
    pub symbol: String,
    pub interval: String,
    /// Начало свечи, unix-секунды (UTC).
    pub start_ts: i64,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64,
    pub turnover: f64,
    /// true = закрытая (финальная) свеча, например из REST-бэкфилла.
    pub final_bar: bool,
}

/// Извлекает (символ, интервал) из топика `/market/candles:SYM_1min`.
pub fn symbol_interval_from_topic(topic: &str) -> Option<(String, String)> {
    let rest = topic.strip_prefix("/market/candles:")?;
    let (symbol, interval) = rest.split_once('_')?;
    Some((symbol.to_string(), interval.to_string()))
}

/// Разбирает WS-сообщение канала свечей (новый и старый форматы).
pub fn parse_ws_candle(v: &Value, topic: &str, exchange: &str) -> Option<CandleUpdate> {
    let data = v.get("data")?;
    let (topic_symbol, interval) = symbol_interval_from_topic(topic)?;

    let (symbol, candles) = match data {
        // Новый формат: {"symbol": "...", "candles": [...], "time": ...}
        Value::Object(map) => {
            let candles = map.get("candles")?.as_array()?;
            let symbol = map
                .get("symbol")
                .and_then(|s| s.as_str())
                .map(|s| s.to_string())
                .unwrap_or(topic_symbol);
            (symbol, candles)
        }
        // Старый формат: data — сразу массив свечи.
        Value::Array(candles) => (topic_symbol, candles),
        _ => return None,
    };

    candle_from_parts(exchange, &symbol, &interval, candles).map(|mut c| {
        c.final_bar = false;
        c
    })
}

/// Собирает свечу из массива `[start, open, close, high, low, volume, turnover]`.
fn candle_from_parts(
    exchange: &str,
    symbol: &str,
    interval: &str,
    candles: &[Value],
) -> Option<CandleUpdate> {
    if candles.len() != 7 {
        return None;
    }
    let num = |i: usize| candles[i].as_str().and_then(|s| s.parse::<f64>().ok());
    Some(CandleUpdate {
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        interval: interval.to_string(),
        start_ts: candles[0].as_str()?.parse::<i64>().ok()?,
        open: num(1)?,
        close: num(2)?,
        high: num(3)?,
        low: num(4)?,
        volume: num(5)?,
        turnover: num(6)?,
        final_bar: false,
    })
}

/// Собирает свечу из REST-строки (закрытые бары).
pub fn from_rest_candle(
    exchange: &str,
    symbol: &str,
    interval: &str,
    c: RestCandle,
) -> CandleUpdate {
    CandleUpdate {
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        interval: interval.to_string(),
        start_ts: c.start_ts,
        open: c.open,
        close: c.close,
        high: c.high,
        low: c.low,
        volume: c.volume,
        turnover: c.turnover,
        final_bar: true,
    }
}

/// JSON-строка для stdout в формате, совместимом с WS-выводом.
pub fn candle_to_json_line(c: &CandleUpdate) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let mut rec = serde_json::json!({
        "type": "candle",
        "exchange": c.exchange,
        "ts": ts,
        "symbol": c.symbol,
        "interval": c.interval,
        "start": c.start_ts,
        "open": c.open,
        "close": c.close,
        "high": c.high,
        "low": c.low,
        "volume": c.volume,
        "turnover": c.turnover,
    });
    if c.final_bar {
        rec["final"] = Value::Bool(true);
        rec["source"] = Value::String("backfill".into());
    }
    rec.to_string()
}
