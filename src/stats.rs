//! Счётчики принятых сообщений: общий и по интервалам свечей.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Разделяемая статистика WS-потока.
#[derive(Default)]
pub struct Stats {
    /// Всего принято сообщений канала свечей.
    total: AtomicU64,
    /// Принято по каждому интервалу ("1hour", "4hour", ...).
    by_interval: Mutex<BTreeMap<String, u64>>,
    /// Сообщения свечей, которые не удалось разобрать (только при включённом
    /// выводе свечей — иначе парсинг не выполняется).
    parse_errors: AtomicU64,
}

impl Stats {
    /// Учитывает пришедшее сообщение свечи заданного интервала.
    pub fn record(&self, interval: &str) {
        self.total.fetch_add(1, Ordering::Relaxed);
        let mut map = self.by_interval.lock().expect("stats lock");
        *map.entry(interval.to_string()).or_default() += 1;
    }

    /// Учитывает неразобранное сообщение свечи.
    pub fn record_parse_error(&self) {
        self.parse_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Снимок: (всего, по интервалам, ошибок разбора).
    pub fn snapshot(&self) -> (u64, Vec<(String, u64)>, u64) {
        let map = self.by_interval.lock().expect("stats lock");
        (
            self.total.load(Ordering::Relaxed),
            map.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            self.parse_errors.load(Ordering::Relaxed),
        )
    }
}
