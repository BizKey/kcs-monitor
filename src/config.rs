//! Конфигурация приложения (переменные окружения).

/// Настройки, читаемые из окружения.
#[derive(Debug, Clone)]
pub struct Config {
    /// Базовый URL REST API биржи KuCoin.
    pub api_base: String,
    /// Таймфреймы свечей (KuCoin): 1min, 3min, 5min, 15min, 30min, 1hour,
    /// 2hour, 4hour, 6hour, 8hour, 12hour, 1day, 1week.
    /// Подписка создаётся на каждую пару × каждый интервал.
    pub kline_intervals: Vec<String>,
    /// Сколько подписок (топиков свечей) держать на одном WS-соединении.
    /// Документированный жёсткий лимит KuCoin — 100 подписок на соединение;
    /// берём ровно этот потолок (без запаса). Переопределяется через
    /// KCS_SUBS_PER_CONNECTION, если нужно меньше.
    pub subs_per_connection: usize,
    /// Отладочное ограничение списка символов ("BTC-USDT,ETH-USDT");
    /// пусто = все торгуемые пары.
    pub symbols_override: Option<String>,
    /// Выводить ли каждую свечу отдельной JSON-строкой в stdout.
    /// По умолчанию выключено: считаем только статистику.
    pub emit_candles: bool,
    /// Период вывода статистики в stdout, секунды.
    pub stats_period_secs: u64,
    /// Период пересмотра списка торгуемых пар (секунды). При появлении новых
    /// пар соединения плавно переподключаются на обновлённый набор топиков.
    /// 0 = ресинк выключен (список фиксируется при старте).
    pub resync_secs: u64,
    /// Отладка: принудительный ресинк на каждом тике (для проверки перезапуска).
    pub resync_force: bool,
    /// REST-бэкфилл при старте: сколько последних ЗАКРЫТЫХ свечей вывести для
    /// каждой пары × интервала (0 = выключен). Вывод идёт в stdout, в БД
    /// ничего не пишется.
    pub backfill_bars: usize,
    /// Максимум одновременных REST-запросов бэкфилла.
    pub backfill_concurrency: usize,
}

impl Config {
    pub fn from_env() -> Self {
        let env = |key: &str| std::env::var(key).ok().filter(|v| !v.is_empty());
        Config {
            api_base: env("KCS_API_BASE")
                .unwrap_or_else(|| crate::kucoin::DEFAULT_API_BASE.to_string()),
            // KCS_KLINE_INTERVALS="1min,1hour,4hour,1day,1week";
            // одиночный KCS_KLINE_INTERVAL принимается для совместимости.
            // По умолчанию — старшие таймфреймы без 1min.
            kline_intervals: env("KCS_KLINE_INTERVALS")
                .or_else(|| env("KCS_KLINE_INTERVAL"))
                .map(|s| {
                    s.split(',')
                        .map(|x| x.trim().to_string())
                        .filter(|x| !x.is_empty())
                        .collect()
                })
                .filter(|v: &Vec<String>| !v.is_empty())
                .unwrap_or_else(|| {
                    vec![
                        "1hour".to_string(),
                        "4hour".to_string(),
                        "1day".to_string(),
                        "1week".to_string(),
                    ]
                }),
            subs_per_connection: env("KCS_SUBS_PER_CONNECTION")
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            symbols_override: env("KCS_SYMBOLS"),
            emit_candles: env("KCS_EMIT_CANDLES")
                .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
                .unwrap_or(false),
            stats_period_secs: env("KCS_STATS_SECONDS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(5)
                .max(1),
            resync_secs: env("KCS_RESYNC_SECONDS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            resync_force: env("KCS_RESYNC_FORCE")
                .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
                .unwrap_or(false),
            backfill_bars: env("KCS_BACKFILL_BARS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            backfill_concurrency: env("KCS_BACKFILL_CONCURRENCY")
                .and_then(|v| v.parse().ok())
                .unwrap_or(8),
        }
    }
}
