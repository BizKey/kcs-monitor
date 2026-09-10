-- Разовый скрипт: исправление перепутанных колонок OHLC.
--
-- Контекст: до коммита 429efdb вставка свечей передавала bind() в порядке
-- (open, close, high, low), а колонки в SQL шли (open, high, low, close),
-- поэтому в таблицу попадало:
--     open  = trueOpen
--     high  = trueClose
--     low   = trueHigh
--     close = trueLow
-- Ошибка детерминированная, поэтому значения достаточно вернуть местами —
-- перезагрузка истории не нужна.
--
-- ВНИМАНИЕ: запускать РОВНО ОДИН РАЗ на базе, которая наполнялась до фикса.
-- Повторный запуск снова испортит данные (это поворот из трёх колонок:
-- три применения возвращают исходное состояние).
--
-- Проверка ДО (должно быть много нарушений):
--   SELECT count(*) FILTER (WHERE high < low) AS broken FROM candles;
--
-- Проверка ПОСЛЕ (должно быть 0):
--   SELECT count(*) FILTER (WHERE high < GREATEST(open, close, low)
--                             OR low > LEAST(open, close, high)) AS broken
--   FROM candles;
--
-- Рекомендуется затем вернуть место:
--   REINDEX INDEX CONCURRENTLY candles_pkey;
--   REINDEX INDEX CONCURRENTLY candles_recent_idx;

BEGIN;

UPDATE candles
SET close = high,
    high  = low,
    low   = close;

COMMIT;
