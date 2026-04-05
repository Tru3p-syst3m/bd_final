-- ============================================
-- 1. Raw events: детальные заказы для гибкой аналитики
-- ============================================
CREATE TABLE IF NOT EXISTS orders_raw
(
    event_id      String,
    event_type    String,
    entity_id     String,       -- order_id
    timestamp     DateTime64(3),
    processed_at  DateTime64(3),
    source        String,
    version       String,
    
    -- Вложенные поля из payload
    order_id      String,
    customer_id   String,
    amount        Float64,
    currency      LowCardinality(String),
    status        LowCardinality(String),
    
    -- Технические поля
    _ingested_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (customer_id, timestamp, order_id)
TTL timestamp + INTERVAL 90 DAY  -- Хранить 90 дней детальных данных
SETTINGS index_granularity = 8192;

-- ============================================
-- 2. Агрегаты по клиентам: витрина для CRM-аналитики
-- ============================================
CREATE TABLE IF NOT EXISTS customer_totals
(
    customer_id   String,
    total_amount  Float64,
    currency      LowCardinality(String),
    timestamp     DateTime64(3),
    
    _updated_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_updated_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (customer_id, timestamp)
TTL timestamp + INTERVAL 365 DAY  -- Хранить год агрегатов
SETTINGS index_granularity = 8192;

-- ============================================
-- 3. Оконные агрегаты: мониторинг потока событий
-- ============================================
CREATE TABLE IF NOT EXISTS order_windows
(
    window        String,       -- например "2024-01-15-14:00"
    count         UInt64,
    timestamp     DateTime64(3),
    
    _processed_at DateTime DEFAULT now()
)
ENGINE = SummingMergeTree(count)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (window, timestamp)
TTL timestamp + INTERVAL 30 DAY  -- Короткое хранение для оперативной аналитики
SETTINGS index_granularity = 8192;

-- ============================================
-- 4. Материализованное представление: дашборд "Заказы по статусам"
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS orders_by_status_daily
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, status, currency)
AS SELECT
    toDate(timestamp) AS day,
    status,
    currency,
    count() AS orders_count,
    sum(amount) AS total_amount,
    avg(amount) AS avg_amount
FROM orders_raw
GROUP BY day, status, currency;

-- ============================================
-- 5. Материализованное представление: топ клиентов за день
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS top_customers_daily
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, customer_id)
AS SELECT
    toDate(timestamp) AS day,
    customer_id,
    count() AS orders_count,
    sum(amount) AS total_spent,
    max(timestamp) AS last_order
FROM orders_raw
GROUP BY day, customer_id;

-- ============================================
-- Примеры аналитических запросов
-- ============================================

-- 🔹 Конверсия по статусам за вчера
SELECT 
    status,
    count() AS cnt,
    round(cnt / sum(cnt) OVER() * 100, 2) AS percent
FROM orders_raw
WHERE timestamp >= yesterday()
GROUP BY status
ORDER BY cnt DESC;

-- 🔹 Топ-10 клиентов по сумме покупок за месяц
SELECT 
    customer_id,
    sum(total_amount) AS spent,
    count() AS orders
FROM customer_totals
WHERE timestamp >= toStartOfMonth(now())
GROUP BY customer_id
ORDER BY spent DESC
LIMIT 10;

-- 🔹 Нагрузка по часам (аномалии в потоке)
SELECT 
    toStartOfHour(timestamp) AS hour,
    sum(count) AS events
FROM order_windows
WHERE timestamp >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour;

-- 🔹 Средний чек по валютам (из материализованного представления)
SELECT 
    currency,
    round(avg(avg_amount), 2) AS avg_check,
    sum(orders_count) AS total_orders
FROM orders_by_status_daily
WHERE day >= today() - INTERVAL 7
GROUP BY currency;