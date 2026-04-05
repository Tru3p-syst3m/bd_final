-- ============================================
-- 1. Raw events
-- ============================================
CREATE TABLE IF NOT EXISTS orders_raw
(
    event_id      String,
    event_type    String,
    entity_id     String,
    timestamp     DateTime64(3),
    processed_at  DateTime64(3),
    source        String,
    version       String,
    order_id      String,
    customer_id   String,
    amount        Float64,
    currency      String,
    status        String,
    _ingested_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (customer_id, timestamp, order_id)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- ============================================
-- 2. Агрегаты по клиентам
-- ============================================
CREATE TABLE IF NOT EXISTS customer_totals
(
    customer_id   String,
    total_amount  Float64,
    currency      String,
    timestamp     DateTime64(3),
    _updated_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_updated_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (customer_id, timestamp)
TTL timestamp + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- ============================================
-- 3. Оконные агрегаты
-- ============================================
CREATE TABLE IF NOT EXISTS order_windows
(
    window        String,
    count         UInt64,
    timestamp     DateTime64(3),
    _processed_at DateTime DEFAULT now()
)
ENGINE = SummingMergeTree(count)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (window, timestamp)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- ============================================
-- 4. Материализованное представление: заказы по статусам
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
-- 5. Материализованное представление: топ клиентов
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