CREATE TABLE IF NOT EXISTS orders (
    event_id VARCHAR(255) PRIMARY KEY,
    event_type VARCHAR(50),
    entity_id VARCHAR(255),
    timestamp VARCHAR(50),
    source VARCHAR(255),
    version VARCHAR(10),
    order_id VARCHAR(255),
    customer_id VARCHAR(255),
    amount DOUBLE PRECISION,
    currency VARCHAR(10),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
