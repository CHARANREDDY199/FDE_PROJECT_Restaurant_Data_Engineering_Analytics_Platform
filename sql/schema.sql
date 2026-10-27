-- =========================================================
-- Restaurant Data Engineering Analytics Platform - Schema
-- =========================================================

CREATE TABLE IF NOT EXISTS customers (
    customer_id     SERIAL PRIMARY KEY,
    name            VARCHAR(120) NOT NULL,
    email           VARCHAR(150) UNIQUE,
    phone           VARCHAR(20),
    signup_date     DATE NOT NULL,
    city            VARCHAR(80)
);

CREATE TABLE IF NOT EXISTS menu (
    menu_item_id    SERIAL PRIMARY KEY,
    item_name       VARCHAR(120) NOT NULL,
    category        VARCHAR(60),
    price           NUMERIC(8,2) NOT NULL CHECK (price >= 0),
    is_active       BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS restaurant_tables (
    table_id        SERIAL PRIMARY KEY,
    table_number    INT UNIQUE NOT NULL,
    capacity        INT NOT NULL
);

CREATE TABLE IF NOT EXISTS reservations (
    reservation_id  SERIAL PRIMARY KEY,
    customer_id     INT REFERENCES customers(customer_id),
    table_id        INT REFERENCES restaurant_tables(table_id),
    reservation_time TIMESTAMP NOT NULL,
    party_size      INT NOT NULL,
    status          VARCHAR(20) DEFAULT 'confirmed'
);

CREATE TABLE IF NOT EXISTS orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INT REFERENCES customers(customer_id),
    table_id        INT REFERENCES restaurant_tables(table_id),
    order_time      TIMESTAMP NOT NULL,
    order_type      VARCHAR(20) NOT NULL,       -- dine_in / takeaway / delivery
    prep_start_time TIMESTAMP,
    prep_end_time   TIMESTAMP,
    status          VARCHAR(20) DEFAULT 'placed', -- placed/preparing/completed/cancelled
    total_amount    NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id   SERIAL PRIMARY KEY,
    order_id        INT REFERENCES orders(order_id) ON DELETE CASCADE,
    menu_item_id    INT REFERENCES menu(menu_item_id),
    quantity        INT NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(8,2) NOT NULL,
    line_total      NUMERIC(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id      SERIAL PRIMARY KEY,
    order_id        INT REFERENCES orders(order_id),
    payment_method  VARCHAR(30),
    amount_paid     NUMERIC(10,2),
    payment_time    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id     SERIAL PRIMARY KEY,
    order_id        INT REFERENCES orders(order_id),
    dispatch_time   TIMESTAMP,
    delivered_time  TIMESTAMP,
    delivery_status VARCHAR(20) DEFAULT 'pending' -- pending/in_transit/delivered/delayed
);

-- =========================================================
-- Analytics (aggregate) tables populated by Spark/ETL jobs
-- =========================================================

CREATE TABLE IF NOT EXISTS analytics_daily_revenue (
    order_date      DATE PRIMARY KEY,
    total_revenue   NUMERIC(12,2),
    total_orders    INT,
    avg_order_value NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS analytics_top_dishes (
    menu_item_id    INT REFERENCES menu(menu_item_id),
    item_name       VARCHAR(120),
    total_quantity  INT,
    total_revenue   NUMERIC(12,2),
    rank_position   INT,
    generated_at    TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (menu_item_id, generated_at)
);

CREATE TABLE IF NOT EXISTS analytics_peak_hours (
    hour_of_day     INT PRIMARY KEY,
    total_orders    INT,
    total_revenue   NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS analytics_prep_performance (
    order_date      DATE PRIMARY KEY,
    avg_prep_minutes NUMERIC(6,2),
    max_prep_minutes NUMERIC(6,2),
    cancelled_orders INT
);

CREATE INDEX IF NOT EXISTS idx_orders_order_time ON orders(order_time);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_menu ON order_items(menu_item_id);
