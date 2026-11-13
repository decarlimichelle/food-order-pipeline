-- ============================================================
-- STAR SCHEMA for Food Order Analytics
-- ============================================================

-- ─── Dimension Tables ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id     SERIAL PRIMARY KEY,
    customer_name   VARCHAR(100) NOT NULL,
    email           VARCHAR(150),
    city            VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_items (
    item_id         SERIAL PRIMARY KEY,
    item_name       VARCHAR(100) NOT NULL,
    category        VARCHAR(50) NOT NULL,
    unit_price      NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_restaurants (
    restaurant_id   SERIAL PRIMARY KEY,
    restaurant_name VARCHAR(150) NOT NULL,
    cuisine_type    VARCHAR(50),
    city            VARCHAR(100)
);

-- ─── Fact Table ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES dim_customers(customer_id),
    restaurant_id   INTEGER REFERENCES dim_restaurants(restaurant_id),
    item_id         INTEGER REFERENCES dim_items(item_id),
    quantity        INTEGER NOT NULL DEFAULT 1,
    total_price     NUMERIC(10, 2) NOT NULL,
    status          VARCHAR(30) NOT NULL DEFAULT 'pending',
    order_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    delivered_at    TIMESTAMP,
    delivery_city   VARCHAR(100)
);

-- ─── Staging Table (raw landing zone from Kafka consumer) ───

CREATE TABLE IF NOT EXISTS raw_orders (
    id              SERIAL PRIMARY KEY,
    payload         JSONB NOT NULL,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- ─── Indexes ────────────────────────────────────────────────

CREATE INDEX idx_fact_orders_status ON fact_orders(status);
CREATE INDEX idx_fact_orders_timestamp ON fact_orders(order_timestamp);
CREATE INDEX idx_fact_orders_customer ON fact_orders(customer_id);
CREATE INDEX idx_fact_orders_restaurant ON fact_orders(restaurant_id);
CREATE INDEX idx_raw_orders_ingested ON raw_orders(ingested_at);

-- ─── Seed Data: Items & Restaurants ─────────────────────────

INSERT INTO dim_items (item_name, category, unit_price) VALUES
    ('Margherita Pizza',    'Pizza',    8.50),
    ('Pepperoni Pizza',     'Pizza',    10.00),
    ('Quattro Formaggi',    'Pizza',    11.50),
    ('Caesar Salad',        'Salad',    7.00),
    ('Greek Salad',         'Salad',    7.50),
    ('Spaghetti Carbonara', 'Pasta',    9.50),
    ('Penne Arrabbiata',    'Pasta',    8.00),
    ('Tiramisu',            'Dessert',  5.50),
    ('Coca-Cola',           'Drink',    2.50),
    ('Water',               'Drink',    1.50),
    ('Bruschetta',          'Starter',  6.00),
    ('Garlic Bread',        'Starter',  4.00)
ON CONFLICT DO NOTHING;

INSERT INTO dim_restaurants (restaurant_name, cuisine_type, city) VALUES
    ('Bella Napoli',        'Italian',  'Milan'),
    ('Sakura Sushi',        'Japanese', 'Milan'),
    ('La Piazzetta',        'Italian',  'Rome'),
    ('Burger Factory',      'American', 'Turin'),
    ('Trattoria da Mario',  'Italian',  'Florence')
ON CONFLICT DO NOTHING;

INSERT INTO dim_customers (customer_name, email, city) VALUES
    ('Marco Rossi',     'marco@example.com',    'Milan'),
    ('Giulia Bianchi',  'giulia@example.com',   'Rome'),
    ('Luca Verdi',      'luca@example.com',     'Turin'),
    ('Sara Russo',      'sara@example.com',     'Florence'),
    ('Andrea Moretti',  'andrea@example.com',   'Milan'),
    ('Elena Conti',     'elena@example.com',    'Rome'),
    ('Davide Romano',   'davide@example.com',   'Milan'),
    ('Francesca Ricci', 'francesca@example.com','Turin')
ON CONFLICT DO NOTHING;
