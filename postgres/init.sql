-- ============================================================
-- CDC Lakehouse — PostgreSQL Source Database
-- E-Commerce schema: customers, products, orders
-- ============================================================

-- Create tables

CREATE TABLE customers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(150) UNIQUE NOT NULL,
    city        VARCHAR(100),
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE products (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    category    VARCHAR(100),
    price       NUMERIC(10,2) NOT NULL,
    stock_qty   INT DEFAULT 0,
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE orders (
    id            SERIAL PRIMARY KEY,
    customer_id   INT REFERENCES customers(id),
    product_id    INT REFERENCES products(id),
    quantity      INT NOT NULL,
    total_amount  NUMERIC(10,2) NOT NULL,
    status        VARCHAR(30) DEFAULT 'placed',
    order_date    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- Seed data — initial rows that Debezium captures via snapshot
-- ============================================================

INSERT INTO customers (name, email, city) VALUES
    ('Amit Sharma',    'amit@gmail.com',    'Delhi'),
    ('Priya Verma',    'priya@gmail.com',   'Mumbai'),
    ('Rahul Singh',    'rahul@gmail.com',   'Bangalore'),
    ('Sneha Patel',    'sneha@gmail.com',   'Ahmedabad'),
    ('Kiran Rao',      'kiran@gmail.com',   'Hyderabad');

INSERT INTO products (name, category, price, stock_qty) VALUES
    ('MacBook Air M3',      'Electronics', 99999.00,  50),
    ('Sony WH-1000XM5',     'Electronics', 29999.00, 100),
    ('The Pragmatic Programmer', 'Books',    599.00, 200),
    ('Levi''s 501 Jeans',   'Clothing',    3499.00, 150),
    ('Nike Air Max',         'Footwear',    8999.00,  80);

INSERT INTO orders (customer_id, product_id, quantity, total_amount, status) VALUES
    (1, 1, 1, 99999.00,  'placed'),
    (2, 2, 2, 59998.00,  'shipped'),
    (3, 3, 3,  1797.00,  'delivered'),
    (4, 4, 1,  3499.00,  'placed'),
    (5, 5, 2, 17998.00,  'shipped');

-- ============================================================
-- Create publication for Debezium (pgoutput plugin)
-- ============================================================
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
