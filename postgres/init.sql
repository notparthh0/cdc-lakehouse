-- Source OLTP database (captured by Debezium) and a separate `search` database for the
-- pgvector index. Separate databases keep the index out of the CDC publication.

CREATE TABLE customers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(150) UNIQUE NOT NULL,
    city        VARCHAR(100),
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE products (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    category    VARCHAR(100) NOT NULL,
    description TEXT NOT NULL,
    price       NUMERIC(10,2) NOT NULL,
    stock_qty   INT NOT NULL DEFAULT 0,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE orders (
    id            SERIAL PRIMARY KEY,
    customer_id   INT NOT NULL REFERENCES customers(id),
    product_id    INT NOT NULL REFERENCES products(id),
    quantity      INT NOT NULL,
    total_amount  NUMERIC(10,2) NOT NULL,
    status        VARCHAR(30) NOT NULL DEFAULT 'placed',
    order_date    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO customers (name, email, city)
SELECT 'Customer ' || g, 'customer' || g || '@example.com',
       (ARRAY['Delhi','Mumbai','Bangalore','Pune','Chennai','Hyderabad','Kolkata','Jaipur'])[1 + g % 8]
FROM generate_series(1, 50) g;

INSERT INTO products (name, category, description, price, stock_qty) VALUES
 ('MacBook Air M3', 'Electronics', 'Thin and light 13-inch laptop with all-day battery life', 99999.00, 50),
 ('Sony WH-1000XM5', 'Electronics', 'Over-ear wireless headphones with active noise cancelling', 29999.00, 100),
 ('JBL Flip 6', 'Electronics', 'Portable waterproof Bluetooth speaker with punchy bass', 9999.00, 120),
 ('Kindle Paperwhite', 'Electronics', 'Glare-free e-reader with adjustable warm light for reading at night', 13999.00, 80),
 ('Logitech MX Master 3S', 'Electronics', 'Ergonomic wireless mouse with quiet clicks for long work sessions', 8995.00, 90),
 ('Samsung T7 SSD 1TB', 'Electronics', 'Pocket-sized external solid state drive for fast backups', 8499.00, 70),
 ('Apple Watch SE', 'Electronics', 'Smartwatch with heart rate tracking, sleep tracking and fall detection', 29900.00, 60),
 ('Anker 20000mAh Power Bank', 'Electronics', 'High-capacity portable charger for phones on long trips', 3999.00, 200),
 ('Mechanical Keyboard TKL', 'Electronics', 'Tenkeyless mechanical keyboard with tactile brown switches', 5499.00, 75),
 ('USB-C Hub 7-in-1', 'Electronics', 'Adapter with HDMI, SD card reader and USB ports for laptops', 2499.00, 150),
 ('The Pragmatic Programmer', 'Books', 'Classic software engineering book on habits of effective programmers', 599.00, 200),
 ('Designing Data-Intensive Applications', 'Books', 'Book on databases, replication, streaming and distributed systems', 1299.00, 120),
 ('Atomic Habits', 'Books', 'Self-improvement book about building good habits in small steps', 499.00, 300),
 ('Clean Code', 'Books', 'Guide to writing readable and maintainable code', 699.00, 150),
 ('The Psychology of Money', 'Books', 'Short essays about how people think about wealth and spending', 399.00, 250),
 ('Indian Vegetarian Cookbook', 'Books', 'Recipes for everyday dals, curries and breads', 899.00, 90),
 ('Levi''s 501 Jeans', 'Clothing', 'Straight-fit denim jeans in classic dark blue', 3499.00, 150),
 ('Cotton Kurta', 'Clothing', 'Breathable cotton kurta for summer and festive wear', 1299.00, 180),
 ('Puffer Jacket', 'Clothing', 'Warm insulated winter jacket with water-resistant shell', 4999.00, 60),
 ('Merino Wool Sweater', 'Clothing', 'Soft lightweight sweater that keeps you warm without itching', 3999.00, 70),
 ('Rain Poncho', 'Clothing', 'Packable waterproof poncho for monsoon commutes', 799.00, 140),
 ('Dry-Fit Running T-Shirt', 'Clothing', 'Sweat-wicking quick-dry shirt for running and gym workouts', 899.00, 220),
 ('Nike Air Max', 'Footwear', 'Cushioned running shoes with visible air sole', 8999.00, 80),
 ('Hiking Boots', 'Footwear', 'Waterproof ankle boots with grippy soles for mountain trails', 6999.00, 50),
 ('Leather Sandals', 'Footwear', 'Handmade Kolhapuri leather sandals', 1499.00, 110),
 ('Canvas Sneakers', 'Footwear', 'Everyday low-top canvas sneakers in white', 1999.00, 160),
 ('Yoga Mat 6mm', 'Sports', 'Non-slip thick yoga mat for home workouts', 1299.00, 130),
 ('Adjustable Dumbbells 20kg', 'Sports', 'Space-saving dumbbell set with quick weight change', 7999.00, 40),
 ('Badminton Racket Pair', 'Sports', 'Lightweight rackets with shuttlecocks for casual play', 1799.00, 100),
 ('Cricket Bat English Willow', 'Sports', 'Full-size English willow bat for leather ball cricket', 9999.00, 30),
 ('Insulated Steel Bottle', 'Home', 'Keeps water cold for 24 hours and tea hot for 12', 999.00, 250),
 ('Pressure Cooker 5L', 'Home', 'Stainless steel pressure cooker for fast dal and rice', 2799.00, 90),
 ('Air Purifier', 'Home', 'HEPA filter air purifier that removes smoke and dust in Delhi winters', 12999.00, 40),
 ('Robot Vacuum', 'Home', 'Self-charging robot vacuum cleaner that maps your rooms', 19999.00, 35),
 ('Ceramic Coffee Mug Set', 'Home', 'Set of four microwave-safe ceramic mugs', 799.00, 200),
 ('LED Desk Lamp', 'Home', 'Dimmable desk lamp with eye-care light for studying', 1499.00, 120),
 ('Electric Kettle 1.5L', 'Home', 'Fast-boil kettle with auto shut-off', 1299.00, 140),
 ('Organic Green Tea', 'Grocery', 'Loose-leaf Darjeeling green tea, 100 g', 449.00, 300),
 ('Cold Brew Coffee Pack', 'Grocery', 'Ready-to-brew coffee bags for smooth iced coffee', 599.00, 180),
 ('Basmati Rice 5kg', 'Grocery', 'Aged long-grain basmati rice', 899.00, 220);

INSERT INTO orders (customer_id, product_id, quantity, total_amount, status)
SELECT 1 + g % 50, p.id, 1 + g % 3, (1 + g % 3) * p.price,
       (ARRAY['placed','shipped','delivered'])[1 + g % 3]
FROM generate_series(1, 100) g
JOIN products p ON p.id = 1 + (g * 7) % 40;

CREATE PUBLICATION dbz_publication FOR TABLE customers, products, orders;

-- ---------------------------------------------------------------- search (pgvector)
CREATE DATABASE search;
\connect search
CREATE EXTENSION vector;
CREATE TABLE product_embeddings (
    id           INT PRIMARY KEY,           -- products.id in the ecommerce database
    content      TEXT NOT NULL,             -- the text that was embedded
    content_hash TEXT NOT NULL,             -- sha256 of content: skip re-embedding when it has not changed
    embedding    vector(384) NOT NULL,       -- bge-small-en-v1.5
    source_ts    TIMESTAMP NOT NULL,        -- Postgres commit time of the change that produced this row
    synced_at    TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX ON product_embeddings USING hnsw (embedding vector_cosine_ops);
