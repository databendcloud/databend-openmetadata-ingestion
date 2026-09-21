CREATE DATABASE IF NOT EXISTS shop;
CREATE DATABASE IF NOT EXISTS "dotted.db";

CREATE OR REPLACE TABLE shop.customers (
    id BIGINT NOT NULL COMMENT 'customer id',
    name VARCHAR,
    email VARCHAR,
    tier DECIMAL(10, 2),
    tags ARRAY(STRING),
    attrs MAP(STRING, STRING),
    profile TUPLE(age INT32, city STRING),
    props VARIANT,
    created_at TIMESTAMP
) COMMENT = 'customers master';

CREATE OR REPLACE TABLE shop.orders (
    order_id BIGINT,
    customer_id BIGINT,
    amount DECIMAL(18, 4),
    order_date DATE
);

INSERT INTO shop.customers (id, name, email, tier, created_at)
VALUES (1, 'alice', 'a@x.io', 1.5, now()), (2, 'bob', 'b@x.io', 2.0, now());
INSERT INTO shop.orders VALUES (10, 1, 100.5, today()), (11, 2, 20, today());

-- CTAS
CREATE OR REPLACE TABLE shop.customer_orders AS
SELECT c.id AS customer_id, c.name AS customer_name, o.order_id, o.amount
FROM shop.customers c JOIN shop.orders o ON c.id = o.customer_id;

-- DML into an existing table
CREATE OR REPLACE TABLE shop.daily_revenue (day DATE, revenue DECIMAL(18, 4), customer_count BIGINT);
INSERT INTO shop.daily_revenue
SELECT order_date, SUM(amount), COUNT(DISTINCT customer_id) FROM shop.orders GROUP BY order_date;

-- Views (CREATE_VIEW lineage)
CREATE OR REPLACE VIEW shop.v_big_orders AS SELECT order_id, customer_id, amount FROM shop.orders WHERE amount > 50;
CREATE OR REPLACE VIEW shop.v_customer_revenue AS
SELECT co.customer_name, SUM(co.amount) AS total FROM shop.customer_orders co GROUP BY co.customer_name;

-- Cross-database with a dotted database name
CREATE OR REPLACE TABLE "dotted.db".copy_orders AS SELECT * FROM shop.orders;
