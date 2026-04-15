CREATE TABLE IF NOT EXISTS customers (
  customer_id BIGINT PRIMARY KEY,
  customer_name VARCHAR(255) NOT NULL,
  region VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  order_id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  order_total DECIMAL(12, 2) NOT NULL,
  order_ts TIMESTAMP NOT NULL,
  CONSTRAINT fk_orders_customers
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

INSERT INTO customers (customer_id, customer_name, region) VALUES
  (1, 'Acme Corp', 'west'),
  (2, 'Globex', 'east'),
  (3, 'Initech', 'central')
ON DUPLICATE KEY UPDATE
  customer_name = VALUES(customer_name),
  region = VALUES(region);

INSERT INTO orders (order_id, customer_id, order_total, order_ts) VALUES
  (101, 1, 1200.50, '2026-04-01 10:15:00'),
  (102, 2, 845.00, '2026-04-03 14:45:00'),
  (103, 1, 90.99, '2026-04-08 09:30:00')
ON DUPLICATE KEY UPDATE
  customer_id = VALUES(customer_id),
  order_total = VALUES(order_total),
  order_ts = VALUES(order_ts);

