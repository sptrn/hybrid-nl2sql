CREATE TABLE IF NOT EXISTS inventory_products (
  product_id BIGINT PRIMARY KEY,
  product_name TEXT NOT NULL,
  category TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shipments (
  shipment_id BIGINT PRIMARY KEY,
  product_id BIGINT NOT NULL REFERENCES inventory_products(product_id),
  shipped_at TIMESTAMP NOT NULL,
  quantity INTEGER NOT NULL
);

INSERT INTO inventory_products (product_id, product_name, category) VALUES
  (10, 'Widget A', 'hardware'),
  (11, 'Widget B', 'hardware'),
  (12, 'Support Plan', 'service')
ON CONFLICT (product_id) DO UPDATE SET
  product_name = EXCLUDED.product_name,
  category = EXCLUDED.category;

INSERT INTO shipments (shipment_id, product_id, shipped_at, quantity) VALUES
  (1001, 10, '2026-04-02 08:00:00', 4),
  (1002, 11, '2026-04-05 11:30:00', 2),
  (1003, 10, '2026-04-09 16:10:00', 7)
ON CONFLICT (shipment_id) DO UPDATE SET
  product_id = EXCLUDED.product_id,
  shipped_at = EXCLUDED.shipped_at,
  quantity = EXCLUDED.quantity;

