SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;

-- Create an orders table.
CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT,
  created_at TIMESTAMPTZ,
  price INT,
  INDEX (created_at),
  INDEX (user_id, created_at)
);

-- Insert 100,000 orders into the table with a random user_id between 0 and
-- 1000, and a random timestamp between 2022-01-01 and 2022-01-26.
INSERT INTO orders
SELECT i, floor(random()*1000)::INT, '2022-01-01T00:00:00Z' + (floor(random()*25)::INT::STRING || ' days')::INTERVAL
FROM generate_series(1, 100000) g(i);

-- Collect statistics.
ANALYZE orders;
SELECT pg_sleep(3);

-- Query for two user_ids with a timestamp inclusive of the max value in the
-- histogram.
EXPLAIN ANALYZE
SELECT * FROM orders where user_id IN (333, 555) AND created_at > '2022-01-22T00:00:00Z'
ORDER BY created_at LIMIT 10;

-- Insert 5000 orders with a random user_id and a timestamp outside of the max
-- timestamp in the histogram.
INSERT INTO orders
SELECT i, floor(random()*1000)::INT, '2022-02-01T00:00:00Z' + (floor(random()*5)::INT::STRING || ' days')::INTERVAL
FROM generate_series(100001, 105000) g(i);

-- If we collected partial stats here, we would know that the new max created_at
-- is ~2022-02-06, and we would assume that a scan of the created_at index over
-- the interval [/'2022-02-01 00:00:00.000001+00:00' - ] would produce more than
-- ~0 rows. So we'd pick the good query plan which scans over the
-- (user_id, created_at) index.

EXPLAIN ANALYZE
SELECT * FROM orders where user_id IN (333, 555) AND created_at > '2022-02-01T00:00:00Z'
ORDER BY created_at LIMIT 10;

-- CREATE STATISTICS sp ON created_at FROM orders USING EXTREMES;

-- ANALYZE orders;

SELECT pg_sleep(3);

-- Now that we've done a full stats collection, the good query plan will be
-- picked below.

-- EXPLAIN ANALYZE
-- SELECT * FROM orders where user_id IN (333, 555) AND created_at > '2022-02-01T00:00:00Z'
-- ORDER BY created_at LIMIT 10;

ANALYZE orders;

EXPLAIN ANALYZE
SELECT * FROM orders where user_id IN (333, 555) AND created_at > '2022-02-01T00:00:00Z'
ORDER BY created_at LIMIT 10;


