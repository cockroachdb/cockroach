CREATE OR REPLACE FUNCTION get_top_orders(min_amount DECIMAL, lim INT)
RETURNS SETOF INT
STABLE
LANGUAGE SQL
AS $body$
WITH filtered AS (SELECT id, amount FROM orders WHERE amount > min_amount AND status = 'active')
SELECT id FROM filtered ORDER BY amount DESC LIMIT lim;
DO $a$ BEGIN RAISE NOTICE 'queried'; DO $b$ BEGIN RAISE NOTICE 'inner'; END $b$; END $a$
$body$
