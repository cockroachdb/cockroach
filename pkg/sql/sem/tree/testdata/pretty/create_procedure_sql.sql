CREATE PROCEDURE insert_and_notify(id INT, amount DECIMAL, status STRING)
LANGUAGE SQL
AS $body$
INSERT INTO orders VALUES (id, amount, status);
DO $a$ BEGIN RAISE NOTICE 'inserted'; DO $b$ BEGIN RAISE NOTICE 'inner'; END $b$; END $a$
$body$
