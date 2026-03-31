CREATE PROCEDURE transfer(from_id INT, to_id INT, amount DECIMAL)
LANGUAGE plpgsql
AS $body$
BEGIN
IF amount <= 0 THEN
RAISE EXCEPTION 'amount must be positive';
END IF;
UPDATE accounts SET balance = balance - amount WHERE id = from_id;
UPDATE accounts SET balance = balance + amount WHERE id = to_id;
DO $a$ BEGIN RAISE NOTICE 'done'; DO $b$ BEGIN RAISE NOTICE 'inner'; END $b$; END $a$;
END
$body$
