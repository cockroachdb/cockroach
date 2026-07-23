CREATE FUNCTION fizzbuzz(n INT) RETURNS TEXT LANGUAGE plpgsql AS $body$
DECLARE
result TEXT := '';
i INT;
word TEXT;
BEGIN
FOR i IN 1..n LOOP
IF i > 1 THEN
result := result || ' ';
END IF;
word := '';
IF i % 3 = 0 THEN
word := word || 'Fizz';
END IF;
IF i % 5 = 0 THEN
word := word || 'Buzz';
END IF;
IF word = '' THEN
word := i::TEXT;
END IF;
result := result || word;
END LOOP;
DO $a$ BEGIN RAISE NOTICE 'done'; DO $b$ BEGIN RAISE NOTICE 'inner'; END $b$; END $a$;
RETURN result;
END
$body$
