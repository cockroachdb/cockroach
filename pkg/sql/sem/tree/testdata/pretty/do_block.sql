DO $$ DECLARE i INT := 0; total INT := 0; BEGIN LOOP IF i > 10 THEN EXIT; END IF; total := total + i; i := i + 1; END LOOP; RAISE NOTICE 'sum = %', total; END $$
