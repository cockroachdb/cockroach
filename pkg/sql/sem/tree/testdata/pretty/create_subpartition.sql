CREATE TABLE students (
    id SERIAL,
    name STRING,
    email STRING,
    country STRING,
    expected_graduation_date DATE,
    PRIMARY KEY (country, expected_graduation_date, id))
    PARTITION BY LIST (country)(
        PARTITION australia VALUES IN ('AU','NZ') PARTITION BY RANGE (expected_graduation_date)(PARTITION graduated_au VALUES FROM (MINVALUE) TO ('2017-08-15'), PARTITION current_au VALUES FROM ('2017-08-15') TO (MAXVALUE)),
        PARTITION north_america VALUES IN ('US','CA') PARTITION BY RANGE (expected_graduation_date)(PARTITION graduated_us VALUES FROM (MINVALUE) TO ('2017-08-15'), PARTITION current_us VALUES FROM ('2017-08-15') TO (MAXVALUE))
    )
