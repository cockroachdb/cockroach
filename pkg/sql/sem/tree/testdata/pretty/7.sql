INSERT INTO mnop (m, n) SELECT i, (1e9 + i/2e4)::float FROM generate_series(1, 2e4) AS i(i) RETURNING NOTHING
