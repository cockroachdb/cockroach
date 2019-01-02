CREATE INDEX foo_b_idx ON foo (b, c, d) PARTITION BY LIST (b) (
    PARTITION baz VALUES IN ('baz'),
    PARTITION default VALUES IN (DEFAULT)
)
