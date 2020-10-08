CREATE TABLE test (
	i INT8,
	s STRING,
	f FLOAT8,
	b BYTES,
	PRIMARY KEY (s, i),
	FAMILY f1 (i, s, f),
	FAMILY f2 (b)
);

INSERT INTO test VALUES (1, 'blah', 2.3e4, 'some bytes');
