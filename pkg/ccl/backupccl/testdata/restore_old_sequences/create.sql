-- The below SQL is used to create the data that is then exported with BACKUP
-- for use in the RestoreOldSequences test.

CREATE DATABASE test;

SET database = test;

-- t1 is a table with a SERIAL sequence and a DEFAULT value sequence.

CREATE SEQUENCE s;

SET serial_normalization = 'sql_sequence';

CREATE TABLE t1 (i SERIAL PRIMARY KEY, j INT NOT NULL DEFAULT nextval('test.public.s'));

INSERT INTO t1 VALUES (default, default);

CREATE SEQUENCE s2;

CREATE VIEW v AS (SELECT nextval('s2'));

CREATE VIEW v2 AS (SELECT k FROM (SELECT nextval('s2') AS k));
