-- The below SQL is used to create the data that is then exported with BACKUP
-- for use in the RestoreOldVersions test. Each of t1, t2, and t3 should contain
-- the same rows when ordered by k.

CREATE DATABASE test;

SET database = test;

-- t1 gets some modifications.

CREATE TABLE t1 (k INT8 PRIMARY KEY, v1 INT8);

ALTER TABLE t1 ADD COLUMN v2 INT8;

CREATE INDEX t1_v2 ON t1 (v2);

-- t2 is an unmodified table.

CREATE TABLE t2 (k INT8 PRIMARY KEY, v1 INT8, v2 INT8);

-- t3 and t4 are interleaved.

CREATE TABLE t3 (k INT8 PRIMARY KEY);

CREATE TABLE t4 (k INT8 PRIMARY KEY, v1 INT8, v2 INT8, CONSTRAINT fk_t3 FOREIGN KEY (k) REFERENCES t3)
    INTERLEAVE IN PARENT t3 (k);

INSERT INTO t1 VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);

INSERT INTO t2 VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);

INSERT INTO t3 VALUES (1), (2), (3);

INSERT INTO t4 VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);

