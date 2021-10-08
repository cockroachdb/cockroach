/*
Test cases are grouped as: i) CREATE TABLE(s), ii) Schema change(s) iii) BACKUP
This implies that for a given table, a BACKUP was taken while the schema changes
were running. If there are multiple schema changes, they were all running
simultaneously.

N.B.: The binary was modified to add a time.Sleep to block the schema change
after it completed the backfill portion of the schema change.
 */

CREATE TABLE midaddcol as select * from generate_series(1,3) as a;
ALTER TABLE midaddcol ADD COLUMN b DECIMAL NOT NULL DEFAULT (DECIMAL '1.3');
BACKUP defaultdb.* TO 'nodelocal://1/midaddcol';
DROP TABLE midaddcol;

CREATE TABLE midaddconst as select * from generate_series(1,3) as a;
ALTER TABLE midaddconst ADD CONSTRAINT my_const CHECK (a > 0);
BACKUP defaultdb.* TO 'nodelocal://1/midaddconst';
DROP TABLE midaddconst;

CREATE TABLE midaddindex as select * from generate_series(1,3) as a;
CREATE INDEX my_idx ON midaddindex(a);
BACKUP defaultdb.* TO 'nodelocal://1/midaddindex';
DROP TABLE midaddindex;

CREATE TABLE middropcol as select * from generate_series(1,3) as a, generate_series(1,3) as b;
ALTER TABLE middropcol DROP COLUMN b;
BACKUP defaultdb.* TO 'nodelocal://1/middropcol';
DROP TABLE middropcol;

CREATE TABLE midmany as select * from generate_series(1,3) as a;
ALTER TABLE midmany ADD COLUMN b DECIMAL NOT NULL DEFAULT (DECIMAL '1.3');
ALTER TABLE midmany ADD CONSTRAINT my_const CHECK (a > 0);
CREATE INDEX my_idx ON midmany(a);
BACKUP defaultdb.* TO 'nodelocal://1/midmany';
DROP TABLE midmany;

CREATE TABLE midmultitxn as SELECT * from generate_series(1,3) as a;
BEGIN;
ALTER TABLE midmultitxn ADD COLUMN b DECIMAL NOT NULL DEFAULT (DECIMAL '1.3');
ALTER TABLE midmultitxn ADD CONSTRAINT my_const CHECK (a > 0);
CREATE INDEX my_idx ON midmultitxn(a);
COMMIT;
BACKUP defaultdb.* TO 'nodelocal://1/midmultitxn';
DROP TABLE midmultitxn;

CREATE TABLE midmultitable1 AS SELECT * FROM generate_series(1, 3) AS a;
CREATE TABLE midmultitable2 AS SELECT * FROM generate_series(1, 3) AS a;
ALTER TABLE midmultitable1 ADD COLUMN b DECIMAL NOT NULL DEFAULT (DECIMAL '1.3');
ALTER TABLE midmultitable2 ADD CONSTRAINT my_const CHECK (a > 0);
BACKUP defaultdb.* TO 'nodelocal://1/midmultitable';
DROP TABLE midmultitable1;
DROP TABLE midmultitable2;

-- Primary key swaps are only supported on 20.1+.
CREATE TABLE midprimarykeyswap AS SELECT * FROM generate_series(1,3) AS a;
-- This schema change is used to enable the primary key swap. The backup is not taken during this schema change.
ALTER TABLE midprimarykeyswap ALTER COLUMN a SET NOT NULL;
ALTER TABLE midprimarykeyswap ALTER PRIMARY KEY USING COLUMNS (a);
BACKUP defaultdb.* TO 'nodelocal://1/midprimarykeyswap';
DROP TABLE midprimarykeyswap;

CREATE TABLE midprimarykeyswapcleanup AS SELECT * FROM generate_series(1,3) AS a;
-- This schema change is used to enable the primary key swap. The backup is not taken during this schema change.
ALTER TABLE midprimarykeyswapcleanup ALTER COLUMN a SET NOT NULL;
ALTER TABLE midprimarykeyswapcleanup ALTER PRIMARY KEY USING COLUMNS (a);
BACKUP defaultdb.* TO 'nodelocal://1/midprimarykeyswapcleanup';
DROP TABLE midprimarykeyswapcleanup;
