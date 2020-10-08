--- input file for Example_read_from_file.

--- don't report timestamps: it makes the output non-deterministic.
\unset show_times

USE defaultdb;
CREATE TABLE test(s STRING);

-- make the reminder echo its SQL.
\set echo
INSERT INTO test(s) VALUES ('hello'), ('world');
SELECT * FROM test;

-- produce an error, to test that processing stops.
SELECT undefined;

-- this is not executed
SELECT 'unseen';
