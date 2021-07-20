-- The below SQL is used to create the data that is then exported with BACKUP.
-- This should be run on a v20.1 cluster, the ZONECONFIG bit is incorrectly
-- updated to be interpreted as USAGE in versions of 20.2 and onward.
-- The restore test should ensure that ZONECONFIG stays as ZONECONFIG.

CREATE DATABASE test;

SET database = test;

CREATE USER testuser;

CREATE TABLE test_table();

CREATE TABLE test_table2();

GRANT ZONECONFIG ON DATABASE test TO testuser;

GRANT ZONECONFIG ON test_table TO testuser;

GRANT ALL ON test_table2 TO testuser;
