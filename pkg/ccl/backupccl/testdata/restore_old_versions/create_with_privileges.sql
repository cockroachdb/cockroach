-- The below SQL is used to create the data that is then exported with BACKUP
-- finish this comment

CREATE DATABASE test;

SET database = test;

CREATE USER testuser;

CREATE TABLE test_table();

CREATE TABLE test_table2();

GRANT ZONECONFIG ON DATABASE test TO testuser;

GRANT ZONECONFIG ON test_table TO testuser;

GRANT ALL ON test_table2 TO testuser;
