-- The below SQL is used to create the data that is then exported with BACKUP
-- for use in the TestRestoreOldVersions test. This should be run on a v22.2
-- cluster and used to test that after a restore on a v23.1 cluster, the user
-- ID column in the system.external_connections table is backfilled.

CREATE DATABASE test;

SET database = test;

CREATE USER testuser1;

CREATE USER testuser2;

GRANT SYSTEM EXTERNALCONNECTION TO testuser1;

GRANT SYSTEM EXTERNALCONNECTION TO testuser2;

SET ROLE TO testuser1;

CREATE EXTERNAL CONNECTION connection1 AS 'userfile:///connection1';

SET ROLE TO testuser2;

CREATE EXTERNAL CONNECTION connection2 AS 'userfile:///connection2';
