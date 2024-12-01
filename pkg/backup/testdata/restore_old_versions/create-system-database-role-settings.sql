-- The below SQL is used to create the data that is then exported with BACKUP
-- for use in the TestRestoreOldVersions test. This should be run on a v22.2
-- cluster and used to test that after a restore on a v23.1 cluster, the user
-- ID column in the system.database_role_settings table is backfilled.

CREATE DATABASE test;

SET database = test;

CREATE USER testuser1;

CREATE USER testuser2;

ALTER USER testuser1 SET application_name = 'roachdb';

ALTER USER testuser2 SET disallow_full_table_scans = on;

ALTER ROLE ALL SET timezone = 'America/New_York';
