-- The below SQL is used to create the data that is then exported with BACKUP
-- for use in the TestRestoreOldVersions test. This should be run on a v22.2
-- cluster and used to test that after a restore on a v23.1 cluster, the ID
-- columns in the system.role_members table are backfilled.

CREATE DATABASE test;

SET database = test;

CREATE ROLE testrole;

CREATE USER testuser1;

CREATE USER testuser2;

GRANT testrole TO testuser1;

GRANT testrole TO testuser2 WITH ADMIN OPTION;
