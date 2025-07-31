-- The below SQL is used to create the data that is then exported with BACKUP
-- for use in the TestRestoreOldVersions test. This should be run on a v22.2
-- cluster and used to test that after a restore on a v23.1 cluster, the user
-- ID column in the system.privileges table is backfilled.

CREATE DATABASE test;

SET database = test;

CREATE USER testuser1;

CREATE USER testuser2;

GRANT SYSTEM VIEWACTIVITY TO testuser1;

GRANT SYSTEM MODIFYCLUSTERSETTING TO testuser2;

REVOKE SELECT ON crdb_internal.tables FROM public;
