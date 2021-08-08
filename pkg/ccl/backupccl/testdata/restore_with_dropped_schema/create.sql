-- The below SQL is used to create a backup of a database that
-- contains a corrupted database descriptor. Data is produced
-- using version 20.2.7. This backup is used in
-- TestRestoreWithDroppedSchemaCorruption test.

CREATE DATABASE foo;

SET DATABASE = foo;

CREATE SCHEMA bar;

DROP SCHEMA bar;

BACKUP DATABASE foo to 'nodelocal://0/foo_backup';
