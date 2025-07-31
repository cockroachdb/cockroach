-- The below SQL is used to create the data that is then exported with BACKUP
-- for use in the RestoreImportingTable test.
--
-- Note, these statements must be run with a `time.Sleep(1 * time.Minute)` before the descriptors
-- are published to an online state. This simulates the import job being paused after the data has
-- been ingested but before the tables are online.
CREATE DATABASE d;
USE d;
CREATE TABLE foo (i INT PRIMARY KEY, s STRING);
CREATE TABLE foofoo (i INT PRIMARY KEY, s STRING);
INSERT INTO foofoo VALUES (10, 'x0');
CREATE TABLE goodfoo (i INT PRIMARY KEY, s STRING);
CREATE INDEX goodfoo_idx on goodfoo (s);
CREATE TABLE baz (i INT PRIMARY KEY, s STRING);
INSERT INTO baz VALUES (1, 'x'),(2,'y'),(3,'z');

EXPORT INTO CSV 'nodelocal://0/export1/' FROM SELECT * FROM baz;

-- The imports will hang after ingesting data.
IMPORT INTO foo (i,s) CSV DATA ('nodelocal://0/export1/export*-n*.0.csv') WITH detached;

IMPORT INTO foofoo (i,s) CSV DATA ('nodelocal://0/export1/export*-n*.0.csv') WITH detached;

IMPORT INTO goodfoo (i,s) CSV DATA ('nodelocal://0/export1/export*-n*.0.csv') WITH detached;

-- Run these backups while the import jobs are suspended. See the note at the top of the file on
-- how to simulate this.
BACKUP DATABASE d INTO 'nodelocal://0/database/' with revision_history;

BACKUP DATABASE d INTO 'nodelocal://0/database_no_hist/';

BACKUP DATABASE d INTO 'nodelocal://0/database_double_inc/' with revision_history;

BACKUP INTO 'nodelocal://0/cluster/' with revision_history;

-- Take an incremental before the import jobs complete because we want two incrementals with the
-- tables in an offline state.
INSERT INTO baz VALUES (4, 'a');
BACKUP DATABASE d INTO LATEST IN 'nodelocal://0/database_double_inc/' with revision_history;

-- CANCEL JOB <import1>;
-- CANCEL JOB <import2>;
-- Wait for <import3> to complete.

BACKUP DATABASE d INTO LATEST IN 'nodelocal://0/database/' with revision_history;

BACKUP DATABASE d INTO LATEST IN 'nodelocal://0/database_no_hist/';

BACKUP DATABASE d INTO LATEST IN 'nodelocal://0/database_double_inc/' with revision_history;

BACKUP INTO LATEST IN 'nodelocal://0/cluster/' with revision_history;
