-- The below SQL is used to create the data that is then exported with BACKUP
-- for use in the RestoreOldRevVersions test. It includes revisions to tables
-- that reference themselves or eachother both from the PK and secondary index
-- columns.

CREATE DATABASE test;

SET database = test;

CREATE TABLE circular (k INT8 PRIMARY KEY, selfid INT8 UNIQUE);
ALTER TABLE circular ADD CONSTRAINT self_fk FOREIGN KEY (selfid) REFERENCES circular (selfid);

CREATE TABLE parent (k INT8 PRIMARY KEY, j INT8 UNIQUE);
CREATE TABLE child (k INT8 PRIMARY KEY, parent_i INT8 REFERENCES parent, parent_j INT8 REFERENCES parent (j));
CREATE TABLE child_pk (k INT8 PRIMARY KEY REFERENCES parent);

CREATE TABLE rev_times (id INT PRIMARY KEY, logical_time DECIMAL);
INSERT INTO rev_times VALUES (1, cluster_logical_timestamp());

CREATE USER newuser;
GRANT ALL ON circular TO newuser;
GRANT ALL ON parent TO newuser;

INSERT INTO rev_times VALUES (2, cluster_logical_timestamp());

GRANT ALL ON child TO newuser;

INSERT INTO rev_times VALUES (3, cluster_logical_timestamp());

GRANT ALL ON child_pk TO newuser;
