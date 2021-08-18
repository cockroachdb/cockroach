-- The below SQL is used to create the data that is then exported with BACKUP.
-- This should be run on a v21.1, where an interleaved table is intentionally,
-- added into the backup

SET CLUSTER SETTING sql.defaults.interleaved_tables.enabled=yes;

CREATE DATABASE test;

SET database = test;

CREATE TABLE test.customers (id INT PRIMARY KEY, name STRING(50));

CREATE TABLE test.orders (customer INT, id INT, total DECIMAL(20, 5), PRIMARY KEY (customer, id), CONSTRAINT fk_customer FOREIGN KEY (customer) REFERENCES customers) INTERLEAVE IN PARENT customers (customer);

INSERT INTO CUSTOMERS values(1,'BOB');
INSERT INTO CUSTOMERS values(2,'BILL');
INSERT INTO ORDERS values(1, 1, 20);
INSERT INTO ORDERS values(2, 2, 30);
