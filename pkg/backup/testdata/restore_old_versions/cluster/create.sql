CREATE USER craig;

CREATE DATABASE data;
USE data;
CREATE SCHEMA foo;
CREATE TYPE data.foo.bat AS ENUM ('a', 'b', 'c');
CREATE TABLE data.bank (a INT, b data.foo.bat);
INSERT INTO data.bank VALUES (1, 'a'), (2, 'b'), (3, 'c');

COMMENT ON TABLE data.bank IS 'table comment string';
COMMENT ON DATABASE data IS 'database comment string';

INSERT INTO system.locations ("localityKey", "localityValue", latitude, longitude) VALUES ('city', 'nyc', 10, 10);

ALTER TABLE data.bank CONFIGURE ZONE USING gc.ttlseconds = 3600;

CREATE FUNCTION add(x INT, y INT) RETURNS INT LANGUAGE SQL AS 'SELECT x+y';
