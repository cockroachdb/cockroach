CREATE USER craig;
CREATE DATABASE data;
CREATE TABLE data.bank (a INT);
INSERT INTO TABLE data.bank VALUES (1);

COMMENT ON TABLE data.bank IS 'table comment string';
COMMENT ON DATABASE data IS 'database comment string';

INSERT INTO system.locations ("localityKey", "localityValue", latitude, longitude) VALUES ('city', 'nyc', 10, 10);

ALTER TABLE data.bank CONFIGURE ZONE USING gc.ttlseconds = 3600;
