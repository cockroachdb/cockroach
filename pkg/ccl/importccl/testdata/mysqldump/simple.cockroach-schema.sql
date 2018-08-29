CREATE SEQUENCE simple_auto_inc;

CREATE TABLE simple (
  i INT4 PRIMARY KEY DEFAULT nextval('simple_auto_inc':::string),
  s text,
  b bytea
)
