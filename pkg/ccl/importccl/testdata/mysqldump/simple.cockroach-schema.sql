CREATE SEQUENCe simple_auto_inc;

CREATE TABLE simple (
  i INT PRIMARY KEY DEFAULT nextval('simple_auto_inc':::string),
  s text,
  b bytea
)
