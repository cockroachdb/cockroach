CREATE TABLE everything (
  i     INT PRIMARY KEY,

  c     CHAR(10),
  s     VARCHAR(100),
  tx    TEXT,

  bin   BYTEA,
  vbin  BYTEA,
  bl    BLOB,

  dt    TIMESTAMPTZ,
  d     DATE,
  ts    TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
  t     TIME,
  -- TODO(dt): Fix year parsing.
  -- y  SMALLINT,

  de    DECIMAL(10, 0),
  nu    NUMERIC(10, 0),
  d53   DECIMAL(5,3),

  iw    INT,
  iz    INT,
  ti    SMALLINT,
  si    SMALLINT,
  mi    INT,
  bi    BIGINT,

  fl    FLOAT4,
  rl    DOUBLE PRECISION,
  db    DOUBLE PRECISION,

  f17   FLOAT4,
  f47   DOUBLE PRECISION,
  f75   FLOAT4
)
