CREATE TABLE everything (
  i     INT4 PRIMARY KEY,

  ex    INT4 DEFAULT 4:::INT4 * 4:::INT4 + 4:::INT4,
  c     CHAR(10) NOT NULL,
  s     VARCHAR(100) NULL DEFAULT e'this is s\'s default value':::STRING,
  tx    TEXT,
  e     STRING,

  bin   BYTEA NOT NULL,
  vbin  BYTEA,
  bl    BLOB,

  dt    TIMESTAMPTZ NOT NULL DEFAULT '2000-01-01 00:00:00':::TIMESTAMPTZ,
  d     DATE,
  ts    TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
  t     TIME,
  -- TODO(dt): Fix year parsing.
  -- y  SMALLINT,

  de    DECIMAL(10, 0),
  nu    NUMERIC(10, 0),
  d53   DECIMAL(5,3),

  iw    INT4 NOT NULL,
  iz    INT8,
  ti    SMALLINT DEFAULT 5:::INT,
  si    SMALLINT,
  mi    INT4,
  bi    BIGINT,

  fl    FLOAT4 NOT NULL,
  rl    DOUBLE PRECISION,
  db    DOUBLE PRECISION,

  f17   FLOAT4,
  f47   DOUBLE PRECISION,
  f75   FLOAT4,
  j     JSON,
  CONSTRAINT imported_from_enum_e CHECK (e IN ('Small':::STRING, 'Medium':::STRING, 'Large':::STRING))
)
