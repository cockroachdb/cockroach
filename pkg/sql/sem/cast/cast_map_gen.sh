#!/bin/bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# casts_gen.sh generates castMap entries by reading from Postgres's pg_cast
# table. To use this script, Postgres must be installed with the PostGIS
# extension and already running.
#
# By default the script connects to the "postgres" database. To use a different
# database, supply its name as the first argument to the script.

DATABASE="${1-postgres}"

psql $DATABASE -Xqtc "SELECT postgis_full_version()" &> /dev/null
if [ $? -ne 0 ]; then
  echo "error: postgis must be installed in database $DATABASE";
  echo "hint: you can specify another database as the first argument";
  exit 1;
fi

PG_CAST_QUERY="
  SELECT
    (
      CASE castsource::REGTYPE::TEXT
      WHEN 'bigint' THEN 'int8'
      WHEN 'bit varying' THEN 'varbit'
      WHEN 'boolean' THEN 'bool'
      WHEN '\"char\"' THEN 'char'
      WHEN 'character' THEN 'bpchar'
      WHEN 'character varying' THEN 'varchar'
      WHEN 'double precision' THEN 'float8'
      WHEN 'integer' THEN 'int4'
      WHEN 'real' THEN 'float4'
      WHEN 'smallint' THEN 'int2'
      WHEN 'timestamp with time zone' THEN 'timestamptz'
      WHEN 'timestamp without time zone' THEN 'timestamp'
      WHEN 'time with time zone' THEN 'timetz'
      WHEN 'time without time zone' THEN 'time'
      ELSE castsource::REGTYPE::TEXT
      END
    ),
    (
      CASE casttarget::REGTYPE::TEXT
      WHEN 'bigint' THEN 'int8'
      WHEN 'bit varying' THEN 'varbit'
      WHEN 'boolean' THEN 'bool'
      WHEN '\"char\"' THEN 'char'
      WHEN 'character' THEN 'bpchar'
      WHEN 'character varying' THEN 'varchar'
      WHEN 'double precision' THEN 'float8'
      WHEN 'integer' THEN 'int4'
      WHEN 'real' THEN 'float4'
      WHEN 'smallint' THEN 'int2'
      WHEN 'timestamp with time zone' THEN 'timestamptz'
      WHEN 'timestamp without time zone' THEN 'timestamp'
      WHEN 'time with time zone' THEN 'timetz'
      WHEN 'time without time zone' THEN 'time'
      ELSE casttarget::REGTYPE::TEXT
      END
    ),
    (
      CASE castcontext
      WHEN 'e' THEN 'CastContextExplicit'
      WHEN 'a' THEN 'CastContextAssignment'
      WHEN 'i' THEN 'CastContextImplicit'
      END
    )
  FROM pg_cast
  ORDER BY 1, 2"

psql $DATABASE --csv -Xqt -c "$PG_CAST_QUERY" |
  awk -F, '
    {
      if ($1 != src)
      {
        src = $1;
        if (NR > 1) print "},";
        print "oid.T_" $1 ": {";
      }
      print "\toid.T_" $2 ": {maxContext: " $3 ", origin: pgCast},";
    }
    END { print "}"; }'
