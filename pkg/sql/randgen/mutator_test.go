// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package randgen

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestPostgresCreateTableMutator(t *testing.T) {
	q := `
CREATE TABLE table1 (
  col1_0
    TIMESTAMP,
  col1_1
    REGPROC,
  col1_2
    BOX2D NOT NULL,
  col1_3
    "char" NOT NULL,
  col1_4
    GEOGRAPHY NULL,
  col1_5
    GEOGRAPHY,
  col1_6
    REGROLE NOT NULL,
  col1_7
    REGROLE NOT NULL,
  col1_8
    "char",
  col1_9
    INTERVAL,
  col1_10
    STRING AS (lower(col1_3)) STORED NOT NULL,
  col1_11
    STRING AS (lower(CAST(col1_1 AS STRING))) STORED,
  col1_12
    STRING AS (lower(CAST(col1_5 AS STRING))) STORED,
  col1_13
    STRING AS (lower(CAST(col1_0 AS STRING))) VIRTUAL,
  col1_14
    STRING AS (lower(CAST(col1_4 AS STRING))) STORED NULL,
  col1_15
    STRING AS (lower(CAST(col1_7 AS STRING))) STORED NOT NULL,
  col1_16
    STRING AS (lower(CAST(col1_0 AS STRING))) STORED,
  INDEX (col1_9, col1_1, col1_12 ASC)
    WHERE
      (
        (table1.col1_8 != 'X':::STRING OR table1.col1_16 < e'\x00':::STRING)
        AND table1.col1_15 <= '':::STRING
      )
      AND table1.col1_14 >= e'\U00002603':::STRING,
  UNIQUE (
    col1_1 ASC,
    lower(CAST(col1_5 AS STRING)) ASC,
    col1_0,
    col1_10,
    col1_11 ASC,
    col1_12 ASC,
    col1_2 ASC,
    col1_7,
    col1_3,
    col1_6,
    col1_16
  )
    WHERE
      (
        (
          (
            (
              (
                (
                  (
                    (
                      table1.col1_0 < '-4713-11-24 00:00:00':::TIMESTAMP
                      AND table1.col1_14 = e'\'':::STRING
                    )
                    AND table1.col1_3 = '"':::STRING
                  )
                  OR table1.col1_8 >= e'\U00002603':::STRING
                )
                OR table1.col1_13 > '':::STRING
              )
              AND table1.col1_12 < '':::STRING
            )
            OR table1.col1_11 >= 'X':::STRING
          )
          AND table1.col1_10 <= e'\'':::STRING
        )
        OR table1.col1_16 != '':::STRING
      )
      OR table1.col1_15 >= '':::STRING,
  UNIQUE (col1_1, col1_16 DESC, col1_6, col1_11 DESC, col1_9 ASC, col1_3 DESC, col1_2 ASC),
  UNIQUE (col1_14 DESC, col1_15 DESC)
    WHERE
      (
        (
          (
            (
              (
                (
                  (
                    (table1.col1_14 >= e'\'':::STRING OR table1.col1_16 >= 'X':::STRING)
                    OR table1.col1_3 <= 'X':::STRING
                  )
                  AND table1.col1_15 = 'X':::STRING
                )
                OR table1.col1_13 > '"':::STRING
              )
              OR table1.col1_12 >= '':::STRING
            )
            AND table1.col1_11 != '':::STRING
          )
          OR table1.col1_10 != e'\U00002603':::STRING
        )
        AND table1.col1_8 != e'\U00002603':::STRING
      )
      OR table1.col1_0 <= '-4713-11-24 00:00:00':::TIMESTAMP,
  INVERTED INDEX (
    col1_13,
    col1_16,
    col1_12,
    col1_8,
    col1_3 ASC,
    col1_6,
    col1_2,
    col1_7,
    col1_0 DESC,
    lower(CAST(col1_7 AS STRING)) DESC,
    col1_1,
    col1_15 DESC,
    col1_4
  )
)`
	rng, _ := randutil.NewPseudoRand()
	// Set a deterministic seed so that PostgresCreateTableMutator performs a
	// deterministic transformation.
	rng.Seed(123)
	mutated, changed := ApplyString(rng, q, PostgresCreateTableMutator)
	if !changed {
		t.Fatal("expected changed")
	}
	expect := `
CREATE TABLE table1 (col1_0 TIMESTAMP, col1_1 REGPROC, col1_2 BOX2D NOT NULL, col1_3 "char" NOT NULL, col1_4 GEOGRAPHY NULL, col1_5 GEOGRAPHY, col1_6 REGROLE NOT NULL, col1_7 REGROLE NOT NULL, col1_8 "char", col1_9 INTERVAL, col1_10 STRING NOT NULL AS (lower(col1_3)) STORED, col1_11 STRING AS (CASE WHEN col1_1 IS NULL THEN 'L*h':::STRING ELSE '#':::STRING END) STORED, col1_12 STRING AS (lower(CAST(col1_5 AS STRING))) STORED, col1_13 STRING AS (CASE WHEN col1_0 IS NULL THEN e'\x1c\t':::STRING ELSE e'(,Zh\x05\x1dW':::STRING END) VIRTUAL, col1_14 STRING NULL AS (lower(CAST(col1_4 AS STRING))) STORED, col1_15 STRING NOT NULL AS (CASE WHEN col1_7 IS NULL THEN e'\x0e,\x162/BJ\x12':::STRING ELSE e'#\x17\x07;\x0emi':::STRING END) STORED, col1_16 STRING AS (CASE WHEN col1_0 IS NULL THEN e'\x1eM\x02\x12_\x05\r':::STRING ELSE e'[jUDO\nt':::STRING END) STORED);
CREATE INDEX ON table1 (col1_9, col1_1, col1_12 ASC);
CREATE UNIQUE INDEX ON table1 (col1_1 ASC, lower(CAST(col1_5 AS STRING)) ASC, col1_0, col1_10, col1_11 ASC, col1_12 ASC, col1_7, col1_3, col1_6, col1_16);
CREATE UNIQUE INDEX ON table1 (col1_1, col1_16 DESC, col1_6, col1_11 DESC, col1_9 ASC, col1_3 DESC);
CREATE UNIQUE INDEX ON table1 (col1_14 DESC, col1_15 DESC);`
	if strings.TrimSpace(mutated) != strings.TrimSpace(expect) {
		t.Fatalf("expected:\n%s\ngot:\n%s", expect, mutated)
	}
}

func TestPostgresMutator(t *testing.T) {
	q := `
		CREATE TABLE t (s STRING FAMILY fam1, b BYTES, FAMILY fam2 (b), PRIMARY KEY (s ASC, b DESC), INDEX (s) STORING (b))
		    PARTITION BY LIST (s)
		        (
		            PARTITION europe_west VALUES IN ('a', 'b')
		        );
		ALTER TABLE table1 INJECT STATISTICS 'blah';
		SET CLUSTER SETTING "sql.stats.automatic_collection.enabled" = false;
	`

	rng, _ := randutil.NewPseudoRand()
	{
		mutated, changed := ApplyString(rng, q, PostgresMutator)
		if !changed {
			t.Fatal("expected changed")
		}
		mutated = strings.TrimSpace(mutated)
		expect := `CREATE TABLE t (s TEXT, b BYTEA, PRIMARY KEY (s ASC, b DESC), INDEX (s) INCLUDE (b));`
		if mutated != expect {
			t.Fatalf("unexpected: %s", mutated)
		}
	}
	{
		mutated, changed := ApplyString(rng, q, PostgresCreateTableMutator, PostgresMutator)
		if !changed {
			t.Fatal("expected changed")
		}
		mutated = strings.TrimSpace(mutated)
		expect := "CREATE TABLE t (s TEXT, b BYTEA, PRIMARY KEY (s, b));\nCREATE INDEX ON t (s) INCLUDE (b);"
		if mutated != expect {
			t.Fatalf("unexpected: %s", mutated)
		}
	}
}
