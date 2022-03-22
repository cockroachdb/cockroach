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

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestPostgresMutator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	q := `
		CREATE TABLE t (s STRING FAMILY fam1, b BYTES, FAMILY fam2 (b), PRIMARY KEY (s ASC, b DESC), INDEX (s) STORING (b))
		    PARTITION BY LIST (s)
		        (
		            PARTITION europe_west VALUES IN ('a', 'b')
		        );
		ALTER TABLE table1 INJECT STATISTICS 'blah';
		SET CLUSTER SETTING "sql.stats.automatic_collection.enabled" = false;
	`

	rng, _ := randutil.NewTestRand()
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
