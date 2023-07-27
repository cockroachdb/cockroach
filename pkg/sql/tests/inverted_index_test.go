// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const numRandomJSONs = 1000
const numProbes = 10
const docsToUpdate = 100
const docsToDelete = 100
const jsonComplexity = 25

func TestInvertedIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(tc.Conns[0])

	db.Exec(t, "CREATE DATABASE IF NOT EXISTS test")
	db.Exec(t, "CREATE TABLE test.jsons (i INT PRIMARY KEY, j JSONB)")

	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	// Grab a bunch of random JSONs. We insert half before we add the inverted
	// index and half after.
	jsons := make([]json.JSON, numRandomJSONs)
	for i := 0; i < numRandomJSONs; i++ {
		var err error
		jsons[i], err = json.Random(jsonComplexity, r)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < numRandomJSONs/2; i++ {
		db.Exec(t, `INSERT INTO test.jsons VALUES ($1, $2)`, i, jsons[i].String())
	}
	db.Exec(t, `CREATE INVERTED INDEX ON test.jsons (j)`)
	for i := numRandomJSONs / 2; i < numRandomJSONs; i++ {
		db.Exec(t, `INSERT INTO test.jsons VALUES ($1, $2)`, i, jsons[i].String())
	}

	t.Run("ensure we're using the inverted index", func(t *testing.T) {
		// Just to make sure we're using the inverted index.
		explain := db.Query(t, `SELECT count(*) FROM [EXPLAIN SELECT * FROM test.jsons WHERE j @> '{"a": 1}'] WHERE info LIKE '%jsons@jsons_j_idx%'`)
		explain.Next()
		var c int
		if err := explain.Scan(&c); err != nil {
			t.Fatal(err)
		}
		explain.Close()

		if c != 1 {
			t.Fatalf("Query not using inverted index as expected")
		}
	})

	t.Run("probe database after inserts", func(t *testing.T) {
		probeInvertedIndex(t, db, jsons)
	})

	// Now let's do some updates: we're going to pick a handful of the JSON
	// documents we inserted and change them to something else.
	perm := rand.Perm(len(jsons))
	for i := 0; i < docsToUpdate; i++ {
		var err error
		jsons[perm[i]], err = json.Random(jsonComplexity, r)
		if err != nil {
			t.Fatal(err)
		}

		db.Exec(t, `UPDATE test.jsons SET j = $1 WHERE i = $2`, jsons[perm[i]].String(), perm[i])
	}

	t.Run("probe database after updates", func(t *testing.T) {
		probeInvertedIndex(t, db, jsons)
	})

	// Now do some updates of the primary keys to prompt some deletions and
	// re-insertions.  Slightly biased because we always add the number of keys
	// just as a simple way to avoid a conflict.
	perm = rand.Perm(len(jsons))
	for i := 0; i < docsToUpdate; i++ {
		db.Exec(t, `UPDATE test.jsons SET i = $1 WHERE i = $2`, perm[i], perm[i]+numRandomJSONs)
	}

	t.Run("probe database after pk updates", func(t *testing.T) {
		probeInvertedIndex(t, db, jsons)
	})

	// Now do some deletions.
	perm = rand.Perm(len(jsons))
	for i := 0; i < docsToDelete; i++ {
		db.Exec(t, `DELETE FROM test.jsons WHERE i = $1`, perm[i])
		jsons[perm[i]] = nil
	}

	// Collect the new set of json values by iterating over the rest of perm.
	newJSONS := make([]json.JSON, len(jsons)-docsToDelete)
	for i := 0; i < len(jsons)-docsToDelete; i++ {
		newJSONS[i] = jsons[perm[i+docsToDelete]]
	}
	jsons = newJSONS

	t.Run("probe database after deletes", func(t *testing.T) {
		probeInvertedIndex(t, db, jsons)
	})
}

func probeInvertedIndex(t *testing.T, db *sqlutils.SQLRunner, jsons []json.JSON) {
	perm := rand.Perm(len(jsons))

	// Now probe it to make sure the data makes sense.
	for i := 0; i < numProbes; i++ {
		j := jsons[perm[i]]
		paths, err := json.AllPaths(j)
		if err != nil {
			t.Fatal(err)
		}

		for _, p := range paths {
			seenOriginal := true
			numResults := 0
			rows := db.Query(t, "SELECT j FROM test.jsons WHERE j @> $1", p.String())
			for rows.Next() {
				numResults++
				var s string
				if err := rows.Scan(&s); err != nil {
					t.Fatal(err)
				}
				returnedJSON, err := json.ParseJSON(s)
				if err != nil {
					t.Fatal(err)
				}

				cmp, err := j.Compare(returnedJSON)
				if err != nil {
					t.Fatal(err)
				}
				if cmp == 0 {
					seenOriginal = true
				}

				c, err := json.Contains(returnedJSON, p)
				if err != nil {
					t.Fatal(err)
				}
				if !c {
					t.Fatalf(
						"json %s was returned from inverted index query but does not contain %s",
						returnedJSON,
						p,
					)
				}
			}

			if !seenOriginal {
				t.Fatalf("%s was not returned by querying path %s", j, p)
			}

			// Now let's verify the results ourselves...
			countedResults := 0
			for _, j := range jsons {
				c, err := json.Contains(j, p)
				if err != nil {
					t.Fatal(err)
				}
				if c {
					countedResults++
				}
			}

			if countedResults != numResults {
				t.Fatalf("query returned %d results but there were actually %d results", numResults, countedResults)
			}

			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
