// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tests_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

const numRandomJSONs = 1000
const numProbes = 100

func TestInvertedIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.TODO())

	db := sqlutils.MakeSQLRunner(tc.Conns[0])

	db.Exec(t, "CREATE DATABASE IF NOT EXISTS test")
	db.Exec(t, "CREATE TABLE test.jsons (i INT PRIMARY KEY, j JSONB, INVERTED INDEX (j))")

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Grab a bunch of random JSONs.
	jsons := make([]json.JSON, numRandomJSONs)
	for i := 0; i < numRandomJSONs; i++ {
		var err error
		jsons[i], err = json.Random(100, r)
		if err != nil {
			t.Fatal(err)
		}

		db.Exec(t, `INSERT INTO test.jsons VALUES ($1, $2)`, i, jsons[i].String())
	}

	// Now probe it to make sure the data makes sense.
	for i := 0; i < numProbes; i++ {
		probeInvertedIndex(t, db, jsons)
	}
}

func probeInvertedIndex(t *testing.T, db *sqlutils.SQLRunner, jsons []json.JSON) {
	j := jsons[rand.Intn(len(jsons))]

	paths, err := json.AllPaths(j)
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range paths {
		seenOriginal := false
		rows := db.Query(t, "SELECT j FROM test.jsons WHERE j @> $1", p.String())
		for rows.Next() {
			var s string
			rows.Scan(&s)
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

		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
