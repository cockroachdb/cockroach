// Copyright 2017 The Cockroach Authors.
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

package sql_test

import (
	"context"
	gosql "database/sql"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestIndexBackfiller tests the scenarios described in docs/tech-notes/index-backfill.md
func TestIndexBackfiller(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()

	moveToTDelete := make(chan bool)
	moveToTWrite := make(chan bool)
	moveToTScan := make(chan bool)
	moveToBackfill := make(chan bool)

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforePublishWriteAndDelete: func() {
				// Signal that we've moved into DELETE_ONLY.
				moveToTDelete <- true
				// Wait until we get a signal to move to DELETE_AND_WRITE_ONLY.
				<-moveToTWrite
			},
			RunBeforeBackfill: func() error {
				// Wait until we get a signal to pick our scan timestamp.
				<-moveToTScan
				return nil
			},
			RunBeforeIndexBackfill: func() {
				// Wait until we get a signal to begin backfill.
				<-moveToBackfill
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	tc := serverutils.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ServerArgs: params,
		})
	defer tc.Stopper().Stop(context.TODO())
	sqlDB := tc.ServerConn(0)

	execOrFail := func(query string) gosql.Result {
		if res, err := sqlDB.Exec(query); err != nil {
			t.Fatal(err)
		} else {
			return res
		}
		return nil
	}

	// The sequence of events here exactly matches the test cases in
	// docs/tech-notes/index-backfill.md. If you update this, please remember to
	// update the tech note as well.

	execOrFail("CREATE DATABASE t")
	execOrFail("CREATE TABLE t.public.kv (k int PRIMARY KEY, v char)")
	execOrFail("INSERT INTO t.public.kv VALUES (1, 'a'), (3, 'c'), (4, 'e'), (6, 'f'), (7, 'g'), (9, 'h')")

	// Start the schema change.
	var finishedSchemaChange sync.WaitGroup
	finishedSchemaChange.Add(1)
	go func() {
		execOrFail("CREATE UNIQUE INDEX vidx on t.public.kv(v)")
		finishedSchemaChange.Done()
	}()

	// Wait until the schema change has moved the cluster into DELETE_ONLY mode.
	<-moveToTDelete
	execOrFail("DELETE FROM t.public.kv WHERE k=9")
	execOrFail("INSERT INTO t.public.kv VALUES (9, 'h')")

	// Move to WRITE_ONLY mode.
	moveToTWrite <- true
	execOrFail("INSERT INTO t.public.kv VALUES (2, 'b')")

	// Pick our scan timestamp.
	moveToTScan <- true
	execOrFail("UPDATE t.public.kv SET v = 'd' WHERE k = 3")
	execOrFail("UPDATE t.public.kv SET k = 5 WHERE v = 'e'")
	execOrFail("DELETE FROM t.public.kv WHERE k = 6")

	// Begin the backfill.
	moveToBackfill <- true

	finishedSchemaChange.Wait()

	pairsPrimary := queryPairs(t, sqlDB, "SELECT k, v FROM t.public.kv ORDER BY k ASC")
	pairsIndex := queryPairs(t, sqlDB, "SELECT k, v FROM t.public.kv@vidx ORDER BY k ASC")

	if len(pairsPrimary) != len(pairsIndex) {
		t.Fatalf("Mismatched entries in table and index: %+v %+v", pairsPrimary, pairsIndex)
	}

	for i, pPrimary := range pairsPrimary {
		if pPrimary != pairsIndex[i] {
			t.Fatalf("Mismatched entry in table and index: %+v %+v", pPrimary, pairsIndex[i])
		}
	}
}

type pair struct {
	k int
	v string
}

func queryPairs(t *testing.T, sqlDB *gosql.DB, query string) []pair {
	rows, err := sqlDB.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	ret := make([]pair, 0)
	for rows.Next() {
		p := pair{}
		if err := rows.Scan(&p.k, &p.v); err != nil {
			t.Fatal(err)
		}
		ret = append(ret, p)
	}
	return ret
}
