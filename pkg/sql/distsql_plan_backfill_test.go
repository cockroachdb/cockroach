// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDistBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("short flag #13645")
	}

	// This test sets up various queries using these tables:
	//  - a NumToSquare table of size N that maps integers from 1 to n to their
	//    squares
	//  - a NumToStr table of size N^2 that maps integers to their string
	//    representations. This table is split and distributed to all the nodes.
	n := 100
	if util.RaceEnabled {
		// Race builds are a lot slower, so use a smaller number of rows.
		n = 10
	}
	const numNodes = 5

	tc := serverutils.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
				Knobs: base.TestingKnobs{
					SQLSchemaChanger: &SchemaChangerTestingKnobs{
						// Aggressively write checkpoints, so that
						// we test checkpointing functionality while
						// a schema change backfill is progressing.
						WriteCheckpointInterval: time.Nanosecond,
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.TODO())
	cdb := tc.Server(0).DB()

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "numtosquare", "x INT PRIMARY KEY, xsquared INT",
		n,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, func(row int) tree.Datum {
			return tree.NewDInt(tree.DInt(row * row))
		}),
	)

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "numtostr", "y INT PRIMARY KEY, str STRING",
		n*n,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowEnglishFn),
	)
	// Split the table into multiple ranges.
	descNumToStr := sqlbase.GetTableDescriptor(cdb, "test", "numtostr")
	// SplitTable moves the right range, so we split things back to front
	// in order to move less data.
	for i := numNodes - 1; i > 0; i-- {
		SplitTable(t, tc, descNumToStr, i, n*n/numNodes*i)
	}

	r := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	r.DB.SetMaxOpenConns(1)
	r.Exec(t, "SET DISTSQL = OFF")
	if _, err := tc.ServerConn(0).Exec(`CREATE INDEX foo ON numtostr (str)`); err != nil {
		t.Fatal(err)
	}
	r.Exec(t, "SET DISTSQL = ALWAYS")
	res := r.QueryStr(t, `SELECT str FROM numtostr@foo`)
	if len(res) != n*n {
		t.Errorf("expected %d entries, got %d", n*n, len(res))
	}
	// Check res is sorted.
	curr := ""
	for i, str := range res {
		if curr > str[0] {
			t.Errorf("unexpected unsorted %s > %s at %d", curr, str[0], i)
		}
		curr = str[0]
	}
}
