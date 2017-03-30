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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package sql

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// SplitTable splits a range in the table, creates a replica for the right
// side of the split on targetNodeIdx, and moves the lease for the right
// side of the split to targetNodeIdx. This forces the querying against
// the table to be distributed. vals is a list of values forming a primary
// key for the table.
//
// TODO(radu): SplitTable or its equivalent should be added to TestCluster.
//
// TODO(radu): we should verify that the queries in tests using SplitTable
// are indeed distributed as intended.
func SplitTable(
	t *testing.T,
	tc serverutils.TestClusterInterface,
	desc *sqlbase.TableDescriptor,
	targetNodeIdx int,
	vals ...interface{},
) {
	pik, err := sqlbase.MakePrimaryIndexKey(desc, vals...)
	if err != nil {
		t.Fatal(err)
	}

	splitKey := keys.MakeRowSentinelKey(pik)
	_, rightRange, err := tc.Server(0).SplitRange(splitKey)
	if err != nil {
		t.Fatal(err)
	}

	rightRangeStartKey := rightRange.StartKey.AsRawKey()
	rightRange, err = tc.AddReplicas(rightRangeStartKey, tc.Target(targetNodeIdx))
	if err != nil {
		t.Fatal(err)
	}

	if err := tc.TransferRangeLease(rightRange, tc.Target(targetNodeIdx)); err != nil {
		t.Fatal(err)
	}
}

func TestDistBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test sets up various queries using these tables:
	//  - a NumToSquare table of size N that maps integers from 1 to n to their
	//    squares
	//  - a NumToStr table of size N^2 that maps integers to their string
	//    representations. This table is split and distributed to all the nodes.
	const n = 100
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
	defer tc.Stopper().Stop()
	cdb := tc.Server(0).KVClient().(*client.DB)

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "NumToSquare", "x INT PRIMARY KEY, xsquared INT",
		n,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, func(row int) parser.Datum {
			return parser.NewDInt(parser.DInt(row * row))
		}),
	)

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "NumToStr", "y INT PRIMARY KEY, str STRING",
		n*n,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowEnglishFn),
	)
	// Split the table into multiple ranges.
	descNumToStr := sqlbase.GetTableDescriptor(cdb, "test", "NumToStr")
	// SplitTable moves the right range, so we split things back to front
	// in order to move less data.
	for i := numNodes - 1; i > 0; i-- {
		SplitTable(t, tc, descNumToStr, i, n*n/numNodes*i)
	}

	r := sqlutils.MakeSQLRunner(t, tc.ServerConn(0))
	r.DB.SetMaxOpenConns(1)
	r.Exec("SET DISTSQL = ALWAYS")

	r = r.Subtest(t)
	r.Exec("SET DISTSQL = OFF")
	if _, err := tc.ServerConn(0).Exec("CREATE INDEX foo ON NumToStr (str)"); err != nil {
		t.Fatal(err)
	}
	r.Exec("SET DISTSQL = ALWAYS")
	res := r.QueryStr("SELECT str FROM NumToStr@foo")
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
