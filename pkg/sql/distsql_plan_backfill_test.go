// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDistBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderShort(t, "13645")

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

	tc := serverutils.StartNewTestCluster(t, numNodes,
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
	defer tc.Stopper().Stop(context.Background())
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
	descNumToStr := catalogkv.TestingGetTableDescriptor(cdb, keys.SystemSQLCodec, "test", "numtostr")
	var sps []SplitPoint
	//for i := 1; i <= numNodes-1; i++ {
	for i := numNodes - 1; i > 0; i-- {
		sps = append(sps, SplitPoint{i, []interface{}{n * n / numNodes * i}})
	}
	SplitTable(t, tc, descNumToStr, sps)

	db := tc.ServerConn(0)
	db.SetMaxOpenConns(1)
	r := sqlutils.MakeSQLRunner(db)
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
