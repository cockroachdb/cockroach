// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestGossipInvalidation verifies that the cache gets invalidated automatically
// when a new stat is generated.
func TestGossipInvalidation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sc := stats.NewTableStatisticsCache(
		10, /* cacheSize */
		gossip.MakeExposedGossip(tc.Server(0).GossipI().(*gossip.Gossip)),
		tc.Server(0).DB(),
		tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor),
		keys.SystemSQLCodec,
	)

	sr0 := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sr0.Exec(t, "CREATE DATABASE test")
	sr0.Exec(t, "CREATE TABLE test.t (k INT PRIMARY KEY, v INT)")
	sr0.Exec(t, "INSERT INTO test.t VALUES (1, 1), (2, 2), (3, 3)")

	tableDesc := sqlbase.TestingGetTableDescriptor(tc.Server(0).DB(), keys.SystemSQLCodec, "test", "t")
	tableID := tableDesc.ID

	expectNStats := func(n int) error {
		stats, err := sc.GetTableStats(ctx, tableID)
		if err != nil {
			t.Fatal(err)
		}
		if len(stats) != n {
			return fmt.Errorf("expected %d stats, got: %v", n, stats)
		}
		return nil
	}

	if err := expectNStats(0); err != nil {
		t.Fatal(err)
	}
	sr1 := sqlutils.MakeSQLRunner(tc.ServerConn(1))
	sr1.Exec(t, "CREATE STATISTICS k ON k FROM test.t")

	testutils.SucceedsSoon(t, func() error {
		return expectNStats(1)
	})

	sr2 := sqlutils.MakeSQLRunner(tc.ServerConn(2))
	sr2.Exec(t, "CREATE STATISTICS v ON v FROM test.t")

	testutils.SucceedsSoon(t, func() error {
		return expectNStats(2)
	})
}
