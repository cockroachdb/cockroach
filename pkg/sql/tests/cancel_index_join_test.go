// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// This test reproduces an issue seen in the wild where the context
// cancellation of the context passed to an internal executor query
// does not seem to propagate to distsql flows.
func TestCancelDistSQLIndexJoinCountWithInternalExecutor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var filter atomic.Value
	filter.Store(kvserverbase.ReplicaRequestFilter(nil))
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{

				Store: &kvserver.StoreTestingKnobs{
					TestingRequestFilter: func(ctx context.Context, request roachpb.BatchRequest) *roachpb.Error {
						f, _ := filter.Load().(kvserverbase.ReplicaRequestFilter)
						if f != nil {
							return f(ctx, request)
						}
						return nil
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	runQueries := func(t *testing.T, queries ...string) {
		for _, q := range queries {
			tdb.Exec(t, q)
		}
	}
	runQueries(t,
		`SET CLUSTER SETTING sql.defaults.distsql = 'on'`,
		`SET CLUSTER SETTING sql.stats.automatic_collection.enabled = 'off'`,
		`CREATE TABLE foo (i INT PRIMARY KEY, j INT, k BOOL, INDEX (j));`,
		`INSERT INTO foo SELECT i, i, true FROM generate_series(1, 10000) AS t(i);`,
		`ALTER TABLE foo SPLIT AT select generate_series(500, 10000, 500);`,
		`ALTER INDEX foo@foo_j_idx SPLIT AT select generate_series(500, 10000, 500);`,
	)
	require.NoError(t, tc.WaitForFullReplication())
	runQueries(t,
		`ALTER TABLE foo SCATTER;`,
		`ALTER INDEX foo@foo_j_idx SCATTER;`,
	)
	isUserScan := func(request roachpb.BatchRequest) bool {
		for _, request := range request.Requests {
			if sc := request.GetScan(); sc != nil && keys.UserTableDataMin.Compare(sc.Key) < 1 {
				return true
			}
		}
		return false
	}
	blockCh := make(chan struct{})
	blocked := int64(0)
	done := make(chan struct{})
	maybeBlock := func(ctx context.Context) {
		if !strings.Contains(string(debug.Stack()), "colflow") {
			return
		}
		if atomic.AddInt64(&blocked, 1) > 1 {
			return
		}
		log.Infof(ctx, "blocked %s", debug.Stack())
		select {
		case blockCh <- struct{}{}:
		case <-ctx.Done():
		}
		defer close(done)
		<-ctx.Done()
	}

	// Try to get the caches up to date with information about the leaseholders
	// so distsql plans a distributed flow.
	tdb.Exec(t, "SELECT count(1) FROM defaultdb.public.foo@foo_j_idx WHERE NOT k")
	tdb.Exec(t, "SELECT count(1) FROM foo WHERE NOT k")

	print := func(q string) {
		s := tdb.QueryStr(t, q)
		log.Infof(ctx, "s: %v", sqlutils.MatrixToStr(s))
	}
	print("EXPLAIN (DISTSQL) SELECT count(1) FROM defaultdb.public.foo@foo_j_idx WHERE NOT k")
	print("SHOW RANGES FROM INDEX  defaultdb.public.foo@foo_j_idx ")

	filter.Store(kvserverbase.ReplicaRequestFilter(func(
		ctx context.Context, request roachpb.BatchRequest,
	) *roachpb.Error {
		if !isUserScan(request) {
			return nil
		}
		if request.Replica.NodeID != 1 {
			maybeBlock(ctx)
		}
		return nil
	}))

	// Run a transaction with the internal executor, wait for the request
	// to get blocked, cancel the context, make sure it gets unblocked.
	errCh := make(chan error, 1)
	toCancel, cancel := context.WithCancel(ctx)

	go func() {
		ie := tc.Server(0).InternalExecutor().(*sql.InternalExecutor)
		sd := sql.NewFakeSessionData(&tc.Server(0).ClusterSettings().SV)
		ie.SetSessionData(sd)
		_, err := ie.Exec(toCancel, "foo", nil,
			"SELECT count(1) FROM defaultdb.public.foo@foo_j_idx WHERE NOT k")
		errCh <- err
	}()
	select {
	case <-blockCh:
	case err := <-errCh:
		t.Fatalf("here %v", err)
	}
	cancel()
	<-done
	require.Regexp(t, context.Canceled.Error(), <-errCh)
}
