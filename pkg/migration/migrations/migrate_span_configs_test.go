// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestEnsureSpanConfigReconciliation verifies that the migration waits for a
// span config reconciliation attempt, blocking until it occurs.
func TestEnsureSpanConfigReconciliation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	blockReconcilerCh := make(chan struct{})
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			EnableSpanConfigs: true,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.EnsureSpanConfigReconciliation - 1,
					),
				},
				SpanConfig: &spanconfig.TestingKnobs{
					ReconcilerInitialInterceptor: func() {
						<-blockReconcilerCh
					},
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	scKVAccessor := ts.SpanConfigKVAccessor().(spanconfig.KVAccessor)
	scReconciler := ts.SpanConfigReconciler().(spanconfig.Reconciler)

	tdb.Exec(t, `SET CLUSTER SETTING spanconfig.reconciliation_job.enabled = true`)
	tdb.Exec(t, `SET CLUSTER SETTING spanconfig.reconciliation_job.checkpoint_interval = '100ms'`)

	{ // Ensure that no span config entries are found.
		entries, err := scKVAccessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{
			keys.EverythingSpan,
		})
		require.NoError(t, err)
		require.Empty(t, entries)
	}

	// Ensure that upgrade attempts without having reconciled simply fail.
	tdb.Exec(t, "SET statement_timeout='500ms'")
	tdb.ExpectErr(t, "query execution canceled due to statement timeout",
		"SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.EnsureSpanConfigReconciliation).String(),
	)

	close(blockReconcilerCh) // unblock the reconciliation process allows the upgrade to proceed
	tdb.ExecSucceedsSoon(t,
		"SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.EnsureSpanConfigReconciliation).String(),
	)
	require.False(t, scReconciler.Checkpoint().IsEmpty())

	{ // Ensure that the host tenant's span configs are installed.
		entries, err := scKVAccessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{
			keys.EverythingSpan,
		})
		require.NoError(t, err)
		require.NotEmpty(t, entries)
	}
}

func TestEnsureSpanConfigReconciliationMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	blockReconcilerCh := make(chan struct{})

	serverArgs := make(map[int]base.TestServerArgs)
	const numNodes = 2
	for i := 0; i < numNodes; i++ {
		var spanConfigKnobs = spanconfig.TestingKnobs{}
		if i == 0 {
			spanConfigKnobs.ManagerDisableJobCreation = true
		} else {
			spanConfigKnobs.ReconcilerInitialInterceptor = func() {
				<-blockReconcilerCh
			}
		}
		serverArgs[i] = base.TestServerArgs{
			EnableSpanConfigs: true,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.EnsureSpanConfigReconciliation - 1,
					),
				},
				SpanConfig: &spanConfigKnobs,
			},
		}
	}
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.EnsureSpanConfigReconciliation - 1,
					),
				},
			},
		},
		ServerArgsPerNode: serverArgs,
	})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	scKVAccessor := ts.SpanConfigKVAccessor().(spanconfig.KVAccessor)
	scReconciler := tc.Server(1).SpanConfigReconciler().(spanconfig.Reconciler)

	tdb.Exec(t, `SET CLUSTER SETTING spanconfig.reconciliation_job.enabled = true`)
	tdb.Exec(t, `SET CLUSTER SETTING spanconfig.reconciliation_job.checkpoint_interval = '100ms'`)

	{ // Ensure that no span config entries are to be found.
		entries, err := scKVAccessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{
			keys.EverythingSpan,
		})
		require.NoError(t, err)
		require.Empty(t, entries)
	}

	// Ensure that upgrade attempts without having reconciled simply fail.
	tdb.Exec(t, "SET statement_timeout='500ms'")
	tdb.ExpectErr(t, "query execution canceled due to statement timeout",
		"SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.EnsureSpanConfigReconciliation).String(),
	)

	close(blockReconcilerCh) // unblock the reconciliation process allows the upgrade to proceed
	tdb.ExecSucceedsSoon(t,
		"SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.EnsureSpanConfigReconciliation).String(),
	)
	require.False(t, scReconciler.Checkpoint().IsEmpty())

	{ // Ensure that the host tenant's span configs are installed.
		entries, err := scKVAccessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{
			keys.EverythingSpan,
		})
		require.NoError(t, err)
		require.NotEmpty(t, entries)
	}
}

// TestEnsureSpanConfigSubscription verifies that the migration waits for all
// stores to have observed a reconciliation state.
func TestEnsureSpanConfigSubscription(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	blockSubscriberCh := make(chan struct{})
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			EnableSpanConfigs: true,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.EnsureSpanConfigSubscription - 1,
					),
				},
				SpanConfig: &spanconfig.TestingKnobs{
					KVSubscriberPostRangefeedStartInterceptor: func() {
						<-blockSubscriberCh
					},
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	scKVAccessor := ts.SpanConfigKVAccessor().(spanconfig.KVAccessor)
	scKVSubscriber := ts.SpanConfigKVSubscriber().(spanconfig.KVSubscriber)

	tdb.Exec(t, `SET CLUSTER SETTING spanconfig.reconciliation_job.enabled = true`)

	testutils.SucceedsSoon(t, func() error {
		entries, err := scKVAccessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{
			keys.EverythingSpan,
		})
		require.NoError(t, err)
		if len(entries) == 0 {
			return fmt.Errorf("empty global span configuration state")
		}
		return nil
	})

	// Ensure that upgrade attempts without having subscribed simply fail.
	tdb.Exec(t, "SET statement_timeout='500ms'")
	tdb.ExpectErr(t, "query execution canceled due to statement timeout",
		"SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.EnsureSpanConfigSubscription).String(),
	)

	// Unblocking the subscription process allows the upgrade to proceed.
	close(blockSubscriberCh)
	tdb.ExecSucceedsSoon(t,
		"SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.EnsureSpanConfigSubscription).String(),
	)
	require.False(t, scKVSubscriber.LastUpdated().IsEmpty())
}
