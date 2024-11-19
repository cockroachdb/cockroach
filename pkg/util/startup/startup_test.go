// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package startup_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}

// TestStartupFailure can help reproducing failures found in randomized test as
// it may be tricky to get the same results easily. To reproduce failure, use
// failing range from test and plug in into faultKey.
// As of time of writing following ranges trigger retries:
// - /System/NodeLivenessMax (keys.BootstrapVersionKey)
// - /Table/11 (keys.SystemSQLCodec.TablePrefix(11))
// If startup.IsRetryableReplicaError is changed to return false every time,
// test should fail if range from above is used.
func TestStartupFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.IgnoreLint(t, "this test should be used to reproduce failures from random tests")

	// Set faultyKey to suspected range that is causing startup failure, comment
	// out skip statement above and run the test.
	faultyKey := keys.BootstrapVersionKey
	runCircuitBreakerTestForKey(t,
		func(spans []roachpb.Span) (good, bad []roachpb.Span) {
			for _, span := range spans {
				if span.ContainsKey(faultyKey) {
					bad = append(bad, span)
				} else {
					good = append(good, span)
				}
			}
			return good, bad
		})
}

// TestStartupFailureRandomRange verifies that no range with transient failure
// would cause node startup to fail.
// To achieve this, test picks a random range then relocates replicas of the
// range so that restarting a node would cause this range to lose quorum,
// stops node, ensures circuit breaker is set and starts node back. The node
// should start without raising error (for detailed explanation see doc on
// runCircuitBreakerTestForKey).
// If this test fails, error should indicate which range failed and hopefully
// error will also contain call stack that caused failure. This range could be
// used in TestStartupFailure to investigate code path not covered by startup
// retries.
func TestStartupFailureRandomRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test takes 30s and so we don't want it to run in the "blocking path"
	// of CI at all, and we also don't want to stress it in nightlies as part of
	// a big package (where it will take a lot of time that could be spent running
	// "faster" tests). In this package, it is the only test and so it's fine to
	// run it under nightly (skipping race builds because with many nodes they are
	// very resource intensive and tend to collapse).
	skip.UnderRace(t, "6 nodes with replication is too slow for race")
	if !skip.NightlyStress() {
		skip.IgnoreLint(t, "test takes 30s to run due to circuit breakers and timeouts")
	}

	rng, seed := randutil.NewTestRand()
	t.Log("TestStartupFailureRandomRange using seed", seed)
	runCircuitBreakerTestForKey(t,
		func(spans []roachpb.Span) (good, bad []roachpb.Span) {
			spans = append([]roachpb.Span(nil), spans...) // avoid mutating input slice
			badRange := rng.Intn(len(spans))
			t.Log("triggering loss of quorum on range", spans[badRange].Key.String())
			spans[0], spans[badRange] = spans[badRange], spans[0] // swap badRange to the front
			return spans[1:], spans[:1]                           // good, bad
		})
}

// runCircuitBreakerTestForKey performs node restart in presence of ranges that
// lost their quorum.
// To achieve this, selected range will be placed so that quorum-1 replicas will
// be located on live nodes, one replica would be placed on node under test,
// and remaining replicas would be placed on nodes that would be stopped.
// All remaining ranges would be placed so that live nodes would maintain quorum
// throughout the test.
// Node lifecycle during test:
// 1, 2, 3 - live
// 4, 5    - stopped
// 6       - stopped and restarted
// To ensure that replicas are not moved, all queues and rebalancer are stopped
// once initial replication is done.
// To ensure circuit breaker is engaged on the broken range, test will perform
// a probe write to it after node is stopped.
// After that node is restarted and test verifies that start doesn't raise an
// error.
func runCircuitBreakerTestForKey(
	t *testing.T, faultyRangeSelector func([]roachpb.Span) (good, bad []roachpb.Span),
) {
	const (
		nodes = 6
	)
	ctx := context.Background()

	lReg := listenerutil.NewListenerRegistry()
	defer lReg.Close()
	reg := fs.NewStickyRegistry()

	// TODO: Disable expiration based leases metamorphism since it currently
	// breaks closed timestamps and prevent rangefeeds from advancing checkpoint
	// which used by TestCluster for zone config propagation tracking.
	st := cluster.MakeTestingClusterSettings()
	kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, false)

	args := base.TestClusterArgs{
		ServerArgsPerNode:   make(map[int]base.TestServerArgs),
		ReusableListenerReg: lReg,
	}
	var enableFaults atomic.Bool
	for i := 0; i < nodes; i++ {
		a := base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: reg,
				},
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
			},
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Listener: lReg.MustGetOrCreate(t, i),
		}
		args.ServerArgsPerNode[i] = a
	}
	tc := testcluster.NewTestCluster(t, nodes, args)
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitFor5NodeReplication(), "failed to succeed 5x replication")

	tc.ToggleReplicateQueues(false)
	c := tc.Server(0).SystemLayer().SQLConn(t)
	_, err := c.ExecContext(ctx, "set cluster setting kv.allocator.load_based_rebalancing='off'")
	require.NoError(t, err, "failed to disable load rebalancer")
	_, err = c.ExecContext(ctx,
		fmt.Sprintf("set cluster setting kv.replica_circuit_breaker.slow_replication_threshold='%s'",
			base.SlowRequestThreshold.String()))
	require.NoError(t, err, "failed to lower circuit breaker threshold")

	// replicaTargets holds template for replica placements for available and
	// unavailable range configurations.
	type replicaTargets struct {
		safe, unsafe []roachpb.StoreID
	}

	// Templates define replica placements different replication factors.
	// Safe configuration ensures that replicas will maintain quorum when nodes
	// are stopped while unsafe will lose quorum.
	t3repl := replicaTargets{
		safe:   []roachpb.StoreID{1, 2, 3},
		unsafe: []roachpb.StoreID{1, 4, 6},
	}
	t5repl := replicaTargets{
		safe:   []roachpb.StoreID{1, 2, 3, 4, 5},
		unsafe: []roachpb.StoreID{1, 2, 4, 5, 6},
	}
	db := tc.Server(0).DB()
	prepRange := func(rk roachpb.Key, fail bool) roachpb.RKey {
		d := tc.LookupRangeOrFatal(t, rk)
		replicaTemplate := t3repl
		if len(d.InternalReplicas) > 3 {
			replicaTemplate = t5repl
		}
		targets := replicaTemplate.safe
		if fail {
			targets = replicaTemplate.unsafe
		}
		var voters []roachpb.ReplicationTarget
		for _, storeID := range targets {
			voters = append(voters, roachpb.ReplicationTarget{
				NodeID:  roachpb.NodeID(storeID),
				StoreID: storeID,
			})
		}
		for {
			err := db.AdminRelocateRange(ctx, rk, voters, nil, true)
			if err == nil {
				break
			}
		}
		return d.StartKey
	}

	var rangeSpans []roachpb.Span
	r, err := c.QueryContext(ctx, "select range_id, start_key, end_key from crdb_internal.ranges_no_leases order by start_key")
	require.NoError(t, err, "failed to query ranges")
	for r.Next() {
		var rangeID int
		var key roachpb.Key
		var endKey roachpb.Key
		require.NoError(t, r.Scan(&rangeID, &key, &endKey), "failed to scan range data from query")
		rangeSpans = append(rangeSpans, roachpb.Span{
			Key:    key,
			EndKey: endKey,
		})
	}
	good, bad := faultyRangeSelector(rangeSpans)
	for _, span := range good {
		prepRange(span.Key, false)
	}
	var ranges []string
	for _, span := range bad {
		prepRange(span.Key, true)
		ranges = append(ranges, span.String())
	}
	rangesList := fmt.Sprintf("[%s]", strings.Join(ranges, ", "))

	// Remove nodes permanently to only leave quorum on planned ranges.
	tc.StopServer(3)
	tc.StopServer(4)

	// Stop node with replicas that would leave ranges without quorum.
	tc.StopServer(5)

	// Probe compromised ranges to trigger circuit breakers on them. If we don't
	// do this, then restart queries will wait for quorum to be reestablished with
	// restarting node without failing.
	var wg sync.WaitGroup
	wg.Add(len(bad))
	for _, span := range bad {
		go func(key roachpb.Key) {
			defer wg.Done()
			_ = db.Put(context.Background(), keys.RangeProbeKey(roachpb.RKey(key)), "")
		}(span.Key)
	}
	wg.Wait()

	// Restart node and check that it succeeds in reestablishing range quorum
	// necessary for startup actions.
	require.NoError(t, lReg.MustGet(t, 5).Reopen())
	err = tc.RestartServer(5)
	require.NoError(t, err, "restarting server with range(s) %s tripping circuit breaker", rangesList)

	// Disable faults to make it easier for cluster to stop.
	enableFaults.Store(false)
}
