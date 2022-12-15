// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDecommissionPreCheckEvaluation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t) // can't handle 7-node clusters

	tsArgs := func(attrs ...string) base.TestServerArgs {
		return base.TestServerArgs{
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{
						Key:   "region",
						Value: "a",
					},
				},
			},
			StoreSpecs: []base.StoreSpec{
				{InMemory: true, Attributes: roachpb.Attributes{Attrs: attrs}},
			},
		}
	}

	// Set up test cluster.
	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 7, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: tsArgs("ns1", "origin"),
			1: tsArgs("ns2", "west"),
			2: tsArgs("ns3", "central"),
			3: tsArgs("ns4", "central"),
			4: tsArgs("ns5", "east"),
			5: tsArgs("ns6", "east"),
			6: tsArgs("ns7", "east"),
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Evaluate decommission readiness of several nodes given the replicas that
	// exist on these nodes.
	firstSvr := tc.Server(0).(*TestServer)
	db := tc.ServerConn(0)
	runQueries := func(queries ...string) {
		for _, q := range queries {
			if _, err := db.Exec(q); err != nil {
				t.Fatalf("error executing '%s': %s", q, err)
			}
		}
	}
	// Create database and tables.
	ac := firstSvr.AmbientCtx()
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()
	setupQueries := []string{
		"CREATE DATABASE test",
		"CREATE TABLE test.tblA (val STRING)",
		"CREATE TABLE test.tblB (val STRING)",
		"INSERT INTO test.tblA VALUES ('testvalA')",
		"INSERT INTO test.tblB VALUES ('testvalB')",
	}
	runQueries(setupQueries...)
	alterQueries := []string{
		"ALTER TABLE test.tblA CONFIGURE ZONE USING num_replicas = 3, constraints = '{+west: 1, +central: 1, +east: 1}', " +
			"range_max_bytes = 500000, range_min_bytes = 100",
		"ALTER TABLE test.tblB CONFIGURE ZONE USING num_replicas = 3, constraints = '{+east}', " +
			"range_max_bytes = 500000, range_min_bytes = 100",
	}
	runQueries(alterQueries...)
	tblAID, err := firstSvr.admin.queryTableID(ctx, username.RootUserName(), "test", "tblA")
	require.NoError(t, err)
	tblBID, err := firstSvr.admin.queryTableID(ctx, username.RootUserName(), "test", "tblB")
	require.NoError(t, err)
	startKeyTblA := keys.TODOSQLCodec.TablePrefix(uint32(tblAID))
	startKeyTblB := keys.TODOSQLCodec.TablePrefix(uint32(tblBID))

	// Split off ranges for tblA and tblB.
	_, rDescA, err := firstSvr.SplitRange(startKeyTblA)
	require.NoError(t, err)
	_, rDescB, err := firstSvr.SplitRange(startKeyTblB)
	require.NoError(t, err)

	// Ensure all nodes have the correct span configs for tblA and tblB.
	waitForSpanConfig(t, tc, rDescA.StartKey, 500000)
	waitForSpanConfig(t, tc, rDescB.StartKey, 500000)

	// Transfer tblA to [west, central, east] and tblB to [east].
	tc.AddVotersOrFatal(t, startKeyTblA, tc.Target(1), tc.Target(2), tc.Target(4))
	tc.TransferRangeLeaseOrFatal(t, rDescA, tc.Target(1))
	tc.RemoveVotersOrFatal(t, startKeyTblA, tc.Target(0))
	tc.AddVotersOrFatal(t, startKeyTblB, tc.Target(4), tc.Target(5), tc.Target(6))
	tc.TransferRangeLeaseOrFatal(t, rDescB, tc.Target(4))
	tc.RemoveVotersOrFatal(t, startKeyTblB, tc.Target(0))

	// Validate range distribution.
	rDescA = tc.LookupRangeOrFatal(t, startKeyTblA)
	rDescB = tc.LookupRangeOrFatal(t, startKeyTblB)
	for _, desc := range []roachpb.RangeDescriptor{rDescA, rDescB} {
		require.Lenf(t, desc.Replicas().VoterAndNonVoterDescriptors(), 3, "expected 3 replicas, have %v", desc)
	}

	require.True(t, hasReplicaOnServers(tc, &rDescA, 1, 2, 4))
	require.True(t, hasReplicaOnServers(tc, &rDescB, 4, 5, 6))

	// Evaluate n5 decommission check.
	decommissioningNodeIDs := []roachpb.NodeID{tc.Server(4).NodeID()}
	result, err := firstSvr.DecommissionPreCheck(ctx, decommissioningNodeIDs, true, true, 0)
	require.NoError(t, err)
	require.Equal(t, 2, result.rangesChecked, "unexpected number of ranges checked")
	require.Equalf(t, 2, result.actionCounts[allocatorimpl.AllocatorReplaceDecommissioningVoter],
		"unexpected allocator actions, got %v", result.actionCounts)
	require.Lenf(t, result.rangesNotReady, 1, "unexpected number of unready ranges")

	// Validate error on tblB's range as it requires 3 replicas in "east".
	unreadyResult := result.rangesNotReady[0]
	require.Equalf(t, rDescB.StartKey, unreadyResult.desc.StartKey,
		"expected tblB's range to be unready, got %s", unreadyResult.desc,
	)
	require.Errorf(t, unreadyResult.err, "expected error on %s", unreadyResult.desc)
	require.NotEmptyf(t, unreadyResult.tracingSpans, "expected tracing spans on %s", unreadyResult.desc)
	var allocatorError allocator.AllocationError
	require.ErrorAsf(t, unreadyResult.err, &allocatorError, "expected allocator error on %s", unreadyResult.desc)

	// Evaluate n3 decommission check (not required to satisfy constraints).
	decommissioningNodeIDs = []roachpb.NodeID{tc.Server(2).NodeID()}
	result, err = firstSvr.DecommissionPreCheck(ctx, decommissioningNodeIDs, true, true, 0)
	require.NoError(t, err)
	require.Equal(t, 1, result.rangesChecked, "unexpected number of ranges checked")
	require.Equalf(t, 1, result.actionCounts[allocatorimpl.AllocatorReplaceDecommissioningVoter],
		"unexpected allocator actions, got %v", result.actionCounts)
	require.Lenf(t, result.rangesNotReady, 0, "unexpected number of unready ranges")
}

// hasReplicaOnServers returns true if the range has replicas on given servers.
func hasReplicaOnServers(
	tc serverutils.TestClusterInterface, desc *roachpb.RangeDescriptor, serverIdxs ...int,
) bool {
	hasAll := true
	for _, idx := range serverIdxs {
		hasAll = hasAll && desc.Replicas().HasReplicaOnNode(tc.Server(idx).NodeID())
	}
	return hasAll
}

// waitForSpanConfig waits until all servers in the test cluster have a span
// config for the key with the expected number of max bytes for the range.
func waitForSpanConfig(
	t *testing.T, tc serverutils.TestClusterInterface, key roachpb.RKey, exp int64,
) {
	testutils.SucceedsSoon(t, func() error {
		for i := 0; i < tc.NumServers(); i++ {
			s := tc.Server(i)
			store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
			if err != nil {
				return errors.Wrapf(err, "missing store on server %d", i)
			}
			conf, err := store.GetStoreConfig().SpanConfigSubscriber.GetSpanConfigForKey(context.Background(), key)
			if err != nil {
				return errors.Wrapf(err, "missing span config for %s on server %d", key, i)
			}
			if conf.RangeMaxBytes != exp {
				return errors.Errorf("expected %d max bytes, got %d", exp, conf.RangeMaxBytes)
			}
		}
		return nil
	})
}
