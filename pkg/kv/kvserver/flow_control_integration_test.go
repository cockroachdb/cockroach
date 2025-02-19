// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"github.com/stretchr/testify/require"
)

// TestFlowControlBasicV2 runs a basic end-to-end test of the v2 kvflowcontrol
// machinery, replicating + admitting a single 1MiB write. The vmodule
// flags for running these tests with full logging are:
//
//	--vmodule='replica_raft=1,replica_proposal_buf=1,raft_transport=2,
//	           kvadmission=1,work_queue=1,replica_flow_control=1,
//	           tracker=1,client_raft_helpers_test=1,range_controller=2,
//	           token_counter=2,token_tracker=2,processor=2'
func TestFlowControlBasicV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "v2_enabled_when_leader_level", []kvflowcontrol.V2EnabledWhenLeaderLevel{
		kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding,
	}, func(t *testing.T, v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel) {
		testutils.RunValues(t, "kvadmission.flow_control.mode", []kvflowcontrol.ModeT{
			kvflowcontrol.ApplyToElastic,
			kvflowcontrol.ApplyToAll,
		}, func(t *testing.T, mode kvflowcontrol.ModeT) {
			testutils.RunTrueAndFalse(t, "always-enqueue", func(t *testing.T, alwaysEnqueue bool) {
				ctx := context.Background()
				settings := cluster.MakeTestingClusterSettings()
				tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
					ReplicationMode: base.ReplicationManual,
					ServerArgs: base.TestServerArgs{
						Settings: settings,
						Knobs: base.TestingKnobs{
							Store: &kvserver.StoreTestingKnobs{
								FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
									UseOnlyForScratchRanges: true,
									OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
										return v2EnabledWhenLeaderLevel
									},
									OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
										// This test asserts on the exact values of tracked tokens. In
										// non-test code, the tokens deducted are a few bytes off (give
										// or take) from the size of the proposals. We don't care about
										// such differences.
										return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
									},
								},
							},
							AdmissionControl: &admission.TestingKnobs{
								DisableWorkQueueFastPath: alwaysEnqueue,
							},
						},
					},
				})
				defer tc.Stopper().Stop(ctx)

				// Setup the test state with 3 voters, one on each of the three
				// node/stores.
				k := tc.ScratchRange(t)
				tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)
				h := newFlowControlTestHelperV2(t, tc, v2EnabledWhenLeaderLevel)
				h.init(mode)
				defer h.close(makeV2EnabledTestFileName(v2EnabledWhenLeaderLevel, mode, "basic"))

				desc, err := tc.LookupRange(k)
				require.NoError(t, err)
				h.enableVerboseRaftMsgLoggingForRange(desc.RangeID)
				n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

				h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
				// Reset the token metrics, since a send queue may have instantly
				// formed when adding one of the replicas, before being quickly
				// drained.
				h.resetV2TokenMetrics(ctx)

				h.comment(`-- Flow token metrics, before issuing the 1MiB replicated write.`)
				h.query(n1, v2FlowTokensQueryStr)

				h.comment(`-- (Issuing + admitting a 1MiB, triply replicated write...)`)
				h.log("sending put request")
				h.put(ctx, k, 1, testFlowModeToPri(mode))
				h.log("sent put request")

				h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)
				h.comment(`
-- Stream counts as seen by n1 post-write. We should see three {regular,elastic}
-- streams given there are three nodes and we're using a replication factor of
-- three.
`)
				h.query(n1, `
  SELECT name, value
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvflowcontrol%stream%'
ORDER BY name ASC;
`)

				h.comment(`-- Another view of the stream count, using /inspectz-backed vtables.`)
				h.query(n1, v2FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

				h.comment(`
-- Flow token metrics from n1 after issuing the 1MiB replicated write,
-- and it being admitted on n1, n2 and n3. We should see 3*1MiB = 3MiB of
-- {regular,elastic} tokens deducted and returned, and {8*3=24MiB,16*3=48MiB} of
-- {regular,elastic} tokens available. Everything should be accounted for.
`)
				h.query(n1, v2FlowTokensQueryStr)
			})
		})
	})
}

// TestFlowControlRangeSplitMergeV2 walks through what happens to flow tokens
// when a range splits/merges.
func TestFlowControlRangeSplitMergeV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// TODO(pav-kv): remove when flakes are fixed.
	defer setRACv2DebugVModule(t)()

	testutils.RunValues(t, "v2_enabled_when_leader_level", []kvflowcontrol.V2EnabledWhenLeaderLevel{
		kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding,
	}, func(t *testing.T, v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel) {
		testutils.RunValues(t, "kvadmission.flow_control.mode", []kvflowcontrol.ModeT{
			kvflowcontrol.ApplyToElastic,
			kvflowcontrol.ApplyToAll,
		}, func(t *testing.T, mode kvflowcontrol.ModeT) {
			ctx := context.Background()
			settings := cluster.MakeTestingClusterSettings()
			tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: settings,
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
								UseOnlyForScratchRanges: true,
								OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
									return v2EnabledWhenLeaderLevel
								},
								OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
									// This test asserts on the exact values of tracked tokens. In
									// non-test code, the tokens deducted are a few bytes off (give
									// or take) from the size of the proposals. We don't care about
									// such differences.
									return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
								},
							},
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			k := tc.ScratchRange(t)
			tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

			n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

			h := newFlowControlTestHelperV2(t, tc, v2EnabledWhenLeaderLevel)
			h.init(mode)
			defer h.close(makeV2EnabledTestFileName(v2EnabledWhenLeaderLevel, mode, "split_merge"))

			desc, err := tc.LookupRange(k)
			require.NoError(t, err)
			h.enableVerboseRaftMsgLoggingForRange(desc.RangeID)
			// NB: There will be no other splits and rangeIDs are allocated
			// sequentially.
			h.enableVerboseRaftMsgLoggingForRange(desc.RangeID + 1)

			h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
			// Reset the token metrics, since a send queue may have instantly
			// formed when adding one of the replicas, before being quickly
			// drained.
			h.resetV2TokenMetrics(ctx)
			h.comment("(Sending 1 MiB put request to pre-split range.)")
			h.put(ctx, k, 1, testFlowModeToPri(mode))
			h.comment("(Sent 1 MiB put request to pre-split range.)")

			h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)
			h.comment(`
-- Flow token metrics from n1 after issuing + admitting the 1MiB 3x
-- replicated write to the pre-split range. There should be 3MiB of
-- {regular,elastic} tokens {deducted,returned}.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- (Splitting range.)`)
			left, right := tc.SplitRangeOrFatal(t, k.Next())
			h.waitForConnectedStreams(ctx, right.RangeID, 3, 0 /* serverIdx */)

			h.comment("(Sending 2 x 1 MiB put request to post-split LHS range.)")
			h.put(ctx, k, 1, testFlowModeToPri(mode))
			h.put(ctx, k, 1, testFlowModeToPri(mode))
			h.comment("(Sent 2 x 1 MiB put request to post-split LHS range.)")

			h.comment("(Sending 3 x 1 MiB put request to post-split RHS range.)")
			h.put(ctx, roachpb.Key(right.StartKey), 1, testFlowModeToPri(mode))
			h.put(ctx, roachpb.Key(right.StartKey), 1, testFlowModeToPri(mode))
			h.put(ctx, roachpb.Key(right.StartKey), 1, testFlowModeToPri(mode))
			h.comment("(Sent 3 x 1 MiB put request to post-split RHS range.)")

			h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)
			h.comment(`
-- Flow token metrics from n1 after further issuing 2MiB and 3MiB writes to
-- post-split LHS and RHS ranges respectively. We should see 15MiB extra tokens
-- {deducted,returned}, which comes from (2MiB+3MiB)*3=15MiB. So we stand at
-- 3MiB+15MiB=18MiB now.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- Observe the newly split off replica, with its own three streams.`)
			h.query(n1, v2FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

			h.comment(`-- (Merging ranges.)`)
			merged := tc.MergeRangesOrFatal(t, left.StartKey.AsRawKey())

			h.comment("(Sending 4 x 1 MiB put request to post-merge range.)")
			h.put(ctx, roachpb.Key(merged.StartKey), 1, testFlowModeToPri(mode))
			h.put(ctx, roachpb.Key(merged.StartKey), 1, testFlowModeToPri(mode))
			h.put(ctx, roachpb.Key(merged.StartKey), 1, testFlowModeToPri(mode))
			h.put(ctx, roachpb.Key(merged.StartKey), 1, testFlowModeToPri(mode))
			h.comment("(Sent 4 x 1 MiB put request to post-merge range.)")

			h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)
			h.comment(`
-- Flow token metrics from n1 after issuing 4MiB of replicated writes to
-- the post-merged range. We should see 12MiB extra tokens {deducted,returned},
-- which comes from 4MiB*3=12MiB. So we stand at 18MiB+12MiB=30MiB now.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- Observe only the merged replica with its own three streams.`)
			h.query(n1, v2FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)
		})
	})
}

// TestFlowControlBlockedAdmissionV2 tests token tracking behavior by explicitly
// blocking below-raft admission.
func TestFlowControlBlockedAdmissionV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "v2_enabled_when_leader_level", []kvflowcontrol.V2EnabledWhenLeaderLevel{
		kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding,
	}, func(t *testing.T, v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel) {
		testutils.RunValues(t, "kvadmission.flow_control.mode", []kvflowcontrol.ModeT{
			kvflowcontrol.ApplyToElastic,
			kvflowcontrol.ApplyToAll,
		}, func(t *testing.T, mode kvflowcontrol.ModeT) {
			ctx := context.Background()
			var disableWorkQueueGranting atomic.Bool
			disableWorkQueueGranting.Store(true)
			settings := cluster.MakeTestingClusterSettings()
			tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: settings,
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
								UseOnlyForScratchRanges: true,
								OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
									return v2EnabledWhenLeaderLevel
								},
								OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
									// This test asserts on the exact values of tracked tokens. In
									// non-test code, the tokens deducted are a few bytes off (give
									// or take) from the size of the proposals. We don't care about
									// such differences.
									return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
								},
							},
						},
						AdmissionControl: &admission.TestingKnobs{
							DisableWorkQueueFastPath: true,
							DisableWorkQueueGranting: func() bool {
								return disableWorkQueueGranting.Load()
							},
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			k := tc.ScratchRange(t)
			tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

			n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

			h := newFlowControlTestHelperV2(t, tc, v2EnabledWhenLeaderLevel)
			h.init(mode)
			defer h.close(makeV2EnabledTestFileName(v2EnabledWhenLeaderLevel, mode, "blocked_admission"))

			desc, err := tc.LookupRange(k)
			require.NoError(t, err)
			h.enableVerboseRaftMsgLoggingForRange(desc.RangeID)
			h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
			// Reset the token metrics, since a send queue may have instantly
			// formed when adding one of the replicas, before being quickly
			// drained.
			h.resetV2TokenMetrics(ctx)

			h.comment(`-- (Issuing 5 1MiB, 3x replicated write that's not admitted.)`)
			h.log("sending put requests")
			for i := 0; i < 5; i++ {
				h.put(ctx, k, 1, testFlowModeToPri(mode))
			}
			h.log("sent put requests")

			h.comment(`
-- Flow token metrics from n1 after issuing 5 1MiB 3x replicated writes
-- that are yet to get admitted. We see 5*1MiB*3=15MiB deductions of
-- {regular,elastic} tokens with no corresponding returns.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- Observe the total tracked tokens per-stream on n1.`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

			h.comment(`-- Observe the individual tracked tokens per-stream on the scratch range.`)
			h.query(n1, v2FlowPerStoreDeductionQueryStr, flowPerStoreDeductionQueryHeaderStrs...)

			h.comment(`-- (Allow below-raft admission to proceed.)`)
			disableWorkQueueGranting.Store(false)
			h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */) // wait for admission

			h.comment(`
-- Flow token metrics from n1 after work gets admitted. We see 15MiB returns of
-- {regular,elastic} tokens, and the available capacities going back to what
-- they were.
`)
			h.query(n1, v2FlowTokensQueryStr)
		})
	})
}

// TestFlowControlAdmissionPostSplitMergeV2 walks through what happens with flow
// tokens when a range after undergoes splits/merges. It does this by blocking
// and later unblocking below-raft admission, verifying:
// - tokens for the RHS are released at the post-merge subsuming leaseholder,
// - admission for the RHS post-merge does not cause a double return of tokens,
// - admission for the LHS can happen post-merge,
// - admission for the LHS and RHS can happen post-split.
func TestFlowControlAdmissionPostSplitMergeV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// TODO(pav-kv): remove when flakes are fixed.
	defer setRACv2DebugVModule(t)()

	testutils.RunValues(t, "v2_enabled_when_leader_level", []kvflowcontrol.V2EnabledWhenLeaderLevel{
		kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding,
	}, func(t *testing.T, v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel) {
		testutils.RunValues(t, "kvadmission.flow_control.mode", []kvflowcontrol.ModeT{
			kvflowcontrol.ApplyToElastic,
			kvflowcontrol.ApplyToAll,
		}, func(t *testing.T, mode kvflowcontrol.ModeT) {
			ctx := context.Background()
			var disableWorkQueueGranting atomic.Bool
			disableWorkQueueGranting.Store(true)
			settings := cluster.MakeTestingClusterSettings()
			// This test doesn't want leadership changing hands, and leader leases (by
			// virtue of raft fortification) help ensure this. Override to disable any
			// metamorphosis.
			kvserver.OverrideDefaultLeaseType(ctx, &settings.SV, roachpb.LeaseLeader)
			// Using a manual clock here ensures that StoreLiveness support, once
			// established, never expires. By extension, leadership should stay
			// sticky.
			manualClock := hlc.NewHybridManualClock()
			tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							WallClock: manualClock,
						},
						Store: &kvserver.StoreTestingKnobs{
							FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
								UseOnlyForScratchRanges: true,
								OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
									return v2EnabledWhenLeaderLevel
								},
								OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
									// This test asserts on the exact values of tracked tokens. In
									// non-test code, the tokens deducted are a few bytes off (give
									// or take) from the size of the proposals. We don't care about
									// such differences.
									return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
								},
							},
						},
						AdmissionControl: &admission.TestingKnobs{
							DisableWorkQueueFastPath: true,
							DisableWorkQueueGranting: func() bool {
								return disableWorkQueueGranting.Load()
							},
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			k := tc.ScratchRange(t)
			tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

			n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

			h := newFlowControlTestHelperV2(t, tc, v2EnabledWhenLeaderLevel)
			h.init(mode)
			defer h.close(makeV2EnabledTestFileName(v2EnabledWhenLeaderLevel, mode, "admission_post_split_merge"))

			desc, err := tc.LookupRange(k)
			require.NoError(t, err)
			h.enableVerboseRaftMsgLoggingForRange(desc.RangeID)
			// NB: There will be no other splits and rangeIDs are allocated
			// sequentially.
			h.enableVerboseRaftMsgLoggingForRange(desc.RangeID + 1)

			h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
			// Reset the token metrics, since a send queue may have instantly
			// formed when adding one of the replicas, before being quickly
			// drained.
			h.resetV2TokenMetrics(ctx)

			h.comment(`(Sending 2 x 1 MiB put request to pre-split range)`)
			h.put(ctx, k, 1, testFlowModeToPri(mode))
			h.put(ctx, k.Next(), 1, testFlowModeToPri(mode))
			h.comment(`(Sent 2 x 1 MiB put request to pre-split range)`)

			h.comment(`
-- Flow token metrics from n1 after issuing a 2*1MiB 3x replicated write
-- that are yet to get admitted. We see 2*3*1MiB=6MiB deductions of
-- {regular,elastic} tokens with no corresponding returns. The 2*1MiB writes
-- happened on what is soon going to be the LHS and RHS of a range being split.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- (Splitting range.)`)
			left, right := tc.SplitRangeOrFatal(t, k.Next())
			h.waitForConnectedStreams(ctx, right.RangeID, 3, 0 /* serverIdx */)

			h.comment(`(Sending 2 x 1 MiB put request to post-split LHS range)`)
			h.put(ctx, k, 1, testFlowModeToPri(mode))
			h.put(ctx, k, 1, testFlowModeToPri(mode))
			h.comment(`(Sent 2 x 1 MiB put request to post-split LHS range)`)

			h.comment(`(Sending 3 x 1 MiB put request to post-split RHS range)`)
			h.put(ctx, roachpb.Key(right.StartKey), 1, testFlowModeToPri(mode))
			h.put(ctx, roachpb.Key(right.StartKey), 1, testFlowModeToPri(mode))
			h.put(ctx, roachpb.Key(right.StartKey), 1, testFlowModeToPri(mode))
			h.comment(`(Sent 3 x 1 MiB put request to post-split RHS range)`)

			h.comment(`
-- Flow token metrics from n1 after further issuing 2MiB and 3MiB writes to
-- post-split LHS and RHS ranges respectively. We should see 15MiB extra tokens
-- deducted which comes from (2MiB+3MiB)*3=15MiB. So we stand at
-- 6MiB+15MiB=21MiB now.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- Observe the newly split off replica, with its own three streams.`)
			h.query(n1, v2FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

			h.comment(`-- (Merging ranges.)`)
			merged := tc.MergeRangesOrFatal(t, left.StartKey.AsRawKey())

			h.comment(`(Sending 4 x 1 MiB put request to post-merge range)`)
			h.put(ctx, roachpb.Key(merged.StartKey), 1, testFlowModeToPri(mode))
			h.put(ctx, roachpb.Key(merged.StartKey), 1, testFlowModeToPri(mode))
			h.put(ctx, roachpb.Key(merged.StartKey), 1, testFlowModeToPri(mode))
			h.put(ctx, roachpb.Key(merged.StartKey), 1, testFlowModeToPri(mode))
			h.comment(`(Sent 4 x 1 MiB put request to post-merge range)`)

			h.comment(`
-- Flow token metrics from n1 after issuing 4MiB of replicated writes to
-- the post-merged range. We should see 12MiB extra tokens deducted which comes
-- from 4MiB*3=12MiB. So we stand at 21MiB+12MiB=33MiB tokens deducted now. The
-- RHS of the range is gone now, and the previously 3*3MiB=9MiB of tokens
-- deducted for it are released at the subsuming LHS leaseholder.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- Observe only the merged replica with its own three streams.`)
			h.query(n1, v2FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

			h.comment(`-- (Allow below-raft admission to proceed.)`)
			disableWorkQueueGranting.Store(false)
			h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */) // wait for admission

			h.comment(`
-- Flow token metrics from n1 after work gets admitted. We see all outstanding
-- {regular,elastic} tokens returned, including those from:
-- - the LHS before the merge, and
-- - the LHS and RHS before the original split.
`)
			h.query(n1, v2FlowTokensQueryStr)
		})
	})
}

// TestFlowControlCrashedNodeV2 tests flow token behavior in the presence of
// crashed nodes.
func TestFlowControlCrashedNodeV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "v2_enabled_when_leader_level", []kvflowcontrol.V2EnabledWhenLeaderLevel{
		kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding,
	}, func(t *testing.T, v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel) {
		testutils.RunValues(t, "kvadmission.flow_control.mode", []kvflowcontrol.ModeT{
			kvflowcontrol.ApplyToElastic,
			kvflowcontrol.ApplyToAll,
		}, func(t *testing.T, mode kvflowcontrol.ModeT) {
			ctx := context.Background()
			settings := cluster.MakeTestingClusterSettings()
			// This test doesn't want leadership changing hands, and leader leases (by
			// virtue of raft fortification) help ensure this. Override to disable any
			// metamorphosis.
			kvserver.OverrideDefaultLeaseType(ctx, &settings.SV, roachpb.LeaseLeader)
			tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: settings,
					RaftConfig: base.RaftConfig{
						// Reduce the RangeLeaseDuration to speeds up failure detection
						// below.
						RangeLeaseDuration: time.Second,
					},
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
								UseOnlyForScratchRanges: true,
								OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
									return v2EnabledWhenLeaderLevel
								},
								OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
									// This test asserts on the exact values of tracked tokens. In
									// non-test code, the tokens deducted are a few bytes off (give
									// or take) from the size of the proposals. We don't care about
									// such differences.
									return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
								},
							},
						},
						AdmissionControl: &admission.TestingKnobs{
							DisableWorkQueueFastPath: true,
							DisableWorkQueueGranting: func() bool {
								return true
							},
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			k := tc.ScratchRange(t)
			tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

			n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

			h := newFlowControlTestHelperV2(t, tc, v2EnabledWhenLeaderLevel)
			h.init(mode)
			defer h.close(makeV2EnabledTestFileName(v2EnabledWhenLeaderLevel, mode, "crashed_node"))

			desc, err := tc.LookupRange(k)
			require.NoError(t, err)
			tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(0))
			h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
			// Reset the token metrics, since a send queue may have instantly
			// formed when adding one of the replicas, before being quickly
			// drained.
			h.resetV2TokenMetrics(ctx)

			h.comment(`-- (Issuing 5x1MiB, 3x replicated writes that are not admitted.)`)
			h.log("sending put requests")
			for i := 0; i < 5; i++ {
				h.put(ctx, k, 1, testFlowModeToPri(mode))
			}
			h.log("sent put requests")

			h.comment(`
-- Flow token metrics from n1 after issuing 5 1MiB 3x replicated writes
-- that are yet to get admitted. We see 5*1MiB*3=15MiB deductions of
-- {regular,elastic} tokens with no corresponding returns.
`)
			h.query(n1, v2FlowTokensQueryStr)
			h.comment(`-- Observe the per-stream tracked tokens on n1, before n2 is crashed.`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

			h.comment(`-- (Crashing n2)`)
			tc.StopServer(1)
			h.waitForConnectedStreams(ctx, desc.RangeID, 2, 0 /* serverIdx */)

			h.comment(`
-- Observe the per-stream tracked tokens on n1, after n2 crashed. We're no
-- longer tracking the 5MiB held by n2.
`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

			h.comment(`
-- Flow token metrics from n1 after n2 crashed. Observe that we've returned the
-- 5MiB previously held by n2.
`)
			h.query(n1, v2FlowTokensQueryStr)
		})
	})
}

// TestFlowControlRaftSnapshotV2 tests flow token behavior when one replica
// needs to be caught up via raft snapshot.
func TestFlowControlRaftSnapshotV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// TODO(#138103): remove when flakes are fixed.
	defer setRACv2DebugVModule(t)()

	const numServers int = 5

	testutils.RunValues(t, "v2_enabled_when_leader_level", []kvflowcontrol.V2EnabledWhenLeaderLevel{
		kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding,
	}, func(t *testing.T, v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel) {
		testutils.RunValues(t, "kvadmission.flow_control.mode", []kvflowcontrol.ModeT{
			kvflowcontrol.ApplyToElastic,
			kvflowcontrol.ApplyToAll,
		}, func(t *testing.T, mode kvflowcontrol.ModeT) {
			stickyServerArgs := make(map[int]base.TestServerArgs)
			var disableWorkQueueGranting atomic.Bool
			disableWorkQueueGranting.Store(true)
			var bypassReplicaUnreachable atomic.Bool
			bypassReplicaUnreachable.Store(false)
			ctx := context.Background()
			settings := cluster.MakeTestingClusterSettings()
			// This test doesn't want leadership changing hands, and leader leases (by
			// virtue of raft fortification) help ensure this. Override to disable any
			// metamorphosis.
			kvserver.OverrideDefaultLeaseType(ctx, &settings.SV, roachpb.LeaseLeader)
			// Using a manual clock here ensures that StoreLiveness support, once
			// established, never expires. By extension, leadership should stay
			// sticky.
			manualClock := hlc.NewHybridManualClock()
			for i := 0; i < numServers; i++ {
				stickyServerArgs[i] = base.TestServerArgs{
					Settings: settings,
					StoreSpecs: []base.StoreSpec{
						{
							InMemory:    true,
							StickyVFSID: strconv.FormatInt(int64(i), 10),
						},
					},
					Knobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							StickyVFSRegistry: fs.NewStickyRegistry(),
							WallClock:         manualClock,
						},
						Store: &kvserver.StoreTestingKnobs{
							RaftReportUnreachableBypass: func(_ roachpb.ReplicaID) bool {
								// This test is going to crash nodes, then truncate the raft log
								// and assert that tokens are returned upon an replica entering
								// StateSnapshot. To avoid the stopped replicas entering
								// StateProbe returning tokens, we disable reporting a replica
								// as unreachable while nodes are down.
								return bypassReplicaUnreachable.Load()
							},
							FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
								UseOnlyForScratchRanges: true,
								OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
									return v2EnabledWhenLeaderLevel
								},
								OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
									// This test makes use of (small) increment
									// requests, but wants to see large token
									// deductions/returns.
									return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
								},
							},
						},
						AdmissionControl: &admission.TestingKnobs{
							DisableWorkQueueFastPath: true,
							DisableWorkQueueGranting: func() bool {
								return disableWorkQueueGranting.Load()
							},
						},
						RaftTransport: &kvserver.RaftTransportTestingKnobs{
							OverrideIdleTimeout: func() time.Duration {
								// Effectively disable token returns due to underlying
								// raft transport streams disconnecting due to
								// inactivity.
								return time.Hour
							},
						},
					},
				}
			}

			tc := testcluster.StartTestCluster(t, numServers,
				base.TestClusterArgs{
					ReplicationMode:   base.ReplicationManual,
					ServerArgsPerNode: stickyServerArgs,
				})
			defer tc.Stopper().Stop(ctx)

			n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))
			h := newFlowControlTestHelperV2(t, tc, v2EnabledWhenLeaderLevel)
			h.init(mode)
			defer h.close(makeV2EnabledTestFileName(v2EnabledWhenLeaderLevel, mode, "raft_snapshot"))

			store := tc.GetFirstStoreFromServer(t, 0)

			incA := int64(5)
			incB := int64(7)
			incAB := incA + incB

			k := tc.ScratchRange(t)
			tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

			tc.AddVotersOrFatal(t, k, tc.Targets(3, 4)...)
			repl := store.LookupReplica(roachpb.RKey(k))
			require.NotNil(t, repl)
			h.enableVerboseRaftMsgLoggingForRange(repl.Desc().RangeID)
			h.waitForConnectedStreams(ctx, repl.RangeID, 5, 0 /* serverIdx */)
			// Reset the token metrics, since a send queue may have instantly
			// formed when adding one of the replicas, before being quickly
			// drained.
			h.resetV2TokenMetrics(ctx)

			// Set up a key to replicate across the cluster. We're going to modify this
			// key and truncate the raft logs from that command after killing one of the
			// nodes to check that it gets the new value after it comes up.
			incArgs := incrementArgs(k, incA)
			if _, err := kv.SendWrappedWithAdmission(ctx, tc.Server(0).DB().NonTransactionalSender(), kvpb.Header{}, kvpb.AdmissionHeader{
				Priority: int32(testFlowModeToPri(mode)),
				Source:   kvpb.AdmissionHeader_FROM_SQL,
			}, incArgs); err != nil {
				t.Fatal(err)
			}
			// We don't need to assert that the tokens are tracked, but doing so
			// will make debugging this test failure easier.
			h.waitForTotalTrackedTokens(ctx, repl.RangeID, 5<<20 /* 5 MiB */, 0 /* serverIdx */)
			h.waitForAllTokensAvailable(ctx, 5, 0 /* serverIdx */, h.tokensAvailableLimitWithDelta(tokensAvailableDeltaModeEnabled(
				mode,
				v2EnabledWhenLeaderLevel,
				-(1<<20), /* 1 MiB */
			)))

			h.comment(`
-- Flow token metrics from n1 after issuing 1 1MiB 5x replicated write
-- that's not admitted. Since this test is ignoring crashed nodes for token
-- deduction purposes, we see a deduction of 5MiB tokens.
	`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`
-- Observe the total tracked tokens per-stream on n1. 1MiB is tracked for n1-n5.
	`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

			tc.WaitForValues(t, k, []int64{incA, incA, incA, incA, incA})

			h.comment(`
-- (Killing n2 and n3, but preventing their tokens from being returned +
-- artificially allowing tokens to get deducted.)`)

			// Kill stores 1 + 2, increment the key on the other stores and truncate
			// their logs to make sure that when store 1 + 2 comes back up they will
			// require a snapshot from Raft.
			//
			// Also prevent replicas on the killed nodes from being marked as
			// unreachable, in order to prevent them from returning tokens via
			// entering StateProbe, before we're able to truncate the log and assert
			// on the snapshot behavior.
			bypassReplicaUnreachable.Store(true)
			tc.StopServer(1)
			tc.StopServer(2)

			h.comment(`
-- Observe the total tracked tokens per-stream on n1. 1MiB is (still) tracked
-- for n1-n5, because they are not in StateSnapshot yet and have likely been
-- in StateProbe for less than the close timer.
	`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

			h.comment(`
-- (Issuing another 1MiB of 5x replicated writes while n2 and n3 are down and
-- below-raft admission is paused.)
`)
			incArgs = incrementArgs(k, incB)
			if _, err := kv.SendWrappedWithAdmission(ctx, tc.Server(0).DB().NonTransactionalSender(), kvpb.Header{}, kvpb.AdmissionHeader{
				Priority: int32(testFlowModeToPri(mode)),
				Source:   kvpb.AdmissionHeader_FROM_SQL,
			}, incArgs); err != nil {
				t.Fatal(err)
			}

			h.comment(`
-- Flow token metrics from n1 after issuing 1 1MiB 5x replicated write
-- that's not admitted. We'll have deducted another 5*1MiB=5MiB worth of tokens.
	`)
			h.query(n1, v2FlowTokensQueryStr)
			h.comment(`
-- Observe the total tracked tokens per-stream on n1. 2MiB is tracked for n1-n5;
-- see last comment for an explanation why we're still deducting for n2, n3.
`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

			tc.WaitForValues(t, k, []int64{incAB, 0 /* stopped */, 0 /* stopped */, incAB, incAB})

			index := repl.GetLastIndex()
			h.comment(`-- (Truncating raft log.)`)

			// Truncate the log at index+1 (log entries < N are removed, so this
			// includes the increment).
			truncArgs := truncateLogArgs(index+1, repl.GetRangeID())
			if _, err := kv.SendWrappedWithAdmission(ctx, tc.Server(0).DB().NonTransactionalSender(), kvpb.Header{}, kvpb.AdmissionHeader{
				Priority: int32(testFlowModeToPri(mode)),
				Source:   kvpb.AdmissionHeader_FROM_SQL,
			}, truncArgs); err != nil {
				t.Fatal(err)
			}

			h.comment(`-- (Restarting n2 and n3.)`)
			require.NoError(t, tc.RestartServer(1))
			require.NoError(t, tc.RestartServer(2))
			bypassReplicaUnreachable.Store(false)

			tc.WaitForValues(t, k, []int64{incAB, incAB, incAB, incAB, incAB})

			h.comment(`
-- Flow token metrics from n1 after restarting n2 and n3. We've returned the
-- 2MiB previously held by those nodes (2MiB each). We're reacting to it's raft
-- progress state, noting that since we've truncated our log, we need to catch
-- it up via snapshot. So we release all held tokens.
		`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`
-- Observe the total tracked tokens per-stream on n1. There's nothing tracked
-- for n2 and n3 anymore.
`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

			h.waitForConnectedStreams(ctx, repl.RangeID, 5, 0 /* serverIdx */)
			h.comment(`-- (Allow below-raft admission to proceed.)`)
			disableWorkQueueGranting.Store(false)

			h.waitForAllTokensReturned(ctx, 5, 0 /* serverIdx */)

			h.comment(`
-- Flow token metrics from n1 after work gets admitted. We see the remaining
-- 6MiB of tokens returned.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`
-- Observe the total tracked tokens per-stream on n1; there should be nothing.
`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

			h.comment(`-- Another view of tokens, using /inspectz-backed vtables.`)
			h.query(n1, `
SELECT store_id,
	   crdb_internal.humanize_bytes(available_eval_regular_tokens),
	   crdb_internal.humanize_bytes(available_eval_elastic_tokens)
  FROM crdb_internal.kv_flow_controller_v2
 ORDER BY store_id ASC;
`, "store_id", "eval_regular_available", "eval_elastic_available")
		})
	})
}

// TestFlowControlRaftMembershipV2 tests flow token behavior when the raft
// membership changes.
func TestFlowControlRaftMembershipV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "v2_enabled_when_leader_level", []kvflowcontrol.V2EnabledWhenLeaderLevel{
		kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding,
	}, func(t *testing.T, v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel) {
		testutils.RunValues(t, "kvadmission.flow_control.mode", []kvflowcontrol.ModeT{
			kvflowcontrol.ApplyToElastic,
			kvflowcontrol.ApplyToAll,
		}, func(t *testing.T, mode kvflowcontrol.ModeT) {
			ctx := context.Background()
			settings := cluster.MakeTestingClusterSettings()
			var disableWorkQueueGranting atomic.Bool
			disableWorkQueueGranting.Store(true)
			// This test doesn't want leadership changing hands, and leader leases (by
			// virtue of raft fortification) help ensure this. Override to disable any
			// metamorphosis.
			kvserver.OverrideDefaultLeaseType(ctx, &settings.SV, roachpb.LeaseLeader)
			// Using a manual clock here ensures that StoreLiveness support, once
			// established, never expires. By extension, leadership should stay
			// sticky.
			manualClock := hlc.NewHybridManualClock()
			tc := testcluster.StartTestCluster(t, 5, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: settings,
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
								UseOnlyForScratchRanges: true,
								OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
									return v2EnabledWhenLeaderLevel
								},
								OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
									// This test asserts on the exact values of tracked tokens. In
									// non-test code, the tokens deducted are a few bytes off (give
									// or take) from the size of the proposals. We don't care about
									// such differences.
									return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
								},
							},
						},
						AdmissionControl: &admission.TestingKnobs{
							DisableWorkQueueFastPath: true,
							DisableWorkQueueGranting: func() bool {
								return disableWorkQueueGranting.Load()
							},
						},
						Server: &server.TestingKnobs{
							WallClock: manualClock,
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			k := tc.ScratchRange(t)
			tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

			n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

			h := newFlowControlTestHelperV2(t, tc, v2EnabledWhenLeaderLevel)
			h.init(mode)
			defer h.close(makeV2EnabledTestFileName(v2EnabledWhenLeaderLevel, mode, "raft_membership"))

			desc, err := tc.LookupRange(k)
			require.NoError(t, err)
			h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
			// Reset the token metrics, since a send queue may have instantly
			// formed when adding one of the replicas, before being quickly
			// drained.
			h.resetV2TokenMetrics(ctx)

			h.comment(`-- (Issuing 1x1MiB, 3x replicated write that's not admitted.)`)
			h.put(ctx, k, 1, testFlowModeToPri(mode))

			h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of tokens with
-- no corresponding returns.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- (Adding a voting replica on n4.)`)
			tc.AddVotersOrFatal(t, k, tc.Target(3))
			h.waitForConnectedStreams(ctx, desc.RangeID, 4, 0 /* serverIdx */)

			h.comment(`
-- Observe the total tracked tokens per-stream on n1. s1-s3 should have 1MiB
-- tracked each, and s4 should have none.`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

			h.comment(`-- (Issuing 1x1MiB, 4x replicated write that's not admitted.)`)
			h.put(ctx, k, 1, testFlowModeToPri(mode))

			h.comment(`
-- Observe the individual tracked tokens per-stream on the scratch range. s1-s3
-- should have 2MiB tracked (they've observed 2x1MiB writes), s4 should have
-- 1MiB.
`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

			h.comment(`-- (Removing voting replica from n3.)`)
			tc.RemoveVotersOrFatal(t, k, tc.Target(2))
			h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)

			h.comment(`-- (Adding non-voting replica to n5.)`)
			tc.AddNonVotersOrFatal(t, k, tc.Target(4))
			h.waitForConnectedStreams(ctx, desc.RangeID, 4, 0 /* serverIdx */)

			h.comment(`-- (Issuing 1x1MiB, 4x replicated write (w/ one non-voter) that's not admitted.`)
			h.put(ctx, k, 1, testFlowModeToPri(mode))

			h.comment(`
-- Observe the individual tracked tokens per-stream on the scratch range. s1-s2
-- should have 3MiB tracked (they've observed 3x1MiB writes), there should be
-- no s3 since it was removed, s4 and s5 should have 2MiB and 1MiB
-- respectively.
`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

			h.comment(`-- (Allow below-raft admission to proceed.)`)
			disableWorkQueueGranting.Store(false)
			h.waitForAllTokensReturned(ctx, 5, 0 /* serverIdx */)

			h.comment(`-- Observe that there no tracked tokens across s1,s2,s4,s5.`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

			h.comment(`
-- Flow token metrics from n1 after work gets admitted. All {regular,elastic}
-- tokens deducted are returned, including from when s3 was removed as a raft
-- member.
`)
			h.query(n1, v2FlowTokensQueryStr)
		})
	})
}

// TestFlowControlRaftMembershipRemoveSelf tests flow token behavior when the
// raft leader removes itself from the raft group.
func TestFlowControlRaftMembershipRemoveSelfV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// TODO(#137510): remove when flakes are fixed.
	defer setRACv2DebugVModule(t)()

	testutils.RunValues(t, "v2_enabled_when_leader_level", []kvflowcontrol.V2EnabledWhenLeaderLevel{
		kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding,
	}, func(t *testing.T, v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel) {
		testutils.RunValues(t, "kvadmission.flow_control.mode", []kvflowcontrol.ModeT{
			kvflowcontrol.ApplyToElastic,
			kvflowcontrol.ApplyToAll,
		}, func(t *testing.T, mode kvflowcontrol.ModeT) {
			testutils.RunTrueAndFalse(t, "transfer-lease-first", func(t *testing.T, transferLeaseFirst bool) {
				ctx := context.Background()
				settings := cluster.MakeTestingClusterSettings()
				var disableWorkQueueGranting atomic.Bool
				disableWorkQueueGranting.Store(true)
				tc := testcluster.StartTestCluster(t, 4, base.TestClusterArgs{
					ReplicationMode: base.ReplicationManual,
					ServerArgs: base.TestServerArgs{
						Settings: settings,
						Knobs: base.TestingKnobs{
							Store: &kvserver.StoreTestingKnobs{
								FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
									UseOnlyForScratchRanges: true,
									OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
										return v2EnabledWhenLeaderLevel
									},
									OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
										// This test asserts on the exact values of tracked tokens. In
										// non-test code, the tokens deducted are a few bytes off (give
										// or take) from the size of the proposals. We don't care about
										// such differences.
										return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
									},
								},
							},
							AdmissionControl: &admission.TestingKnobs{
								DisableWorkQueueFastPath: true,
								DisableWorkQueueGranting: func() bool {
									return disableWorkQueueGranting.Load()
								},
							},
						},
					},
				})
				defer tc.Stopper().Stop(ctx)

				k := tc.ScratchRange(t)
				tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

				n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))
				n4 := sqlutils.MakeSQLRunner(tc.ServerConn(3))

				h := newFlowControlTestHelperV2(t, tc, v2EnabledWhenLeaderLevel)
				h.init(mode)
				// Note this test behaves identically independent of we transfer the lease
				// first.
				defer h.close(makeV2EnabledTestFileName(v2EnabledWhenLeaderLevel, mode, "raft_membership_remove_self"))

				desc, err := tc.LookupRange(k)
				require.NoError(t, err)
				h.enableVerboseRaftMsgLoggingForRange(desc.RangeID)

				// Make sure the lease is on n1 and that we're triply connected.
				tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(0))
				h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
				// Reset the token metrics, since a send queue may have instantly
				// formed when adding one of the replicas, before being quickly
				// drained.
				h.resetV2TokenMetrics(ctx)

				h.comment(`-- (Issuing 1x1MiB, 3x replicated write that's not admitted.)`)
				h.put(ctx, k, 1, testFlowModeToPri(mode))

				h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of tokens with
-- no corresponding returns.
`)
				h.query(n1, v2FlowTokensQueryStr)

				h.comment(`-- (Replacing current raft leader on n1 in raft group with new n4 replica.)`)
				testutils.SucceedsSoon(t, func() error {
					// Relocate range from n1 -> n4.
					if err := tc.Servers[2].DB().
						AdminRelocateRange(
							context.Background(), desc.StartKey.AsRawKey(),
							tc.Targets(3, 2, 1), nil, transferLeaseFirst); err != nil {
						return err
					}
					leaseHolder, err := tc.FindRangeLeaseHolder(desc, nil)
					if err != nil {
						return err
					}
					if !leaseHolder.Equal(tc.Target(3)) {
						return errors.Errorf("expected leaseholder to be n4, found %v", leaseHolder)
					}
					return nil
				})
				h.waitForAllTokensReturned(ctx, 4, 0 /* serverIdx */)
				h.waitForConnectedStreams(ctx, desc.RangeID, 3, 3 /* serverIdx */)

				h.comment(`
-- Flow token metrics from n1 after raft leader removed itself from raft group.
-- All {regular,elastic} tokens deducted are returned. Note that the available
-- tokens increases, as n1 has seen 4 replication streams, s1,s2,s3,s4.
`)
				h.query(n1, v2FlowTokensQueryStr)

				h.comment(`
-- n1 should have no connected streams now after transferring the lease to n4.
-- While, n4 should have 3 connected streams to s2,s3,s4. Query the stream count
-- on n1, then on n4.
-- n1 connected v2 streams:
`)
				h.query(n1, v2FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

				h.comment(`-- n4 connected v2 streams:`)
				h.query(n4, v2FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

				h.comment(`-- (Allow below-raft admission to proceed.)`)
				disableWorkQueueGranting.Store(false)
				h.waitForAllTokensReturned(ctx, 4, 0 /* serverIdx */)

				h.comment(`
-- Flow token metrics from n1 after work gets admitted. Tokens were already
-- returned earlier, so there's no change.
`)
				h.query(n1, v2FlowTokensQueryStr)
			})
		})
	})
}

// TestFlowControlClassPrioritizationV2 shows how tokens are managed for both
// regular and elastic work. It does so by replicating + admitting a single
// 1MiB {regular,elastic} write.
func TestFlowControlClassPrioritizationV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "v2_enabled_when_leader_level", []kvflowcontrol.V2EnabledWhenLeaderLevel{
		kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding,
	}, func(t *testing.T, v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel) {
		testutils.RunValues(t, "kvadmission.flow_control.mode", []kvflowcontrol.ModeT{
			kvflowcontrol.ApplyToElastic,
			kvflowcontrol.ApplyToAll,
		}, func(t *testing.T, mode kvflowcontrol.ModeT) {
			ctx := context.Background()
			var disableWorkQueueGranting atomic.Bool
			disableWorkQueueGranting.Store(true)
			settings := cluster.MakeTestingClusterSettings()
			tc := testcluster.StartTestCluster(t, 5, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: settings,
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
								UseOnlyForScratchRanges: true,
								OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
									return v2EnabledWhenLeaderLevel
								},
								OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
									// This test asserts on the exact values of tracked tokens. In
									// non-test code, the tokens deducted are a few bytes off (give
									// or take) from the size of the proposals. We don't care about
									// such differences.
									return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
								},
							},
						},
						AdmissionControl: &admission.TestingKnobs{
							DisableWorkQueueFastPath: true,
							DisableWorkQueueGranting: func() bool {
								return disableWorkQueueGranting.Load()
							},
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			k := tc.ScratchRange(t)
			tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

			n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

			h := newFlowControlTestHelperV2(t, tc, v2EnabledWhenLeaderLevel)
			h.init(mode)
			defer h.close(makeV2EnabledTestFileName(v2EnabledWhenLeaderLevel, mode, "class_prioritization"))

			desc, err := tc.LookupRange(k)
			require.NoError(t, err)
			h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
			// Reset the token metrics, since a send queue may have instantly
			// formed when adding one of the replicas, before being quickly
			// drained.
			h.resetV2TokenMetrics(ctx)

			h.comment(`-- (Issuing 1x1MiB, 3x replicated elastic write that's not admitted.)`)
			h.put(ctx, k, 1, testFlowModeToPri(mode))

			h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB elastic 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of elastic tokens with
-- no corresponding returns.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- (Issuing 1x1MiB, 3x replicated write that's not admitted.)`)
			h.put(ctx, k, 1, testFlowModeToPri(mode))

			h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of {regular,elastic}
-- tokens with no corresponding returns.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- (Allow below-raft admission to proceed.)`)
			disableWorkQueueGranting.Store(false)
			h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)

			h.comment(`
-- Flow token metrics from n1 after work gets admitted. All {regular,elastic}
-- tokens deducted are returned.
`)
			h.query(n1, v2FlowTokensQueryStr)
		})
	})
}

// TestFlowControlUnquiescedRangeV2 tests that flow tokens are reliably returned
// via the normal flow of MsgApp and MsgAppResp messages, with MsgApp pings if
// the admissions are lagging. It also ensures that the range does not quiesce
// until all deducted flow tokens are returned.
func TestFlowControlUnquiescedRangeV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "v2_enabled_when_leader_level", []kvflowcontrol.V2EnabledWhenLeaderLevel{
		kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding,
	}, func(t *testing.T, v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel) {
		testutils.RunValues(t, "kvadmission.flow_control.mode", []kvflowcontrol.ModeT{
			kvflowcontrol.ApplyToElastic,
			kvflowcontrol.ApplyToAll,
		}, func(t *testing.T, mode kvflowcontrol.ModeT) {
			ctx := context.Background()
			var disableWorkQueueGranting atomic.Bool
			var disablePiggybackTokenDispatch atomic.Bool
			disableWorkQueueGranting.Store(true)
			disablePiggybackTokenDispatch.Store(true)

			settings := cluster.MakeTestingClusterSettings()
			// Override metamorphism to allow range quiescence.
			kvserver.OverrideDefaultLeaseType(ctx, &settings.SV, roachpb.LeaseEpoch)
			tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: settings,
					RaftConfig: base.RaftConfig{
						// Suppress timeout-based elections. This test doesn't want to deal
						// with leadership changing hands.
						RaftElectionTimeoutTicks: 1000000,
					},
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
								UseOnlyForScratchRanges: true,
								OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
									return v2EnabledWhenLeaderLevel
								},
								OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
									// This test asserts on the exact values of tracked tokens. In
									// non-test code, the tokens deducted are a few bytes off (give
									// or take) from the size of the proposals. We don't care about
									// such differences.
									return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
								},
							},
						},
						AdmissionControl: &admission.TestingKnobs{
							DisableWorkQueueFastPath: true,
							DisableWorkQueueGranting: func() bool {
								return disableWorkQueueGranting.Load()
							},
						},
						RaftTransport: &kvserver.RaftTransportTestingKnobs{
							DisableFallbackFlowTokenDispatch: func() bool {
								return disablePiggybackTokenDispatch.Load()
							},
							DisablePiggyBackedFlowTokenDispatch: func() bool {
								return disablePiggybackTokenDispatch.Load()
							},
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			k := tc.ScratchRange(t)
			desc, err := tc.LookupRange(k)
			require.NoError(t, err)

			tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)
			h := newFlowControlTestHelperV2(t, tc, v2EnabledWhenLeaderLevel)
			h.init(mode)
			defer h.close(makeV2EnabledTestFileName(v2EnabledWhenLeaderLevel, mode, "unquiesced_range"))

			h.enableVerboseRaftMsgLoggingForRange(desc.RangeID)
			n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

			h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
			// Reset the token metrics, since a send queue may have instantly
			// formed when adding one of the replicas, before being quickly
			// drained.
			h.resetV2TokenMetrics(ctx)

			h.comment(`-- (Issuing 1x1MiB, 3x replicated elastic write that's not admitted.)`)
			h.put(ctx, k, 1, admissionpb.BulkNormalPri)
			h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB elastic 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of elastic tokens with
-- no corresponding returns.
`)
			h.query(n1, v2FlowTokensQueryStr)

			// The range must not quiesce because the leader holds send tokens.
			leader := tc.GetRaftLeader(t, roachpb.RKey(k))
			require.NotNil(t, leader)
			require.False(t, leader.IsQuiescent())

			h.comment(`
-- (Allow below-raft admission to proceed. We've disabled the piggybacked token
-- return mechanism so no tokens are returned via this path. But the tokens will
-- be returned anyway because the range is not quiesced and keeps pinging.)`)
			disableWorkQueueGranting.Store(false)
			h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- (Issuing another 1x1MiB 3x elastic write.)`)
			disableWorkQueueGranting.Store(true)
			h.put(ctx, k, 1, admissionpb.BulkNormalPri)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`
-- (Allow below-raft admission to proceed. We've enabled the piggybacked token
-- return mechanism so tokens are returned either via this path, or the normal
-- MsgAppResp flow, depending on which is exercised first.)`)
			disablePiggybackTokenDispatch.Store(false)
			disableWorkQueueGranting.Store(false)
			h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- (Now the range can quiesce. Wait for it.)`)
			testutils.SucceedsSoon(t, func() error {
				if !leader.IsQuiescent() {
					return errors.Errorf("%s not quiescent", leader)
				}
				return nil
			})
		})
	})
}

// TestFlowControlTransferLeaseV2 tests flow control behavior when the range
// lease is transferred, and the raft leadership along with it.
func TestFlowControlTransferLeaseV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "v2_enabled_when_leader_level", []kvflowcontrol.V2EnabledWhenLeaderLevel{
		kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding,
	}, func(t *testing.T, v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel) {
		testutils.RunValues(t, "kvadmission.flow_control.mode", []kvflowcontrol.ModeT{
			kvflowcontrol.ApplyToElastic,
			kvflowcontrol.ApplyToAll,
		}, func(t *testing.T, mode kvflowcontrol.ModeT) {
			ctx := context.Background()
			var disableWorkQueueGranting atomic.Bool
			disableWorkQueueGranting.Store(true)
			settings := cluster.MakeTestingClusterSettings()
			tc := testcluster.StartTestCluster(t, 5, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: settings,
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
								UseOnlyForScratchRanges: true,
								OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
									return v2EnabledWhenLeaderLevel
								},
								OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
									// This test asserts on the exact values of tracked tokens. In
									// non-test code, the tokens deducted are a few bytes off (give
									// or take) from the size of the proposals. We don't care about
									// such differences.
									return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
								},
							},
						},
						AdmissionControl: &admission.TestingKnobs{
							DisableWorkQueueFastPath: true,
							DisableWorkQueueGranting: func() bool {
								return disableWorkQueueGranting.Load()
							},
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			k := tc.ScratchRange(t)
			tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

			n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

			h := newFlowControlTestHelperV2(t, tc, v2EnabledWhenLeaderLevel)
			h.init(mode)
			defer h.close(makeV2EnabledTestFileName(v2EnabledWhenLeaderLevel, mode, "transfer_lease"))

			desc, err := tc.LookupRange(k)
			require.NoError(t, err)
			h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
			// Reset the token metrics, since a send queue may have instantly
			// formed when adding one of the replicas, before being quickly
			// drained.
			h.resetV2TokenMetrics(ctx)

			h.comment(`-- (Issuing 1x1MiB, 3x replicated write that's not admitted.)`)
			h.put(ctx, k, 1, testFlowModeToPri(mode))

			h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of tokens with
-- no corresponding returns.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- (Transferring range lease to n2 and allowing leadership to follow.)`)
			tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(1))
			testutils.SucceedsSoon(t, func() error {
				if leader := tc.GetRaftLeader(t, roachpb.RKey(k)); leader.NodeID() != tc.Target(1).NodeID {
					return errors.Errorf("expected raft leadership to transfer to n1, found n%d", leader.NodeID())
				}
				return nil
			})
			h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)

			h.comment(`
-- Flow token metrics from n1 having lost the lease and raft leadership. All
-- deducted tokens are returned.
`)
			h.query(n1, v2FlowTokensQueryStr)
		})
	})
}

// TestFlowControlLeaderNotLeaseholderV2 tests flow control behavior when the
// range leaseholder is not the raft leader.
//
// NOTE: This test diverges from TestFlowControlLeaderNotLeaseholder, as v1
// replication flow control doesn't admit via the store work queue when the
// replica is a leaseholder but not the raft leader. Tracked in #130948.
func TestFlowControlLeaderNotLeaseholderV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "v2_enabled_when_leader_level", []kvflowcontrol.V2EnabledWhenLeaderLevel{
		kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding,
	}, func(t *testing.T, v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel) {
		testutils.RunValues(t, "kvadmission.flow_control.mode", []kvflowcontrol.ModeT{
			kvflowcontrol.ApplyToElastic,
			kvflowcontrol.ApplyToAll,
		}, func(t *testing.T, mode kvflowcontrol.ModeT) {
			ctx := context.Background()
			var disableWorkQueueGranting atomic.Bool
			disableWorkQueueGranting.Store(true)
			settings := cluster.MakeTestingClusterSettings()
			tc := testcluster.StartTestCluster(t, 5, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: settings,
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							// Disable leader transfers during leaseholder changes so
							// that we can easily create leader-not-leaseholder
							// scenarios.
							DisableLeaderFollowsLeaseholder: true,
							FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
								UseOnlyForScratchRanges: true,
								OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
									return v2EnabledWhenLeaderLevel
								},
								OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
									// This test asserts on the exact values of tracked tokens. In
									// non-test code, the tokens deducted are a few bytes off (give
									// or take) from the size of the proposals. We don't care about
									// such differences.
									return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
								},
							},
						},
						AdmissionControl: &admission.TestingKnobs{
							DisableWorkQueueFastPath: true,
							DisableWorkQueueGranting: func() bool {
								return disableWorkQueueGranting.Load()
							},
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			k := tc.ScratchRange(t)
			tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

			n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))
			n2 := sqlutils.MakeSQLRunner(tc.ServerConn(1))

			h := newFlowControlTestHelperV2(t, tc, v2EnabledWhenLeaderLevel)
			h.init(mode)
			defer h.close(makeV2EnabledTestFileName(v2EnabledWhenLeaderLevel, mode, "leader_not_leaseholder"))

			desc, err := tc.LookupRange(k)
			require.NoError(t, err)
			h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
			// Reset the token metrics, since a send queue may have instantly
			// formed when adding one of the replicas, before being quickly
			// drained.
			h.resetV2TokenMetrics(ctx)

			h.comment(`-- (Issuing 1x1MiB, 3x replicated write that's not admitted.)`)
			h.put(ctx, k, 1, testFlowModeToPri(mode))

			h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of tokens with
-- no corresponding returns.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- (Transferring only range lease, not raft leadership, to n2.)`)
			tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(1))
			require.Equal(t, tc.GetRaftLeader(t, roachpb.RKey(k)).NodeID(), tc.Target(0).NodeID)

			h.comment(`
-- Flow token metrics from n1 having lost the lease but retained raft
-- leadership. No deducted tokens are released.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`
-- (Allow below-raft admission to proceed. All tokens should be returned.)
`)
			disableWorkQueueGranting.Store(false)
			h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`
-- (Issuing another 1x1MiB, 3x replicated write that's admitted via 
-- the work queue on the leaseholder. It shouldn't deduct any tokens.)
`)
			h.put(ctx, k, 1, testFlowModeToPri(mode))

			h.comment(`
-- Looking at n1's flow token metrics, there's no change. No additional tokens
-- are deducted since the write is not being proposed here.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`
-- Looking at n2's flow token metrics, there's no activity. n2 never acquired
-- the raft leadership.
`)
			h.query(n2, v2FlowTokensQueryStr)
		})
	})
}

// TestFlowControlGranterAdmitOneByOneV2 is a reproduction for #105185.
// Internal admission code that relied on admitting at most one waiting request
// was in fact admitting more than one, and doing so recursively with call
// stacks as deep as the admit chain. This triggered panics (and is also just
// undesirable, design-wise). This test intentionally queues a 1000+ small
// requests, to that end.
func TestFlowControlGranterAdmitOneByOneV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "v2_enabled_when_leader_level", []kvflowcontrol.V2EnabledWhenLeaderLevel{
		kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding,
	}, func(t *testing.T, v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel) {
		testutils.RunValues(t, "kvadmission.flow_control.mode", []kvflowcontrol.ModeT{
			kvflowcontrol.ApplyToElastic,
			kvflowcontrol.ApplyToAll,
		}, func(t *testing.T, mode kvflowcontrol.ModeT) {
			ctx := context.Background()
			var disableWorkQueueGranting atomic.Bool
			disableWorkQueueGranting.Store(true)
			settings := cluster.MakeTestingClusterSettings()
			tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: settings,
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
								UseOnlyForScratchRanges: true,
								OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
									return v2EnabledWhenLeaderLevel
								},
								OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
									// This test asserts on the exact values of tracked
									// tokens. In non-test code, the tokens deducted are
									// a few bytes off (give or take) from the size of
									// the proposals. We don't care about such
									// differences.
									return kvflowcontrol.Tokens(1 << 10 /* 1KiB */)
								},
							},
						},
						AdmissionControl: &admission.TestingKnobs{
							DisableWorkQueueFastPath: true,
							DisableWorkQueueGranting: func() bool {
								return disableWorkQueueGranting.Load()
							},
							AlwaysTryGrantWhenAdmitted: true,
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			k := tc.ScratchRange(t)
			tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

			n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

			h := newFlowControlTestHelperV2(t, tc, v2EnabledWhenLeaderLevel)
			h.init(mode)
			defer h.close(makeV2EnabledTestFileName(v2EnabledWhenLeaderLevel, mode, "granter_admit_one_by_one"))

			desc, err := tc.LookupRange(k)
			require.NoError(t, err)
			h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
			// Reset the token metrics, since a send queue may have instantly
			// formed when adding one of the replicas, before being quickly
			// drained.
			h.resetV2TokenMetrics(ctx)

			h.comment(`-- (Issuing 1024*1KiB, 3x replicated writes that are not admitted.)`)
			h.log("sending put requests")
			for i := 0; i < 1024; i++ {
				// TODO(kvoli): This sleep is necessary because we fill up the (raft)
				// send queue and delay sending + tracking. We need to determine why this
				// occasionally occurs under race.
				time.Sleep(1 * time.Millisecond)
				h.put(ctx, k, 1, testFlowModeToPri(mode))
			}
			h.log("sent put requests")

			h.comment(`
-- Flow token metrics from n1 after issuing 1024KiB, i.e. 1MiB 3x replicated writes
-- that are yet to get admitted. We see 3*1MiB=3MiB deductions of
-- {regular,elastic} tokens with no corresponding returns.
`)
			h.query(n1, v2FlowTokensQueryStr)

			h.comment(`-- Observe the total tracked tokens per-stream on n1.`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

			h.comment(`-- (Allow below-raft admission to proceed.)`)
			disableWorkQueueGranting.Store(false)
			h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */) // wait for admission

			h.comment(`
-- Flow token metrics from n1 after work gets admitted. We see 3MiB returns of
-- {regular,elastic} tokens, and the available capacities going back to what
-- they were. In #105185, by now we would've observed panics.
`)
			h.query(n1, v2FlowTokensQueryStr)

		})
	})
}

// TestFlowControlV1ToV2Transition exercises the transition from replication
// flow control:
//
//   - v1 protocol with v1 encoding =>
//   - v2 protocol with v1 encoding =>
//   - v2 protocol with v2 encoding
//
// The test is structured as follows:
//
//	(1) Start n1, n2, n3 with v1 protocol and v1 encoding.
//	(2) Upgrade n1 to v2 protocol with v1 encoding.
//	(3) Transfer the range lease to n2.
//	(4) Upgrade n2 to v2 protocol with v1 encoding.
//	(5) Upgrade n3 to v2 protocol with v1 encoding.
//	(6) Upgrade n1 to v2 protocol with v2 encoding.
//	(7) Transfer the range lease to n1.
//	(8) Upgrade n2,n3 to v2 protocol with v2 encoding.
//	(9) Transfer the range lease to n3.
//
// Between each step, we issue writes, (un)block admission and observe the flow
// control metrics and vtables.
func TestFlowControlV1ToV2Transition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 3
	var disableWorkQueueGranting atomic.Bool
	disableWorkQueueGranting.Store(true)
	serverLevels := make([]atomic.Uint32, numNodes)
	settings := cluster.MakeTestingClusterSettings()

	argsPerServer := make(map[int]base.TestServerArgs)
	// This test doesn't want leadership changing hands, and leader leases (by
	// virtue of raft fortification) help ensure this. Override to disable any
	// metamorphosis.
	kvserver.OverrideDefaultLeaseType(ctx, &settings.SV, roachpb.LeaseLeader)
	// Using a manual clock here ensures that StoreLiveness support, once
	// established, never expires. By extension, leadership should stay sticky.
	manualClock := hlc.NewHybridManualClock()
	for i := range serverLevels {
		// Every node starts off using the v1 protocol but we will ratchet up the
		// levels on servers at different times as we go to test the transition.
		serverLevels[i].Store(kvflowcontrol.V2NotEnabledWhenLeader)
		argsPerServer[i] = base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					WallClock: manualClock,
				},
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
						OverridePullPushMode: func() bool {
							// Push mode.
							return false
						},
						OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
							return serverLevels[i].Load()
						},
						OverrideTokenDeduction: func(tokens kvflowcontrol.Tokens) kvflowcontrol.Tokens {
							// This test sends several puts, with each put potentially
							// diverging by a few bytes between runs, in aggregate this can
							// accumulate to enough tokens to produce a diff in metrics.
							// Round the token deductions to the nearest MiB avoid this.
							return kvflowcontrol.Tokens(
								int64(math.Round(float64(tokens)/float64(1<<20))) * 1 << 20)
						},
					},
				},
				AdmissionControl: &admission.TestingKnobs{
					DisableWorkQueueFastPath: true,
					DisableWorkQueueGranting: func() bool {
						return disableWorkQueueGranting.Load()
					},
				},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual,
		ServerArgsPerNode: argsPerServer,
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)
	// We use the base constructor here because we will be modifying the enabled
	// level throughout.
	h := newFlowControlTestHelper(
		t, tc, "flow_control_integration_v2", /* testdata */
		kvflowcontrol.V2NotEnabledWhenLeader, false, /* isStatic */
	)

	h.init(kvflowcontrol.ApplyToAll)
	defer h.close("v1_to_v2_transition")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	h.enableVerboseRaftMsgLoggingForRange(desc.RangeID)
	n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	n2 := sqlutils.MakeSQLRunner(tc.ServerConn(1))
	n3 := sqlutils.MakeSQLRunner(tc.ServerConn(2))

	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
	h.comment(`
-- This test exercises the transition from replication flow control:
--   - v1 protocol with v1 encoding =>
--   - v2 protocol with v1 encoding =>
--   - v2 protocol with v2 encoding
-- The test is structured as follows:
--   (1) Start n1, n2, n3 with v1 protocol and v1 encoding.
--   (2) Upgrade n1 to v2 protocol with v1 encoding.
--   (3) Transfer the range lease to n2.
--   (4) Upgrade n2 to v2 protocol with v1 encoding.
--   (5) Upgrade n3 to v2 protocol with v1 encoding.
--   (6) Upgrade n1 to v2 protocol with v2 encoding.
--   (7) Transfer the range lease to n1.
--   (8) Upgrade n2,n3 to v2 protocol with v2 encoding.
--   (9) Transfer the range lease to n3.
-- Between each step, we issue writes, (un)block admission and observe the
-- flow control metrics and vtables.
-- 
-- Start by checking that the leader (n1) has 3 connected v1 streams.
`)
	h.query(n1, v1FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

	h.comment(`-- (Issuing 1x1MiB regular, 3x replicated write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)

	h.comment(`-- The v1 flow token metrics, there should be 3x1 MiB = 3 MiB of tokens deducted.`)
	h.query(n1, v1FlowTokensQueryStr)
	h.comment(`-- The v2 flow token metrics, there should be no tokens or deductions.`)
	h.query(n1, v2FlowTokensQueryStr)

	h.comment(`
-- The v1 tracked tokens per-stream on n1 should be 1 MiB for (s1,s2,s3).
`)
	h.query(n1, v1FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`-- (Allow below-raft admission to proceed.)`)
	disableWorkQueueGranting.Store(false)
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */, kvflowcontrol.V2NotEnabledWhenLeader)
	h.comment(`
-- The v1 flow token metrics on n1, there should be 3x1 MiB = 3 MiB of tokens deducted
-- and returned now. With all tokens available.
`)
	h.query(n1, v1FlowTokensQueryStr)

	h.comment(`
-- The v1 tracked tokens per-stream on n1 should now be 0.
`)
	h.query(n1, v1FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`-- (Block below-raft admission again.)`)
	disableWorkQueueGranting.Store(true)

	h.comment(`-- (Issuing 1 x 1MiB regular, 3x replicated write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)

	h.comment(`
-- The v1 tracked tokens per-stream on n1 should again be 1 MiB for (s1,s2,s3).
`)
	h.query(n1, v1FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`
--------------------------------------------------------------------------------
-- (Upgrading n1 to v2 protocol with v1 encoding.)
--------------------------------------------------------------------------------
`)
	serverLevels[0].Store(kvflowcontrol.V2EnabledWhenLeaderV1Encoding)
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */, kvflowcontrol.V2NotEnabledWhenLeader)
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV1Encoding)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV1Encoding)

	h.comment(`
-- Viewing the range's v2 connected streams, there now should be three.
-- These are lazily instantiated on the first raft event the leader 
-- RangeController sees.
`)
	h.query(n1, v2FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

	h.comment(`
-- There should also now be no connected streams for the v1 protocol,
-- at the leader n1.
`)
	h.query(n1, v1FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

	h.comment(`
-- The v1 flow token metrics, all deducted tokens should be returned after
-- the leader switches to the rac2 protocol.
`)
	h.query(n1, v1FlowTokensQueryStr)

	h.comment(`-- (Issuing 1x2MiB regular, 3x replicated write that's not admitted.)`)
	h.put(ctx, k, 2<<20 /* 2MiB */, admissionpb.NormalPri)

	h.comment(`
-- The v2 flow token metrics, the 3 MiB of earlier token deductions from v1 are dropped.
-- Expect 3 * 2 MiB = 6 MiB of deductions, from the most recent write.
-- Note that the v2 protocol with v1 encoding will only ever deduct elastic tokens.
`)
	h.query(n1, v2FlowTokensQueryStr)

	h.comment(`
-- The v2 tracked tokens per-stream on n1 should now also be 2 MiB for (s1,s2,s3).
`)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`-- (Allow below-raft admission to proceed.)`)
	disableWorkQueueGranting.Store(false)
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV1Encoding)
	h.comment(`-- The v2 flow token metrics. The 6 MiB of tokens should be returned.`)
	h.query(n1, v2FlowTokensQueryStr)

	h.comment(`-- (Block below-raft admission again.)`)
	disableWorkQueueGranting.Store(true)

	h.comment(`-- (Issuing 1 x 1MiB regular, 3x replicated write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)

	h.comment(`
-- The v2 tracked tokens per-stream on n1 reflect the most recent write
-- and should be 1 MiB per stream now.
`)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`
-- There should also be a corresponding elastic token deduction (not regular),
-- as v2 protocol with v1 encoding will only ever deduct elastic tokens.
`)
	h.query(n1, v2FlowTokensQueryStr)

	h.comment(`
-- (Transferring range lease to n2 (running v1) and allowing leadership to follow.)
`)
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(1))
	testutils.SucceedsSoon(t, func() error {
		if leader := tc.GetRaftLeader(t, roachpb.RKey(k)); leader.NodeID() != tc.Target(1).NodeID {
			return errors.Errorf("expected raft leadership to transfer to n2, found n%d", leader.NodeID())
		}
		return nil
	})
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV1Encoding)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 1 /* serverIdx */)

	h.comment(`
-- The v2 flow token metrics from n1 having lost the lease and raft leadership. 
-- All deducted tokens are returned.
`)
	h.query(n1, v2FlowTokensQueryStr)

	h.comment(`
-- Now expect to see 3 connected v1 streams on n2.
`)
	h.query(n2, v1FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

	h.comment(`-- (Issuing 1 x 3MiB elastic, 3x replicated write that's not admitted.)`)
	// We specify the serverIdx to ensure that the write is routed to n2 and not
	// n1. If the write were routed to n1, it would skip flow control because
	// there isn't a handle (leader isn't there) and instead block indefinitely
	// on the store work queue.
	h.put(ctx, k, 3<<20 /* 3MiB */, admissionpb.NormalPri, 1 /* serverIdx */)

	h.comment(`
-- The v1 tracked tokens per-stream on n2 should be 3 MiB. 
`)
	h.query(n2, v1FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`
-- Corresponding v1 token metrics on the new leader n2.
-- These should reflect the 3 x 3 MiB = 9 MiB write.
`)
	h.query(n2, v1FlowTokensQueryStr)
	h.comment(`
-- Corresponding v2 token metrics on the new leader n2.
-- These should be unpopulated, similar to when n1 was first the leader.
`)
	h.query(n2, v2FlowTokensQueryStr)

	h.comment(`-- (Allow below-raft admission to proceed.)`)
	disableWorkQueueGranting.Store(false)
	h.waitForAllTokensReturned(ctx, 3, 1 /* serverIdx */, kvflowcontrol.V2NotEnabledWhenLeader)

	h.comment(`
-- The v1 token metrics on the new leader n2 should now reflect
-- the 9 MiB write and admission, all tokens should be returned.
`)
	h.query(n2, v1FlowTokensQueryStr)

	h.comment(`-- (Issuing 1 x 1MiB regular, 3x replicated write that's admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri, 1 /* serverIdx */)

	h.waitForAllTokensReturned(ctx, 3, 1 /* serverIdx */, kvflowcontrol.V2NotEnabledWhenLeader)
	h.comment(`
-- The v1 token metrics on the new leader n2 should now also reflect
-- the 9 + 3 = 12 MiB write and admission, all tokens should be returned.
`)
	h.query(n2, v1FlowTokensQueryStr)

	h.comment(`-- (Block below-raft admission.)`)
	disableWorkQueueGranting.Store(true)

	h.comment(`-- (Issuing 1 x 4MiB regular, 3x replicated write that's not admitted.)`)
	h.put(ctx, k, 4<<20 /* 4MiB */, admissionpb.NormalPri, 1 /* serverIdx */)
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 12<<20 /* 12MiB */, 1, /* serverIdx */
		kvflowcontrol.V2NotEnabledWhenLeader)

	h.comment(`
-- The v1 tracked tokens per-stream on n2 should be 4 MiB. 
`)
	h.query(n2, v1FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`
-- Corresponding v1 token metrics.
-- These should reflect the 3 x 4 MiB = 12 MiB write.
`)
	h.query(n2, v1FlowTokensQueryStr)

	h.comment(`
--------------------------------------------------------------------------------
-- (Upgrading n2 to v2 protocol with v1 encoding.)
--------------------------------------------------------------------------------
`)
	serverLevels[1].Store(kvflowcontrol.V2EnabledWhenLeaderV1Encoding)
	h.waitForAllTokensReturned(ctx, 3, 1 /* serverIdx */)

	h.comment(`-- (Issuing another 1x1MiB regular, 3x replicated write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri, 1 /* serverIdx */)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 1 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV1Encoding)

	h.comment(`
-- Corresponding v1 token metrics on the new leader n2. 
-- All tokens should be returned.
`)
	h.query(n2, v1FlowTokensQueryStr)

	h.comment(`
-- Also expect to see 0 connected v1 streams on n2.
`)
	h.query(n2, v1FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

	h.comment(`
-- There should be 3 connected streams on n2 for the v2 protocol.
`)
	h.query(n2, v2FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

	h.comment(`
-- Corresponding v2 token metrics on the new leader n2. The most recent 
-- 3 x 1 MiB = 3 MiB write should be reflected in the token deductions.
-- Recall that v2 protocol with v1 encoding will only ever deduct elastic tokens.
`)
	h.query(n2, v2FlowTokensQueryStr)

	h.comment(`
--------------------------------------------------------------------------------
-- (Upgrading n3 to v2 protocol with v1 encoding.)
--------------------------------------------------------------------------------
`)
	serverLevels[2].Store(kvflowcontrol.V2EnabledWhenLeaderV1Encoding)

	h.comment(`-- (Allow below-raft admission to proceed.)`)
	disableWorkQueueGranting.Store(false)
	h.waitForAllTokensReturned(ctx, 3, 1 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV1Encoding)

	h.comment(`
-- The v2 flow token metrics on n2.
-- The 3 MiB of elastic tokens should be returned.
`)
	h.query(n2, v2FlowTokensQueryStr)

	h.comment(`-- (Block below-raft admission.)`)
	disableWorkQueueGranting.Store(true)

	h.comment(`-- (Issuing 1x1MiB regular, 3x replicated write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri, 1 /* serverIdx */)

	h.comment(`
-- The v2 tracked tokens per-stream on n2 should be 1 MiB. 
`)
	h.query(n2, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`
--------------------------------------------------------------------------------
-- (Upgrading n1 to v2 protocol with v2 encoding.)
--------------------------------------------------------------------------------
`)
	serverLevels[0].Store(kvflowcontrol.V2EnabledWhenLeaderV2Encoding)

	h.comment(`
-- The v2 tracked tokens per-stream on n2 should still be 1 MiB. 
`)
	h.query(n2, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`-- (Allow below-raft admission to proceed.)`)
	disableWorkQueueGranting.Store(false)
	h.waitForAllTokensReturned(ctx, 3, 1 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV1Encoding)

	h.comment(`
-- There should no longer be any tracked tokens on n2, as admission occurs.
`)
	h.query(n2, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`
-- Corresponding v2 token metrics on n2. All tokens should be returned.
`)
	h.query(n2, v2FlowTokensQueryStr)

	h.comment(`-- (Block below-raft admission.)`)
	disableWorkQueueGranting.Store(true)

	h.comment(`-- (Issuing 1x1MiB regular, 3x replicated write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri, 1 /* serverIdx */)

	h.comment(`
-- Corresponding v2 token metrics on n2. The 3 x 1 MiB = 3 MiB write 
-- should be reflected.
`)
	h.query(n2, v2FlowTokensQueryStr)

	h.comment(`-- (Transferring range lease back to n1.)`)
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(0))
	testutils.SucceedsSoon(t, func() error {
		if leader := tc.GetRaftLeader(t, roachpb.RKey(k)); leader.NodeID() != tc.Target(0).NodeID {
			return errors.Errorf("expected raft leadership to transfer to n1, found n%d", leader.NodeID())
		}
		return nil
	})
	h.waitForAllTokensReturned(ctx, 3, 1 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV1Encoding)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV2Encoding)

	h.comment(`
-- There should no longer be any tracked tokens on n2, as it's no longer the
-- leader.
`)
	h.query(n2, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`
-- Corresponding v2 token metrics on n2. All tokens should be returned.
`)
	h.query(n2, v2FlowTokensQueryStr)

	h.comment(`
-- Viewing n1's v2 connected streams, there now should be three, as n1 acquired
-- the leadership and lease.
`)
	h.query(n1, v2FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

	h.comment(`-- (Issuing 1x1MiB regular, 3x replicated write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)

	h.comment(`
-- The v2 tracked tokens per-stream on n1.
`)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`-- (Allow below-raft admission to proceed.)`)
	disableWorkQueueGranting.Store(false)
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV2Encoding)
	h.comment(`
-- Corresponding v2 token metrics on n1. 
-- All tokens should be returned via admission.
`)
	h.query(n1, v2FlowTokensQueryStr)

	h.comment(`-- (Block below-raft admission.)`)
	disableWorkQueueGranting.Store(true)

	h.comment(`-- (Issuing 1x1MiB regular, 3x replicated write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)

	h.comment(`
-- Corresponding v2 token metrics on n1. 
-- The 3 x 1 MiB replicated write should be deducted.
`)
	h.query(n1, v2FlowTokensQueryStr)

	h.comment(`
-- The v2 tracked tokens per-stream on n1.
`)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`
-- The v1 tracked tokens per-stream on n1. 
-- There should be no tokens tracked.
`)
	h.query(n1, v1FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`
--------------------------------------------------------------------------------
-- (Upgrading n2 and n3 to v2 protocol with v2 encoding.)
--------------------------------------------------------------------------------
`)
	serverLevels[1].Store(kvflowcontrol.V2EnabledWhenLeaderV2Encoding)
	serverLevels[2].Store(kvflowcontrol.V2EnabledWhenLeaderV2Encoding)

	h.comment(`-- (Allow below-raft admission to proceed.)`)
	disableWorkQueueGranting.Store(false)
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV2Encoding)

	h.comment(`-- (Issuing 2x1MiB regular, 3x replicated write that's admitted.)`)
	h.put(ctx, k, 2<<20 /* 2MiB */, admissionpb.NormalPri)
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV2Encoding)
	h.comment(`
-- Corresponding v2 token metrics on n1. 
-- The 3 x 2 MiB replicated write should be deducted and returned.
`)
	h.query(n1, v2FlowTokensQueryStr)

	h.comment(`
-- (Transferring range lease to n3, running v2 protocol with v2 encoding,
-- and allowing leadership to follow.)
`)
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(2))
	testutils.SucceedsSoon(t, func() error {
		if leader := tc.GetRaftLeader(t, roachpb.RKey(k)); leader.NodeID() != tc.Target(2).NodeID {
			return errors.Errorf("expected raft leadership to transfer to n2, found n%d", leader.NodeID())
		}
		return nil
	})
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV2Encoding)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 2 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV2Encoding)

	h.comment(`-- (Issuing 1x1MiB regular, 3x replicated write that's admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri, 2 /* serverIdx */)
	h.waitForAllTokensReturned(ctx, 3, 2 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV2Encoding)

	// Ensure that there are no outstanding tokens in either protocol after
	// allowing admission one last time.
	//
	// Note n3 was never the leader while having the v1 protocol enabled, only
	// v2.
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */, kvflowcontrol.V2NotEnabledWhenLeader)
	h.waitForAllTokensReturned(ctx, 3, 1 /* serverIdx */, kvflowcontrol.V2NotEnabledWhenLeader)
	h.waitForAllTokensReturned(ctx, 0, 2 /* serverIdx */, kvflowcontrol.V2NotEnabledWhenLeader)
	// Note all three nodes were the leader while having the v2 protocol enabled.
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV2Encoding)
	h.waitForAllTokensReturned(ctx, 3, 1 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV2Encoding)
	h.waitForAllTokensReturned(ctx, 3, 2 /* serverIdx */, kvflowcontrol.V2EnabledWhenLeaderV2Encoding)

	h.comment(`
-- The v1 and v2 flow token metrics on n3.
-- The 3 x 1 MiB write should have been deducted and returned.    
`)
	h.query(n3, v1FlowTokensQueryStr)
	h.query(n3, v2FlowTokensQueryStr)

	h.comment(`-- The v1 and v2 flow token metrics on n1.`)
	h.query(n1, v1FlowTokensQueryStr)
	h.query(n1, v2FlowTokensQueryStr)

	h.comment(`-- The v1 and v2 flow token metrics on n2.`)
	h.query(n2, v1FlowTokensQueryStr)
	h.query(n2, v2FlowTokensQueryStr)
}

type testGeneratedPut struct{}

func contextWithTestGeneratedPut(ctx context.Context) context.Context {
	return context.WithValue(ctx, testGeneratedPut{}, &testGeneratedPut{})
}

func isTestGeneratedPut(ctx context.Context) bool {
	val := ctx.Value(testGeneratedPut{})
	_, ok := val.(*testGeneratedPut)
	return ok
}

// TestFlowControlSendQueue exercises the send queue formation, prevention and
// flushing via selective (logical) admission of entries and token return. See
// the initial comment for an overview of the test structure.
func TestFlowControlSendQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 5
	var noopWaitForEval atomic.Bool
	disableWorkQueueGrantingServers := make([]atomic.Bool, numNodes)
	setTokenReturnEnabled := func(enabled bool, serverIdxs ...int) {
		for _, serverIdx := range serverIdxs {
			disableWorkQueueGrantingServers[serverIdx].Store(!enabled)
		}
	}

	settings := cluster.MakeTestingClusterSettings()
	kvflowcontrol.Mode.Override(ctx, &settings.SV, kvflowcontrol.ApplyToAll)
	// We want to exhaust tokens but not overload the test, so we set the limits
	// lower (8 and 16 MiB default).
	kvflowcontrol.ElasticTokensPerStream.Override(ctx, &settings.SV, 2<<20)
	kvflowcontrol.RegularTokensPerStream.Override(ctx, &settings.SV, 4<<20)

	// This test doesn't want leadership changing hands, and leader leases (by
	// virtue of raft fortification) help ensure this. Override to disable any
	// metamorphosis.
	kvserver.OverrideDefaultLeaseType(ctx, &settings.SV, roachpb.LeaseLeader)
	// Using a manual clock here ensures that StoreLiveness support, once
	// established, never expires. By extension, leadership should stay sticky.
	manualClock := hlc.NewHybridManualClock()
	stickyArgsPerServer := make(map[int]base.TestServerArgs)
	for i := range disableWorkQueueGrantingServers {
		// Start with admission (logical token return) disabled across all nodes.
		disableWorkQueueGrantingServers[i].Store(true)
		stickyArgsPerServer[i] = base.TestServerArgs{
			Settings: settings,
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: fs.NewStickyRegistry(),
					WallClock:         manualClock,
				},
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
						OverrideTokenDeduction: func(tokens kvflowcontrol.Tokens) kvflowcontrol.Tokens {
							// This test sends several puts, with each put potentially
							// diverging by a few bytes between runs, in aggregate this can
							// accumulate to enough tokens to produce a diff in metrics. In
							// addition, under stress/race the larger writes may overload the
							// system with many concurrent tests. Deduct every write as 1
							// MiB, regardless of how large it actually is.
							return kvflowcontrol.Tokens(1 << 20)
						},

						OverrideBypassAdmitWaitForEval: func(ctx context.Context) (bypass bool, waited bool) {
							bypassAndWaited := noopWaitForEval.Load()
							if bypassAndWaited {
								return true, true
							}
							if !isTestGeneratedPut(ctx) {
								return true, false
							}
							return false, false
						},
						// We want to test the behavior of the send queue, so we want to
						// always have up-to-date stats. This ensures that the send queue
						// stats are always refreshed on each call to
						// RangeController.HandleRaftEventRaftMuLocked.
						OverrideAlwaysRefreshSendStreamStats: true,
					},
				},
				AdmissionControl: &admission.TestingKnobs{
					DisableWorkQueueFastPath: true,
					DisableWorkQueueGranting: func() bool {
						idx := i
						return disableWorkQueueGrantingServers[idx].Load()
					},
				},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, 5, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual,
		ServerArgsPerNode: stickyArgsPerServer,
	})
	defer tc.Stopper().Stop(ctx)

	// We setup 3 voters initially on n1, n2, n3. Later, we add n4, n5.
	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

	h := newFlowControlTestHelper(
		t, tc, "flow_control_integration_v2", /* testdata */
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding, true, /* isStatic */
	)
	h.init(kvflowcontrol.ApplyToAll)
	defer h.close("send_queue")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	h.enableVerboseRaftMsgLoggingForRange(desc.RangeID)
	n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
	// We shouldn't need this, since we use contextWithTestGeneratedPut to only
	// subject the test's puts to RAC. But there was a test failure that had
	// 4KiB of tokens deducted and returned, so a send-queue must have formed,
	// possibly because the replica was not in StateReplicate when some entries
	// not subject to RAC were processed.
	h.resetV2TokenMetrics(ctx)

	h.comment(`
-- This test exercises send queue formation, prevention and flushing.
-- The structure roughly follows:
--   Start with three voters on [n1,n2,n3], where n1 is the leader+leaseholder.
--   Large regular write -4 MiB.
--   Allow admission [n1,n2].
--   - Tokens should be returned for n1 and n2.
--   Block admission [n2].
--   Regular write -1 MiB.
--   - Shouldn't be blocked on wait-for-eval because of quorum [n1,n2].
--   - Metrics should reflect send queue formation on n3.
--   Stop n2.
--   Regular write -1 MiB.
--   - Blocks on wait-for-eval, however the test bypasses this instance.
--   - Metrics should reflect n3 being force flushed.
--   Allow admission [n1,n2,n3].
--   Start n2.
--   Add n4, n5, the voters now are [n1,n2,n3,n4,n5].
--   Block admission [n4,n5] (already blocked)
--   Regular write -4 MiB.
--   Regular write -1  MiB.
--   - Shouldn't be blocked on wait-for-eval because of quorum [n1,n2,n3]
--   - Metrics should reflect send queue formation on n4,n5.
--   Unblock admission [n4,n5].
--   - Wait for tokens to be returned.
--   Block admission [n2,n3,n4,n5].
--   Regular write -4 MiB.
--   Regular write -1  MiB.
--   - Blocks on wait-for-eval, however the test bypasses this instance.    
--   - Metrics should reflect 2 streams being prevented from forming a send queue.
--   Allow admission [n1,n2,n3,n4,n5] (all).
--   Assert all tokens returned.
--
-- Start by printing the relevant metrics on n1, first the flow token metrics.
`)
	h.query(n1, v2FlowTokensQueryStr)
	h.comment(`-- Send queue metrics from n1.`)
	h.query(n1, flowSendQueueQueryStr)
	h.comment(`-- Per-store tokens available from n1.`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`-- (Issuing 4x1MiB regular, 3x replicated write that's not admitted.)`)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 12<<20 /* 12MiB */, 0 /* serverIdx */)
	h.comment(`-- Observe the total tracked tokens per-stream on n1.`)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)
	h.comment(`-- And, the per-store tokens available post-write from n1.`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`
-- (Allowing below-raft admission to proceed on n1 and n2.)
-- [n1(enabled),n2(enabled),n3(blocked)]`)
	setTokenReturnEnabled(true /* enabled */, 0, 1)
	// Wait for token return on n1, n2. We should only be tracking the tokens for
	// n3 now.
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0 /* serverIdx */, testingMkFlowStream(0), testingMkFlowStream(1))
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 4<<20 /* 4MiB */, 0 /* serverIdx */)
	h.comment(`-- Observe the total tracked tokens per-stream on n1.`)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)
	h.comment(`-- Per-store tokens available from n1.`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	// Re-disable admission on n2. This is to track a write to n2 that won't be
	// admitted before it's stopped.
	h.comment(`-- (Blocking below-raft admission on n2.)`)
	setTokenReturnEnabled(false /* enabled */, 1)

	h.comment(`-- (Issuing 1x1MiB regular, 3x replicated write that's not admitted.)`)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	// NB: The write won't be tracked because the quorum [n1,n2] have tokens for
	// eval.
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 5<<20 /* 5 MiB */, 0 /* serverIdx */)
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0 /* serverIdx */, testingMkFlowStream(0))
	h.waitForSendQueueSize(ctx, desc.RangeID, 1<<20 /* 1MiB expSize */, 0 /* serverIdx */)
	h.comment(`
-- The send queue metrics from n1 should reflect the 1 MiB write being queued
-- for n3 and 1 MiB tracked for n2 that is yet to be admitted.
`)
	h.query(n1, flowSendQueueQueryStr)
	h.comment(`-- Per-store tokens available from n1.`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`-- (Stopping n2.)`)
	tc.StopServer(1 /* n2 */)
	// There should now be 2 connected streams (n1,n3).
	h.waitForConnectedStreams(ctx, desc.RangeID, 2, 0 /* serverIdx */)
	// There should also be 5 MiB of tracked tokens for n1->n3, 4 + 1 MiB.
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 5<<20 /* 5 MiB */, 0 /* serverIdx */)
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0 /* serverIdx */, testingMkFlowStream(0), testingMkFlowStream(1))
	h.waitForSendQueueSize(ctx, desc.RangeID, 0 /* expSize */, 0 /* serverIdx */)
	h.comment(`
-- Flow token metrics from n1, the disconnect should be reflected in the metrics.`)
	h.query(n1, v2FlowTokensQueryStr)

	h.comment(`
-- Send queue metrics from n1, n3's send queue should have been force-flushed.`)
	h.query(n1, flowSendQueueQueryStr)
	h.comment(`
-- Observe the total tracked tokens per-stream on n1, n3's flushed entries 
-- will also be tracked here.`)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)
	h.comment(`
-- Per-store tokens available from n1, these should reflect the deducted 
-- tokens from force-flushing n3.`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`-- (Enabling wait-for-eval bypass.)`)
	noopWaitForEval.Store(true)
	h.comment(`-- (Issuing 1x1MiB regular, 3x replicated write that's not admitted.)`)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	h.comment(`-- (Disabling wait-for-eval bypass.)`)
	noopWaitForEval.Store(false)
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 6<<20 /* 6 MiB */, 0 /* serverIdx */)
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0 /* serverIdx */, testingMkFlowStream(0), testingMkFlowStream(1))

	h.comment(`
-- Send queue metrics from n1, n3's should not be allowed to form a send queue.`)
	h.query(n1, flowSendQueueQueryStr)
	h.comment(`
-- Observe the total tracked tokens per-stream on n1, n3's should track the latest write
-- as well.`)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`
-- (Allowing below-raft admission to proceed on n1, n2, and n3. Note that n2 is still down.)
-- [n1(enabled),n2(enabled),n3(enabled)]`)
	setTokenReturnEnabled(true /* enabled */, 0, 1, 2)
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)
	h.comment(`-- Flow token metrics from n1.`)
	h.query(n1, v2FlowTokensQueryStr)
	h.comment(`-- Per-store tokens available from n1.`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`-- (Starting n2.)`)
	require.NoError(t, tc.RestartServer(1))
	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
	h.comment(`-- There should now be 3 connected streams again.`)
	h.query(n1, v2FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

	h.comment(`-- (Adding VOTER to n4 and n5.)`)
	tc.AddVotersOrFatal(t, k, tc.Targets(3, 4)...)
	h.waitForConnectedStreams(ctx, desc.RangeID, 5, 0 /* serverIdx */)
	h.waitForAllTokensReturned(ctx, 5, 0 /* serverIdx */)
	h.comment(`
-- Now, after adding n4,n5, there should be 5 connected streams.
-- [n1,n2,n3,n4,n5]
`)
	h.query(n1, v2FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

	h.comment(`-- Per-store tokens available from n1.`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`-- (Issuing 4x1MiB regular, 5x replicated write that's not admitted.)`)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	// Expect the unblocked streams (n1,n2,n3) to track, then untrack quickly as
	// admission is allowed. While n4,n5 will continue to track as they are
	// blocked from admitting.
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 8<<20 /* 8 MiB */, 0 /* serverIdx */)
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0 /* serverIdx */, testingMkFlowStream(0), testingMkFlowStream(1), testingMkFlowStream(2))
	h.comment(`
-- From n1. We should expect to see the unblocked streams quickly
-- untrack as admission is allowed (so not observed here), while n4,n5 will continue
-- to track as they are blocked from admitting (logically).
`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`-- (Issuing 1x1MiB regular, 5x replicated write that's not admitted.)`)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	// The total tracked tokens should not change, as the quorum (n1,n2,n3)
	// quickly admits and untracks. While n4,n5 queue the write, not sending the
	// msg, deducting and tracking the entry tokens.
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 8<<20 /* 8 MiB */, 0 /* serverIdx */)
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0 /* serverIdx */, testingMkFlowStream(0), testingMkFlowStream(1), testingMkFlowStream(2))
	h.comment(`
-- Send queue and flow token metrics from n1. The 1 MiB write should be queued
-- for n4,n5, while the quorum (n1,n2,n3) proceeds.
`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`
-- (Allowing below-raft admission to proceed on n4 and n5.)
-- [n1(enabled),n2(enabled),n3(enabled),n4(enabled),n5(enabled)]`)
	setTokenReturnEnabled(true /* enabled */, 0, 1, 2, 3, 4)
	h.waitForAllTokensReturned(ctx, 5, 0 /* serverIdx */)
	h.comment(`
-- Per-store tokens available from n1. Expect these to return to the same as 
-- the initial state.
`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`-- (Blocking below-raft admission on [n2,n3,n4,n5].)`)
	setTokenReturnEnabled(false /* enabled */, 1, 2, 3, 4)

	h.comment(`-- (Issuing 4x1MiB regular, 5x replicated write that's not admitted.)`)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	// XXX:
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 16<<20 /* 16 MiB */, 0 /* serverIdx */)
	h.comment(`
-- Send queue and flow token metrics from n1. The 4 MiB write should not be
-- queued, but instead exhaust all available regular eval and send tokens across
-- each stream, except s1 (as admission is not blocked).
`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`-- (Enabling wait-for-eval bypass.)`)
	noopWaitForEval.Store(true)
	h.comment(`-- (Issuing 1x1MiB regular, 5x replicated write that's not admitted.)`)
	h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	h.comment(`-- (Disabling wait-for-eval bypass.)`)
	noopWaitForEval.Store(false)
	// Expect 4 x 4 MiB tracked tokens for the 4 MiB write = 16 MiB.
	// Expect 2 x 1 MiB tracked tokens for the 1 MiB write =  2 MiB.
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 18<<20 /* 18MiB */, 0 /* serverIdx */)
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0 /* serverIdx */, testingMkFlowStream(0))
	h.comment(`
-- Observe the total tracked tokens per-stream on n1. We should expect to see the
-- 1 MiB write being tracked across a quorum of streams, while the 4 MiB write
-- is tracked across each stream (except s1). Two(/4 non-leader) replica send 
-- streams should be prevented from forming a send queue and have higher tracked
-- tokens than the other two.
`)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)
	h.comment(`-- Send queue and flow token metrics from n1.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`-- (Allowing below-raft admission on [n1,n2,n3,n4,n5].)`)
	setTokenReturnEnabled(true /* enabled */, 0, 1, 2, 3, 4)

	h.waitForAllTokensReturned(ctx, 5, 0 /* serverIdx */)
	h.comment(`
-- Send queue and flow token metrics from n1. All tokens should be returned.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)
}

func TestFlowControlRepeatedlySwitchMode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	// This test doesn't want leadership changing hands, and leader leases (by
	// virtue of raft fortification) help ensure this. Override to disable any
	// metamorphosis.
	kvserver.OverrideDefaultLeaseType(ctx, &settings.SV, roachpb.LeaseLeader)
	// Using a manual clock here ensures that StoreLiveness support, once
	// established, never expires. By extension, leadership should stay sticky.
	manualClock := hlc.NewHybridManualClock()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
						OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
							// This test makes use of (small) increment requests, but
							// wants to see large token deductions/returns.
							return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
						},
					},
				},
				Server: &server.TestingKnobs{
					WallClock: manualClock,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Setup the test state with 3 voters, one on each of the three
	// node/stores.
	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)
	h := newFlowControlTestHelperV2(t, tc, kvflowcontrol.V2EnabledWhenLeaderV2Encoding)
	mode := kvflowcontrol.ApplyToElastic
	h.init(mode)

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
	h.resetV2TokenMetrics(ctx)

	finishedCh := make(chan struct{})
	go func() {
		defer close(finishedCh)
		// Switch the mode 10 times, with 100ms of sleep in between.
		for i := 0; i < 10; i++ {
			time.Sleep(100 * time.Millisecond)
			switch mode {
			case kvflowcontrol.ApplyToElastic:
				mode = kvflowcontrol.ApplyToAll
			case kvflowcontrol.ApplyToAll:
				mode = kvflowcontrol.ApplyToElastic
			}
			kvflowcontrol.Mode.Override(ctx, &tc.Server(0).ClusterSettings().SV, mode)
		}
	}()
	// Loop until finishedCh is signaled.
	for done := false; !done; {
		// Randomly put NormalPri or BulkLowPri.
		pri := admissionpb.NormalPri
		if h.rng.Intn(2) == 0 {
			pri = admissionpb.BulkLowPri
		}
		h.put(contextWithTestGeneratedPut(ctx), k, 1, pri)
		select {
		case <-finishedCh:
			done = true
		default:
		}
	}
	// All tokens must be returned.
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0 /* serverIdx */)
}

// TODO(kvoli): Add the following tests which exercise interesting events while
// send tokens are exhausted on a partial number, or on all streams:
// - TestFlowControlSendQueueRangeSplitMerge
// - TestFlowControlSendQueueTransferLease
// - TestFlowControlSendQueueRaftMembershipRemoveSelf
// - TestFlowControlSendQueueRaftMembership
// - TestFlowControlSendQueueRaftSnapshot
// - TestFlowControlSendQueueLeaderNotLeaseholder
// - TestFlowControlSendQueueGranterAdmitOneByOne

// TestFlowControlSendQueueManyInflight exercises send queue formation,
// prevention and quickly draining 1k+ entries tracked in the send queue, in
// order to exercise a raft inflight tracking with a large number of inflight
// entries.
func TestFlowControlSendQueueManyInflight(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var disableWorkQueueGranting atomic.Bool
	disableWorkQueueGranting.Store(false)
	var tokenDeduction atomic.Int64
	tokenDeduction.Store(1 /* 1b */)
	var noopWaitForEval atomic.Bool

	settings := cluster.MakeTestingClusterSettings()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
						OverrideTokenDeduction: func(tokens kvflowcontrol.Tokens) kvflowcontrol.Tokens {
							return kvflowcontrol.Tokens(tokenDeduction.Load())
						},
						OverrideBypassAdmitWaitForEval: func(ctx context.Context) (bypass bool, waited bool) {
							bypassAndWaited := noopWaitForEval.Load()
							if bypassAndWaited {
								return true, true
							}
							if !isTestGeneratedPut(ctx) {
								return true, false
							}
							return false, false
						},
						// We want to test the behavior of the send queue, so we want to
						// always have up-to-date stats. This ensures that the send queue
						// stats are always refreshed on each call to
						// RangeController.HandleRaftEventRaftMuLocked.
						OverrideAlwaysRefreshSendStreamStats: true,
					},
				},
				AdmissionControl: &admission.TestingKnobs{
					DisableWorkQueueFastPath: true,
					DisableWorkQueueGranting: func() bool {
						return disableWorkQueueGranting.Load()
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	h := newFlowControlTestHelper(
		t, tc, "flow_control_integration_v2", /* testdata */
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding, true, /* isStatic */
	)
	h.init(kvflowcontrol.ApplyToAll)
	defer h.close("send_queue_many_inflight")

	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)
	desc, err := tc.LookupRange(k)
	require.NoError(t, err)

	n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
	// Reset the token metrics, since a send queue may have instantly
	// formed when adding one of the replicas, before being quickly
	// drained.
	h.resetV2TokenMetrics(ctx)
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)
	h.comment(`
-- We will exhaust the tokens across all streams while admission is blocked,
-- using a single 16 MiB (deduction, the write itself is small) write. Then,
-- we will write a thousand or so entries (@ 4KiB deduction) which should be
-- queued towards one of the replica send streams, while the other has a send
-- queue prevented from forming. Lastly, we will unblock admission and stress
-- the raft in-flights tracker as the queue is drained.`)
	h.comment(`
-- Initial per-store tokens available from n1. 
`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)
	h.comment(`-- (Blocking below-raft admission on [n1,n2,n3].)`)
	disableWorkQueueGranting.Store(true)

	h.comment(`-- (Issuing 16MiB regular write that's not admitted.)`)
	tokenDeduction.Store(16 << 20)
	h.put(contextWithTestGeneratedPut(ctx), k, 1 /* 16 MiB deducted */, admissionpb.NormalPri)
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 48<<20 /* 48MiB */, 0 /* serverIdx */)

	h.comment(`
-- Per-store tokens available from n1, these should reflect the prior
-- large write.`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)
	h.comment(`
-- Observe the total tracked tokens per-stream on n1, these should also reflect the
-- large write.`)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`-- (Enabling wait-for-eval bypass.)`)
	noopWaitForEval.Store(true)
	h.comment(`-- (Issuing 1024x4KiB(=4MiB) regular writes that are not admitted.)`)
	// We issue exactly 4 MiB worth of writes by issuing 1024 writes of 4KiB
	// each.
	tokenDeduction.Store(4 << 10 /* 4KiB */)
	for i := 0; i < 1024; i++ {
		h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri)
	}
	h.comment(`-- (Disabling wait-for-eval bypass.)`)
	noopWaitForEval.Store(false)

	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 56<<20 /* 48+2*4=56MiB */, 0 /* serverIdx */)
	h.waitForSendQueueSize(ctx, desc.RangeID, 4<<20 /* 1024*4KiB=4MiB expSize */, 0 /* serverIdx */)
	h.comment(`
-- Per-store tokens available from n1, these should reflect the deducted
-- tokens from preventing send queue formation.`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)
	h.comment(`
-- Send queue metrics from n1, a send queue should have formed for one of the
-- replica send streams, while the other (non-leader stream) should have been
-- prevented from forming. It should be 1024*4KiB=4MiB in size.`)
	h.query(n1, flowSendQueueQueryStr)
	h.comment(`
-- Observe the total tracked tokens per-stream on n1, one of the three
-- streams will only be tracking the 16 MiB write, while the other two will
-- track the 1024x4KiB writes as well.`)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

	h.comment(`-- (Allowing below-raft admission to proceed on [n1,n2,n3].)`)
	disableWorkQueueGranting.Store(false)
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)
	h.comment(`
-- Send queue and flow token metrics from n1. All tokens should be returned.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)
}

func testingMkFlowStream(serverIdx int) kvflowcontrol.Stream {
	return kvflowcontrol.Stream{
		StoreID:  roachpb.StoreID(serverIdx + 1),
		TenantID: roachpb.SystemTenantID,
	}
}

// TestFlowControlSendQueueRangeRelocate exercises the send queue formation,
// prevention and flushing via selective (logical) admission of entries and token
// return. It also exercises the behavior of the send queue when a range is
// relocated. See the initial comment for an overview of the test structure.
func TestFlowControlSendQueueRangeRelocate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// We use three relocate variations (*=lh,^=send_queue):
	// - [n1*,n2 ,n3^,n4 ,n5] -> [n2 ,n3^,n4 ,n5 ,n6*] (transfer_lease)
	//   - The leader and leaseholder is relocated.
	// - [n1*,n2 ,n3^,n4 ,n5] -> [n1*,n2 ,n4 ,n5 ,n6 ]
	//   - The replica with a send queue is relocated.
	// - [n1*,n2 ,n3^,n4 ,n5] -> [n1*,n2 ,n3^,n4 ,n6 ]
	//   - The replica without a send queue is relocated.
	testutils.RunValues(t, "from", []int{0, 2, 4}, func(t *testing.T, fromIdx int) {
		testutils.RunTrueAndFalse(t, "transfer_lease", func(t *testing.T, transferLease bool) {
			const numNodes = 6
			// The transferLease arg indicates whether the AdminRelocateRange request
			// will also transfer the lease to the voter which is in the first
			// position of the target list. We always place n6 in the first position
			// and pass in the transferLease arg to AdminRelocateRange.
			fromNode := roachpb.NodeID(fromIdx + 1)
			toNode := roachpb.NodeID(numNodes)
			fromServerIdxs := []int{0, 1, 2, 3, 4}
			toServerIdxs := []int{numNodes - 1}
			for i := 0; i < numNodes-1; i++ {
				if i != fromIdx {
					toServerIdxs = append(toServerIdxs, i)
				}
			}
			var fromString string
			if fromIdx == 0 {
				fromString = "leader_store"
			} else if fromIdx == 2 {
				fromString = "send_queue_store"
			} else {
				fromString = "has_token_store"
			}
			if transferLease {
				fromString += "_transfer_lease"
			}
			// If n1 is removing itself from the range, the leaseholder will be
			// transferred to n6 regardless of the value of transferLease.
			newLeaseholderIdx := 0
			if transferLease || fromIdx == 0 {
				newLeaseholderIdx = 5
			}
			newLeaseNode := roachpb.NodeID(newLeaseholderIdx + 1)

			ctx := context.Background()
			settings := cluster.MakeTestingClusterSettings()
			kvflowcontrol.Mode.Override(ctx, &settings.SV, kvflowcontrol.ApplyToAll)
			// We want to exhaust tokens but not overload the test, so we set the limits
			// lower (8 and 16 MiB default).
			kvflowcontrol.ElasticTokensPerStream.Override(ctx, &settings.SV, 2<<20)
			kvflowcontrol.RegularTokensPerStream.Override(ctx, &settings.SV, 4<<20)

			disableWorkQueueGrantingServers := make([]atomic.Bool, numNodes)
			setTokenReturnEnabled := func(enabled bool, serverIdxs ...int) {
				for _, serverIdx := range serverIdxs {
					disableWorkQueueGrantingServers[serverIdx].Store(!enabled)
				}
			}

			argsPerServer := make(map[int]base.TestServerArgs)
			for i := range disableWorkQueueGrantingServers {
				disableWorkQueueGrantingServers[i].Store(true)
				argsPerServer[i] = base.TestServerArgs{
					Settings: settings,
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
								UseOnlyForScratchRanges: true,
								OverrideTokenDeduction: func(tokens kvflowcontrol.Tokens) kvflowcontrol.Tokens {
									// Deduct every write as 1 MiB, regardless of how large it
									// actually is.
									return kvflowcontrol.Tokens(1 << 20)
								},
								// We want to test the behavior of the send queue, so we want to
								// always have up-to-date stats. This ensures that the send queue
								// stats are always refreshed on each call to
								// RangeController.HandleRaftEventRaftMuLocked.
								OverrideAlwaysRefreshSendStreamStats: true,
							},
						},
						AdmissionControl: &admission.TestingKnobs{
							DisableWorkQueueFastPath: true,
							DisableWorkQueueGranting: func() bool {
								idx := i
								return disableWorkQueueGrantingServers[idx].Load()
							},
						},
					},
				}
			}

			tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
				ReplicationMode:   base.ReplicationManual,
				ServerArgsPerNode: argsPerServer,
			})
			defer tc.Stopper().Stop(ctx)

			k := tc.ScratchRange(t)
			tc.AddVotersOrFatal(t, k, tc.Targets(1, 2, 3, 4)...)

			h := newFlowControlTestHelper(
				t, tc, "flow_control_integration_v2", /* testdata */
				kvflowcontrol.V2EnabledWhenLeaderV2Encoding, true, /* isStatic */
			)
			h.init(kvflowcontrol.ApplyToAll)
			defer h.close(fmt.Sprintf("send_queue_range_relocate_from_%s", fromString))

			desc, err := tc.LookupRange(k)
			require.NoError(t, err)
			h.enableVerboseRaftMsgLoggingForRange(desc.RangeID)
			n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))
			newLeaseDB := sqlutils.MakeSQLRunner(tc.ServerConn(newLeaseholderIdx))
			h.waitForConnectedStreams(ctx, desc.RangeID, 5, 0 /* serverIdx */)
			h.resetV2TokenMetrics(ctx)
			h.waitForConnectedStreams(ctx, desc.RangeID, 5, 0 /* serverIdx */)

			// Block admission on n3, while allowing every other node to admit.
			setTokenReturnEnabled(true /* enabled */, 0, 1, 3, 4, 5)
			setTokenReturnEnabled(false /* enabled */, 2)
			// Drain the tokens to n3 by blocking admission and issuing the buffer
			// size of writes to the range.
			h.put(contextWithTestGeneratedPut(ctx), roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
			h.put(contextWithTestGeneratedPut(ctx), roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
			h.put(contextWithTestGeneratedPut(ctx), roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
			h.put(contextWithTestGeneratedPut(ctx), roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
			h.waitForTotalTrackedTokens(ctx, desc.RangeID, 4<<20 /* 4 MiB */, 0 /* serverIdx */)

			h.comment(`(Sending 1 MiB put request to develop a send queue)`)
			h.put(contextWithTestGeneratedPut(ctx), roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
			h.comment(`(Sent 1 MiB put request)`)
			h.waitForTotalTrackedTokens(ctx, desc.RangeID, 4<<20 /* 4 MiB */, 0 /* serverIdx */)
			h.waitForAllTokensReturnedForStreamsV2(ctx, 0, /* serverIdx */
				testingMkFlowStream(0), testingMkFlowStream(1),
				testingMkFlowStream(3), testingMkFlowStream(4))
			h.waitForSendQueueSize(ctx, desc.RangeID, 1<<20 /* expSize 1 MiB */, 0 /* serverIdx */)

			h.comment(`
-- Send queue metrics from n1, n3's send queue should have 1 MiB for s3.`)
			h.query(n1, flowSendQueueQueryStr)
			h.comment(`
-- Observe the total tracked tokens per-stream on n1, s3's entries will still
-- be tracked here.`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)
			h.comment(`
-- Per-store tokens available from n1, these should reflect the lack of tokens 
-- for s3.`)
			h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

			beforeString := fmt.Sprintf("%v", fromServerIdxs)
			afterString := fmt.Sprintf("%v", toServerIdxs)

			h.comment(fmt.Sprintf(`
-- Issuing RelocateRange:
--   before=%s
--   after =%s
-- Transferring the lease: %v.`, beforeString, afterString, transferLease))
			testutils.SucceedsSoon(t, func() error {
				if err := tc.Servers[2].DB().
					AdminRelocateRange(
						context.Background(),
						desc.StartKey.AsRawKey(),
						tc.Targets(toServerIdxs...),
						nil,           /* nonVoterTargets */
						transferLease, /* transferLeaseToFirstVoter */
					); err != nil {
					return err
				}
				var err error
				desc, err = tc.LookupRange(k)
				if err != nil {
					return err
				}
				rset := desc.Replicas()
				if fullDescs := rset.VoterFullAndNonVoterDescriptors(); len(fullDescs) != 5 {
					return errors.Errorf(
						"expected 5 voters, got %v (replica_set=%v)", fullDescs, rset)
				}
				if rset.HasReplicaOnNode(fromNode) {
					return errors.Errorf(
						"expected no replica on node %v (replica_set=%v)", fromNode, rset)
				}
				if !rset.HasReplicaOnNode(toNode) {
					return errors.Errorf(
						"expected replica on node 6 (replica_set=%v)", rset)
				}
				leaseHolder, err := tc.FindRangeLeaseHolder(desc, nil)
				if err != nil {
					return err
				}
				expLeaseTarget := tc.Target(newLeaseholderIdx)
				if !leaseHolder.Equal(expLeaseTarget) {
					return errors.Errorf(
						"expected leaseholder to be on %v found %v (replica_set=%v)",
						expLeaseTarget, leaseHolder, rset)
				}
				return nil
			})

			h.waitForConnectedStreams(ctx, desc.RangeID, 5, newLeaseholderIdx)
			h.comment(`(Sending 1 MiB put request to the relocated range)`)
			h.put(contextWithTestGeneratedPut(ctx), k, 1, admissionpb.NormalPri, newLeaseholderIdx)
			h.comment(`(Sent 1 MiB put request to the relocated range)`)

			h.waitForAllTokensReturnedForStreamsV2(ctx, 0, /* serverIdx */
				testingMkFlowStream(0), testingMkFlowStream(1),
				testingMkFlowStream(3), testingMkFlowStream(4))

			toStreams := make([]kvflowcontrol.Stream, 0, len(toServerIdxs))
			for _, toServerIdx := range toServerIdxs {
				if toServerIdx != 2 /* send queue server */ {
					toStreams = append(toStreams, testingMkFlowStream(toServerIdx))
				}
			}
			h.waitForAllTokensReturnedForStreamsV2(ctx, newLeaseholderIdx, toStreams...)

			h.comment(`-- Observe the total tracked tokens per-stream on n1.`)
			h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)

			if newLeaseholderIdx != 0 {
				// Avoid double printing if the lease hasn't moved.
				h.comment(fmt.Sprintf(`
-- Observe the total tracked tokens per-stream on new leaseholder n%v.`, newLeaseNode))
				h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)
			}

			// Allow admission to proceed on n3 and wait for all tokens to be returned.
			h.comment(`-- (Allowing below-raft admission to proceed on n3.)`)
			setTokenReturnEnabled(true /* enabled */, 2)
			h.waitForAllTokensReturned(ctx, 6 /* expStreamCount */, 0 /* serverIdx */)
			if transferLease && fromIdx != 0 {
				// When the lease is transferred first, the leaseholder is relocated to
				// n6 after the fromNode is removed. In this case, we expect the
				// leaseholder will have only 5 streams, because it will have never
				// seen s3's stream as its replica was already removed from the range.
				h.waitForAllTokensReturned(ctx, 5 /* expStreamCount */, newLeaseholderIdx)
			} else {
				h.waitForAllTokensReturned(ctx, 6 /* expStreamCount */, newLeaseholderIdx)
			}

			h.comment(`
-- Send queue and flow token metrics from n1. All tokens should be returned.`)
			h.query(n1, flowSendQueueQueryStr)
			h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)
			h.comment(fmt.Sprintf(`
-- Send queue and flow token metrics from leaseholder n%v.
-- All tokens should be returned.`, newLeaseNode))
			h.query(newLeaseDB, flowSendQueueQueryStr)
			h.query(newLeaseDB, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)
		})
	})
}

// TestFlowControlRangeSplitMergeMixedVersion attempts a split and merge while
// the elastic tokens are exhausted to one store and the flow control mode is
// apply_to_elastic. See the longer SQL comment for a detailed view of the
// steps taken by the test.
func TestFlowControlRangeSplitMergeMixedVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 3
	prevVKey := clusterversion.PreviousRelease
	latestVKey := clusterversion.Latest
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		latestVKey.Version(), prevVKey.Version(), false)
	var disableWorkQueueGranting atomic.Bool
	disableWorkQueueGranting.Store(true)
	// We want to exhaust the elastic tokens, but not the regular tokens.
	kvflowcontrol.ElasticTokensPerStream.Override(ctx, &settings.SV, 3<<20)

	disableWorkQueueGrantingServers := make([]atomic.Bool, numNodes)
	setTokenReturnEnabled := func(enabled bool, serverIdxs ...int) {
		for _, serverIdx := range serverIdxs {
			disableWorkQueueGrantingServers[serverIdx].Store(!enabled)
		}
	}

	argsPerServer := make(map[int]base.TestServerArgs)
	for i := range disableWorkQueueGrantingServers {
		disableWorkQueueGrantingServers[i].Store(true)
		argsPerServer[i] = base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					ClusterVersionOverride:         prevVKey.Version(),
					DisableAutomaticVersionUpgrade: make(chan struct{}),
				},
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
						OverrideTokenDeduction: func(tokens kvflowcontrol.Tokens) kvflowcontrol.Tokens {
							return kvflowcontrol.Tokens(1 << 20)
						},
						OverrideAlwaysRefreshSendStreamStats: true,
					},
				},
				AdmissionControl: &admission.TestingKnobs{
					DisableWorkQueueFastPath: true,
					DisableWorkQueueGranting: func() bool {
						idx := i
						return disableWorkQueueGrantingServers[idx].Load()
					},
				},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual,
		ServerArgsPerNode: argsPerServer,
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

	h := newFlowControlTestHelper(
		t, tc, "flow_control_integration_v2", /* testdata */
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding, true, /* isStatic */
	)

	const mode = kvflowcontrol.ApplyToElastic
	h.init(mode)
	defer h.close("range_split_merge_mixed_version")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	h.enableVerboseRaftMsgLoggingForRange(desc.RangeID)
	h.enableVerboseRaftMsgLoggingForRange(desc.RangeID + 1)
	n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
	h.resetV2TokenMetrics(ctx)

	h.log(fmt.Sprintf("cluster_version=%v(%+v) latest_version=%v(%+v)",
		prevVKey.Version(), prevVKey, latestVKey.Version(), latestVKey))
	setTokenReturnEnabled(true /* enabled */, 0, 1)
	h.comment(`
-- This test is running with kvadmission.flow_control.mode="apply_to_elastic"
-- and the cluster version set to the previous version. We will exhaust the
-- elastic tokens towards s3. Then, we will split and merge the range,
-- expecting no send queue to form for any stream, as apply_to_elastic is the
-- flow control mode. We also expect that the split and merge will proceed
-- without issue. Note that admission is currently blocked on n3(s3).
`)

	h.put(ctx, roachpb.Key(desc.StartKey), 1, admissionpb.BulkNormalPri)
	h.put(ctx, roachpb.Key(desc.StartKey), 1, admissionpb.BulkNormalPri)
	h.put(ctx, roachpb.Key(desc.StartKey), 1, admissionpb.BulkNormalPri)
	h.comment(`(Sent 3x1 MiB BulkNormalPri put request to pre-split range)`)

	var cancels []func()
	var recordingFns []func() tracingpb.Recording
	var recordings []tracingpb.Recording
	cancelAll := func() {
		require.Equal(t, len(cancels), len(recordingFns))
		recordings = make([]tracingpb.Recording, len(cancels))
		for i := range cancels {
			cancels[i]()
			// We also need to finish the tracing spans.
			recordings = append(recordings, recordingFns[i]())
		}
	}

	// For each async put, we will record the tracing span and retain the ability
	// to cancel it. This involves some more tracking to ensure we complete
	// tracing spans and avoid leaving dangling goroutines (the stopper will get
	// them eventually however).
	preIdx := len(cancels)
	traceCtxPre, recPre := tracing.ContextWithRecordingSpan(ctx,
		tc.GetFirstStoreFromServer(t, 0 /* server */).GetStoreConfig().Tracer(), "pre-split")
	recordingFns = append(recordingFns, recPre)
	cancelPre, chPre := h.putAsync(traceCtxPre, roachpb.Key(desc.StartKey), 1, admissionpb.BulkNormalPri)
	cancels = append(cancels, cancelPre)
	h.comment(`(Sent 1 MiB BulkNormalPri put request to pre-split range)`)

	h.comment(`-- (Splitting range.)`)
	left, right := tc.SplitRangeOrFatal(t, k.Next())
	h.waitForConnectedStreams(ctx, left.RangeID, 3, 0 /* serverIdx */)
	h.waitForConnectedStreams(ctx, right.RangeID, 3, 0 /* serverIdx */)

	h.comment(`-- Observe the newly split off replica, with its own three streams.`)
	h.query(n1, v2FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)

	// LHS post-split put.
	leftIdx := len(cancels)
	traceCtxLeft, recLeft := tracing.ContextWithRecordingSpan(ctx,
		tc.GetFirstStoreFromServer(t, 0 /* server */).GetStoreConfig().Tracer(), "lhs")
	recordingFns = append(recordingFns, recLeft)
	cancelLeft, chLeft := h.putAsync(traceCtxLeft, roachpb.Key(left.StartKey), 1, admissionpb.BulkNormalPri)
	cancels = append(cancels, cancelLeft)
	h.comment(`(Sent 1 MiB BulkNormalPri put request to post-split LHS range)`)

	// RHS post-split put.
	rightIdx := len(cancels)
	traceCtxRight, recRight := tracing.ContextWithRecordingSpan(ctx,
		tc.GetFirstStoreFromServer(t, 0 /* server */).GetStoreConfig().Tracer(), "rhs")
	recordingFns = append(recordingFns, recRight)
	cancelRight, chRight := h.putAsync(traceCtxRight, roachpb.Key(right.StartKey), 1, admissionpb.BulkNormalPri)
	cancels = append(cancels, cancelRight)
	h.comment(`(Sent 1 MiB BulkNormalPri put request to post-split RHS range)`)
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0, /* serverIdx */
		testingMkFlowStream(0), testingMkFlowStream(1))

	h.comment(`
-- Send queue and flow token metrics from n1, post-split and 1 MiB put on
-- each side.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`-- (Merging ranges.)`)
	merged := tc.MergeRangesOrFatal(t, left.StartKey.AsRawKey())
	h.waitForConnectedStreams(ctx, merged.RangeID, 3, 0 /* serverIdx */)
	h.waitForSendQueueSize(ctx, merged.RangeID, 0 /* expSize 0 MiB */, 0 /* serverIdx */)

	h.comment(`
-- Send queue and flow token metrics from n1, post-split-merge.
-- We expect to not see a force flush of the send queue for s3 again.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	// Post-split-merge put.
	mergeIdx := len(cancels)
	traceCtxMerge, recMerge := tracing.ContextWithRecordingSpan(ctx,
		tc.GetFirstStoreFromServer(t, 0 /* server */).GetStoreConfig().Tracer(), "merge")
	recordingFns = append(recordingFns, recMerge)
	cancelMerge, chMerge := h.putAsync(traceCtxMerge, roachpb.Key(merged.StartKey), 1, admissionpb.BulkNormalPri)
	cancels = append(cancels, cancelMerge)
	h.comment(`(Sent 1 MiB BulkNormalPri put request to post-split-merge range)`)
	h.waitForSendQueueSize(ctx, merged.RangeID, 0 /* expSize 0 MiB */, 0 /* serverIdx */)

	h.comment(`
-- Send queue and flow token metrics from n1, post-split-merge. 
-- We do not expect to see the send queue develop for s3.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`
-- Allow admission to proceed on all nodes and wait for all tokens to be
-- returned.`)
	setTokenReturnEnabled(true /* enabled */, 0, 1, 2)
	// We expect that there are no tracked tokens initially, as there are no
	// tokens available towards s3. Therefore, we wait for the total tracked
	// tokens to be greater than zero before proceeding to check that all tokens
	// are returned. This prevents an edge case where the test will fail because
	// not all tokens are actually returned, due to not having yet sent the
	// elastic pri entries to followers, passing the all token returned check
	// here but not later in the testdata file.
	h.waitForTotalTrackedTokensGE(ctx, merged.RangeID, 1<<20 /* 1MiB expTotalTrackedTokensGE */, 0 /* serverIdx */)
	h.waitForTotalTrackedTokensForDuration(ctx, merged.RangeID, 0 /* expTotalTrackedTokens */, 0 /* serverIdx */, 2*time.Second)
	h.waitForAllTokensReturned(ctx, 3 /* expStreamCount */, 0 /* serverIdx */)

	select {
	// The pre-split put should have returned after enabling admission.
	case res := <-chPre:
		h.comment(fmt.Sprintf("pre-split response br=%v pErr=%v", res.BatchResponse, res.Error))
	case <-time.After(2 * time.Second):
		cancelAll()
		t.Fatalf("expected pre-split put to return after enabling admission, trace: %s", recordings[preIdx])
	}
	select {
	// Likewise for the post-split LHS put.
	case res := <-chLeft:
		h.comment(fmt.Sprintf("post-split LHS response br=%v pErr=%v", res.BatchResponse, res.Error))
	case <-time.After(2 * time.Second):
		cancelAll()
		t.Fatalf("expected LHS put to return after enabling admission, trace: %s", recordings[leftIdx])
	}
	select {
	// And for the post-split RHS put. Note that the RHS put will return to the
	// dist sender after the RHS range controller is closed (merge). After this,
	// the dist sender will exhaust the other replicas which have also been
	// removed from the range (n2,n3), before refreshing the range cache and
	// redirecting the request to the merged range which succeeds.
	case res := <-chRight:
		h.comment(fmt.Sprintf("post-split RHS response br=%v pErr=%v", res.BatchResponse, res.Error))
	case <-time.After(2 * time.Second):
		cancelAll()
		t.Fatalf("expected RHS put to return after enabling admission, trace: %s", recordings[rightIdx])
	}
	select {
	// And for the post-split-merge put.
	case res := <-chMerge:
		h.comment(fmt.Sprintf("post-merge response br=%v pErr=%v", res.BatchResponse, res.Error))
	case <-time.After(2 * time.Second):
		cancelAll()
		t.Fatalf("expected merge put to return after enabling admission, trace: %s", recordings[mergeIdx])
	}

	h.comment(`-- Flow token metrics from n1, all tokens should be returned.`)
	h.query(n1, v2FlowTokensQueryStr)

	// Ensure all the tracing spans are closed. They will be closed if we hit a
	// non result (test failure) case above.
	recPre()
	recLeft()
	recRight()
	recMerge()
}

func TestFlowControlSendQueueRangeMigrate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 3
	// We're going to be transitioning from startV to endV. Think a cluster of
	// binaries running vX, but with active version vX-1.
	startV := clusterversion.PreviousRelease.Version()
	endV := clusterversion.Latest.Version()
	settings := cluster.MakeTestingClusterSettingsWithVersions(endV, startV, false)
	kvflowcontrol.Mode.Override(ctx, &settings.SV, kvflowcontrol.ApplyToAll)
	// We want to exhaust tokens but not overload the test, so we set the limits
	// lower (8 and 16 MiB default).
	kvflowcontrol.ElasticTokensPerStream.Override(ctx, &settings.SV, 2<<20)
	kvflowcontrol.RegularTokensPerStream.Override(ctx, &settings.SV, 4<<20)
	disableWorkQueueGrantingServers := make([]atomic.Bool, numNodes)
	setTokenReturnEnabled := func(enabled bool, serverIdxs ...int) {
		for _, serverIdx := range serverIdxs {
			disableWorkQueueGrantingServers[serverIdx].Store(!enabled)
		}
	}

	argsPerServer := make(map[int]base.TestServerArgs)
	for i := range disableWorkQueueGrantingServers {
		disableWorkQueueGrantingServers[i].Store(true)
		argsPerServer[i] = base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					ClusterVersionOverride:         startV,
					DisableAutomaticVersionUpgrade: make(chan struct{}),
				},
				Store: &kvserver.StoreTestingKnobs{
					EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
						// Because we are migrating from a version (currently) prior to the
						// range force flush key version gate, we won't trigger the force
						// flush via migrate until we're on the endV, which defeats the
						// purpose of this test. We override the behavior here to allow the
						// force flush to be triggered on the startV from a Migrate
						// request.
						OverrideDoTimelyApplicationToAllReplicas: true,
					},
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
							return kvflowcontrol.V2EnabledWhenLeaderV2Encoding
						},
						UseOnlyForScratchRanges: true,
						OverrideTokenDeduction: func(tokens kvflowcontrol.Tokens) kvflowcontrol.Tokens {
							// Deduct every write as 1 MiB, regardless of how large it
							// actually is.
							return kvflowcontrol.Tokens(1 << 20)
						},
						// We want to test the behavior of the send queue, so we want to
						// always have up-to-date stats. This ensures that the send queue
						// stats are always refreshed on each call to
						// RangeController.HandleRaftEventRaftMuLocked.
						OverrideAlwaysRefreshSendStreamStats: true,
					},
				},
				AdmissionControl: &admission.TestingKnobs{
					DisableWorkQueueFastPath: true,
					DisableWorkQueueGranting: func() bool {
						idx := i
						return disableWorkQueueGrantingServers[idx].Load()
					},
				},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual,
		ServerArgsPerNode: argsPerServer,
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

	h := newFlowControlTestHelper(
		t, tc, "flow_control_integration_v2", /* testdata */
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding, true, /* isStatic */
	)
	h.init(kvflowcontrol.ApplyToAll)
	defer h.close("send_queue_range_migrate")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	h.enableVerboseRaftMsgLoggingForRange(desc.RangeID)
	n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
	h.resetV2TokenMetrics(ctx)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)

	store := tc.GetFirstStoreFromServer(t, 0)
	assertVersion := func(expV roachpb.Version) error {
		repl, err := store.GetReplica(desc.RangeID)
		if err != nil {
			t.Fatal(err)
		}
		if gotV := repl.Version(); gotV != expV {
			return errors.Errorf("expected in-memory version %s, got %s", expV, gotV)
		}

		sl := stateloader.Make(desc.RangeID)
		persistedV, err := sl.LoadVersion(ctx, store.TODOEngine())
		if err != nil {
			return err
		}
		if persistedV != expV {
			return errors.Errorf("expected persisted version %s, got %s", expV, persistedV)
		}
		return nil
	}

	require.NoError(t, assertVersion(startV))

	migrated := false
	unregister := batcheval.TestingRegisterMigrationInterceptor(endV, func() {
		migrated = true
	})
	defer unregister()

	h.comment(`
-- We will exhaust the tokens across all streams while admission is blocked on
-- n3, using a single 4 MiB (deduction, the write itself is small) write. Then,
-- we will write a 1 MiB put to the range, migrate the range, and write a 1 MiB
-- put to the migrated range. We expect that the migration will trigger a force
-- flush of the send queue.`)
	// Block admission on n3, while allowing every other node to admit.
	setTokenReturnEnabled(true /* enabled */, 0, 1)
	setTokenReturnEnabled(false /* enabled */, 2)
	// Drain the tokens to n3 by blocking admission and issuing the buffer
	// size of writes to the range.
	h.put(ctx, roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
	h.put(ctx, roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
	h.put(ctx, roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
	h.put(ctx, roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 4<<20 /* 4 MiB */, 0 /* serverIdx */)

	h.comment(`(Sending 1 MiB put request to develop a send queue)`)
	h.put(ctx, roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
	h.comment(`(Sent 1 MiB put request)`)
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 4<<20 /* 4 MiB */, 0 /* serverIdx */)
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0, /* serverIdx */
		testingMkFlowStream(0), testingMkFlowStream(1))
	h.waitForSendQueueSize(ctx, desc.RangeID, 1<<20 /* expSize 1 MiB */, 0 /* serverIdx */)

	h.comment(`
-- Send queue metrics from n1, n3's send queue should have 1 MiB for s3.`)
	h.query(n1, flowSendQueueQueryStr)
	h.comment(`
-- Observe the total tracked tokens per-stream on n1, s3's entries will still
-- be tracked here.`)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)
	h.comment(`
-- Per-store tokens available from n1, these should reflect the lack of tokens 
-- for s3.`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`-- (Issuing MigrateRequest to range)`)
	req := &kvpb.MigrateRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    desc.StartKey.AsRawKey(),
			EndKey: desc.EndKey.AsRawKey(),
		},
		Version: endV,
	}
	kvDB := tc.Servers[0].DB()
	require.NoError(t, func() error {
		if _, pErr := kv.SendWrappedWith(ctx, kvDB.GetFactory().NonTransactionalSender(), kvpb.Header{RangeID: desc.RangeID}, req); pErr != nil {
			return pErr.GoError()
		}
		if !migrated {
			return errors.Errorf("expected migration interceptor to have been called")
		}
		return assertVersion(endV)
	}())

	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
	h.waitForSendQueueSize(ctx, desc.RangeID, 0 /* expSize */, 0 /* serverIdx */)
	h.comment(`
-- Send queue and flow token metrics from n1 post-migrate. The migrate should
-- have triggered a force flush of the send queue.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`(Sending 1 MiB put request to the migrated range)`)
	h.put(ctx, roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
	h.comment(`(Sent 1 MiB put request to the migrated range)`)
	h.waitForSendQueueSize(ctx, desc.RangeID, 1<<20 /* expSize 1 MiB */, 0 /* serverIdx */)
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0, /* serverIdx */
		testingMkFlowStream(0), testingMkFlowStream(1))

	h.comment(`
-- Send queue and flow token metrics from n1 post-migrate and post 1 MiB put.
-- We expect to see the send queue develop for s3 again.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`-- (Allowing below-raft admission to proceed on n3.)`)
	setTokenReturnEnabled(true /* enabled */, 2)
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)
	h.waitForSendQueueSize(ctx, desc.RangeID, 0 /* expSize 0 MiB */, 0 /* serverIdx */)

	h.comment(`
-- Send queue and flow token metrics from n1. All tokens should be returned.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)
}

// TestFlowControlSendQueueRangeSplitMerge exercises the send queue formation,
// prevention and force flushing due to range split and merge operations. See
// the initial comment for an overview of the test structure.
func TestFlowControlSendQueueRangeSplitMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 3
	settings := cluster.MakeTestingClusterSettings()
	kvflowcontrol.Mode.Override(ctx, &settings.SV, kvflowcontrol.ApplyToAll)
	// We want to exhaust tokens but not overload the test, so we set the limits
	// lower (8 and 16 MiB default).
	kvflowcontrol.ElasticTokensPerStream.Override(ctx, &settings.SV, 2<<20)
	kvflowcontrol.RegularTokensPerStream.Override(ctx, &settings.SV, 4<<20)
	// TODO(kvoli): There are unexpected messages popping up, which cause a send
	// queue to be created on the RHS range post-split. This appears related to
	// leader leases, or at least disablng them deflakes the test. We should
	// re-enable leader leases likely by adjusting the test to ignore the 500b
	// send queue formatiion:
	//
	//  r3=(is_state_replicate=true has_send_queue=true send_queue_size=500 B / 1 entries
	//      [idx_to_send=12 next_raft_idx=13 next_raft_idx_initial=13 force_flush_stop_idx=0])
	//
	// See #136258 for more debug info.
	kvserver.OverrideDefaultLeaseType(ctx, &settings.SV, roachpb.LeaseEpoch)
	disableWorkQueueGrantingServers := make([]atomic.Bool, numNodes)
	setTokenReturnEnabled := func(enabled bool, serverIdxs ...int) {
		for _, serverIdx := range serverIdxs {
			disableWorkQueueGrantingServers[serverIdx].Store(!enabled)
		}
	}

	argsPerServer := make(map[int]base.TestServerArgs)
	for i := range disableWorkQueueGrantingServers {
		disableWorkQueueGrantingServers[i].Store(true)
		argsPerServer[i] = base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
						OverrideTokenDeduction: func(tokens kvflowcontrol.Tokens) kvflowcontrol.Tokens {
							// Deduct every write as 1 MiB, regardless of how large it
							// actually is.
							return kvflowcontrol.Tokens(1 << 20)
						},
						// We want to test the behavior of the send queue, so we want to
						// always have up-to-date stats. This ensures that the send queue
						// stats are always refreshed on each call to
						// RangeController.HandleRaftEventRaftMuLocked.
						OverrideAlwaysRefreshSendStreamStats: true,
					},
				},
				AdmissionControl: &admission.TestingKnobs{
					DisableWorkQueueFastPath: true,
					DisableWorkQueueGranting: func() bool {
						idx := i
						return disableWorkQueueGrantingServers[idx].Load()
					},
				},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual,
		ServerArgsPerNode: argsPerServer,
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

	h := newFlowControlTestHelper(
		t, tc, "flow_control_integration_v2", /* testdata */
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding, true, /* isStatic */
	)
	h.init(kvflowcontrol.ApplyToAll)
	defer h.close("send_queue_range_split_merge")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	h.enableVerboseRaftMsgLoggingForRange(desc.RangeID)
	h.enableVerboseRaftMsgLoggingForRange(desc.RangeID + 1)
	n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
	// Reset the token metrics, since a send queue may have instantly
	// formed when adding one of the replicas, before being quickly
	// drained.
	h.resetV2TokenMetrics(ctx)

	// TODO(kvoli): Update this comment to also mention writing to the RHS range,
	// once we resolve the subsume wait for application issue. See #136649.
	h.comment(`
-- We will exhaust the tokens across all streams while admission is blocked on
-- n3, using a single 4 MiB (deduction, the write itself is small) write. Then,
-- we will write a 1 MiB put to the range, split it, write a 1 MiB put to the
-- LHS range, and a 1MiB put to the RHS range, merge the ranges, and write a 1
-- MiB put to the merged range. We expect that at each stage where a send
-- queue develops n1->s3, the send queue will be flushed by the range merge
-- and range split range operations.`)
	h.comment(`
-- Start by exhausting the tokens from n1->s3 and blocking admission on s3.
-- (Issuing 4x1MiB regular, 3x replicated write that's not admitted on s3.)`)
	setTokenReturnEnabled(true /* enabled */, 0, 1)
	setTokenReturnEnabled(false /* enabled */, 2)
	h.put(ctx, k, 1, admissionpb.NormalPri)
	h.put(ctx, k, 1, admissionpb.NormalPri)
	h.put(ctx, k, 1, admissionpb.NormalPri)
	h.put(ctx, k, 1, admissionpb.NormalPri)
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 4<<20 /* 4 MiB */, 0 /* serverIdx */)

	h.comment(`(Sending 1 MiB put request to pre-split range)`)
	h.put(ctx, k, 1, admissionpb.NormalPri)
	h.comment(`(Sent 1 MiB put request to pre-split range)`)

	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 4<<20 /* 4 MiB */, 0 /* serverIdx */)
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0, /* serverIdx */
		testingMkFlowStream(0), testingMkFlowStream(1))
	h.waitForSendQueueSize(ctx, desc.RangeID, 1<<20 /* expSize 1 MiB */, 0 /* serverIdx */)

	h.comment(`
-- Send queue metrics from n1, n3's send queue should have 1 MiB for s3.`)
	h.query(n1, flowSendQueueQueryStr)
	h.comment(`
-- Observe the total tracked tokens per-stream on n1, s3's entries will still
-- be tracked here.`)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)
	h.comment(`
-- Per-store tokens available from n1, these should reflect the lack of tokens 
-- for s3.`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`-- (Splitting range.)`)
	left, right := tc.SplitRangeOrFatal(t, k.Next())
	h.waitForConnectedStreams(ctx, left.RangeID, 3, 0 /* serverIdx */)
	h.waitForConnectedStreams(ctx, right.RangeID, 3, 0 /* serverIdx */)
	h.waitForSendQueueSize(ctx, left.RangeID, 0 /* expSize 0 MiB */, 0 /* serverIdx */)
	h.waitForSendQueueSize(ctx, right.RangeID, 0 /* expSize 0 MiB */, 0 /* serverIdx */)

	h.comment(`-- Observe the newly split off replica, with its own three streams.`)
	h.query(n1, v2FlowPerRangeStreamQueryStr, flowPerRangeStreamQueryHeaderStrs...)
	h.comment(`
-- Send queue and flow token metrics from n1, post-split.
-- We expect to see a force flush of the send queue for s3.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`(Sending 1 MiB put request to post-split LHS range)`)
	h.put(ctx, roachpb.Key(left.StartKey), 1, admissionpb.NormalPri)
	h.comment(`(Sent 1 MiB put request to post-split LHS range)`)
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0, /* serverIdx */
		testingMkFlowStream(0), testingMkFlowStream(1))

	h.comment(`(Sending 1 MiB put request to post-split RHS range)`)
	h.put(ctx, roachpb.Key(right.StartKey), 1, admissionpb.NormalPri)
	h.comment(`(Sent 1 MiB put request to post-split RHS range)`)
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0, /* serverIdx */
		testingMkFlowStream(0), testingMkFlowStream(1))

	h.comment(`
-- Send queue and flow token metrics from n1, post-split and 1 MiB put on
-- each side.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`-- (Merging ranges.)`)
	merged := tc.MergeRangesOrFatal(t, left.StartKey.AsRawKey())
	h.waitForConnectedStreams(ctx, merged.RangeID, 3, 0 /* serverIdx */)
	h.waitForSendQueueSize(ctx, merged.RangeID, 0 /* expSize 0 MiB */, 0 /* serverIdx */)

	h.comment(`
-- Send queue and flow token metrics from n1, post-split-merge.
-- We expect to see a force flush of the send queue for s3 again.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`(Sending 1 MiB put request to post-split-merge range)`)
	h.put(ctx, k, 1, admissionpb.NormalPri)
	h.comment(`(Sent 1 MiB put request to post-split-merge range)`)
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0, /* serverIdx */
		testingMkFlowStream(0), testingMkFlowStream(1))
	h.waitForSendQueueSize(ctx, merged.RangeID, 1<<20 /* expSize 1 MiB */, 0 /* serverIdx */)

	h.comment(`
-- Send queue and flow token metrics from n1, post-split-merge and 1 MiB put.
-- We expect to see the send queue develop for s3 again.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	h.comment(`-- (Allowing below-raft admission to proceed on [n1,n2,n3].)`)
	setTokenReturnEnabled(true /* enabled */, 0, 1, 2)

	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)
	h.comment(`
-- Send queue and flow token metrics from n1, all tokens should be returned.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)
}

func TestFlowControlSendQueueRangeFeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// rangeFeed will create a rangefeed suitable for testing. It will start a
	// rangefeed and return a function that can be used to stop it.
	rangeFeed := func(
		ctx context.Context,
		dsI interface{},
		sp roachpb.Span,
		startFrom hlc.Timestamp,
		onValue func(event kvcoord.RangeFeedMessage),
		opts ...kvcoord.RangeFeedOption,
	) func() {
		ds := dsI.(*kvcoord.DistSender)
		events := make(chan kvcoord.RangeFeedMessage)
		cancelCtx, cancel := context.WithCancel(ctx)

		g := ctxgroup.WithContext(cancelCtx)
		g.GoCtx(func(ctx context.Context) (err error) {
			return ds.RangeFeed(ctx, []kvcoord.SpanTimePair{{Span: sp, StartAfter: startFrom}}, events, opts...)
		})
		g.GoCtx(func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ev := <-events:
					onValue(ev)
				}
			}
		})

		return func() {
			cancel()
			_ = g.Wait()
		}
	}
	// We will use the below logic later to assert on the node that the rangefeed
	// is running on.
	var curRangeFeedNodeID atomic.Value
	curRangeFeedNodeID.Store(roachpb.NodeID(0))
	checkRangeFeedNodeID := func(nodeID roachpb.NodeID, include bool) error {
		curNodeID := curRangeFeedNodeID.Load().(roachpb.NodeID)
		if curNodeID == 0 {
			return errors.New("no rangefeed node yet")
		}
		if curNodeID != nodeID && include {
			return errors.Errorf("expected rangefeed on n%v, got n%v", nodeID, curNodeID)
		}
		if curNodeID == nodeID && !include {
			return errors.Errorf("expected rangefeed not on n%v", nodeID)
		}
		return nil
	}

	ctx := context.Background()
	const numNodes = 3
	settings := cluster.MakeTestingClusterSettings()
	kvflowcontrol.Mode.Override(ctx, &settings.SV, kvflowcontrol.ApplyToAll)
	// We want to exhaust tokens but not overload the test, so we set the limits
	// lower (8 and 16 MiB default).
	kvflowcontrol.ElasticTokensPerStream.Override(ctx, &settings.SV, 2<<20)
	kvflowcontrol.RegularTokensPerStream.Override(ctx, &settings.SV, 4<<20)
	kvserver.RangefeedEnabled.Override(ctx, &settings.SV, true)
	// Speed up cancellation, default is 20x the target duration.
	kvserver.RangeFeedLaggingCTCancelMultiple.Override(ctx, &settings.SV, 6)
	// Also speed up cancellation by shortening the required lag duration,
	// default is 60s.
	kvserver.RangeFeedLaggingCTCancelDuration.Override(ctx, &settings.SV, 3*time.Second)
	// Likewise, the default target duration is 3s, lower it by a factor of 20.
	closedts.TargetDuration.Override(ctx, &settings.SV, 150*time.Millisecond)

	disableWorkQueueGrantingServers := make([]atomic.Bool, numNodes)
	setTokenReturnEnabled := func(enabled bool, serverIdxs ...int) {
		for _, serverIdx := range serverIdxs {
			disableWorkQueueGrantingServers[serverIdx].Store(!enabled)
		}
	}

	argsPerServer := make(map[int]base.TestServerArgs)
	for i := range disableWorkQueueGrantingServers {
		disableWorkQueueGrantingServers[i].Store(true)
		argsPerServer[i] = base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
						OverrideTokenDeduction: func(tokens kvflowcontrol.Tokens) kvflowcontrol.Tokens {
							// Deduct every write as 1 MiB, regardless of how large it
							// actually is.
							return kvflowcontrol.Tokens(1 << 20)
						},
						// We want to test the behavior of the send queue, so we want to
						// always have up-to-date stats. This ensures that the send queue
						// stats are always refreshed on each call to
						// RangeController.HandleRaftEventRaftMuLocked.
						OverrideAlwaysRefreshSendStreamStats: true,
					},
				},
				AdmissionControl: &admission.TestingKnobs{
					DisableWorkQueueFastPath: true,
					DisableWorkQueueGranting: func() bool {
						idx := i
						return disableWorkQueueGrantingServers[idx].Load()
					},
				},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual,
		ServerArgsPerNode: argsPerServer,
	})
	defer tc.Stopper().Stop(ctx)
	setTokenReturnEnabled(true /* enabled */, 0, 1, 2)

	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

	h := newFlowControlTestHelper(
		t, tc, "flow_control_integration_v2", /* testdata */
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding, true, /* isStatic */
	)
	h.init(kvflowcontrol.ApplyToAll)
	defer h.close("send_queue_range_feed")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	h.enableVerboseRaftMsgLoggingForRange(desc.RangeID)
	n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	n3 := sqlutils.MakeSQLRunner(tc.ServerConn(2))
	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)
	h.resetV2TokenMetrics(ctx)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)

	ts := tc.Server(2)
	span := desc.KeySpan().AsRawSpanWithNoLocals()
	ignoreValues := func(event kvcoord.RangeFeedMessage) {}

	ctx2, cancel := context.WithCancel(context.Background())
	g := ctxgroup.WithContext(ctx2)
	defer func() {
		cancel()
		err := g.Wait()
		require.True(t, testutils.IsError(err, "context canceled"))
	}()
	observer := func(fn kvcoord.ForEachRangeFn) {
		g.GoCtx(func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(200 * time.Millisecond):
				}
				err := fn(func(rfCtx kvcoord.RangeFeedContext, feed kvcoord.PartialRangeFeed) error {
					curRangeFeedNodeID.Store(feed.NodeID)
					return nil
				})
				if err != nil {
					return err
				}
			}
		})
	}

	// We will use this metric to observe the server-side rangefeed cancellation,
	// which should be zero prior to the send queue developing. Then, non-zero
	// shortly after.
	const rangeFeedCancelMetricQueryStr = `
  SELECT
    name,
    value
  FROM crdb_internal.node_metrics
  WHERE name LIKE 'kv.rangefeed.closed_timestamp.slow_ranges.cancelled'
  ORDER BY name ASC;
`

	closeFeed := rangeFeed(
		ctx,
		ts.DistSenderI(),
		span,
		tc.Server(0).Clock().Now(),
		ignoreValues,
		kvcoord.WithRangeObserver(observer),
	)
	defer closeFeed()
	testutils.SucceedsSoon(t, func() error { return checkRangeFeedNodeID(3, true /* include */) })
	h.comment(`(Rangefeed on n3)`)

	h.comment(`
-- We will exhaust the tokens across all streams while admission is blocked on
-- n3, using 4x1 MiB (deduction, the write itself is small) writes. Then,
-- we will write 1 MiB to the range and wait for the closedTS to fall
-- behind on n3. We expect that the closedTS falling behind will trigger
-- an error that is returned to the mux rangefeed client, which will in turn 
-- allows the rangefeed  request to be re-routed to another replica.`)
	// Block admission on n3, while allowing every other node to admit.
	setTokenReturnEnabled(true /* enabled */, 0, 1)
	setTokenReturnEnabled(false /* enabled */, 2)
	// Drain the tokens to n3 by blocking admission and issuing the buffer
	// size of writes to the range.
	h.put(ctx, roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
	h.put(ctx, roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
	h.put(ctx, roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
	h.put(ctx, roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 4<<20 /* 4 MiB */, 0 /* serverIdx */)

	h.comment(`
-- Observe the server-side rangefeed cancellation metric on n3, before a send
-- queue develops, it should be zero:`)
	h.query(n3, rangeFeedCancelMetricQueryStr)

	h.comment(`(Sending 1 MiB put request to develop a send queue)`)
	h.put(ctx, roachpb.Key(desc.StartKey), 1, admissionpb.NormalPri)
	h.comment(`(Sent 1 MiB put request)`)
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 4<<20 /* 4 MiB */, 0 /* serverIdx */)
	h.waitForAllTokensReturnedForStreamsV2(ctx, 0, /* serverIdx */
		testingMkFlowStream(0), testingMkFlowStream(1))
	h.waitForSendQueueSize(ctx, desc.RangeID, 1<<20 /* expSize 1 MiB */, 0 /* serverIdx */)

	h.comment(`
-- Send queue metrics from n1, n3's send queue should have 1 MiB for s3.`)
	h.query(n1, flowSendQueueQueryStr)
	h.comment(`
-- Observe the total tracked tokens per-stream on n1, s3's entries will still
-- be tracked here.`)
	h.query(n1, v2FlowPerStreamTrackedQueryStr, flowPerStreamTrackedQueryHeaderStrs...)
	h.comment(`
-- Per-store tokens available from n1, these should reflect the lack of tokens 
-- for s3.`)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)

	testutils.SucceedsSoon(t, func() error {
		return checkRangeFeedNodeID(3, false /* include */)
	})
	newNode := curRangeFeedNodeID.Load().(roachpb.NodeID)
	h.comment(fmt.Sprintf(`(Rangefeed moved to n%v)`, newNode))

	h.comment(`
-- Observe the server-side rangefeed cancellation metric increased on n3:`)
	h.query(n3, rangeFeedCancelMetricQueryStr)

	h.comment(`-- (Allowing below-raft admission to proceed on n3.)`)
	setTokenReturnEnabled(true /* enabled */, 2)
	h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)
	h.waitForSendQueueSize(ctx, desc.RangeID, 0 /* expSize 0 MiB */, 0 /* serverIdx */)

	h.comment(`
-- Send queue and flow token metrics from n1. All tokens should be returned.`)
	h.query(n1, flowSendQueueQueryStr)
	h.query(n1, flowPerStoreTokenQueryStr, flowPerStoreTokenQueryHeaderStrs...)
}

type flowControlTestHelper struct {
	t             testing.TB
	tc            *testcluster.TestCluster
	st            *cluster.Settings
	buf           *strings.Builder
	rng           *rand.Rand
	testdata      string
	level         kvflowcontrol.V2EnabledWhenLeaderLevel
	isStaticLevel bool
}

func newFlowControlTestHelper(
	t testing.TB,
	tc *testcluster.TestCluster,
	testdata string,
	level kvflowcontrol.V2EnabledWhenLeaderLevel,
	isStatic bool,
) *flowControlTestHelper {
	rng, _ := randutil.NewPseudoRand()
	buf := &strings.Builder{}
	return &flowControlTestHelper{
		t:             t,
		tc:            tc,
		st:            tc.Server(0).ClusterSettings(),
		buf:           buf,
		rng:           rng,
		testdata:      testdata,
		level:         level,
		isStaticLevel: isStatic,
	}
}

func newFlowControlTestHelperV2(
	t testing.TB, tc *testcluster.TestCluster, level kvflowcontrol.V2EnabledWhenLeaderLevel,
) *flowControlTestHelper {
	return newFlowControlTestHelper(t,
		tc,
		"flow_control_integration_v2", /* testdata */
		level,
		true, /* isStatic */
	)
}

func testFlowModeToPri(mode kvflowcontrol.ModeT) admissionpb.WorkPriority {
	switch mode {
	case kvflowcontrol.ApplyToElastic:
		return admissionpb.UserLowPri
	case kvflowcontrol.ApplyToAll:
		return admissionpb.UserHighPri
	default:
		panic("unknown flow control mode")
	}
}

func (h *flowControlTestHelper) init(mode kvflowcontrol.ModeT) {
	// Reach into each server's cluster setting and override. This causes any
	// registered change callbacks to run immediately, which is important since
	// running them with some lag (which happens when using SQL and `SET CLUSTER
	// SETTING`) interferes with the later activities in these tests.
	for _, s := range h.tc.Servers {
		kvflowcontrol.Enabled.Override(context.Background(), &s.ClusterSettings().SV, true)
		kvflowcontrol.Mode.Override(context.Background(), &s.ClusterSettings().SV, mode)
	}
}

// waitForAllTokensReturned waits for all tokens to be returned across all
// streams. The expected number of streams and protocol level is passed in as
// an argument, in order to allow switching between v1 and v2 flow control.
func (h *flowControlTestHelper) waitForAllTokensReturned(
	ctx context.Context, expStreamCount, serverIdx int, lvl ...kvflowcontrol.V2EnabledWhenLeaderLevel,
) {
	testutils.SucceedsSoon(h.t, func() error {
		return h.checkAllTokensReturned(ctx, expStreamCount, serverIdx, lvl...)
	})
}

// waitForAllTokensReturnedForStreamsV2 waits for all tokens to be returned
// across the specified streams. This only works for RACv2.
func (h *flowControlTestHelper) waitForAllTokensReturnedForStreamsV2(
	ctx context.Context, serverIdx int, streamIDs ...kvflowcontrol.Stream,
) {
	testutils.SucceedsSoon(h.t, func() error {
		return h.checkTokensAvailableForStreamsV2(ctx, len(streamIDs), serverIdx,
			h.tokensAvailableLimitWithDelta(kvflowinspectpb.Stream{}), streamIDs...)
	})
}

func (h *flowControlTestHelper) waitForSendQueueSize(
	ctx context.Context, rangeID roachpb.RangeID, expSize int64, serverIdx int,
) {
	testutils.SucceedsSoon(h.t, func() error {
		return h.checkSendQueueSize(ctx, rangeID, expSize, serverIdx)
	})
}

func (h *flowControlTestHelper) checkSendQueueSize(
	ctx context.Context, rangeID roachpb.RangeID, expSize int64, serverIdx int,
) error {
	stats := rac2.RangeSendStreamStats{}
	h.tc.GetFirstStoreFromServer(h.t, serverIdx).GetReplicaIfExists(rangeID).SendStreamStats(&stats)
	_, sizeBytes := stats.SumSendQueues()
	if sizeBytes != expSize {
		return errors.Errorf("expected send queue size %d, got %d [%v]", expSize, sizeBytes, &stats)
	}
	return nil
}

// checkAllTokensReturned checks that all tokens have been returned across all
// streams. It also checks that the expected number of streams are present. The
// protocol level is passed in as an argument, in order to allow switching
// between v1 and v2 flow control.
func (h *flowControlTestHelper) checkAllTokensReturned(
	ctx context.Context, expStreamCount, serverIdx int, lvl ...kvflowcontrol.V2EnabledWhenLeaderLevel,
) error {
	return h.checkTokensAvailableWithLevel(
		ctx, expStreamCount, serverIdx, h.tokensAvailableLimitWithDelta(kvflowinspectpb.Stream{}), lvl...)
}

func tokensAvailableDeltaModeEnabled(
	mode kvflowcontrol.ModeT, enabled kvflowcontrol.V2EnabledWhenLeaderLevel, delta int64,
) kvflowinspectpb.Stream {
	streamDelta := kvflowinspectpb.Stream{
		AvailableEvalElasticTokens: delta,
		AvailableSendElasticTokens: delta,
	}
	switch mode {
	case kvflowcontrol.ApplyToElastic:
	// Handled above, nothing to do.
	case kvflowcontrol.ApplyToAll:
		if enabled == kvflowcontrol.V2EnabledWhenLeaderV2Encoding {
			// NB: We cannot reliably assert on the regular tokens when not using the
			// V2 protocol because we will convert all decoded priorities to elastic
			// in processor.go: AdmitRaftEntriesRaftMuLocked.
			streamDelta.AvailableEvalRegularTokens = delta
			streamDelta.AvailableSendRegularTokens = delta
		}
	default:
		panic("unknown flow control mode")
	}

	return streamDelta
}

func (h *flowControlTestHelper) tokensAvailableLimitWithDelta(
	delta kvflowinspectpb.Stream,
) kvflowinspectpb.Stream {
	elasticTokensPerStream := kvflowcontrol.ElasticTokensPerStream.Get(&h.st.SV)
	regularTokensPerStream := kvflowcontrol.RegularTokensPerStream.Get(&h.st.SV)
	return kvflowinspectpb.Stream{
		AvailableEvalRegularTokens: regularTokensPerStream + delta.AvailableEvalRegularTokens,
		AvailableEvalElasticTokens: elasticTokensPerStream + delta.AvailableEvalElasticTokens,
		AvailableSendRegularTokens: regularTokensPerStream + delta.AvailableSendRegularTokens,
		AvailableSendElasticTokens: elasticTokensPerStream + delta.AvailableSendElasticTokens,
	}
}

// waitForAllTokensAvaiable waits for all tokens to be equal to the provided
// expTokensStream across all streams. The expected number of streams and
// protocol level is passed in as an argument, in order to allow switching
// between v1 and v2 flow control.
func (h *flowControlTestHelper) waitForAllTokensAvailable(
	ctx context.Context,
	expStreamCount, serverIdx int,
	expTokensStream kvflowinspectpb.Stream,
	lvl ...kvflowcontrol.V2EnabledWhenLeaderLevel,
) {
	testutils.SucceedsSoon(h.t, func() error {
		return h.checkTokensAvailableWithLevel(ctx, expStreamCount, serverIdx, expTokensStream, lvl...)
	})
}

// checkTokensAvailableForStreamsV2 checks that the expected number of tokens
// are available across the specified streams. This only works for RACv2.
func (h *flowControlTestHelper) checkTokensAvailableForStreamsV2(
	ctx context.Context,
	expStreamCount, serverIdx int,
	expTokensStream kvflowinspectpb.Stream,
	streamIDs ...kvflowcontrol.Stream,
) error {
	streams := h.tc.GetFirstStoreFromServer(h.t, serverIdx).GetStoreConfig().KVFlowStreamTokenProvider.Inspect(ctx)
	filteredStreams := make([]kvflowinspectpb.Stream, 0, len(streams))
	for _, stream := range streams {
		for _, s := range streamIDs {
			if s.TenantID == stream.TenantID && s.StoreID == stream.StoreID {
				filteredStreams = append(filteredStreams, stream)
				break
			}
		}
	}
	return h.checkTokensAvailable(
		ctx, expStreamCount, serverIdx, expTokensStream, filteredStreams, h.level)
}

// checkTokensAvailableWithLevel checks that the expected number of tokens are
// available across all streams. The V2EnabledWhenLeaderLevel may be passed in
// as an argument, in order to allow switching between v1 and v2 flow control.
func (h *flowControlTestHelper) checkTokensAvailableWithLevel(
	ctx context.Context,
	expStreamCount, serverIdx int,
	expTokensStream kvflowinspectpb.Stream,
	lvl ...kvflowcontrol.V2EnabledWhenLeaderLevel,
) error {
	var streams []kvflowinspectpb.Stream
	level := h.resolveLevelArgs(lvl...)
	switch level {
	case kvflowcontrol.V2NotEnabledWhenLeader:
		streams = h.tc.Server(serverIdx).KVFlowController().(kvflowcontrol.Controller).Inspect(ctx)
	case kvflowcontrol.V2EnabledWhenLeaderV1Encoding, kvflowcontrol.V2EnabledWhenLeaderV2Encoding:
		streams = h.tc.GetFirstStoreFromServer(h.t, serverIdx).GetStoreConfig().KVFlowStreamTokenProvider.Inspect(ctx)
	default:
		h.t.Fatalf("unknown level: %v", level)
	}
	return h.checkTokensAvailable(ctx, expStreamCount, serverIdx, expTokensStream, streams, level)
}

// checkTokensAvailable checks that the expected number of tokens are available
// across all streams. The expected number of streams and protocol level is
// passed in as an argument, in order to allow switching between v1 and v2 flow
// control.
func (h *flowControlTestHelper) checkTokensAvailable(
	ctx context.Context,
	expStreamCount, serverIdx int,
	expTokensStream kvflowinspectpb.Stream,
	streams []kvflowinspectpb.Stream,
	level kvflowcontrol.V2EnabledWhenLeaderLevel,
) error {
	if len(streams) != expStreamCount {
		return fmt.Errorf("expected %d replication streams, got %d [%+v]", expStreamCount, len(streams), streams)
	}

	checkTokens := func(
		expTokens, actualTokens int64,
		stream kvflowcontrol.Stream,
		typName string,
	) error {
		if actualTokens != expTokens {
			return fmt.Errorf("expected %v of %v flow tokens for %v, got %v [level=%v stream=%v]",
				humanize.IBytes(uint64(expTokens)), typName, stream,
				humanize.IBytes(uint64(actualTokens)),
				level,
				streams,
			)
		}
		return nil
	}

	for _, stream := range streams {
		s := kvflowcontrol.Stream{
			TenantID: stream.TenantID,
			StoreID:  stream.StoreID,
		}
		if err := checkTokens(
			expTokensStream.AvailableEvalRegularTokens, stream.AvailableEvalRegularTokens, s, "regular eval",
		); err != nil {
			return err
		}
		if err := checkTokens(
			expTokensStream.AvailableEvalElasticTokens, stream.AvailableEvalElasticTokens, s, "elastic eval",
		); err != nil {
			return err
		}
		if level > kvflowcontrol.V2NotEnabledWhenLeader {
			// V2 flow control also has send tokens.
			if err := checkTokens(
				expTokensStream.AvailableSendRegularTokens, stream.AvailableSendRegularTokens, s, "regular send",
			); err != nil {
				return err
			}
			if err := checkTokens(
				expTokensStream.AvailableSendElasticTokens, stream.AvailableSendElasticTokens, s, "elastic send",
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *flowControlTestHelper) waitForConnectedStreams(
	ctx context.Context,
	rangeID roachpb.RangeID,
	expConnectedStreams, serverIdx int,
	lvl ...kvflowcontrol.V2EnabledWhenLeaderLevel,
) {
	h.t.Helper()
	level := h.resolveLevelArgs(lvl...)
	testutils.SucceedsSoon(h.t, func() error {
		state, found := h.getInspectHandlesForLevel(serverIdx, level).LookupInspect(rangeID)
		if !found {
			return fmt.Errorf("handle for %s not found", rangeID)
		}
		require.True(h.t, found)
		var connected int
		for i := range state.ConnectedStreams {
			if !state.ConnectedStreams[i].Disconnected {
				connected++
			}
		}
		if len(state.ConnectedStreams) != expConnectedStreams || connected != expConnectedStreams {
			return fmt.Errorf("expected %d connected streams, got %d/%d",
				expConnectedStreams, connected, len(state.ConnectedStreams))
		}
		return nil
	})
}

func (h *flowControlTestHelper) computeTotalTrackedTokens(
	ctx context.Context,
	rangeID roachpb.RangeID,
	serverIdx int,
	lvl ...kvflowcontrol.V2EnabledWhenLeaderLevel,
) (kvflowcontrol.Tokens, error) {
	level := h.resolveLevelArgs(lvl...)
	state, found := h.getInspectHandlesForLevel(serverIdx, level).LookupInspect(rangeID)
	if !found {
		return 0, fmt.Errorf("handle for %s not found", rangeID)
	}
	require.True(h.t, found)
	var totalTracked int64
	for _, stream := range state.ConnectedStreams {
		for _, tracked := range stream.TrackedDeductions {
			totalTracked += tracked.Tokens
		}
	}
	return kvflowcontrol.Tokens(totalTracked), nil
}

func (h *flowControlTestHelper) waitForTotalTrackedTokens(
	ctx context.Context,
	rangeID roachpb.RangeID,
	expTotalTrackedTokens int64,
	serverIdx int,
	lvl ...kvflowcontrol.V2EnabledWhenLeaderLevel,
) {
	testutils.SucceedsSoon(h.t, func() error {
		if totalTracked, err := h.computeTotalTrackedTokens(ctx, rangeID, serverIdx, lvl...); err != nil {
			return err
		} else if totalTracked != kvflowcontrol.Tokens(expTotalTrackedTokens) {
			return fmt.Errorf("expected to track %d tokens in aggregate, got %d",
				kvflowcontrol.Tokens(expTotalTrackedTokens), totalTracked)
		}
		return nil
	})
}

func (h *flowControlTestHelper) waitForTotalTrackedTokensGE(
	ctx context.Context, rangeID roachpb.RangeID, expTotalTrackedTokensGE int64, serverIdx int,
) {
	testutils.SucceedsSoon(h.t, func() error {
		if totalTracked, err := h.computeTotalTrackedTokens(ctx, rangeID, serverIdx); err != nil {
			return err
		} else if totalTracked < kvflowcontrol.Tokens(expTotalTrackedTokensGE) {
			return fmt.Errorf("expected to track >= %d tokens in aggregate, got %d",
				kvflowcontrol.Tokens(expTotalTrackedTokensGE), totalTracked)
		}
		return nil
	})
}

func (h *flowControlTestHelper) waitForTotalTrackedTokensForDuration(
	ctx context.Context,
	rangeID roachpb.RangeID,
	expTotalTrackedTokens int64,
	serverIdx int,
	d time.Duration,
) {
	testutils.SucceedsSoon(h.t, func() error {
		for start := timeutil.Now(); timeutil.Since(start) < d; {
			if totalTracked, err := h.computeTotalTrackedTokens(ctx, rangeID, serverIdx); err != nil {
				return err
			} else if totalTracked != kvflowcontrol.Tokens(expTotalTrackedTokens) {
				return fmt.Errorf("expected to track %d tokens in aggregate, got %d",
					kvflowcontrol.Tokens(expTotalTrackedTokens), totalTracked)
			} else {
				select {
				// Avoid spinning.
				case <-time.After(200 * time.Millisecond):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		return nil
	})
}

func (h *flowControlTestHelper) comment(comment string) {
	if h.buf.Len() > 0 {
		h.buf.WriteString("\n\n")
	}

	comment = strings.TrimSpace(comment)
	h.buf.WriteString(fmt.Sprintf("%s\n", comment))
	h.log(comment)
}

func (h *flowControlTestHelper) log(msg string) {
	if log.ShowLogs() {
		log.Infof(context.Background(), "%s", msg)
	}
}

// resolveLevelArgs resolves the level to use for the test. If the level is
// static, the level is returned as is. If the level is dynamic, the level is
// resolved via arguments if provided, otherwise the default given at
// construction is used. The function verifies that no more than one level is
// provided.
func (h *flowControlTestHelper) resolveLevelArgs(
	level ...kvflowcontrol.V2EnabledWhenLeaderLevel,
) kvflowcontrol.V2EnabledWhenLeaderLevel {
	if h.isStaticLevel {
		// The level is static and should not change during the test via arguments.
		require.Len(h.t, level, 0)
		return h.level
	}
	// The level is dynamic and should be resolved via arguments if provided,
	// otherwise the default given at construction is used. Verify that no more
	// than one level is provided.
	require.Less(h.t, len(level), 2)
	if len(level) == 0 {
		return h.level
	}
	return level[0]
}

// v1FlowTokensQueryStr is the query string to fetch flow tokens metrics from
// the node metrics table. It fetches all flow token metrics available in v1.
const v1FlowTokensQueryStr = `
SELECT
  name,
  crdb_internal.humanize_bytes(value::INT8)
FROM
  crdb_internal.node_metrics
WHERE
  name LIKE '%kvadmission%tokens%'
ORDER BY
  name ASC;
`

const v1FlowPerRangeStreamQueryStr = `
SELECT
  chr(96 + dense_rank() OVER (ORDER BY range_id)) as range_id,
  count(*) AS streams
FROM
  crdb_internal.kv_flow_control_handles
GROUP BY
  range_id
ORDER BY
  range_id;
`

const v2FlowPerRangeStreamQueryStr = `
SELECT
  chr(96 + dense_rank() OVER (ORDER BY range_id)) as range_id,
  count(*) AS streams
FROM
  crdb_internal.kv_flow_control_handles_v2
GROUP BY
  range_id
ORDER BY
  range_id;
`

var flowPerRangeStreamQueryHeaderStrs = []string{"range_id", "stream_count"}

const v1FlowPerStreamTrackedQueryStr = `
SELECT
  chr(96 + dense_rank() OVER (ORDER BY range_id)) as range_id,
  store_id,
  crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
FROM
  crdb_internal.kv_flow_control_handles
ORDER BY
  range_id, store_id;
`

const v2FlowPerStreamTrackedQueryStr = `
SELECT
  chr(96 + dense_rank() OVER (ORDER BY range_id)) as range_id,
  store_id,
  crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
FROM
  crdb_internal.kv_flow_control_handles_v2
ORDER BY
  range_id, store_id;
`

var flowPerStreamTrackedQueryHeaderStrs = []string{
	"range_id", "store_id", "total_tracked_tokens"}

const v2FlowPerStoreDeductionQueryStr = `
SELECT
  chr(96 + dense_rank() OVER (ORDER BY range_id)) as range_id,
  store_id,
  priority,
  crdb_internal.humanize_bytes(tokens::INT8)
FROM
  crdb_internal.kv_flow_token_deductions_v2
ORDER BY
  range_id, store_id;
`

var flowPerStoreDeductionQueryHeaderStrs = []string{
	"range_id", "store_id", "priority", "tokens"}

// v2FlowTokensQueryStr is the query string to fetch flow tokens metrics from
// the node metrics table. It fetches all metrics related to flow control
// tokens, distinct from v1 token metrics which only track eval tokens.
const v2FlowTokensQueryStr = `
SELECT 
  name,
  crdb_internal.humanize_bytes(value::INT8)
FROM 
  crdb_internal.node_metrics
WHERE 
  name LIKE '%kvflowcontrol%tokens%'
ORDER BY
  name ASC;
`

// flowSendQueueQueryStr is the query string to fetch flow control send queue
// metrics from the node metrics table.
const flowSendQueueQueryStr = `
SELECT 
  name,
  crdb_internal.humanize_bytes(value::INT8)
FROM 
  crdb_internal.node_metrics
WHERE 
  name LIKE '%kvflowcontrol%send_queue%'
  AND name != 'kvflowcontrol.send_queue.count'
ORDER BY
  name ASC;
`

// flowPerStoreTokenQueryStr is the query string to fetch per-store flow tokens
// metrics from the kv_flow_controller_v2 table.
const flowPerStoreTokenQueryStr = `
SELECT
  store_id,
  crdb_internal.humanize_bytes(available_eval_regular_tokens),
  crdb_internal.humanize_bytes(available_eval_elastic_tokens),
  crdb_internal.humanize_bytes(available_send_regular_tokens),
  crdb_internal.humanize_bytes(available_send_elastic_tokens)
FROM
  crdb_internal.kv_flow_controller_v2
ORDER BY
  store_id ASC;
`

// flowPerStoreTokenQueryHeaderStrs are the headers for the per-store flow
// token query.
var flowPerStoreTokenQueryHeaderStrs = []string{
	"store_id",
	"eval_regular_available", "eval_elastic_available",
	"send_regular_available", "send_elastic_available",
}

// query runs the given SQL query against the given SQLRunner, and appends the
// output to the testdata file buffer.
func (h *flowControlTestHelper) query(runner *sqlutils.SQLRunner, sql string, headers ...string) {
	// NB: We update metric gauges here to ensure that periodically updated
	// metrics (via the node metrics loop) are up-to-date.
	for idx, server := range h.tc.Servers {
		if h.tc.ServerStopped(idx) {
			// The test has explicitly stopped this server, so we should skip it.
			continue
		}
		require.NoError(h.t, server.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
			_, err := s.ComputeMetricsPeriodically(context.Background(), nil, 0)
			require.NoError(h.t, err)
			s.GetStoreConfig().KVFlowStreamTokenProvider.UpdateMetricGauges()
			return nil
		}))
	}
	sql = strings.TrimSpace(sql)
	h.log(sql)
	h.buf.WriteString(fmt.Sprintf("%s\n\n", sql))

	rows := runner.Query(h.t, sql)
	tbl := tablewriter.NewWriter(h.buf)
	output, err := sqlutils.RowsToStrMatrix(rows)
	require.NoError(h.t, err)
	tbl.SetAlignment(tablewriter.ALIGN_LEFT)
	tbl.AppendBulk(output)
	tbl.SetBorder(false)
	tbl.SetHeader(headers)
	tbl.SetAutoFormatHeaders(false)
	tbl.Render()
}

type testingSendResult struct {
	serverIdx int
	*kvpb.Error
	*kvpb.BatchResponse
}

// putAsync issues a put request for the given key at the priority specified,
// against the first server in the cluster. Unlike put, this function does not
// wait for the request to be processed. It returns a cancel function and a
// channel to receive the result of the request(s).
func (h *flowControlTestHelper) putAsync(
	ctx context.Context, key roachpb.Key, size int, pri admissionpb.WorkPriority, serverIdxs ...int,
) (context.CancelFunc, chan testingSendResult) {
	if len(serverIdxs) == 0 {
		// Default to the first server if none are given.
		serverIdxs = []int{0}
	}
	// Generate the values now, so that we don't race on the rand source inside
	// the function below.
	values := make([]roachpb.Value, len(serverIdxs))
	for i := range serverIdxs {
		values[i] = roachpb.MakeValueFromString(randutil.RandString(h.rng, size, randutil.PrintableKeyAlphabet))
	}
	resCh := make(chan testingSendResult, len(serverIdxs))
	cancelCtx, cancel := context.WithCancel(ctx)
	for _, serverIdx := range serverIdxs {
		require.NoError(h.t, h.tc.Server(serverIdx).Stopper().RunAsyncTask(cancelCtx,
			fmt.Sprintf("put-async-idx-%d", serverIdx), func(ctx context.Context) {
				value := values[serverIdx]
				ba := &kvpb.BatchRequest{}
				ba.Add(kvpb.NewPut(key, value))
				ba.AdmissionHeader.Priority = int32(pri)
				ba.AdmissionHeader.Source = kvpb.AdmissionHeader_FROM_SQL
				br, pErr := h.tc.Server(serverIdx).DB().NonTransactionalSender().Send(cancelCtx, ba)
				resCh <- testingSendResult{serverIdx: serverIdx, Error: pErr, BatchResponse: br}
			}))
	}
	return cancel, resCh
}

// put issues a put request for the given key at the priority specified,
// against the first server in the cluster.
func (h *flowControlTestHelper) put(
	ctx context.Context, key roachpb.Key, size int, pri admissionpb.WorkPriority, serverIdxs ...int,
) {
	if len(serverIdxs) == 0 {
		// Default to the first server if none are given.
		serverIdxs = []int{0}
	}
	for _, serverIdx := range serverIdxs {
		value := roachpb.MakeValueFromString(randutil.RandString(h.rng, size, randutil.PrintableKeyAlphabet))
		ba := &kvpb.BatchRequest{}
		ba.Add(kvpb.NewPut(key, value))
		ba.AdmissionHeader.Priority = int32(pri)
		ba.AdmissionHeader.Source = kvpb.AdmissionHeader_FROM_SQL
		if _, pErr := h.tc.Server(serverIdx).DB().NonTransactionalSender().Send(
			ctx, ba,
		); pErr != nil {
			h.t.Fatal(pErr.GoError())
		}
	}
}

// close writes the buffer to a file in the testdata directory and compares it
// against the expected output.
func (h *flowControlTestHelper) close(filename string) {
	echotest.Require(
		h.t.(*testing.T), h.buf.String(), datapathutils.TestDataPath(h.t, h.testdata, filename))
}

func (h *flowControlTestHelper) getInspectHandlesForLevel(
	serverIdx int, level kvflowcontrol.V2EnabledWhenLeaderLevel,
) kvflowcontrol.InspectHandles {
	switch level {
	case kvflowcontrol.V2NotEnabledWhenLeader:
		return h.tc.Server(serverIdx).KVFlowHandles().(kvflowcontrol.Handles)
	case kvflowcontrol.V2EnabledWhenLeaderV1Encoding, kvflowcontrol.V2EnabledWhenLeaderV2Encoding:
		return kvserver.MakeStoresForRACv2(h.tc.Server(serverIdx).GetStores().(*kvserver.Stores))
	default:
		h.t.Fatalf("unknown level: %v", level)
	}
	panic("unreachable")
}

// enableVerboseRaftMsgLoggingForRange installs a raft handler on each node,
// which in turn enables verbose message logging.
func (h *flowControlTestHelper) enableVerboseRaftMsgLoggingForRange(rangeID roachpb.RangeID) {
	for i := 0; i < len(h.tc.Servers); i++ {
		si, err := h.tc.Server(i).GetStores().(*kvserver.Stores).GetStore(h.tc.Server(i).GetFirstStoreID())
		require.NoError(h.t, err)
		h.tc.Servers[i].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(si.StoreID(),
			&unreliableRaftHandler{
				rangeID:                    rangeID,
				IncomingRaftMessageHandler: si,
				unreliableRaftHandlerFuncs: noopRaftHandlerFuncs(),
			})
	}
}

func (h *flowControlTestHelper) resetV2TokenMetrics(ctx context.Context) {
	for _, server := range h.tc.Servers {
		require.NoError(h.t, server.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
			s.GetStoreConfig().KVFlowStreamTokenProvider.Metrics().(*rac2.TokenMetrics).TestingClear()
			_, err := s.ComputeMetricsPeriodically(ctx, nil, 0)
			require.NoError(h.t, err)
			s.GetStoreConfig().KVFlowStreamTokenProvider.UpdateMetricGauges()
			return nil
		}))
	}
}

// makeV2EnabledTestFileName is a utility function which returns an updated
// filename for the testdata file based on the v2EnabledWhenLeaderLevel.
func makeV2EnabledTestFileName(
	v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel,
	mode kvflowcontrol.ModeT,
	filename string,
) string {
	var enabledPart string
	switch v2EnabledWhenLeaderLevel {
	case kvflowcontrol.V2NotEnabledWhenLeader:
		panic("unused")
	case kvflowcontrol.V2EnabledWhenLeaderV1Encoding:
		enabledPart = "_v1_encoding"
	case kvflowcontrol.V2EnabledWhenLeaderV2Encoding:
		enabledPart = "_v2_encoding"
	default:
		panic("unknown v2EnabledWhenLeaderLevel")
	}
	return filename + enabledPart + "_" + mode.String()
}

func BenchmarkFlowControlV2Basic(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	testutils.RunValues(b, "v2_enabled_when_leader_level", []kvflowcontrol.V2EnabledWhenLeaderLevel{
		kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		kvflowcontrol.V2EnabledWhenLeaderV2Encoding,
	}, func(b *testing.B, v2EnabledWhenLeaderLevel kvflowcontrol.V2EnabledWhenLeaderLevel) {
		testutils.RunValues(b, "kvadmission.flow_control.mode", []kvflowcontrol.ModeT{
			kvflowcontrol.ApplyToElastic,
			kvflowcontrol.ApplyToAll,
		}, func(b *testing.B, mode kvflowcontrol.ModeT) {
			ctx := context.Background()
			settings := cluster.MakeTestingClusterSettings()
			tc := testcluster.StartTestCluster(b, 3, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: settings,
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
								UseOnlyForScratchRanges: true,
								OverrideV2EnabledWhenLeaderLevel: func() kvflowcontrol.V2EnabledWhenLeaderLevel {
									return v2EnabledWhenLeaderLevel
								},
								OverrideTokenDeduction: func(_ kvflowcontrol.Tokens) kvflowcontrol.Tokens {
									// This test makes use of (small) increment requests, but
									// wants to see large token deductions/returns.
									return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
								},
							},
						},
						AdmissionControl: &admission.TestingKnobs{
							DisableWorkQueueFastPath: false,
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			// Set up the benchmark state with 3 voters, one on each of the three
			// node/stores.
			k := tc.ScratchRange(b)
			tc.AddVotersOrFatal(b, k, tc.Targets(1, 2)...)
			h := newFlowControlTestHelperV2(b, tc, v2EnabledWhenLeaderLevel)
			h.init(mode)

			desc, err := tc.LookupRange(k)
			require.NoError(b, err)
			h.waitForConnectedStreams(ctx, desc.RangeID, 3, 0 /* serverIdx */)

			incArgs := incrementArgs(k, int64(1))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := kv.SendWrappedWithAdmission(
					ctx, tc.Server(0).DB().NonTransactionalSender(), kvpb.Header{},
					kvpb.AdmissionHeader{
						Priority: int32(testFlowModeToPri(mode)),
						Source:   kvpb.AdmissionHeader_FROM_SQL,
					}, incArgs); err != nil {
					b.Fatal(err)
				}
			}
			h.waitForAllTokensReturned(ctx, 3, 0 /* serverIdx */)
			b.StopTimer()
		})
	})
}

func setRACv2DebugVModule(t *testing.T) (reset func()) {
	t.Helper()
	old := log.GetVModule()
	require.NoError(t, log.SetVModule("replica_raft=1,replica_proposal_buf=1,"+
		"raft_transport=2,kvadmission=1,work_queue=1,replica_flow_control=1,"+
		"tracker=1,client_raft_helpers_test=1,range_controller=2,"+
		"token_counter=2,token_tracker=2,processor=2",
	))
	return func() { require.NoError(t, log.SetVModule(old)) }
}
