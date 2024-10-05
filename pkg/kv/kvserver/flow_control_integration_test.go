// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"github.com/stretchr/testify/require"
)

// Tests in this file use the echotest library, printing out progressive state
// in files under testdata/flow_control_integration/*. Every
// TestFlowControlFileName maps to testdata/flow_control_integration/file_name
// -- running TestFlowControl* will match everything. It's instructive to run
// these tests with the following vmodule, as of 04/23:
//
//    --vmodule='replica_raft=1,kvflowcontroller=2,replica_proposal_buf=1,
//               raft_transport=2,kvflowdispatch=1,kvadmission=1,
//               kvflowhandle=1,work_queue=1,replica_flow_control=1,
//               tracker=1,client_raft_helpers_test=1'
//
// TODO(irfansharif): Add end-to-end tests for the following:
// - [ ] Node with full RaftTransport receive queue (I8).
// - [ ] Node with full RaftTransport send queue, with proposals dropped (I8).
// - [ ] Raft commands getting reproposed, either due to timeouts or not having
//       the right MLAI. See TestReplicaRefreshPendingCommandsTicks,
//       TestLogGrowthWhenRefreshingPendingCommands. I7.
// - [ ] Raft proposals getting dropped/abandoned. See
//       (*Replica).cleanupFailedProposalLocked and its uses.

// TestFlowControlBasic runs a basic end-to-end test of the kvflowcontrol
// machinery, replicating + admitting a single 1MiB regular write.
func TestFlowControlBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "always-enqueue", func(t *testing.T, alwaysEnqueue bool) {
		ctx := context.Background()
		const numNodes = 3
		tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
							UseOnlyForScratchRanges: true,
						},
					},
					AdmissionControl: &admission.TestingKnobs{
						DisableWorkQueueFastPath: alwaysEnqueue,
					},
				},
			},
		})
		defer tc.Stopper().Stop(ctx)

		k := tc.ScratchRange(t)
		tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

		desc, err := tc.LookupRange(k)
		require.NoError(t, err)

		for i := 0; i < numNodes; i++ {
			si, err := tc.Server(i).GetStores().(*kvserver.Stores).GetStore(tc.Server(i).GetFirstStoreID())
			require.NoError(t, err)
			tc.Servers[i].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(si.StoreID(),
				&unreliableRaftHandler{
					rangeID:                    desc.RangeID,
					IncomingRaftMessageHandler: si,
					unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
						dropReq: func(req *kvserverpb.RaftMessageRequest) bool {
							// Install a raft handler to get verbose raft logging.
							//
							// TODO(irfansharif): Make this a more ergonomic
							// testing knob instead.
							return false
						},
					},
				})
		}

		n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

		h := newFlowControlTestHelper(t, tc)
		h.init()
		defer h.close("basic") // this test behaves identically with or without the fast path

		h.waitForConnectedStreams(ctx, desc.RangeID, 3)

		h.comment(`-- Flow token metrics, before issuing the regular 1MiB replicated write.`)
		h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

		h.comment(`-- (Issuing + admitting a regular 1MiB, triply replicated write...)`)
		h.log("sending put request")
		h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)
		h.log("sent put request")

		h.waitForAllTokensReturned(ctx, 3)
		h.comment(`
-- Stream counts as seen by n1 post-write. We should see three {regular,elastic}
-- streams given there are three nodes and we're using a replication factor of
-- three.
`)
		h.query(n1, `
  SELECT name, value
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission.flow_controller%stream%'
ORDER BY name ASC;
`)

		h.comment(`-- Another view of the stream count, using /inspectz-backed vtables.`)
		h.query(n1, `
  SELECT range_id, count(*) AS streams
    FROM crdb_internal.kv_flow_control_handles
GROUP BY (range_id)
ORDER BY streams DESC;
`, "range_id", "stream_count")

		h.comment(`
-- Flow token metrics from n1 after issuing the regular 1MiB replicated write,
-- and it being admitted on n1, n2 and n3. We should see 3*1MiB = 3MiB of
-- {regular,elastic} tokens deducted and returned, and {8*3=24MiB,16*3=48MiB} of
-- {regular,elastic} tokens available. Everything should be accounted for.
`)
		h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

		// When run using -v the vmodule described at the top of this file, this
		// test demonstrates end-to-end flow control machinery in the happy
		// path.
		//
		// 1. The request gets admitted for flow tokens for each of the three
		//    replication streams (one per replica).
		//
		//   [T1,n1] admitted request (pri=normal-pri stream=t1/s1 tokens=+16 MiB wait-duration=18.834µs mode=apply_to_all)
		//   [T1,n1] admitted request (pri=normal-pri stream=t1/s2 tokens=+16 MiB wait-duration=1.042µs mode=apply_to_all)
		//   [T1,n1] admitted request (pri=normal-pri stream=t1/s3 tokens=+16 MiB wait-duration=834ns mode=apply_to_all)
		//
		// 2. We encode the raft metadata as part of the raft command. At the
		//    proposer, we track 1 MiB of flow token deductions for each stream,
		//    using the log position the proposal is to end up in.
		//
		//   [T1,n1,s1,r64/1:/{Table/Max-Max}] encoded raft admission meta: pri=normal-pri create-time=1685129503765352000 proposer=n1
		//   [T1,n1,s1,r64/1:/{Table/Max-Max},raft] bound index/log terms for proposal entry: 6/20 EntryNormal <omitted>
		//   [T1,n1,s1,r64/1:/{Table/Max-Max}] 279  tracking +1.0 MiB flow control tokens for pri=normal-pri stream=t1/s1 pos=log-position=6/20
		//   [T1,n1,s1,r64/1:/{Table/Max-Max}] 280  adjusted flow tokens (pri=normal-pri stream=t1/s1 delta=-1.0 MiB): regular=+15 MiB elastic=+7.0 MiB
		//   [T1,n1,s1,r64/1:/{Table/Max-Max}] 281  tracking +1.0 MiB flow control tokens for pri=normal-pri stream=t1/s2 pos=log-position=6/20
		//   [T1,n1,s1,r64/1:/{Table/Max-Max}] 282  adjusted flow tokens (pri=normal-pri stream=t1/s2 delta=-1.0 MiB): regular=+15 MiB elastic=+7.0 MiB
		//   [T1,n1,s1,r64/1:/{Table/Max-Max}] 283  tracking +1.0 MiB flow control tokens for pri=normal-pri stream=t1/s3 pos=log-position=6/20
		//   [T1,n1,s1,r64/1:/{Table/Max-Max}] 284  adjusted flow tokens (pri=normal-pri stream=t1/s3 delta=-1.0 MiB): regular=+15 MiB elastic=+7.0 MiB
		//
		// 3. We decode the raft metadata below-raft on each of the replicas.
		// The local replica does it first, either enqueues it in below-raft
		// work queues or just fast path if tokens are available, informs itself
		// of entries being admitted, and releases the corresponding flow
		// tokens.
		//
		//   [T1,n1,s1,r64/1:/{Table/Max-Max},raft] decoded raft admission meta below-raft: pri=normal-pri create-time=1685129503765352000 proposer=n1 receiver=[n1,s1] tenant=t1 tokens≈+1.0 MiB sideloaded=false raft-entry=6/20
		//   Fast-path:
		//      [T1,n1,s1,r64/1:/{Table/Max-Max},raft] fast-path: admitting t1 pri=normal-pri r64 origin=n1 log-position=6/20 ingested=false
		//   Async-path:
		//      [T1,n1,s1,r64/1:/{Table/Max-Max},raft] async-path: len(waiting-work)=1: enqueued t1 pri=normal-pri r64 origin=n1 log-position=6/20 ingested=false
		//      [T1,n1] async-path: len(waiting-work)=0 dequeued t1 pri=normal-pri r64 origin=n1 log-position=6/20 ingested=false
		//   [T1,n1] dispatching admitted-entries (r64 s1 pri=normal-pri up-to-log-position=6/20) to n1
		//   [T1,n1] released +1.0 MiB flow control tokens for 1 out of 1 tracked deductions for pri=normal-pri stream=t1/s1, up to log-position=6/20; 0 tracked deduction(s) remain
		//   [T1,n1] adjusted flow tokens (pri=normal-pri stream=t1/s1 delta=+1.0 MiB): regular=+16 MiB elastic=+8.0 MiB
		//
		//
		// 4. We propagate MsgApps with encoded raft metadata to the follower
		//    replicas on n2 and n3. Each of them similarly decode, virtually
		//    enqueue/use the fast path, admit, and inform the origin node (n1)
		//    of admission. Below we just show the fast path.
		//
		//   [T1,n2] [raft] r64 Raft message 1->2 MsgApp Term:6 Log:6/19 Commit:19 Entries:[6/20 EntryNormal <omitted>]
		//   [T1,n3] [raft] r64 Raft message 1->3 MsgApp Term:6 Log:6/19 Commit:19 Entries:[6/20 EntryNormal <omitted>]
		//
		//   [T1,n2,s2,r64/2:/{Table/Max-Max},raft] decoded raft admission meta below-raft: pri=normal-pri create-time=1685129503765352000 proposer=n1 receiver=[n2,s2] tenant=t1 tokens≈+1.0 MiB sideloaded=false raft-entry=6/20
		//   [T1,n3,s3,r64/3:/{Table/Max-Max},raft] decoded raft admission meta below-raft: pri=normal-pri create-time=1685129503765352000 proposer=n1 receiver=[n3,s3] tenant=t1 tokens≈+1.0 MiB sideloaded=false raft-entry=6/20
		//   [T1,n2,s2,r64/2:/{Table/Max-Max},raft] 294  fast-path: admitting t1 pri=normal-pri r64 origin=n1 log-position=6/20 ingested=false
		//   [T1,n3,s3,r64/3:/{Table/Max-Max},raft] 298  fast-path: admitting t1 pri=normal-pri r64 origin=n1 log-position=6/20 ingested=false
		//   [T1,n2] dispatching admitted-entries (r64 s2 pri=normal-pri up-to-log-position=6/20) to n1
		//   [T1,n3] dispatching admitted-entries (r64 s3 pri=normal-pri up-to-log-position=6/20) to n1
		//
		// 5. On MsgAppResps (or really any raft messages bound for n1), we
		//    piggyback these token dispatches. n1, on hearing about them,
		//    returns relevant tokens back to the stream.
		//
		//    [T1,n2] informing n1 of below-raft admitted-entries (r64 s2 pri=normal-pri up-to-log-position=6/20): 1 out of 1 dispatches
		//    [T1,n1] [raft] r64 Raft message 2->1 MsgAppResp Term:6 Log:0/20
		//    [T1,n1] released +1.0 MiB flow control tokens for 1 out of 1 tracked deductions for pri=normal-pri stream=t1/s2, up to log-position=6/20; 0 tracked deduction(s) remain
		//    [T1,n1] adjusted flow tokens (pri=normal-pri stream=t1/s2 delta=+1.0 MiB): regular=+16 MiB elastic=+8.0 MiB
		//    [T1,n3] informing n1 of below-raft admitted-entries (r64 s3 pri=normal-pri up-to-log-position=6/20): 1 out of 1 dispatches
		//    [T1,n1] [raft] r64 Raft message 3->1 MsgAppResp Term:6 Log:0/20
		//    [T1,n1] released +1.0 MiB flow control tokens for 1 out of 1 tracked deductions for pri=normal-pri stream=t1/s3, up to log-position=6/20; 0 tracked deduction(s) remain
		//    [T1,n1] adjusted flow tokens (pri=normal-pri stream=t1/s3 delta=+1.0 MiB): regular=+16 MiB elastic=+8.0 MiB
	})
}

// TestFlowControlRangeSplitMerge walks through what happens to flow tokens when
// a range splits/merges.
func TestFlowControlRangeSplitMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 3
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

	n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	h := newFlowControlTestHelper(t, tc)
	h.init()
	defer h.close("split_merge")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)

	h.waitForConnectedStreams(ctx, desc.RangeID, 3)
	h.log("sending put request to pre-split range")
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)
	h.log("sent put request to pre-split range")

	h.waitForAllTokensReturned(ctx, 3)
	h.comment(`
-- Flow token metrics from n1 after issuing + admitting the regular 1MiB 3x
-- replicated write to the pre-split range. There should be 3MiB of
-- {regular,elastic} tokens {deducted,returned}.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- (Splitting range.)`)
	left, right := tc.SplitRangeOrFatal(t, k.Next())
	h.waitForConnectedStreams(ctx, right.RangeID, 3)
	// [T1,n1,s1,r63/1:/{Table/62-Max},*kvpb.AdminSplitRequest] initiating a split of this range at key /Table/Max [r64] (manual)
	// [T1,n1,s1,r64/1:/{Table/Max-Max},raft] connected to stream: t1/s1
	// [T1,n1,s1,r64/1:/{Table/Max-Max},raft] connected to stream: t1/s2
	// [T1,n1,s1,r64/1:/{Table/Max-Max},raft] connected to stream: t1/s3

	h.log("sending 2MiB put request to post-split LHS")
	h.put(ctx, k, 2<<20 /* 2MiB */, admissionpb.NormalPri)
	h.log("sent 2MiB put request to post-split LHS")

	h.log("sending 3MiB put request to post-split RHS")
	h.put(ctx, roachpb.Key(right.StartKey), 3<<20 /* 3MiB */, admissionpb.NormalPri)
	h.log("sent 3MiB put request to post-split RHS")

	h.waitForAllTokensReturned(ctx, 3)
	h.comment(`
-- Flow token metrics from n1 after further issuing 2MiB and 3MiB writes to
-- post-split LHS and RHS ranges respectively. We should see 15MiB extra tokens
-- {deducted,returned}, which comes from (2MiB+3MiB)*3=15MiB. So we stand at
-- 3MiB+15MiB=18MiB now.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- Observe the newly split off replica, with its own three streams.`)
	h.query(n1, `
  SELECT range_id, count(*) AS streams
    FROM crdb_internal.kv_flow_control_handles
GROUP BY (range_id)
ORDER BY streams DESC;
`, "range_id", "stream_count")

	h.comment(`-- (Merging ranges.)`)
	merged := tc.MergeRangesOrFatal(t, left.StartKey.AsRawKey())

	// [T1,n1,s1,r64/1:{/Table/Max-\xfa\x00},*kvpb.AdminMergeRequest] initiating a merge of r65:{\xfa\x00-/Max} [(n1,s1):1, (n2,s2):2, (n3,s3):3, next=4, gen=6, sticky=9223372036.854775807,2147483647] into this range (manual)
	// [T1,n1,s1,r64/1:{/Table/Max-\xfa\x00},raft] 380  removing replica r65/1
	// [T1,n2,s2,r64/2:{/Table/Max-\xfa\x00},raft] 385  removing replica r65/2
	// [T1,n3,s3,r64/3:{/Table/Max-\xfa\x00},raft] 384  removing replica r65/3
	// [T1,n1,s1,r65/1:{\xfa\x00-/Max},raft] disconnected stream: t1/s1
	// [T1,n1,s1,r65/1:{\xfa\x00-/Max},raft] disconnected stream: t1/s2
	// [T1,n1,s1,r65/1:{\xfa\x00-/Max},raft] disconnected stream: t1/s3

	h.log("sending 4MiB put request to post-merge range")
	h.put(ctx, roachpb.Key(merged.StartKey), 4<<20 /* 4MiB */, admissionpb.NormalPri)
	h.log("sent 4MiB put request to post-merged range")

	h.waitForAllTokensReturned(ctx, 3)
	h.comment(`
-- Flow token metrics from n1 after issuing 4MiB of regular replicated writes to
-- the post-merged range. We should see 12MiB extra tokens {deducted,returned},
-- which comes from 4MiB*3=12MiB. So we stand at 18MiB+12MiB=30MiB now.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- Observe only the merged replica with its own three streams.`)
	h.query(n1, `
  SELECT range_id, count(*) AS streams
    FROM crdb_internal.kv_flow_control_handles
GROUP BY (range_id)
ORDER BY streams DESC;
`, "range_id", "stream_count")
}

// TestFlowControlBlockedAdmission tests token tracking behavior by explicitly
// blocking below-raft admission.
func TestFlowControlBlockedAdmission(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 3
	var disableWorkQueueGranting atomic.Bool
	disableWorkQueueGranting.Store(true)

	st := cluster.MakeTestingClusterSettings()
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
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

	h := newFlowControlTestHelper(t, tc)
	h.init()
	defer h.close("blocked_admission")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3)

	h.comment(`-- (Issuing regular 1MiB, 3x replicated write that's not admitted.)`)
	h.log("sending put requests")
	for i := 0; i < 5; i++ {
		h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)
	}
	h.log("sent put requests")

	h.comment(`
-- Flow token metrics from n1 after issuing 5 regular 1MiB 3x replicated writes
-- that are yet to get admitted. We see 5*1MiB*3=15MiB deductions of
-- {regular,elastic} tokens with no corresponding returns.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- Observe the total tracked tokens per-stream on n1.`)
	h.query(n1, `
  SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
    FROM crdb_internal.kv_flow_control_handles
`, "range_id", "store_id", "total_tracked_tokens")

	h.comment(`-- Observe the individual tracked tokens per-stream on the scratch range.`)
	h.query(n1, `
  SELECT range_id, store_id, priority, crdb_internal.humanize_bytes(tokens::INT8)
    FROM crdb_internal.kv_flow_token_deductions
`, "range_id", "store_id", "priority", "tokens")

	h.comment(`-- (Allow below-raft admission to proceed.)`)
	disableWorkQueueGranting.Store(false)
	h.waitForAllTokensReturned(ctx, 3) // wait for admission

	h.comment(`-- Observe flow token dispatch metrics from n1.`)
	h.query(n1, `
  SELECT name, value
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission.flow_token_dispatch.local_regular%'
ORDER BY name ASC;
`)

	h.comment(`-- Observe flow token dispatch metrics from n2.`)
	h.query(n2, `
  SELECT name, value
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission.flow_token_dispatch.remote_regular%'
ORDER BY name ASC;
`)

	h.comment(`
-- Flow token metrics from n1 after work gets admitted. We see 15MiB returns of
-- {regular,elastic} tokens, and the available capacities going back to what
-- they were.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)
}

// TestFlowControlAdmissionPostSplitMerge walks through what happens with flow
// tokens when a range after undergoes splits/merges. It does this by blocking
// and later unblocking below-raft admission, verifying:
// - tokens for the RHS are released at the post-merge subsuming leaseholder,
// - admission for the RHS post-merge does not cause a double return of tokens,
// - admission for the LHS can happen post-merge,
// - admission for the LHS and RHS can happen post-split.
func TestFlowControlAdmissionPostSplitMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 3

	var disableWorkQueueGranting atomic.Bool
	disableWorkQueueGranting.Store(true)

	st := cluster.MakeTestingClusterSettings()
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
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

	h := newFlowControlTestHelper(t, tc)
	h.init()
	defer h.close("admission_post_split_merge")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)

	h.waitForConnectedStreams(ctx, desc.RangeID, 3)

	h.log("sending put request to pre-split range")
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)
	h.put(ctx, k.Next(), 1<<20 /* 1MiB */, admissionpb.NormalPri)
	h.log("sent put request to pre-split range")

	h.comment(`
-- Flow token metrics from n1 after issuing a regular 2*1MiB 3x replicated write
-- that are yet to get admitted. We see 2*3*1MiB=6MiB deductions of
-- {regular,elastic} tokens with no corresponding returns. The 2*1MiB writes
-- happened on what is soon going to be the LHS and RHS of a range being split.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- (Splitting range.)`)
	left, right := tc.SplitRangeOrFatal(t, k.Next())
	h.waitForConnectedStreams(ctx, right.RangeID, 3)

	h.log("sending 2MiB put request to post-split LHS")
	h.put(ctx, k, 2<<20 /* 2MiB */, admissionpb.NormalPri)
	h.log("sent 2MiB put request to post-split LHS")

	h.log("sending 3MiB put request to post-split RHS")
	h.put(ctx, roachpb.Key(right.StartKey), 3<<20 /* 3MiB */, admissionpb.NormalPri)
	h.log("sent 3MiB put request to post-split RHS")

	h.comment(`
-- Flow token metrics from n1 after further issuing 2MiB and 3MiB writes to
-- post-split LHS and RHS ranges respectively. We should see 15MiB extra tokens
-- deducted which comes from (2MiB+3MiB)*3=15MiB. So we stand at
-- 6MiB+15MiB=21MiB now.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- Observe the newly split off replica, with its own three streams.`)
	h.query(n1, `
  SELECT range_id, count(*) AS streams
    FROM crdb_internal.kv_flow_control_handles
GROUP BY (range_id)
ORDER BY streams DESC;
`, "range_id", "stream_count")

	h.comment(`-- (Merging ranges.)`)
	merged := tc.MergeRangesOrFatal(t, left.StartKey.AsRawKey())

	h.log("sending 4MiB put request to post-merge range")
	h.put(ctx, roachpb.Key(merged.StartKey), 4<<20 /* 4MiB */, admissionpb.NormalPri)
	h.log("sent 4MiB put request to post-merged range")

	h.comment(`
-- Flow token metrics from n1 after issuing 4MiB of regular replicated writes to
-- the post-merged range. We should see 12MiB extra tokens deducted which comes
-- from 4MiB*3=12MiB. So we stand at 21MiB+12MiB=33MiB tokens deducted now. The
-- RHS of the range is gone now, and the previously 3*3MiB=9MiB of tokens
-- deducted for it are released at the subsuming LHS leaseholder.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%regular_tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- Observe only the merged replica with its own three streams.`)
	h.query(n1, `
  SELECT range_id, count(*) AS streams
    FROM crdb_internal.kv_flow_control_handles
GROUP BY (range_id)
ORDER BY streams DESC;
`, "range_id", "stream_count")

	h.comment(`-- (Allow below-raft admission to proceed.)`)
	disableWorkQueueGranting.Store(false)
	h.waitForAllTokensReturned(ctx, 3) // wait for admission

	h.comment(`
-- Flow token metrics from n1 after work gets admitted. We see all outstanding
-- {regular,elastic} tokens returned, including those from:
-- - the LHS before the merge, and
-- - the LHS and RHS before the original split.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)
}

// TestFlowControlCrashedNode tests flow token behavior in the presence of
// crashed nodes.
func TestFlowControlCrashedNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 2
	var maintainStreamsForBrokenRaftTransport atomic.Bool

	st := cluster.MakeTestingClusterSettings()
	// See I13 from kvflowcontrol/doc.go. We disable the raft-transport-break
	// mechanism below, and for quiesced ranges, that can effectively disable
	// the last-updated mechanism since quiesced ranges aren't being ticked, and
	// we only check the last-updated state when ticked. So we disable range
	// quiescence.
	kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, true)
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			RaftConfig: base.RaftConfig{
				// Suppress timeout-based elections. This test doesn't want to
				// deal with leadership changing hands.
				RaftElectionTimeoutTicks: 1000000,
				// Reduce the RangeLeaseDuration to speeds up failure detection
				// below.
				RangeLeaseDuration: time.Second,
			},
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
						MaintainStreamsForBrokenRaftTransport: func() bool {
							return maintainStreamsForBrokenRaftTransport.Load()
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
	tc.AddVotersOrFatal(t, k, tc.Targets(1)...)

	n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	h := newFlowControlTestHelper(t, tc)
	h.init()
	defer h.close("crashed_node")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(0))
	h.waitForConnectedStreams(ctx, desc.RangeID, 2)

	h.comment(`-- (Issuing regular 5x1MiB, 2x replicated writes that are not admitted.)`)
	h.log("sending put requests")
	for i := 0; i < 5; i++ {
		h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)
	}
	h.log("sent put requests")

	h.comment(`
-- Flow token metrics from n1 after issuing 5 regular 1MiB 2x replicated writes
-- that are yet to get admitted. We see 5*1MiB*2=10MiB deductions of
-- {regular,elastic} tokens with no corresponding returns.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)
	h.comment(`-- Observe the per-stream tracked tokens on n1, before n2 is crashed.`)
	h.query(n1, `
  SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
    FROM crdb_internal.kv_flow_control_handles
`, "range_id", "store_id", "total_tracked_tokens")

	// In this test we want to see how we integrate with nodes crashing -- we
	// want to make sure that we return all held tokens when nodes crash.
	// There's a secondary mechanism that would release flow tokens in such
	// cases -- we also release tokens when the RaftTransport breaks. We don't
	// want that to kick in here, so we disable it first. See
	// TestFlowControlRaftTransportBreak where that mechanism is tested instead.
	maintainStreamsForBrokenRaftTransport.Store(true)

	h.comment(`-- (Crashing n2 but disabling the raft-transport-break token return mechanism.)`)
	tc.StopServer(1)
	h.waitForConnectedStreams(ctx, desc.RangeID, 1)

	h.comment(`
-- Observe the per-stream tracked tokens on n1, after n2 crashed. We're no
-- longer tracking the 5MiB held by n2.
`)
	h.query(n1, `
  SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
    FROM crdb_internal.kv_flow_control_handles
`, "range_id", "store_id", "total_tracked_tokens")

	h.comment(`
-- Flow token metrics from n1 after n2 crashed. Observe that we've returned the
-- 5MiB previously held by n2.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)
}

// TestFlowControlRaftSnapshot tests flow token behavior when one replica needs
// to be caught up via raft snapshot.
func TestFlowControlRaftSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numServers int = 5
	stickyServerArgs := make(map[int]base.TestServerArgs)
	var maintainStreamsForBehindFollowers atomic.Bool
	var maintainStreamsForInactiveFollowers atomic.Bool
	var maintainStreamsForBrokenRaftTransport atomic.Bool
	var disableWorkQueueGranting atomic.Bool
	disableWorkQueueGranting.Store(true)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)
	kvflowcontrol.Mode.Override(ctx, &st.SV, int64(kvflowcontrol.ApplyToAll))

	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			Settings: st,
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			RaftConfig: base.RaftConfig{
				// Suppress timeout-based elections. This test doesn't want to
				// deal with leadership changing hands.
				RaftElectionTimeoutTicks: 1000000,
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: server.NewStickyVFSRegistry(),
				},
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
						OverrideTokenDeduction: func() kvflowcontrol.Tokens {
							// This test makes use of (small) increment
							// requests, but wants to see large token
							// deductions/returns.
							return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
						},
						MaintainStreamsForBehindFollowers: func() bool {
							return maintainStreamsForBehindFollowers.Load()
						},
						MaintainStreamsForInactiveFollowers: func() bool {
							return maintainStreamsForInactiveFollowers.Load()
						},
						MaintainStreamsForBrokenRaftTransport: func() bool {
							return maintainStreamsForBrokenRaftTransport.Load()
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
	n4 := sqlutils.MakeSQLRunner(tc.ServerConn(3))

	h := newFlowControlTestHelper(t, tc)
	h.init()
	defer h.close("raft_snapshot")

	store := tc.GetFirstStoreFromServer(t, 0)

	incA := int64(5)
	incB := int64(7)
	incAB := incA + incB

	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

	tc.AddVotersOrFatal(t, k, tc.Targets(3, 4)...)
	repl := store.LookupReplica(roachpb.RKey(k))
	require.NotNil(t, repl)
	h.waitForConnectedStreams(ctx, repl.RangeID, 5)

	// Set up a key to replicate across the cluster. We're going to modify this
	// key and truncate the raft logs from that command after killing one of the
	// nodes to check that it gets the new value after it comes up.
	incArgs := incrementArgs(k, incA)
	if _, err := kv.SendWrappedWithAdmission(ctx, tc.Server(0).DB().NonTransactionalSender(), kvpb.Header{}, kvpb.AdmissionHeader{
		Priority: int32(admissionpb.HighPri),
		Source:   kvpb.AdmissionHeader_FROM_SQL,
	}, incArgs); err != nil {
		t.Fatal(err)
	}

	h.comment(`
-- Flow token metrics from n1 after issuing 1 regular 1MiB 5x replicated write
-- that's not admitted. Since this test is ignoring crashed nodes for token
-- deduction purposes, we see a deduction of 5MiB {regular,elastic} tokens.
	`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
	`)
	h.comment(`
-- Observe the total tracked tokens per-stream on n1. 1MiB is tracked for n1-n5.
	`)
	h.query(n1, `
	 SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
	   FROM crdb_internal.kv_flow_control_handles
	`, "range_id", "store_id", "total_tracked_tokens")

	tc.WaitForValues(t, k, []int64{incA, incA, incA, incA, incA})

	h.comment(`
-- (Killing n2 and n3, but preventing their tokens from being returned +
-- artificially allowing tokens to get deducted.)`)

	// In this test we want to see how we integrate with raft progress state
	// when nodes are down long enough to warrant snapshots (post
	// log-truncation). We want to do this without responding to the node
	// actually being down, which is tested separately in
	// TestFlowControlCrashedNode and TestFlowControlRaftTransportBreak.
	maintainStreamsForInactiveFollowers.Store(true)
	maintainStreamsForBrokenRaftTransport.Store(true)

	// Depending on when the raft group gets ticked, we might notice than
	// replicas on n2 and n3 are behind a bit too soon. Disable it first, and
	// re-enable it right when this test wants to react to raft progress state.
	maintainStreamsForBehindFollowers.Store(true)

	// Now kill stores 1 + 2, increment the key on the other stores and
	// truncate their logs to make sure that when store 1 + 2 comes back up
	// they will require a snapshot from Raft.
	tc.StopServer(1)
	tc.StopServer(2)

	h.comment(`
-- Observe the total tracked tokens per-stream on n1. 1MiB is (still) tracked
-- for n1-n5. Typically n2, n3 would release their tokens, but this test is
-- intentionally suppressing that behavior to observe token returns only once
-- issuing a raft snapshot.
	`)
	h.query(n1, `
	 SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
	   FROM crdb_internal.kv_flow_control_handles
	`, "range_id", "store_id", "total_tracked_tokens")

	h.comment(`
-- (Issuing another 1MiB of 5x replicated writes while n2 and n3 are down and
-- below-raft admission is paused.)
`)
	incArgs = incrementArgs(k, incB)
	if _, err := kv.SendWrappedWithAdmission(ctx, tc.Server(0).DB().NonTransactionalSender(), kvpb.Header{}, kvpb.AdmissionHeader{
		Priority: int32(admissionpb.HighPri),
		Source:   kvpb.AdmissionHeader_FROM_SQL,
	}, incArgs); err != nil {
		t.Fatal(err)
	}

	h.comment(`
-- Flow token metrics from n1 after issuing 1 regular 1MiB 5x replicated write
-- that's not admitted. We'll have deducted another 5*1MiB=5MiB worth of tokens.
-- Normally we wouldn't deduct tokens for n2 and n3 since they're dead (both
-- according to the per-replica last-updated map, and according broken
-- RaftTransport streams). But this test is intentionally suppressing that
-- behavior to observe token returns when sending raft snapshots.
	`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
	`)
	h.comment(`
-- Observe the total tracked tokens per-stream on n1. 2MiB is tracked for n1-n5;
-- see last comment for an explanation why we're still deducting for n2, n3.
`)
	h.query(n1, `
	 SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
	   FROM crdb_internal.kv_flow_control_handles
	`, "range_id", "store_id", "total_tracked_tokens")

	tc.WaitForValues(t, k, []int64{incAB, 0 /* stopped */, 0 /* stopped */, incAB, incAB})

	index := repl.GetLastIndex()
	h.comment(`-- (Truncating raft log.)`)

	// Truncate the log at index+1 (log entries < N are removed, so this
	// includes the increment).
	truncArgs := truncateLogArgs(index+1, repl.GetRangeID())
	if _, err := kv.SendWrappedWithAdmission(ctx, tc.Server(0).DB().NonTransactionalSender(), kvpb.Header{}, kvpb.AdmissionHeader{
		Priority: int32(admissionpb.HighPri),
		Source:   kvpb.AdmissionHeader_FROM_SQL,
	}, truncArgs); err != nil {
		t.Fatal(err)
	}

	// Allow the flow control integration layer to react to raft progress state.
	maintainStreamsForBehindFollowers.Store(false)

	h.comment(`-- (Restarting n2 and n3.)`)
	require.NoError(t, tc.RestartServer(1))
	require.NoError(t, tc.RestartServer(2))

	// Go back to the default kvflowcontrol integration behavior.
	maintainStreamsForInactiveFollowers.Store(false)
	maintainStreamsForBrokenRaftTransport.Store(false)

	tc.WaitForValues(t, k, []int64{incAB, incAB, incAB, incAB, incAB})

	// [T1,n1,s1,r63/1:/{Table/Max-Max},raft] 646  adjusted flow tokens (pri=high-pri stream=t1/s2 delta=+1.0 MiB): regular=+16 MiB elastic=+8.0 MiB
	// [T1,n1,s1,r63/1:/{Table/Max-Max},raft] 647  disconnected stream: t1/s2
	// [T1,n1,s1,r63/1:/{Table/Max-Max},raft] 648  tracked disconnected stream: t1/s2 (reason: not actively replicating)
	// [T1,n1,s1,r63/1:/{Table/Max-Max},raft] 705  adjusted flow tokens (pri=high-pri stream=t1/s3 delta=+1.0 MiB): regular=+16 MiB elastic=+8.0 MiB
	// [T1,n1,s1,r63/1:/{Table/Max-Max},raft] 706  disconnected stream: t1/s3
	// [T1,n1,s1,r63/1:/{Table/Max-Max},raft] 707  tracked disconnected stream: t1/s3 (reason: not actively replicating)

	h.comment(`
-- Flow token metrics from n1 after restarting n2 and n3. We've returned the
-- 2MiB previously held by those nodes (2MiB each). We're reacting to it's raft
-- progress state, noting that since we've truncated our log, we need to catch
-- it up via snapshot. So we release all held tokens.
		`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

	h.comment(`
-- Observe the total tracked tokens per-stream on n1. There's nothing tracked
-- for n2 and n3 anymore.
`)
	h.query(n1, `
 SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
   FROM crdb_internal.kv_flow_control_handles
   WHERE total_tracked_tokens > 0
`, "range_id", "store_id", "total_tracked_tokens")

	h.waitForConnectedStreams(ctx, repl.RangeID, 5)
	// [T1,n1,s1,r63/1:/{Table/Max-Max},raft] 651  connected to stream: t1/s2
	// [T1,n1,s1,r63/1:/{Table/Max-Max},raft] 710  connected to stream: t1/s3

	h.comment(`-- (Allow below-raft admission to proceed.)`)
	disableWorkQueueGranting.Store(false)

	h.waitForAllTokensReturned(ctx, 5)

	h.comment(`-- Observe flow token dispatch metrics from n4.`)
	h.query(n4, `
  SELECT name, value
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission.flow_token_dispatch.pending_nodes%'
ORDER BY name ASC;
`)

	h.comment(`
-- Flow token metrics from n1 after work gets admitted. We see the remaining
-- 6MiB of {regular,elastic} tokens returned.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

	h.comment(`
-- Observe the total tracked tokens per-stream on n1; there should be nothing.
`)
	h.query(n1, `
 SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
   FROM crdb_internal.kv_flow_control_handles
`, "range_id", "store_id", "total_tracked_tokens")

	h.comment(`-- Another view of tokens, using /inspectz-backed vtables.`)
	h.query(n1, `
SELECT store_id,
	   crdb_internal.humanize_bytes(available_regular_tokens),
	   crdb_internal.humanize_bytes(available_elastic_tokens)
  FROM crdb_internal.kv_flow_controller
 ORDER BY store_id ASC;
`, "range_id", "regular_available", "elastic_available")
}

// TestFlowControlRaftTransportBreak tests flow token behavior when the raft
// transport breaks (due to crashed nodes, in this test).
func TestFlowControlRaftTransportBreak(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 3
	var maintainStreamsForInactiveFollowers atomic.Bool

	st := cluster.MakeTestingClusterSettings()
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			RaftConfig: base.RaftConfig{
				// Suppress timeout-based elections. This test doesn't want to
				// deal with leadership changing hands.
				RaftElectionTimeoutTicks: 1000000,
			},
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
						MaintainStreamsForInactiveFollowers: func() bool {
							return maintainStreamsForInactiveFollowers.Load()
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

	h := newFlowControlTestHelper(t, tc)
	h.init()
	defer h.close("raft_transport_break")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(0))
	h.waitForConnectedStreams(ctx, desc.RangeID, 3)

	h.comment(`-- (Issuing regular 5x1MiB, 3x replicated writes that are not admitted.)`)
	h.log("sending put requests")
	for i := 0; i < 5; i++ {
		h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)
	}
	h.log("sent put requests")

	h.comment(`
-- Flow token metrics from n1 after issuing 5 regular 1MiB 3x replicated writes
-- that are yet to get admitted. We see 5*1MiB*3=15MiB deductions of
-- {regular,elastic} tokens with no corresponding returns.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)
	h.comment(`
-- Observe the per-stream tracked tokens on n1, before n2 is crashed.
`)
	h.query(n1, `
  SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
    FROM crdb_internal.kv_flow_control_handles
`, "range_id", "store_id", "total_tracked_tokens")

	// In this test we want to see how we integrate with raft transport
	// breaking. To break the raft transport between n1 and n2, we just kill n2.
	// There's a secondary mechanism that would release flow tokens when a node
	// is down for long enough -- the per-replica last-updated map is used to
	// prune out inactive replicas. We don't want that to kick in, so we disable
	// it first. See TestFlowControlCrashedNode where that mechanism is tested
	// instead.
	maintainStreamsForInactiveFollowers.Store(true)

	h.comment(`-- (Crashing n2 but disabling the last-updated token return mechanism.)`)
	tc.StopServer(1)
	h.waitForConnectedStreams(ctx, desc.RangeID, 2)

	h.comment(`
-- Observe the per-stream tracked tokens on n1, after n2 crashed. We're no
-- longer tracking the 5MiB held by n2 because the raft transport between
-- n1<->n2 is broken.
`)
	h.query(n1, `
  SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
    FROM crdb_internal.kv_flow_control_handles
`, "range_id", "store_id", "total_tracked_tokens")

	h.comment(`
-- Flow token metrics from n1 after n2 crashed. Observe that we've returned the
-- 5MiB previously held by n2.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)
}

// TestFlowControlRaftTransportCulled tests flow token behavior when the raft
// transport streams are culled due to inactivity.
func TestFlowControlRaftTransportCulled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 3
	workerTeardownCh := make(chan roachpb.NodeID, 1)
	markSendQueueAsIdleCh := make(chan roachpb.NodeID)
	var disableWorkerTeardown atomic.Bool

	st := cluster.MakeTestingClusterSettings()
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)

	baseServerArgs := base.TestServerArgs{
		Settings: st,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
					UseOnlyForScratchRanges: true,
					OverrideTokenDeduction: func() kvflowcontrol.Tokens {
						// This test asserts on the exact values of tracked
						// tokens. In non-test code, the tokens deducted are
						// a few bytes off (give or take) from the size of
						// the proposals. We don't care about such
						// differences.
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
	}
	baseServerArgsWithRaftTransportKnobs := baseServerArgs
	baseServerArgsWithRaftTransportKnobs.Knobs.RaftTransport = &kvserver.RaftTransportTestingKnobs{
		MarkSendQueueAsIdleCh: markSendQueueAsIdleCh,
		OnWorkerTeardown: func(nodeID roachpb.NodeID) {
			if disableWorkerTeardown.Load() {
				return
			}
			workerTeardownCh <- nodeID
		},
	}
	serverArgsPerNode := map[int]base.TestServerArgs{
		0: baseServerArgs,
		1: baseServerArgsWithRaftTransportKnobs,
		2: baseServerArgs,
	}

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual,
		ServerArgsPerNode: serverArgsPerNode,
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

	n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	h := newFlowControlTestHelper(t, tc)
	h.init()
	defer h.close("raft_transport_culled")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(0))
	h.waitForConnectedStreams(ctx, desc.RangeID, 3)

	h.comment(`-- (Issuing regular 5x1MiB, 3x replicated writes that are not admitted.)`)
	h.log("sending put requests")
	for i := 0; i < 5; i++ {
		h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)
	}
	h.log("sent put requests")

	h.comment(`
-- Flow token metrics from n1 after issuing 5 regular 1MiB 3x replicated writes
-- that are yet to get admitted. We see 5*1MiB*3=15MiB deductions of
-- {regular,elastic} tokens with no corresponding returns.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)
	h.comment(`
-- Observe the per-stream tracked tokens on n1, before we cull the n1<->n2 raft
-- transport stream out of idleness.
`)
	h.query(n1, `
  SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
    FROM crdb_internal.kv_flow_control_handles
`, "range_id", "store_id", "total_tracked_tokens")

	h.comment(`-- (Marking n2->n1 raft transport gRPC stream as idle.)`)

	select {
	case markSendQueueAsIdleCh <- roachpb.NodeID(1):
	case <-time.After(time.Second):
		t.Fatalf("timed out")
	}
	select {
	case gotNodeID := <-workerTeardownCh:
		require.Equal(t, gotNodeID, roachpb.NodeID(1))
	case <-time.After(time.Second):
		t.Fatalf("timed out")
	}

	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 10<<20 /* 5*1MiB*2=10MiB */)

	h.comment(`
-- Observe the per-stream tracked tokens on n1 after n2->n1 raft transport
-- stream is culled. We're no longer tracking the 5MiB held by n2 because the
-- raft transport between n1<->n2 is broken.
`)
	h.query(n1, `
  SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
    FROM crdb_internal.kv_flow_control_handles
   WHERE total_tracked_tokens > 0
`, "range_id", "store_id", "total_tracked_tokens")

	h.comment(`
-- Flow token metrics from n1 after n2->n1 raft transport stream is culled.
-- Observe that we've returned the 5MiB previously held by n2.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

	disableWorkerTeardown.Store(true)
}

// TestFlowControlRaftMembership tests flow token behavior when the raft
// membership changes.
func TestFlowControlRaftMembership(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)

	const numNodes = 5
	var disableWorkQueueGranting atomic.Bool
	disableWorkQueueGranting.Store(true)
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
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

	h := newFlowControlTestHelper(t, tc)
	h.init()
	defer h.close("raft_membership")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3)

	h.comment(`-- (Issuing 1x1MiB, 3x replicated write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)

	h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB regular 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of regular tokens with
-- no corresponding returns.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%regular_tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- (Adding a voting replica on n4.)`)
	tc.AddVotersOrFatal(t, k, tc.Target(3))
	h.waitForConnectedStreams(ctx, desc.RangeID, 4)

	h.comment(`
-- Observe the total tracked tokens per-stream on n1. s1-s3 should have 1MiB
-- tracked each, and s4 should have none.`)
	h.query(n1, `
  SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
    FROM crdb_internal.kv_flow_control_handles
`, "range_id", "store_id", "total_tracked_tokens")

	h.comment(`-- (Issuing 1x1MiB, 4x replicated write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)

	h.comment(`
-- Observe the individual tracked tokens per-stream on the scratch range. s1-s3
-- should have 2MiB tracked (they've observed 2x1MiB writes), s4 should have
-- 1MiB.
`)
	h.query(n1, `
  SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
    FROM crdb_internal.kv_flow_control_handles
`, "range_id", "store_id", "total_tracked_tokens")

	h.comment(`-- (Removing voting replica from n3.)`)
	tc.RemoveVotersOrFatal(t, k, tc.Target(2))
	h.waitForConnectedStreams(ctx, desc.RangeID, 3)

	h.comment(`-- (Adding non-voting replica to n5.)`)
	tc.AddNonVotersOrFatal(t, k, tc.Target(4))
	h.waitForConnectedStreams(ctx, desc.RangeID, 4)

	h.comment(`-- (Issuing 1x1MiB, 4x replicated write (w/ one non-voter) that's not admitted.`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)

	h.comment(`
-- Observe the individual tracked tokens per-stream on the scratch range. s1-s2
-- should have 3MiB tracked (they've observed 3x1MiB writes), there should be
-- no s3 since it was removed, s4 and s5 should have 2MiB and 1MiB
-- respectively.
`)
	h.query(n1, `
  SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
    FROM crdb_internal.kv_flow_control_handles
`, "range_id", "store_id", "total_tracked_tokens")

	h.comment(`-- (Allow below-raft admission to proceed.)`)
	disableWorkQueueGranting.Store(false)
	h.waitForAllTokensReturned(ctx, 5)

	h.comment(`-- Observe that there no tracked tokens across s1,s2,s4,s5.`)
	h.query(n1, `
  SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
    FROM crdb_internal.kv_flow_control_handles
`, "range_id", "store_id", "total_tracked_tokens")

	h.comment(`
-- Flow token metrics from n1 after work gets admitted. All {regular,elastic}
-- tokens deducted are returned, including from when s3 was removed as a raft
-- member.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)
}

// TestFlowControlRaftMembershipRemoveSelf tests flow token behavior when the
// raft leader removes itself from the raft group.
func TestFlowControlRaftMembershipRemoveSelf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "transfer-lease-first", func(t *testing.T, transferLeaseFirst bool) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		kvflowcontrol.Enabled.Override(ctx, &st.SV, true)

		const numNodes = 4
		var disableWorkQueueGranting atomic.Bool
		disableWorkQueueGranting.Store(true)
		tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings:   st,
				RaftConfig: base.RaftConfig{
					// TODO(irfansharif): The AdminRelocateRange used below can
					// occasionally flake if we suppress timeout-based
					// elections. We get logging of the following form:
					//
					// 	I230507 19:47:03.143463 31 kv/kvserver_test/flow_control_integration_test.go:2065  [-] 349  -- (Replacing current raft leader on n1 in raft group with new n4 replica.)
					//	I230507 19:47:03.153105 5430 kv/kvserver/replica_raftstorage.go:514  [T1,n4,s4,r64/4:/{Table/Max-Max}] 352  applied INITIAL snapshot b8cdcb09 from (n1,s1):1 at applied index 23 (total=1ms data=1.0 MiB ingestion=6@1ms)
					//	I230507 19:47:03.167504 629 kv/kvserver/kvflowcontrol/kvflowhandle/kvflowhandle.go:249  [T1,n1,s1,r64/1:/{Table/Max-Max},raft] 353  connected to stream: t1/s4
					//	W230507 19:47:03.186303 4268 go.etcd.io/raft/v3/raft.go:924  [T1,n4,s4,r64/4:/{Table/Max-Max}] 354  4 cannot campaign at term 6 since there are still 1 pending configuration changes to apply
					//	...
					//	W230507 19:47:18.194829 5507 kv/kvserver/spanlatch/manager.go:559  [T1,n4,s4,r64/4:/{Table/Max-Max}] 378  have been waiting 15s to acquire read latch /Local/Range/Table/Max/RangeDescriptor@0,0, held by write latch /Local/Range/Table/Max/RangeDescriptor@0,0
					//	W230507 19:47:19.082183 5891 kv/kvserver/spanlatch/manager.go:559  [T1,n4,s4,r64/4:/{Table/Max-Max}] 379  have been waiting 15s to acquire read latch /Local/Range/Table/Max/RangeDescriptor@0,0, held by write latch /Local/Range/Table/Max/RangeDescriptor@0,0
					//
					// Followed by range unavailability. Are we relying on the
					// new leader to be able to campaign immediately, in order
					// to release the latch? And we're simultaneously preventing
					// other replicas from calling elections?
					//
					// 	RaftElectionTimeoutTicks: 1000000,
				},
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
							UseOnlyForScratchRanges: true,
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

		h := newFlowControlTestHelper(t, tc)
		h.init()
		defer h.close("raft_membership_remove_self") // this test behaves identically independent of we transfer the lease first

		desc, err := tc.LookupRange(k)
		require.NoError(t, err)

		// Make sure the lease is on n1 and that we're triply connected.
		tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(0))
		h.waitForConnectedStreams(ctx, desc.RangeID, 3)

		h.comment(`-- (Issuing 1x1MiB, 3x replicated write that's not admitted.)`)
		h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)

		h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB regular 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of regular tokens with
-- no corresponding returns.
`)
		h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%regular_tokens%'
ORDER BY name ASC;
`)

		h.comment(`-- (Replacing current raft leader on n1 in raft group with new n4 replica.)`)
		testutils.SucceedsSoon(t, func() error {
			// Relocate range from n1 -> n4.
			if err := tc.Servers[2].DB().
				AdminRelocateRange(
					context.Background(), desc.StartKey.AsRawKey(),
					tc.Targets(1, 2, 3), nil, transferLeaseFirst); err != nil {
				return err
			}
			leaseHolder, err := tc.FindRangeLeaseHolder(desc, nil)
			if err != nil {
				return err
			}
			if leaseHolder.Equal(tc.Target(0)) {
				return errors.Errorf("expected leaseholder to not be on n1")
			}
			return nil
		})
		h.waitForAllTokensReturned(ctx, 3)

		h.comment(`
-- Flow token metrics from n1 after raft leader removed itself from raft group.
-- All {regular,elastic} tokens deducted are returned.
`)
		h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

		h.comment(`-- (Allow below-raft admission to proceed.)`)
		disableWorkQueueGranting.Store(false)
		h.waitForAllTokensReturned(ctx, 3)

		h.comment(`
-- Flow token metrics from n1 after work gets admitted. Tokens were already
-- returned earlier, so there's no change.
`)
		h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)
	})
}

// TestFlowControlClassPrioritization shows how tokens are managed for both
// regular and elastic work. It does so by replicating + admitting a single 1MiB
// {regular,elastic} write.
func TestFlowControlClassPrioritization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 5
	var disableWorkQueueGranting atomic.Bool
	disableWorkQueueGranting.Store(true)

	st := cluster.MakeTestingClusterSettings()
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
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

	h := newFlowControlTestHelper(t, tc)
	h.init()
	defer h.close("class_prioritization")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3)

	h.comment(`-- (Issuing 1x1MiB, 3x replicated elastic write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.BulkNormalPri)

	h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB elastic 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of elastic tokens with
-- no corresponding returns.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- (Issuing 1x1MiB, 3x replicated regular write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)

	h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB regular 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of {regular,elastic}
-- tokens with no corresponding returns.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- (Allow below-raft admission to proceed.)`)
	disableWorkQueueGranting.Store(false)
	h.waitForAllTokensReturned(ctx, 3)

	h.comment(`
-- Flow token metrics from n1 after work gets admitted. All {regular,elastic}
-- tokens deducted are returned.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)
}

// TestFlowControlQuiescedRange tests flow token behavior when ranges are
// quiesced. It ensures that we have timely returns of flow tokens even when
// there's no raft traffic to piggyback token returns on top of.
func TestFlowControlQuiescedRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 3
	var disableWorkQueueGranting atomic.Bool
	var disableFallbackTokenDispatch atomic.Bool
	disableWorkQueueGranting.Store(true)
	disableFallbackTokenDispatch.Store(true)

	st := cluster.MakeTestingClusterSettings()
	kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, false) // override metamorphism
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			RaftConfig: base.RaftConfig{
				// Suppress timeout-based elections. This test doesn't want to
				// deal with leadership changing hands.
				RaftElectionTimeoutTicks: 1000000,
			},
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						OverrideTokenDeduction: func() kvflowcontrol.Tokens {
							// This test asserts on the exact values of tracked
							// tokens. In non-test code, the tokens deducted are
							// a few bytes off (give or take) from the size of
							// the proposals. We don't care about such
							// differences.
							return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
						},
						UseOnlyForScratchRanges: true,
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
						return disableFallbackTokenDispatch.Load()
					},
					DisablePiggyBackedFlowTokenDispatch: func() bool {
						return true // we'll only test using the fallback token mechanism
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

	n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	h := newFlowControlTestHelper(t, tc)
	h.init()
	defer h.close("quiesced_range")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3)

	h.comment(`-- (Issuing 1x1MiB, 3x replicated elastic write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.BulkNormalPri)

	h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB elastic 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of elastic tokens with
-- no corresponding returns.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%elastic_tokens%'
ORDER BY name ASC;
`)

	// Wait for the range to quiesce.
	h.comment(`-- (Wait for range to quiesce.)`)
	testutils.SucceedsSoon(t, func() error {
		for i := range tc.Servers {
			rep := tc.GetFirstStoreFromServer(t, i).LookupReplica(roachpb.RKey(k))
			require.NotNil(t, rep)
			if !rep.IsQuiescent() {
				return errors.Errorf("%s not quiescent", rep)
			}
		}
		return nil
	})

	h.comment(`
-- (Allow below-raft admission to proceed. We've disabled the fallback token
-- dispatch mechanism so no tokens are returned yet -- quiesced ranges don't
-- use the piggy-backed token return mechanism since there's no raft traffic.)`)
	disableWorkQueueGranting.Store(false)

	h.comment(`
-- Flow token metrics from n1 after work gets admitted but fallback token
-- dispatch mechanism is disabled. Deducted elastic tokens from remote stores
-- are yet to be returned. Tokens for the local store are.
`)
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 2<<20 /* 2*1MiB=2MiB */)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%elastic_tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- (Enable the fallback token dispatch mechanism.)`)
	disableFallbackTokenDispatch.Store(false)
	h.waitForAllTokensReturned(ctx, 3)

	h.comment(`
-- Flow token metrics from n1 after work gets admitted and all elastic tokens
-- are returned through the fallback mechanism. 
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%elastic_tokens%'
ORDER BY name ASC;
`)
}

// TestFlowControlUnquiescedRange tests flow token behavior when ranges are
// unquiesced. It's a sort of roundabout test to ensure that flow tokens are
// returned through the raft transport piggybacking mechanism, piggybacking on
// raft heartbeats.
func TestFlowControlUnquiescedRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 3
	var disableWorkQueueGranting atomic.Bool
	var disablePiggybackTokenDispatch atomic.Bool
	disableWorkQueueGranting.Store(true)
	disablePiggybackTokenDispatch.Store(true)

	st := cluster.MakeTestingClusterSettings()
	kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, false) // override metamorphism
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			RaftConfig: base.RaftConfig{
				// Suppress timeout-based elections. This test doesn't want to
				// deal with leadership changing hands or followers unquiescing
				// ranges by calling elections.
				RaftElectionTimeoutTicks: 1000000,
			},
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						OverrideTokenDeduction: func() kvflowcontrol.Tokens {
							// This test asserts on the exact values of tracked
							// tokens. In non-test code, the tokens deducted are
							// a few bytes off (give or take) from the size of
							// the proposals. We don't care about such
							// differences.
							return kvflowcontrol.Tokens(1 << 20 /* 1MiB */)
						},
						UseOnlyForScratchRanges: true,
						MaintainStreamsForInactiveFollowers: func() bool {
							// This test deals with quiesced ranges where
							// followers have no activity. We don't want to
							// disconnect streams due to this inactivity.
							return true
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
						return true // we'll only test using the piggy-back token mechanism
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
	tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)

	n1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	h := newFlowControlTestHelper(t, tc)
	h.init()
	defer h.close("unquiesced_range")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)

	for i := 0; i < numNodes; i++ {
		si, err := tc.Server(i).GetStores().(*kvserver.Stores).GetStore(tc.Server(i).GetFirstStoreID())
		require.NoError(t, err)
		tc.Servers[i].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(si.StoreID(),
			&unreliableRaftHandler{
				rangeID:                    desc.RangeID,
				IncomingRaftMessageHandler: si,
				unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
					dropReq: func(req *kvserverpb.RaftMessageRequest) bool {
						// Install a raft handler to get verbose raft logging.
						//
						// TODO(irfansharif): Make this a more ergonomic
						// testing knob instead.
						return false
					},
				},
			})
	}

	h.waitForConnectedStreams(ctx, desc.RangeID, 3)

	h.comment(`-- (Issuing 1x1MiB, 3x replicated elastic write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.BulkNormalPri)

	h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB elastic 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of elastic tokens with
-- no corresponding returns.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%elastic_tokens%'
ORDER BY name ASC;
`)

	// Wait for the range to quiesce.
	h.comment(`-- (Wait for range to quiesce.)`)
	testutils.SucceedsSoon(t, func() error {
		leader := tc.GetRaftLeader(t, roachpb.RKey(k))
		require.NotNil(t, leader)
		if !leader.IsQuiescent() {
			return errors.Errorf("%s not quiescent", leader)
		}
		return nil
	})

	h.comment(`
-- (Allow below-raft admission to proceed. We've disabled the fallback token
-- dispatch mechanism so no tokens are returned yet -- quiesced ranges don't
-- use the piggy-backed token return mechanism since there's no raft traffic.)`)
	disableWorkQueueGranting.Store(false)

	h.comment(`
-- Flow token metrics from n1 after work gets admitted but fallback token
-- dispatch mechanism is disabled. Deducted elastic tokens from remote stores
-- are yet to be returned. Tokens for the local store are.
`)
	h.waitForTotalTrackedTokens(ctx, desc.RangeID, 2<<20 /* 2*1MiB=2MiB */)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%elastic_tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- (Enable the piggyback token dispatch mechanism.)`)
	disablePiggybackTokenDispatch.Store(false)

	h.comment(`-- (Unquiesce the range.)`)
	testutils.SucceedsSoon(t, func() error {
		_, err := tc.GetRaftLeader(t, roachpb.RKey(k)).MaybeUnquiesceAndPropose()
		require.NoError(t, err)
		return h.checkAllTokensReturned(ctx, 3)
	})

	h.comment(`
-- Flow token metrics from n1 after work gets admitted and all elastic tokens
-- are returned through the piggyback mechanism. 
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%elastic_tokens%'
ORDER BY name ASC;
`)
}

// TestFlowControlTransferLease tests flow control behavior when the range lease
// is transferred, and the raft leadership along with it.
func TestFlowControlTransferLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 5
	var disableWorkQueueGranting atomic.Bool
	disableWorkQueueGranting.Store(true)

	st := cluster.MakeTestingClusterSettings()
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
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

	h := newFlowControlTestHelper(t, tc)
	h.init()
	defer h.close("transfer_lease")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3)

	h.comment(`-- (Issuing 1x1MiB, 3x replicated write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)

	h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB regular 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of regular tokens with
-- no corresponding returns.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%regular_tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- (Transferring range lease to n2 and allowing leadership to follow.)`)
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(1))
	testutils.SucceedsSoon(t, func() error {
		if leader := tc.GetRaftLeader(t, roachpb.RKey(k)); leader.NodeID() != tc.Target(1).NodeID {
			return errors.Errorf("expected raft leadership to transfer to n1, found n%d", leader.NodeID())
		}
		return nil
	})
	h.waitForAllTokensReturned(ctx, 3)

	h.comment(`
-- Flow token metrics from n1 having lost the lease and raft leadership. All
-- deducted tokens are returned.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%regular_tokens%'
ORDER BY name ASC;
`)
}

// TestFlowControlLeaderNotLeaseholder tests flow control behavior when the
// range leaseholder is not the raft leader.
func TestFlowControlLeaderNotLeaseholder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 5
	var disableWorkQueueGranting atomic.Bool
	disableWorkQueueGranting.Store(true)

	st := cluster.MakeTestingClusterSettings()
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// Disable leader transfers during leaseholder changes so
					// that we can easily create leader-not-leaseholder
					// scenarios.
					DisableLeaderFollowsLeaseholder: true,
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
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

	h := newFlowControlTestHelper(t, tc)
	h.init()
	defer h.close("leader_not_leaseholder")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3)

	h.comment(`-- (Issuing 1x1MiB, 3x replicated write that's not admitted.)`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)

	h.comment(`
-- Flow token metrics from n1 after issuing 1x1MiB regular 3x replicated write
-- that's not admitted. We see 1*1MiB*3=3MiB deductions of regular tokens with
-- no corresponding returns.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%regular_tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- (Transferring only range lease, not raft leadership, to n2.)`)
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(1))
	require.Equal(t, tc.GetRaftLeader(t, roachpb.RKey(k)).NodeID(), tc.Target(0).NodeID)

	h.comment(`
-- Flow token metrics from n1 having lost the lease but retained raft
-- leadership. No deducted tokens are released.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%regular_tokens%'
ORDER BY name ASC;
`)

	h.comment(`
-- (Issuing another 1x1MiB, 3x replicated write that's not admitted while in
-- this leader != leaseholder state.)
`)
	h.put(ctx, k, 1<<20 /* 1MiB */, admissionpb.NormalPri)

	h.comment(`
-- Looking at n1's flow token metrics, there's no change. No additional tokens
-- are deducted since the write is not being proposed here.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%regular_tokens%'
ORDER BY name ASC;
`)

	h.comment(`
-- Looking at n2's flow token metrics, there's no activity. n2 never acquired
-- the raft leadership.
`)
	h.query(n2, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%regular_tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- (Allow below-raft admission to proceed.)`)
	disableWorkQueueGranting.Store(false)
	h.waitForAllTokensReturned(ctx, 3) // wait for admission

	h.comment(`
-- All deducted flow tokens are returned back to where the raft leader is.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%regular_tokens%'
ORDER BY name ASC;
`)
}

// TestFlowControlGranterAdmitOneByOne is a reproduction for #105185. Internal
// admission code that relied on admitting at most one waiting request was in
// fact admitting more than one, and doing so recursively with call stacks as
// deep as the admit chain. This triggered panics (and is also just undesirable,
// design-wise). This test intentionally queues a 1000+ small requests, to that
// end.
func TestFlowControlGranterAdmitOneByOne(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 3
	var disableWorkQueueGranting atomic.Bool
	disableWorkQueueGranting.Store(true)

	st := cluster.MakeTestingClusterSettings()
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					FlowControlTestingKnobs: &kvflowcontrol.TestingKnobs{
						UseOnlyForScratchRanges: true,
						MaintainStreamsForBehindFollowers: func() bool {
							// TODO(irfansharif): This test is flakey without
							// this change -- we disconnect one stream or
							// another because raft says we're no longer
							// actively replicating through it. Why? Something
							// to do with the many proposals we're issuing?
							return true
						},
						OverrideTokenDeduction: func() kvflowcontrol.Tokens {
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

	h := newFlowControlTestHelper(t, tc)
	h.init()
	defer h.close("granter_admit_one_by_one")

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)
	h.waitForConnectedStreams(ctx, desc.RangeID, 3)

	h.comment(`-- (Issuing regular 1024*1KiB, 3x replicated writes that are not admitted.)`)
	h.log("sending put requests")
	for i := 0; i < 1024; i++ {
		h.put(ctx, k, 1<<10 /* 1KiB */, admissionpb.NormalPri)
	}
	h.log("sent put requests")

	h.comment(`
-- Flow token metrics from n1 after issuing 1024KiB, i.e. 1MiB 3x replicated writes
-- that are yet to get admitted. We see 3*1MiB=3MiB deductions of
-- {regular,elastic} tokens with no corresponding returns.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)

	h.comment(`-- Observe the total tracked tokens per-stream on n1.`)
	h.query(n1, `
  SELECT range_id, store_id, crdb_internal.humanize_bytes(total_tracked_tokens::INT8)
    FROM crdb_internal.kv_flow_control_handles
`, "range_id", "store_id", "total_tracked_tokens")

	h.comment(`-- (Allow below-raft admission to proceed.)`)
	disableWorkQueueGranting.Store(false)
	h.waitForAllTokensReturned(ctx, 3) // wait for admission

	h.comment(`
-- Flow token metrics from n1 after work gets admitted. We see 3MiB returns of
-- {regular,elastic} tokens, and the available capacities going back to what
-- they were. In #105185, by now we would've observed panics.
`)
	h.query(n1, `
  SELECT name, crdb_internal.humanize_bytes(value::INT8)
    FROM crdb_internal.node_metrics
   WHERE name LIKE '%kvadmission%tokens%'
ORDER BY name ASC;
`)
}

type flowControlTestHelper struct {
	t   *testing.T
	tc  *testcluster.TestCluster
	buf *strings.Builder
	rng *rand.Rand
}

func newFlowControlTestHelper(t *testing.T, tc *testcluster.TestCluster) *flowControlTestHelper {
	rng, _ := randutil.NewPseudoRand()
	buf := &strings.Builder{}
	return &flowControlTestHelper{
		t:   t,
		tc:  tc,
		buf: buf,
		rng: rng,
	}
}

func (h *flowControlTestHelper) init() {
	// Reach into each server's cluster setting and override. This causes any
	// registered change callbacks to run immediately, which is important since
	// running them with some lag (which happens when using SQL and `SET CLUSTER
	// SETTING`) interferes with the later activities in these tests.
	for _, s := range h.tc.Servers {
		kvflowcontrol.Enabled.Override(context.Background(), &s.ClusterSettings().SV, true)
		kvflowcontrol.Mode.Override(context.Background(), &s.ClusterSettings().SV, int64(kvflowcontrol.ApplyToAll))
	}
}

func (h *flowControlTestHelper) waitForAllTokensReturned(ctx context.Context, expStreamCount int) {
	testutils.SucceedsSoon(h.t, func() error {
		return h.checkAllTokensReturned(ctx, expStreamCount)
	})
}

func (h *flowControlTestHelper) checkAllTokensReturned(
	ctx context.Context, expStreamCount int,
) error {
	kfc := h.tc.Server(0).KVFlowController().(kvflowcontrol.Controller)
	streams := kfc.Inspect(ctx)
	if len(streams) != expStreamCount {
		return fmt.Errorf("expected %d replication streams, got %d", expStreamCount, len(streams))
	}
	for _, stream := range streams {
		if stream.AvailableRegularTokens != 16<<20 {
			return fmt.Errorf("expected %s of regular flow tokens for %s, got %s",
				humanize.IBytes(16<<20),
				kvflowcontrol.Stream{
					TenantID: stream.TenantID,
					StoreID:  stream.StoreID,
				},
				humanize.IBytes(uint64(stream.AvailableRegularTokens)),
			)
		}
		if stream.AvailableElasticTokens != 8<<20 {
			return fmt.Errorf("expected %s of elastic flow tokens for %s, got %s",
				humanize.IBytes(8<<20),
				kvflowcontrol.Stream{
					TenantID: stream.TenantID,
					StoreID:  stream.StoreID,
				},
				humanize.IBytes(uint64(stream.AvailableElasticTokens)),
			)
		}
	}
	return nil
}

func (h *flowControlTestHelper) waitForConnectedStreams(
	ctx context.Context, rangeID roachpb.RangeID, expConnectedStreams int,
) {
	testutils.SucceedsSoon(h.t, func() error {
		kfh := h.tc.Server(0).KVFlowHandles().(kvflowcontrol.Handles)
		handle, found := kfh.Lookup(rangeID)
		if !found {
			return fmt.Errorf("handle for %s not found", rangeID)
		}
		require.True(h.t, found)
		state := handle.Inspect(ctx)
		if len(state.ConnectedStreams) != expConnectedStreams {
			return fmt.Errorf("expected %d connected streams, got %d",
				expConnectedStreams, len(state.ConnectedStreams))
		}
		return nil
	})
}

func (h *flowControlTestHelper) waitForTotalTrackedTokens(
	ctx context.Context, rangeID roachpb.RangeID, expTotalTrackedTokens int64,
) {
	testutils.SucceedsSoon(h.t, func() error {
		kfh := h.tc.Server(0).KVFlowHandles().(kvflowcontrol.Handles)
		handle, found := kfh.Lookup(rangeID)
		if !found {
			return fmt.Errorf("handle for %s not found", rangeID)
		}
		require.True(h.t, found)
		state := handle.Inspect(ctx)
		var totalTracked int64
		for _, stream := range state.ConnectedStreams {
			for _, tracked := range stream.TrackedDeductions {
				totalTracked += tracked.Tokens
			}
		}
		if totalTracked != expTotalTrackedTokens {
			return fmt.Errorf("expected to track %d tokens in aggregate, got %d",
				kvflowcontrol.Tokens(expTotalTrackedTokens), kvflowcontrol.Tokens(totalTracked))
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

func (h *flowControlTestHelper) query(runner *sqlutils.SQLRunner, sql string, headers ...string) {
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

func (h *flowControlTestHelper) put(
	ctx context.Context, key roachpb.Key, size int, pri admissionpb.WorkPriority,
) *kvpb.BatchRequest {
	value := roachpb.MakeValueFromString(randutil.RandString(h.rng, size, randutil.PrintableKeyAlphabet))
	ba := &kvpb.BatchRequest{}
	ba.Add(kvpb.NewPut(key, value))
	ba.AdmissionHeader.Priority = int32(pri)
	ba.AdmissionHeader.Source = kvpb.AdmissionHeader_FROM_SQL
	if _, pErr := h.tc.Server(0).DB().NonTransactionalSender().Send(
		ctx, ba,
	); pErr != nil {
		h.t.Fatal(pErr.GoError())
	}
	return ba
}

func (h *flowControlTestHelper) close(filename string) {
	echotest.Require(h.t, h.buf.String(), datapathutils.TestDataPath(h.t, "flow_control_integration", filename))
}
