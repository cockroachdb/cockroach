// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// TODO(arul): As this test suite evolves, see if it can be moved into the
// kvstorage package instead.

package kvserver

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/print"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/dd"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

// TestReplicaLifecycleDataDriven is intended to test the behaviour of various
// replica lifecycle events, such as splits, merges, replica destruction, etc.
// The test has a single storage engine that corresponds to n1/s1, and all batch
// operations to storage are printed out. It uses the following format:
//
// create-descriptor start=<key> end=<key> replicas=[<int>,<int>,...]
// ----
//
//	Creates a range descriptor with the specified start and end keys and
//	optional replica list. The range ID is auto-assigned. If provided,
//	replicas specify NodeIDs for replicas of the range. Note that ReplicaIDs
//	are assigned incrementally starting from 1.
//
// create-replica range-id=<int> [initialized]
// ----
//
// Creates a replica on n1/s1 for the specified range ID. The created replica
// may be initialized or uninitialized.
//
// print-range-state
// ----
//
// Prints the current range state in the test context.
func TestReplicaLifecycleDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, "testdata/replica_lifecycle", func(t *testing.T, path string) {
		tc := newTestCtx()
		defer tc.close()
		ctx := context.Background()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "create-descriptor":
				startKey := dd.ScanArg[string](t, d, "start")
				endKey := dd.ScanArg[string](t, d, "end")
				replicaNodeIDs := dd.ScanArg[[]roachpb.NodeID](t, d, "replicas")

				// The test is written from the perspective of n1/s1, so not having n1
				// in this list should return an error.
				require.True(t, slices.Contains(replicaNodeIDs, 1), "replica list must contain n1")

				rangeID := tc.nextRangeID
				tc.nextRangeID++
				var internalReplicas []roachpb.ReplicaDescriptor
				for i, id := range replicaNodeIDs {
					internalReplicas = append(internalReplicas, roachpb.ReplicaDescriptor{
						ReplicaID: roachpb.ReplicaID(i + 1),
						NodeID:    id,
						StoreID:   roachpb.StoreID(id),
						Type:      roachpb.VOTER_FULL,
					})
				}
				desc := roachpb.RangeDescriptor{
					RangeID:          rangeID,
					StartKey:         roachpb.RKey(startKey),
					EndKey:           roachpb.RKey(endKey),
					InternalReplicas: internalReplicas,
					NextReplicaID:    roachpb.ReplicaID(len(internalReplicas) + 1),
				}
				require.True(t, desc.StartKey.Compare(desc.EndKey) < 0)

				// Ranges are expected to be non-overlapping. Before creating a
				// new one, sanity check that we're not violating this property
				// in the test context.
				for existingRangeID, existingRS := range tc.ranges {
					existingDesc := existingRS.desc
					require.False(t, desc.StartKey.Compare(existingDesc.EndKey) < 0 &&
						existingDesc.StartKey.Compare(desc.EndKey) < 0,
						"descriptor overlaps with existing range %d [%s,%s)",
						existingRangeID, existingDesc.StartKey, existingDesc.EndKey)
				}

				rs := newRangeState(desc)
				tc.ranges[rangeID] = rs
				return fmt.Sprintf("created descriptor: %v", desc)

			case "create-replica":
				rangeID := dd.ScanArg[roachpb.RangeID](t, d, "range-id")
				initialized := d.HasArg("initialized")

				rs := tc.mustGetRangeState(t, rangeID)
				if rs.replica != nil {
					return errors.New("initialized replica already exists on n1/s1").Error()
				}
				repl := rs.getReplicaDescriptor(t)

				batch := tc.storage.NewBatch()
				defer batch.Close()

				if initialized {
					require.NoError(t, kvstorage.WriteInitialRangeState(
						ctx, batch, batch,
						rs.desc, repl.ReplicaID, rs.version,
					))
				} else {
					require.NoError(t, kvstorage.CreateUninitializedReplica(
						ctx, kvstorage.TODOState(batch), batch, 1, /* StoreID */
						roachpb.FullReplicaID{RangeID: rs.desc.RangeID, ReplicaID: repl.ReplicaID},
					))
				}
				tc.updatePostReplicaCreateState(t, ctx, rs, batch)

				// Print the descriptor and batch output.
				var sb strings.Builder
				output, err := print.DecodeWriteBatch(batch.Repr())
				require.NoError(t, err, "error decoding batch")
				sb.WriteString(fmt.Sprintf("created replica: %v", repl))
				if output != "" {
					sb.WriteString("\n")
					sb.WriteString(output)
				}
				// Commit the batch.
				err = batch.Commit(true)
				require.NoError(t, err, "error committing batch")
				return sb.String()

			case "print-range-state":
				var sb strings.Builder
				if len(tc.ranges) == 0 {
					return "no ranges in test context"
				}
				// Sort by range IDs for consistent output.
				rangeIDs := maps.Keys(tc.ranges)
				slices.Sort(rangeIDs)

				for _, rangeID := range rangeIDs {
					rs := tc.ranges[rangeID]
					sb.WriteString(fmt.Sprintf("%s\n", rs))
				}
				return sb.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

// rangeState represents the state of a single range in the test context.
type rangeState struct {
	desc    roachpb.RangeDescriptor
	version roachpb.Version
	replica *replicaInfo // replica on n1/s1.
}

// replicaInfo contains the basic info about a replica, used for managing its
// engine (both raft log and state machine) state.
type replicaInfo struct {
	roachpb.FullReplicaID
	hs raftpb.HardState
	ts kvserverpb.RaftTruncatedState
}

// testCtx is a single test's context. It tracks the state of all ranges and any
// intermediate steps when performing replica lifecycle events.
type testCtx struct {
	ranges      map[roachpb.RangeID]*rangeState
	nextRangeID roachpb.RangeID // monotonically-increasing rangeID
	st          *cluster.Settings
	// The storage engine corresponds to a single store, (n1, s1).
	storage storage.Engine
}

// newTestCtx constructs and returns a new testCtx.
func newTestCtx() *testCtx {
	st := cluster.MakeTestingClusterSettings()
	return &testCtx{
		ranges:      make(map[roachpb.RangeID]*rangeState),
		nextRangeID: 1,
		st:          st,
		storage:     storage.NewDefaultInMemForTesting(),
	}
}

// close closes the test context's storage engine.
func (tc *testCtx) close() {
	tc.storage.Close()
}

// newRangeState constructs a new rangeState for the supplied descriptor.
func newRangeState(desc roachpb.RangeDescriptor) *rangeState {
	return &rangeState{
		desc:    desc,
		version: roachpb.Version{Major: 10, Minor: 8, Internal: 7}, // dummy version to avoid churn
	}
}

// mustGetRangeState returns the range state for the given range ID.
func (tc *testCtx) mustGetRangeState(t *testing.T, rangeID roachpb.RangeID) *rangeState {
	rs, ok := tc.ranges[rangeID]
	require.True(t, ok, "range-id %d not found", rangeID)
	return rs
}

func (tc *testCtx) updatePostReplicaCreateState(
	t *testing.T, ctx context.Context, rs *rangeState, batch storage.Batch,
) {
	// Sanity check that we're not overwriting an existing replica.
	require.Nil(t, rs.replica)
	sl := kvstorage.MakeStateLoader(rs.desc.RangeID)
	hs, err := sl.LoadHardState(ctx, batch)
	require.NoError(t, err)
	ts, err := sl.LoadRaftTruncatedState(ctx, batch)
	require.NoError(t, err)
	replID, err := sl.LoadRaftReplicaID(ctx, batch)
	require.NoError(t, err)
	rs.replica = &replicaInfo{
		FullReplicaID: roachpb.FullReplicaID{
			RangeID:   rs.desc.RangeID,
			ReplicaID: replID.ReplicaID,
		},
		hs: hs,
		ts: ts,
	}
}

func (rs *rangeState) getReplicaDescriptor(t *testing.T) *roachpb.ReplicaDescriptor {
	for i, repl := range rs.desc.InternalReplicas {
		if repl.NodeID == roachpb.NodeID(1) {
			return &rs.desc.InternalReplicas[i]
		}
	}
	t.Fatal("replica not found")
	return nil // unreachable
}

func (rs *rangeState) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("range desc: %s", rs.desc))
	if rs.replica != nil {
		sb.WriteString(fmt.Sprintf("\n		replica (n1/s1): %s", rs.replica))
	}
	return sb.String()
}

func (r *replicaInfo) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("id=%s ", r.FullReplicaID.ReplicaID))
	if r.hs == (raftpb.HardState{}) {
		sb.WriteString("uninitialized")
	} else {
		sb.WriteString(fmt.Sprintf("HardState={Term:%d,Vote:%d,Commit:%d} ", r.hs.Term, r.hs.Vote, r.hs.Commit))
		sb.WriteString(fmt.Sprintf("TruncatedState={Index:%d,Term:%d}", r.ts.Index, r.ts.Term))
	}
	return sb.String()
}
