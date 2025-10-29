// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// TODO(arul): As this test suite evolves, see if it can be moved into the
// kvstorage package instead.

package kvserver

import (
	"cmp"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/print"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/dd"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
// create-split range-id=<int> split-key=<key>
// ----
//
//	Creates a split for the specified range at the given split key, which
//	entails creating a SplitTrigger with both the LHS and RHS descriptors.
//	Much like how things work in CRDB, the LHS descriptor is created by
//	narrowing the original range and a new range descriptor is created for
//	the RHS with the same replica set.
//
// set-lease range-id=<int> replica=<int> [lease-type=leader-lease|epoch|expiration]
// ----
//
//	Sets the lease for the specified range to the supplied replica. Note that
//	the replica parameter specifies NodeIDs, not to be confused with
//	ReplicaIDs. By default, the lease is of the leader-lease variety, but
//	this may be overriden to an epoch or expiration based lease by using the
//	lease-type parameter. For now, we treat the associated lease metadata as
//	uninteresting.
//
// run-split-trigger range-id=<int>
// ----
//
//	Executes the split trigger for the specified range on n1.
//
// print-range-state [sort-keys=<bool>]
// ----
//
// Prints the current range state in the test context. By default, ranges are
// sorted by range ID. If sort-keys is set to true, ranges are sorted by their
// descriptor's start key instead.
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
				repl := rs.getReplicaDescriptor(t, roachpb.NodeID(1))

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

			case "create-split":
				rangeID := dd.ScanArg[roachpb.RangeID](t, d, "range-id")
				splitKey := dd.ScanArg[string](t, d, "split-key")
				rs := tc.mustGetRangeState(t, rangeID)
				desc := rs.desc
				require.True(
					t,
					roachpb.RKey(splitKey).Compare(desc.StartKey) > 0 &&
						roachpb.RKey(splitKey).Compare(desc.EndKey) < 0,
					"split key not within range",
				)
				leftDesc := desc
				leftDesc.EndKey = roachpb.RKey(splitKey)
				rightDesc := desc
				rightDesc.RangeID = tc.nextRangeID
				tc.nextRangeID++
				rightDesc.StartKey = roachpb.RKey(splitKey)
				split := &roachpb.SplitTrigger{
					LeftDesc:  leftDesc,
					RightDesc: rightDesc,
				}
				tc.splits[rangeID] = split
				return "ok"

			case "set-lease":
				rangeID := dd.ScanArg[roachpb.RangeID](t, d, "range-id")
				replicaNodeID := dd.ScanArg[roachpb.NodeID](t, d, "replica")
				leaseType := dd.ScanArgOr(t, d, "lease-type", "leader-lease")
				rs := tc.mustGetRangeState(t, rangeID)
				targetReplica := rs.getReplicaDescriptor(t, replicaNodeID)
				// NB: The details of the lease are not important to the test;
				// only the type is.
				var lease roachpb.Lease
				switch leaseType {
				case "leader-lease":
					lease = roachpb.Lease{
						Replica:       *targetReplica,
						Term:          10,
						MinExpiration: hlc.Timestamp{WallTime: 100},
					}
				case "epoch":
					lease = roachpb.Lease{
						Replica: *targetReplica,
						Epoch:   20,
					}
				case "expiration":
					lease = roachpb.Lease{
						Replica:    *targetReplica,
						Expiration: &hlc.Timestamp{WallTime: 300},
					}
				default:
					t.Fatalf("unknown lease type: %s", leaseType)
				}
				rs.lease = lease
				return "ok"

			case "run-split-trigger":
				rangeID := dd.ScanArg[roachpb.RangeID](t, d, "range-id")
				split, ok := tc.splits[rangeID]
				require.True(t, ok, "split trigger not found for range-id %d", rangeID)
				rs := tc.mustGetRangeState(t, rangeID)
				desc := rs.desc
				batch := tc.storage.NewBatch()
				defer batch.Close()

				rec := (&batcheval.MockEvalCtx{
					ClusterSettings:        tc.st,
					Desc:                   &desc,
					Clock:                  tc.clock,
					AbortSpan:              rs.abortspan,
					LastReplicaGCTimestamp: rs.lastGCTimestamp,
					RangeLeaseDuration:     tc.rangeLeaseDuration,
				}).EvalContext()

				in := batcheval.SplitTriggerHelperInput{
					LeftLease:      rs.lease,
					GCThreshold:    &rs.gcThreshold,
					GCHint:         &rs.gcHint,
					ReplicaVersion: rs.version,
				}
				// Actually run the split trigger.
				_, _, err := batcheval.TestingSplitTrigger(
					ctx, rec, batch /* bothDeltaMS */, enginepb.MVCCStats{}, split, in, hlc.Timestamp{},
				)
				require.NoError(t, err)

				// Update the test context's notion of the range state after the
				// split.
				tc.updatePostSplitRangeState(t, ctx, batch, rangeID, split)
				// Print the state of the batch (all keys/values written as part
				// of the split trigger).
				output, err := print.DecodeWriteBatch(batch.Repr())
				require.NoError(t, err)
				// Commit the batch.
				err = batch.Commit(true)
				require.NoError(t, err, "error committing batch")
				// TODO(arul): There are double lines in the output (see tryTxn
				// in debug_print.go) that we need to strip out for the benefit
				// of the datadriven test driver. Until that TODO is addressed,
				// we manually split things out here.
				return strings.ReplaceAll(output, "\n\n", "\n")

			case "print-range-state":
				var sb strings.Builder
				if len(tc.ranges) == 0 {
					return "no ranges in test context"
				}

				sortByKeys := dd.ScanArgOr(t, d, "sort-keys", false)

				rangeIDs := maps.Keys(tc.ranges)
				slices.SortFunc(rangeIDs, func(a, b roachpb.RangeID) int {
					if sortByKeys {
						return tc.ranges[a].desc.StartKey.Compare(tc.ranges[b].desc.StartKey)
					}
					// Else sort by range IDs for consistent output.
					return cmp.Compare(a, b)
				})

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
	desc            roachpb.RangeDescriptor
	version         roachpb.Version
	lease           roachpb.Lease
	gcThreshold     hlc.Timestamp
	gcHint          roachpb.GCHint
	abortspan       *abortspan.AbortSpan
	lastGCTimestamp hlc.Timestamp
	replica         *replicaInfo // replica on n1/s1.
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
	st                 *cluster.Settings
	clock              *hlc.Clock
	rangeLeaseDuration time.Duration

	nextRangeID roachpb.RangeID // monotonically-increasing rangeID
	ranges      map[roachpb.RangeID]*rangeState
	splits      map[roachpb.RangeID]*roachpb.SplitTrigger
	// The storage engine corresponds to a single store, (n1, s1).
	storage storage.Engine
}

// newTestCtx constructs and returns a new testCtx.
func newTestCtx() *testCtx {
	st := cluster.MakeTestingClusterSettings()
	manual := timeutil.NewManualTime(timeutil.Unix(0, 10))
	clock := hlc.NewClockForTesting(manual)
	return &testCtx{
		st:                 st,
		clock:              clock,
		rangeLeaseDuration: 99 * time.Nanosecond,

		nextRangeID: 1,
		ranges:      make(map[roachpb.RangeID]*rangeState),
		splits:      make(map[roachpb.RangeID]*roachpb.SplitTrigger),
		storage:     storage.NewDefaultInMemForTesting(),
	}
}

// close closes the test context's storage engine.
func (tc *testCtx) close() {
	tc.storage.Close()
}

// newRangeState constructs a new rangeState for the supplied descriptor.
func newRangeState(desc roachpb.RangeDescriptor) *rangeState {
	gcThreshold := hlc.Timestamp{WallTime: 4}
	gcHint := roachpb.GCHint{GCTimestamp: gcThreshold}

	return &rangeState{
		desc:            desc,
		version:         roachpb.Version{Major: 10, Minor: 8, Internal: 7}, // dummy version to avoid churn
		gcThreshold:     gcThreshold,
		gcHint:          gcHint,
		abortspan:       abortspan.New(desc.RangeID),
		lastGCTimestamp: hlc.Timestamp{},
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

// updatePostSplitRangeState updates the range state after a split.
func (tc *testCtx) updatePostSplitRangeState(
	t *testing.T,
	ctx context.Context,
	reader storage.Reader,
	lhsRangeID roachpb.RangeID,
	split *roachpb.SplitTrigger,
) {
	originalRangeState := tc.mustGetRangeState(t, lhsRangeID)
	// The range ID should not change for LHS since it's the same range.
	require.Equal(t, lhsRangeID, split.LeftDesc.RangeID)
	// Update LHS by just updating the descriptor.
	originalRangeState.desc = split.LeftDesc
	tc.ranges[lhsRangeID] = originalRangeState
	rhsRangeState := newRangeState(split.RightDesc)
	// Create RHS range state by reading from the reader.
	rhsSl := kvstorage.MakeStateLoader(split.RightDesc.RangeID)
	rhsState, err := rhsSl.Load(ctx, reader, &split.RightDesc)
	require.NoError(t, err)
	rhsRangeState.lease = *rhsState.Lease
	rhsRangeState.gcThreshold = *rhsState.GCThreshold
	rhsRangeState.gcHint = *rhsState.GCHint
	rhsRangeState.version = *rhsState.Version

	tc.ranges[split.RightDesc.RangeID] = rhsRangeState
}

func (rs *rangeState) getReplicaDescriptor(
	t *testing.T, nodeID roachpb.NodeID,
) *roachpb.ReplicaDescriptor {
	for i, repl := range rs.desc.InternalReplicas {
		if repl.NodeID == nodeID {
			return &rs.desc.InternalReplicas[i]
		}
	}
	t.Fatalf("replica with NodeID %d not found in range descriptor", nodeID)
	return nil // unreachable
}

func (rs *rangeState) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("range desc: %s", rs.desc))
	if rs.replica != nil {
		sb.WriteString(fmt.Sprintf("\n		replica (n1/s1): %s", rs.replica))
	}
	if (rs.lease != roachpb.Lease{}) {
		sb.WriteString(fmt.Sprintf("\n		lease: %s", rs.lease))
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
