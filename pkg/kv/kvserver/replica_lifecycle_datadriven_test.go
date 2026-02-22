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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
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
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
// create-descriptor start=<key> end=<key> replicas=[<int>,<int>,...] [replica-id=<int>]
// ----
//
//	Creates a range descriptor with the specified start and end keys and
//	optional replica list. The range ID is auto-assigned. If provided,
//	replicas specify NodeIDs for replicas of the range. ReplicaIDs are
//	assigned incrementally starting from replica-id (default=1).
//
// create-replica range-id=<int> [initialized]
// ----
//
//	Creates a replica on n1/s1 for the specified range ID. The created replica
//	may be initialized or uninitialized.
//
// update-hard-state range-id=<int> [term=<int>] [vote=<int>]
// ----
//
//	Updates the specified fields of the existing replica's HardState. Other
//	fields of the HardState are retained.
//
// eval-split range-id=<int> split-key=<key> [verbose]
// ----
//
//	Evaluates a split for the specified range at the given split key. This
//	creates a SplitTrigger with both the LHS and RHS descriptors, runs the
//	split trigger evaluation, and stashes the resulting batch representing the
//	pending raft log command. The batch is NOT committed until the split is
//	applied. However, the range state is updated to reflect the split -- the LHS
//	narrows, and a new range descriptor is created for the RHS with the same
//	replica set as the LHS. Optionally, we print the evaluated batch if
//	running with the verbose flag.
//
// set-lease range-id=<int> replica=<int> [lease-type=leader-lease|epoch|expiration]
// ----
//
//	Sets the lease for the specified range to the supplied replica. Note that
//	the replica parameter specifies NodeIDs, not to be confused with
//	ReplicaIDs. By default, the lease is of the leader-lease variety, but this
//	may be overriden to an epoch or expiration based lease by using the
//	lease-type parameter. For now, we treat the associated lease metadata as
//	uninteresting.
//
// apply-split range-id=<int>
// ----
//
//	Applies the pending split for the specified range using the stashed batch
//	that was generated during split evaluation. The destroyed status of the
//	post-split RHS replica is automatically determined based on the test
//	context's state; if the replica doesn't exist, or a newer (higher ReplicaID)
//	replica exists, it is considered destroyed.
//
// destroy-replica range-id=<int>
// ----
//
//	Destroys the replica on n1 for the specified range. The replica's state
//	must have already been created via create-replica.
//
// append-raft-entries range-id=<int> num-entries=<int>
// ----
//
//	Appends the specified number of dummy raft entries to the raft log for
//	the replica on n1/s1. The replica must have already been created via
//	create-replica.
//
// create-range-data range-id=<int> [num-user-keys=<int>] [num-system-keys=<int>] [num-lock-table-keys=<int>] [base-key=<key>]
// ----
//
//	Creates the specified number of user, system, and lock table keys in the
//	range. At least one parameter should be non-zero to ensure this directive is
//	not nonsensical. If base-key is provided, it must lie within the range's
//	boundaries and is used as the base key for generating range data; otherwise
//	the range's start key is used.
//
// print-range-state [sort-keys=<bool>]
// ----
//
//	Prints the current range state in the test context. By default, ranges are
//	sorted by range ID. If sort-keys is set to true, ranges are sorted by their
//	descriptor's start key instead.
//
// restart
// ----
//
//	Simulates the node restart. It causes all uninitialized replicas to be
//	forgotten because we don't load them on server startup.
func TestReplicaLifecycleDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Disable some metamorphic values for deterministic output.
	storage.DisableMetamorphicSimpleValueEncoding(t)
	batcheval.DisableMetamorphicSplitScansRightForStatsFirst(t)

	datadriven.Walk(t, "testdata/replica_lifecycle", func(t *testing.T, path string) {
		tc := newTestCtx()
		defer tc.close()
		ctx := context.Background()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "create-descriptor":
				replicaID := dd.ScanArgOr[roachpb.ReplicaID](t, d, "replica-id", 1)
				desc := tc.createRangeDesc(t, replicaID, roachpb.RSpan{
					Key:    roachpb.RKey(dd.ScanArg[string](t, d, "start")),
					EndKey: roachpb.RKey(dd.ScanArg[string](t, d, "end")),
				}, dd.ScanArg[[]roachpb.NodeID](t, d, "replicas"))

				return fmt.Sprintf("created descriptor: %v", desc)

			case "create-replica":
				rangeID := dd.ScanArg[roachpb.RangeID](t, d, "range-id")
				replicaID := dd.ScanArgOr[roachpb.ReplicaID](t, d, "replica-id", 0)
				initialized := d.HasArg("initialized")

				rs := tc.mustGetRangeState(t, rangeID)
				require.Nil(t, rs.replica, "replica already exists on n1/s1")
				repl := *rs.mustGetReplicaDescriptor(t, roachpb.NodeID(1))
				if replicaID != 0 {
					require.False(t, initialized, "custom ReplicaID not supported for initialized replicas")
					repl.ReplicaID = replicaID
				}

				var err error
				output := tc.mutate(t, func(stateBatch, raftBatch storage.Batch) {
					if initialized {
						require.NoError(t, kvstorage.WriteInitialRangeState(
							ctx, stateBatch, raftBatch,
							rs.desc, repl.ReplicaID, rs.version,
						))
					} else {
						err = kvstorage.CreateUninitializedReplica(
							ctx, kvstorage.WrapState(stateBatch), raftBatch, 1, /* StoreID */
							roachpb.FullReplicaID{RangeID: rs.desc.RangeID, ReplicaID: repl.ReplicaID},
						)
					}
					tc.updateReplicaStateFromStorage(t, ctx, rs, stateBatch, raftBatch, true /* justCreated */)
				})
				// CreateUninitializedReplica can return an error if the replica is
				// already destroyed.
				if errors.HasType(err, &kvpb.RaftGroupDeletedError{}) {
					return err.Error()
				}
				require.NoError(t, err)

				return fmt.Sprintf("created replica: %v\n%s", repl, output)

			case "update-hard-state":
				rangeID := dd.ScanArg[roachpb.RangeID](t, d, "range-id")
				rs := tc.mustGetRangeState(t, rangeID)
				require.NotNil(t, rs.replica, "replica does not exist")

				if term, upd := dd.ScanArgOpt[uint64](t, d, "term"); upd {
					rs.replica.hs.Term = term
				}
				if vote, upd := dd.ScanArgOpt[raftpb.PeerID](t, d, "vote"); upd {
					rs.replica.hs.Vote = vote
				}

				require.NoError(t, kvstorage.MakeStateLoader(rangeID).SetHardState(
					ctx, tc.raftStorage, rs.replica.hs,
				))
				return fmt.Sprintf("HardState %+v", rs.replica.hs)

			case "set-lease":
				rangeID := dd.ScanArg[roachpb.RangeID](t, d, "range-id")
				replicaNodeID := dd.ScanArg[roachpb.NodeID](t, d, "replica")
				leaseType := dd.ScanArgOr(t, d, "lease-type", "leader-lease")
				rs := tc.mustGetRangeState(t, rangeID)
				targetReplica := rs.mustGetReplicaDescriptor(t, replicaNodeID)

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

			case "eval-split":
				rangeID := dd.ScanArg[roachpb.RangeID](t, d, "range-id")
				splitKey := roachpb.RKey(dd.ScanArg[string](t, d, "split-key"))
				verbose := d.HasArg("verbose")
				rs := tc.mustGetRangeState(t, rangeID)
				desc := rs.desc
				require.True(
					t,
					desc.RSpan().ContainsKey(splitKey),
					"split key not within range",
				)
				leftDesc := desc
				leftDesc.EndKey = splitKey
				rightDesc := desc
				rightDesc.RangeID = tc.nextRangeID
				tc.nextRangeID++
				rightDesc.StartKey = splitKey
				split := roachpb.SplitTrigger{
					LeftDesc:  leftDesc,
					RightDesc: rightDesc,
				}

				// Run the split trigger evaluation and capture the batch that's
				// generated for replication. Stash it away. This represents the
				// raft log entry that will be applied as part of split
				// application.
				batch := tc.stateStorage.NewBatch()
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
				_, _, err := batcheval.TestingSplitTrigger(
					ctx, rec, batch, enginepb.MVCCStats{}, &split, in, hlc.Timestamp{},
				)
				require.NoError(t, err)

				batchRepr := batch.Repr()
				lhsRepl := rs.replica
				require.NotNil(t, lhsRepl, "LHS replica must exist on n1/s1")
				// Bump the raft index to account for where the split command
				// would go in the raft log.
				lhsRepl.lastIdx++
				tc.splits[rangeID] = pendingSplit{
					trigger:   split,
					batchRepr: batchRepr,
					raftIndex: lhsRepl.lastIdx,
				}
				tc.updatePostSplitRangeState(ctx, t, batch, split)
				batch.Close()

				if verbose {
					output, err := print.DecodeWriteBatch(batchRepr)
					require.NoError(t, err)
					return strings.ReplaceAll(output, "\n\n", "\n")
				}
				return fmt.Sprintf("lhs: r%d [%s, %s), rhs: r%d [%s, %s)",
					split.LeftDesc.RangeID, split.LeftDesc.StartKey, split.LeftDesc.EndKey,
					split.RightDesc.RangeID, split.RightDesc.StartKey, split.RightDesc.EndKey)

			case "apply-split":
				rangeID := dd.ScanArg[roachpb.RangeID](t, d, "range-id")
				ps, ok := tc.splits[rangeID]
				require.True(t, ok, "pending split not found for range-id %d", rangeID)
				delete(tc.splits, rangeID)
				split := ps.trigger

				// Determine if the RHS is "destroyed" by checking replica
				// state. This mirrors the logic in validateAndPrepareSplit:
				// - if there's no replica for the rhs, it's considered
				// destroyed.
				// - if the replica has a higher ReplicaID than in the split
				// trigger, the original was removed and a new one created (also
				// destroyed).
				lhsRangeState := tc.mustGetRangeState(t, rangeID)
				require.NotNil(t, lhsRangeState.replica, "LHS replica must exist on n1/s1")

				rhsRangeState := tc.mustGetRangeState(t, split.RightDesc.RangeID)
				rhsReplDesc := rhsRangeState.mustGetReplicaDescriptor(t, roachpb.NodeID(1))
				destroyed := rhsRangeState.replica == nil ||
					rhsRangeState.replica.ReplicaID > rhsReplDesc.ReplicaID

				in := splitPreApplyInput{
					lhsID:              lhsRangeState.replica.FullReplicaID,
					storeID:            1, // s1
					raftIndex:          ps.raftIndex,
					destroyed:           destroyed,
					rhsDesc:             split.RightDesc,
					initClosedTimestamp: hlc.Timestamp{WallTime: 100}, // dummy timestamp
				}
				wagWriter := wag.MakeWriter(&tc.wagSeq)
				return tc.mutate(t, func(stateBatch, raftBatch storage.Batch) {
					// First, apply the stashed batch from split trigger
					// evaluation.
					require.NoError(t, stateBatch.ApplyBatchRepr(ps.batchRepr, false /* sync */))
					// Then run splitPreApply which does the apply-time tweaks
					// and stages WAG nodes on the writer.
					splitPreApply(ctx, kvstorage.StateRW(stateBatch), kvstorage.WrapRaft(raftBatch), &wagWriter, in)
					// Flush WAG nodes to the raft batch, mirroring what
					// ApplyToStateMachine does in production.
					require.NoError(t, wagWriter.Flush(
						kvstorage.WrapRaft(raftBatch).WO, stateBatch.Repr(),
					))
					// If the RHS replica wasn't destroyed, it is now initialized.
					// Update the in-memory state to reflect this.
					if !destroyed {
						tc.updateReplicaStateFromStorage(t, ctx, rhsRangeState, stateBatch, raftBatch, false /* justCreated */)
					}
				})

			case "destroy-replica":
				rangeID := dd.ScanArg[roachpb.RangeID](t, d, "range-id")
				rs := tc.mustGetRangeState(t, rangeID)
				rs.mustGetReplicaDescriptor(t, roachpb.NodeID(1)) // ensure replica exists

				destroyInfo := kvstorage.DestroyReplicaInfo{
					FullReplicaID: rs.replica.FullReplicaID,
				}
				// NB: destriyInfo.Keys is only set for initialized replicas.
				if rs.replica.initialized() {
					destroyInfo.Keys = rs.desc.RSpan()
				}

				output := tc.mutate(t, func(stateBatch, raftBatch storage.Batch) {
					rw := kvstorage.ReadWriter{
						State: kvstorage.WrapState(stateBatch),
						Raft:  kvstorage.WrapRaft(raftBatch),
					}
					require.NoError(t, kvstorage.DestroyReplica(
						ctx,
						rw,
						destroyInfo,
						rs.desc.NextReplicaID,
					))
				})
				rs.replica = nil // clear the replica from the range state
				return output

			case "append-raft-entries":
				rangeID := dd.ScanArg[roachpb.RangeID](t, d, "range-id")
				numEntries := dd.ScanArg[int](t, d, "num-entries")
				rs := tc.mustGetRangeState(t, rangeID)
				require.NotNil(t, rs.replica, "replica must be created before appending entries")

				sl := logstore.NewStateLoader(rangeID)
				lastIndex := rs.replica.lastIdx
				rs.replica.lastIdx += kvpb.RaftIndex(numEntries)
				term := rs.replica.hs.Term

				return tc.mutate(t, func(_, raftBatch storage.Batch) {
					for i := 0; i < numEntries; i++ {
						entryIndex := lastIndex + 1 + kvpb.RaftIndex(i)
						require.NoError(t, storage.MVCCBlindPutProto(
							ctx, raftBatch,
							sl.RaftLogKey(entryIndex), hlc.Timestamp{},
							&raftpb.Entry{Index: uint64(entryIndex), Term: term},
							storage.MVCCWriteOptions{},
						))
					}
				})

			case "create-range-data":
				rangeID := dd.ScanArg[roachpb.RangeID](t, d, "range-id")
				numUserKeys := dd.ScanArgOr(t, d, "num-user-keys", 0)
				numSystemKeys := dd.ScanArgOr(t, d, "num-system-keys", 0)
				numLockTableKeys := dd.ScanArgOr(t, d, "num-lock-table-keys", 0)
				require.True(t, numUserKeys > 0 || numSystemKeys > 0 || numLockTableKeys > 0)

				rs := tc.mustGetRangeState(t, rangeID)
				ts := hlc.Timestamp{WallTime: 1}

				baseKey := roachpb.Key(dd.ScanArgOr(t, d, "base-key", string(rs.desc.StartKey)))
				require.True(t,
					rs.desc.ContainsKey(roachpb.RKey(baseKey)),
					"base key %q must be within range boundaries [%s, %s)",
					baseKey, rs.desc.StartKey, rs.desc.EndKey,
				)
				getUserKey := func(i int) roachpb.Key {
					return append(baseKey, strconv.Itoa(i)...)
				}

				return tc.mutate(t, func(stateBatch, _ storage.Batch) {
					// 1. User keys.
					for i := 0; i < numUserKeys; i++ {
						require.NoError(t, stateBatch.PutMVCC(
							storage.MVCCKey{Key: getUserKey(i), Timestamp: ts}, storage.MVCCValue{},
						))
					}
					// 2. System keys.
					for i := 0; i < numSystemKeys; i++ {
						key := keys.TransactionKey(getUserKey(i), uuid.NamespaceDNS)
						require.NoError(t, stateBatch.PutMVCC(
							storage.MVCCKey{Key: key, Timestamp: ts}, storage.MVCCValue{},
						))
					}
					// 3. Lock table keys.
					for i := 0; i < numLockTableKeys; i++ {
						ek, _ := storage.LockTableKey{
							Key: getUserKey(i), Strength: lock.Intent, TxnUUID: uuid.UUID{},
						}.ToEngineKey(nil)
						require.NoError(t, stateBatch.PutEngineKey(ek, nil))
					}
				})

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

			case "restart":
				tc.restart()
				return "ok"

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
	hs      raftpb.HardState
	ts      kvserverpb.RaftTruncatedState
	lastIdx kvpb.RaftIndex
}

// initialized returns true iff the replica is initialized.
func (r *replicaInfo) initialized() bool {
	return r.hs.Commit > 0 // NB: or r.ts.Index > 0
}

// pendingSplit represents a split that has been evaluated but not yet applied.
type pendingSplit struct {
	trigger   roachpb.SplitTrigger
	batchRepr []byte
	raftIndex kvpb.RaftIndex
}

// testCtx is a single test's context. It tracks the state of all ranges and any
// intermediate steps when performing replica lifecycle events.
type testCtx struct {
	st                 *cluster.Settings
	clock              *hlc.Clock
	rangeLeaseDuration time.Duration

	nextRangeID roachpb.RangeID // monotonically-increasing rangeID
	ranges      map[roachpb.RangeID]*rangeState
	splits      map[roachpb.RangeID]pendingSplit
	// The storage engines correspond to a single store, (n1, s1). The engines
	// are logically separated into two -- one for the state machine, and one
	// for Raft.
	stateStorage storage.Engine
	raftStorage  storage.Engine

	wagSeq wag.Seq
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

		nextRangeID:  1,
		ranges:       make(map[roachpb.RangeID]*rangeState),
		splits:       make(map[roachpb.RangeID]pendingSplit),
		stateStorage: storage.NewDefaultInMemForTesting(),
		raftStorage:  storage.NewDefaultInMemForTesting(),
	}
}

// close closes the test context's storage engines.
func (tc *testCtx) close() {
	tc.stateStorage.Close()
	tc.raftStorage.Close()
}

// mutate executes a write operation on both state and raft storage batches
// and commits them. All KVs written as part of both batches are returned as a
// string for the benefit of the datadriven test output, with explicit labels
// for each engine.
func (tc *testCtx) mutate(t *testing.T, write func(stateBatch, raftBatch storage.Batch)) string {
	stateBatch := tc.stateStorage.NewBatch()
	defer stateBatch.Close()
	raftBatch := tc.raftStorage.NewBatch()
	defer raftBatch.Close()

	write(stateBatch, raftBatch)

	stateOutput, err := print.DecodeWriteBatch(stateBatch.Repr())
	require.NoError(t, err)
	raftOutput, err := print.DecodeWriteBatch(raftBatch.Repr())
	require.NoError(t, err)

	require.NoError(t, raftBatch.Commit(false))
	require.NoError(t, stateBatch.Commit(false))

	// TODO(arul): There may be double new lines in the output (see tryTxn in
	// debug_print.go) that we need to strip out for the benefit of the
	// datadriven test driver. Until that TODO is addressed, we manually split
	// things out here.
	stateOutput = strings.ReplaceAll(stateOutput, "\n\n", "\n")
	raftOutput = strings.ReplaceAll(raftOutput, "\n\n", "\n")

	var sb strings.Builder
	if stateOutput != "" {
		sb.WriteString("state engine:\n")
		sb.WriteString(stateOutput)
	}
	if raftOutput != "" {
		sb.WriteString("log engine:\n")
		sb.WriteString(raftOutput)
	}
	return sb.String()
}

// createRangeDesc creates a new RangeDescriptor for the given keys span and list
// of replica locations, assigned to the next unused RangeID.
func (tc *testCtx) createRangeDesc(
	t *testing.T, replicaID roachpb.ReplicaID, span roachpb.RSpan, replicasOn []roachpb.NodeID,
) roachpb.RangeDescriptor {
	require.True(t, span.EndKey.Compare(span.Key) > 0)
	require.True(t, slices.Contains(replicasOn, 1), "replica list must contain n1")

	// Ranges are expected to be non-overlapping. Before creating a new one,
	// sanity check that we're not violating this property in the test context.
	for id, rs := range tc.ranges {
		otherSpan := rs.desc.RSpan()
		require.False(t,
			span.Key.Compare(otherSpan.EndKey) < 0 &&
				span.EndKey.Compare(otherSpan.Key) > 0,
			"descriptor overlaps with existing range %d: %v", id, otherSpan,
		)
	}

	desc := roachpb.RangeDescriptor{
		RangeID:          tc.nextRangeID,
		StartKey:         span.Key,
		EndKey:           span.EndKey,
		InternalReplicas: make([]roachpb.ReplicaDescriptor, len(replicasOn)),
		NextReplicaID:    replicaID + roachpb.ReplicaID(len(replicasOn)),
	}
	tc.nextRangeID++
	for i, id := range replicasOn {
		desc.InternalReplicas[i] = roachpb.ReplicaDescriptor{
			ReplicaID: replicaID + roachpb.ReplicaID(i),
			NodeID:    id,
			StoreID:   roachpb.StoreID(id),
			Type:      roachpb.VOTER_FULL,
		}
	}

	tc.ranges[desc.RangeID] = newRangeState(desc)
	return desc
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

func (tc *testCtx) updateReplicaStateFromStorage(
	t *testing.T,
	ctx context.Context,
	rs *rangeState,
	stateReader, raftReader storage.Reader,
	justCreated bool,
) {
	if justCreated {
		// Sanity check that we're not overwriting an existing replica.
		require.Nil(t, rs.replica)
	}
	sl := kvstorage.MakeStateLoader(rs.desc.RangeID)
	hs, err := sl.LoadHardState(ctx, raftReader)
	require.NoError(t, err)
	ts, err := sl.LoadRaftTruncatedState(ctx, raftReader)
	require.NoError(t, err)
	replID, err := sl.LoadRaftReplicaID(ctx, stateReader)
	require.NoError(t, err)
	rs.replica = &replicaInfo{
		FullReplicaID: roachpb.FullReplicaID{
			RangeID:   rs.desc.RangeID,
			ReplicaID: replID.ReplicaID,
		},
		hs:      hs,
		ts:      ts,
		lastIdx: ts.Index,
	}
}

// updatePostSplitRangeState updates the range state after a split.
func (tc *testCtx) updatePostSplitRangeState(
	ctx context.Context, t *testing.T, reader storage.Reader, split roachpb.SplitTrigger,
) {
	lhsRangeState := tc.mustGetRangeState(t, split.LeftDesc.RangeID)
	lhsRangeState.desc = split.LeftDesc // narrow the LHS
	// Create a new range state for the RHS by reading from the batch.
	rhsRangeState := newRangeState(split.RightDesc)
	rhsSl := kvstorage.MakeStateLoader(split.RightDesc.RangeID)
	rhsState, err := rhsSl.Load(ctx, reader, &split.RightDesc)
	require.NoError(t, err)
	rhsRangeState.lease = *rhsState.Lease
	rhsRangeState.gcThreshold = *rhsState.GCThreshold
	rhsRangeState.gcHint = *rhsState.GCHint
	rhsRangeState.version = *rhsState.Version
	tc.ranges[split.RightDesc.RangeID] = rhsRangeState
}

func (rs *rangeState) mustGetReplicaDescriptor(
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

// restart imitates the node restart. It causes all uninitialized replicas to be
// forgotten because we don't load them on server startup.
func (tc *testCtx) restart() {
	for _, rs := range tc.ranges {
		if rs.replica != nil && !rs.replica.initialized() {
			rs.replica = nil
		}
	}
}

func (r *replicaInfo) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("id=%s ", r.FullReplicaID.ReplicaID))
	hs := fmt.Sprintf("HardState={Term:%d,Vote:%d,Commit:%d}", r.hs.Term, r.hs.Vote, r.hs.Commit)

	if !r.initialized() {
		sb.WriteString("[uninitialized] ")
		sb.WriteString(hs)
		return sb.String()
	}

	sb.WriteString(hs)
	sb.WriteString(fmt.Sprintf(" TruncatedState={Index:%d,Term:%d}", r.ts.Index, r.ts.Term))
	sb.WriteString(fmt.Sprintf(" LastIdx=%d", r.lastIdx))
	return sb.String()
}
