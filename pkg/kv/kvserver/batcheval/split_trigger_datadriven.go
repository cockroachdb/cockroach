// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

/*
TestSplitTriggerDatadriven tests the use of the split trigger. The test uses the
following format:

create-descriptor start=<key> end=<key> [replicas=[<int>,<int>,...]]
----

  Creates a range descriptor with the specified start and end keys and optional
  replica list. The range ID is auto-assigned. If provided, replicas specify
  NodeIDs for replicas of the range. Note that ReplicaIDs are assigned
  incrementally starting from 1.

create-split range-id=<int> split-key=<key>
----

  Creates a split for the specified range at the given split key, which entails
  creating a SplitTrigger with both the LHS and RHS descriptors. Much like how
  things work in CRDB, the LHS descriptor is created by narrowing the original
  range and a new range descriptor is created for the RHS with the same replica
  set.

set-lease range-id=<int> replica=<int> [lease-type=leader-lease|epoch|expiration]
----

  Sets the lease for the specified range to the supplied replica. Note that the
  replica parameter specifies NodeIDs, not to be confused with ReplicaIDs. By
  default, the lease is of the leader-lease variety, but this may be overriden to
  an epoch or expiration based lease by using the lease-type parameter. For now,
  we treat the associated lease metadata as uninteresting.

print-lease range-id=<int>
----

  Prints the leaseholder replica and lease type for the specified range.

print-range-state
----

  Prints the entire range state of the test context.

run-split-trigger range-id=<int>
----
  Executes the split trigger for the specified range. The output shows all
  keys and values written to the batch as part of this.

TODO(arul): For now, we do not change things like GC Threshold, GC Hint, etc.
We could/should modify the test to do so in the future. We should also add a
directive to populate the abort span.
*/

func TestSplitTriggerDatadriven(t *testing.T) {
	datadriven.Walk(t, filepath.Join("testdata", "split_trigger"), func(t *testing.T, path string) {
		tc := newTestCtx()
		ctx := context.Background()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "create-descriptor":
				var startKey, endKey string
				d.ScanArgs(t, "start", &startKey)
				d.ScanArgs(t, "end", &endKey)
				var replicaIDs []int
				if d.HasArg("replicas") {
					var replicasStr string
					d.ScanArgs(t, "replicas", &replicasStr)
					replicaIDs = parseReplicas(replicasStr)
				}

				rangeID := tc.nextRangeID
				tc.nextRangeID++
				var internalReplicas []roachpb.ReplicaDescriptor
				for i, id := range replicaIDs {
					internalReplicas = append(internalReplicas, roachpb.ReplicaDescriptor{
						ReplicaID: roachpb.ReplicaID(i + 1),
						NodeID:    roachpb.NodeID(id),
						Type:      roachpb.VOTER_FULL,
					})
				}
				desc := roachpb.RangeDescriptor{
					RangeID:          roachpb.RangeID(rangeID),
					StartKey:         roachpb.RKey(startKey),
					EndKey:           roachpb.RKey(endKey),
					InternalReplicas: internalReplicas,
				}

				// Ranges are expected to be non-overlapping. Before creating a
				// new one, sanity check that we're not violating this property
				// in the test context.
				for existingRangeID, existingRS := range tc.ranges {
					existingDesc := existingRS.desc
					if desc.StartKey.Compare(existingDesc.EndKey) < 0 &&
						existingDesc.StartKey.Compare(desc.EndKey) < 0 {
						return fmt.Sprintf("descriptor overlaps with existing range %d [%s,%s)",
							existingRangeID, existingDesc.StartKey, existingDesc.EndKey)
					}
				}

				tc.ranges[rangeID] = newRangeState(desc)
				return fmt.Sprintf("created descriptor: %v", desc)

			case "create-split":
				var rangeID int
				var splitKey string
				d.ScanArgs(t, "range-id", &rangeID)
				d.ScanArgs(t, "split-key", &splitKey)
				rs := tc.mustGetRangeState(t, rangeID)
				desc := rs.desc
				if !(roachpb.RKey(splitKey).Compare(desc.StartKey) > 0 && roachpb.RKey(splitKey).Compare(desc.EndKey) < 0) {
					return fmt.Sprintf("split-key %q not within range [%q,%q)", splitKey, desc.StartKey, desc.EndKey)
				}
				leftDesc := desc
				leftDesc.EndKey = roachpb.RKey(splitKey)
				rightDesc := desc
				rightDesc.RangeID = roachpb.RangeID(tc.nextRangeID)
				tc.nextRangeID++
				rightDesc.StartKey = roachpb.RKey(splitKey)
				split := &roachpb.SplitTrigger{
					LeftDesc:  leftDesc,
					RightDesc: rightDesc,
				}
				tc.splits[rangeID] = split
				return fmt.Sprintf("created split trigger for range-id %d at split-key %q", rangeID, splitKey)

			case "set-lease":
				var rangeID int
				var replicaNodeID int
				d.ScanArgs(t, "range-id", &rangeID)
				d.ScanArgs(t, "replica", &replicaNodeID)
				var leaseType string
				if d.HasArg("lease-type") {
					d.ScanArgs(t, "lease-type", &leaseType)
				}
				rs := tc.mustGetRangeState(t, rangeID)
				// Find the replica in the range descriptor by NodeID.
				var targetReplica *roachpb.ReplicaDescriptor
				for i := range rs.desc.InternalReplicas {
					if rs.desc.InternalReplicas[i].NodeID == roachpb.NodeID(replicaNodeID) {
						targetReplica = &rs.desc.InternalReplicas[i]
						break
					}
				}
				if targetReplica == nil {
					return fmt.Sprintf("replica with NodeID %d not found in range descriptor", replicaNodeID)
				}
				if leaseType == "" {
					leaseType = "leader-lease" // default to leader-lease
				}
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
					return fmt.Sprintf("unknown lease type: %s", leaseType)
				}
				rs.lease = lease
				return fmt.Sprintf("set lease for range %d replica %d: %s", rangeID, replicaNodeID, leaseType)

			case "print-lease":
				var rangeID int
				d.ScanArgs(t, "range-id", &rangeID)
				rs := tc.mustGetRangeState(t, rangeID)

				lease := rs.lease
				var leaseType string
				if lease.Epoch != 0 {
					leaseType = "epoch"
				} else if lease.Expiration != nil {
					leaseType = "expiration"
				} else {
					leaseType = "leader-lease"
				}
				return fmt.Sprintf("range %d: leaseholder NodeID=%d, type=%s",
					rangeID, lease.Replica.NodeID, leaseType)

			case "print-range-state":
				var sb strings.Builder
				if len(tc.ranges) == 0 {
					return "no ranges in test context"
				}
				// Sort range IDs for consistent output.
				var rangeIDs []int
				for rangeID := range tc.ranges {
					rangeIDs = append(rangeIDs, rangeID)
				}
				sort.Ints(rangeIDs)

				for _, rangeID := range rangeIDs {
					rs := tc.ranges[rangeID]
					sb.WriteString(fmt.Sprintf("range %d: [%s,%s) replicas=",
						rangeID, rs.desc.StartKey, rs.desc.EndKey))

					for i, replica := range rs.desc.InternalReplicas {
						if i > 0 {
							sb.WriteString(",")
						}
						sb.WriteString(fmt.Sprintf("n%d(r%d)", replica.NodeID, replica.ReplicaID))
					}
					sb.WriteString("\n")
				}
				return sb.String()

			case "run-split-trigger":
				var rangeID int
				d.ScanArgs(t, "range-id", &rangeID)
				split, ok := tc.splits[rangeID]
				if !ok {
					return fmt.Sprintf("no split trigger for range-id %d", rangeID)
				}
				rs := tc.mustGetRangeState(t, rangeID)
				desc := rs.desc
				db := storage.NewDefaultInMemForTesting()
				defer db.Close()
				batch := db.NewBatch()
				defer batch.Close()

				rec := (&MockEvalCtx{
					ClusterSettings:        tc.st,
					Desc:                   &desc,
					Clock:                  tc.clock,
					AbortSpan:              rs.abortspan,
					LastReplicaGCTimestamp: rs.lastGCTimestamp,
					RangeLeaseDuration:     tc.rangeLeaseDuration,
				}).EvalContext()

				// Write the range state that will be consulted and copied
				// during the split.
				sl := stateloader.Make(desc.RangeID)
				err := sl.SetLease(ctx, batch, nil, rs.lease)
				require.NoError(t, err)
				err = sl.SetGCThreshold(ctx, batch, nil, &rs.gcThreshold)
				require.NoError(t, err)
				err = sl.SetGCHint(ctx, batch, nil, &rs.gcHint)
				require.NoError(t, err)
				err = sl.SetVersion(ctx, batch, nil, &rs.version)
				require.NoError(t, err)

				// Run the split trigger.
				_, _, err = splitTrigger(ctx, rec, batch /* bothDeltaMS */, enginepb.MVCCStats{}, split, hlc.Timestamp{})
				if err != nil {
					return fmt.Sprintf("splitTrigger error: %v", err)
				}

				// Update the test context's notion of range state after split.
				tc.updatePostSplitRangeState(t, ctx, batch, rangeID, split)

				// Print the state of the batch (all keys/values written).
				var sb strings.Builder
				iter, err := batch.NewEngineIterator(ctx, storage.IterOptions{
					UpperBound: roachpb.KeyMax,
					KeyTypes:   storage.IterKeyTypePointsAndRanges,
				})
				require.NoError(t, err)

				defer iter.Close()
				var lines []string
				valid, err := iter.SeekEngineKeyGE(storage.EngineKey{Key: roachpb.KeyMin})
				require.NoError(t, err)
				for valid {
					hasPoint, hasRange := iter.HasPointAndRange()
					if hasPoint {
						k, err := iter.EngineKey()
						require.NoError(t, err)
						v, err := iter.UnsafeValue()
						if err != nil {
							return fmt.Sprintf("error getting value: %v", err)
						}
						lines = append(lines, fmt.Sprintf("point %s = %x", k, v))
					}
					if hasRange {
						bounds, err := iter.EngineRangeBounds()
						require.NoError(t, err)

						v := iter.EngineRangeKeys()
						for i := range v {
							lines = append(lines, fmt.Sprintf(
								"range [%s,%s) = %x/%x",
								bounds.Key, bounds.EndKey, v[i].Value, v[i].Version))
						}
					}
					valid, err = iter.NextEngineKey()
					require.NoError(t, err)
				}

				for _, line := range lines {
					sb.WriteString(line)
					sb.WriteString("\n")
				}
				return sb.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

type rangeState struct {
	desc            roachpb.RangeDescriptor
	lease           roachpb.Lease
	gcThreshold     hlc.Timestamp
	gcHint          roachpb.GCHint
	version         roachpb.Version
	abortspan       *abortspan.AbortSpan
	lastGCTimestamp hlc.Timestamp
}

func newRangeState(desc roachpb.RangeDescriptor) *rangeState {
	gcThreshold := hlc.Timestamp{WallTime: 4}
	gcHint := roachpb.GCHint{GCTimestamp: gcThreshold}
	return &rangeState{
		desc:            desc,
		gcThreshold:     gcThreshold,
		gcHint:          gcHint,
		version:         cluster.MakeTestingClusterSettings().Version.LatestVersion(),
		abortspan:       abortspan.New(desc.RangeID),
		lastGCTimestamp: hlc.Timestamp{},
	}
}

type testCtx struct {
	ranges             map[int]*rangeState
	splits             map[int]*roachpb.SplitTrigger
	nextRangeID        int
	st                 *cluster.Settings
	clock              *hlc.Clock
	rangeLeaseDuration time.Duration
}

func newTestCtx() *testCtx {
	st := cluster.MakeTestingClusterSettings()
	manual := timeutil.NewManualTime(timeutil.Unix(0, 10))
	clock := hlc.NewClockForTesting(manual)
	return &testCtx{
		ranges:             make(map[int]*rangeState),
		splits:             make(map[int]*roachpb.SplitTrigger),
		nextRangeID:        1,
		st:                 st,
		clock:              clock,
		rangeLeaseDuration: 99 * time.Nanosecond,
	}
}

// mustGetRangeState returns the range state for the given range ID.
func (tc *testCtx) mustGetRangeState(t *testing.T, rangeID int) *rangeState {
	rs, ok := tc.ranges[rangeID]
	if !ok {
		t.Fatalf("range-id %d not found", rangeID)
	}
	return rs
}

// updatePostSplitRangeState updates the range state after a split.
func (tc *testCtx) updatePostSplitRangeState(
	t *testing.T,
	ctx context.Context,
	batch storage.Batch,
	originalRangeID int,
	split *roachpb.SplitTrigger,
) {
	// Get the original range state
	originalRangeState, ok := tc.ranges[originalRangeID]
	if !ok {
		t.Fatalf("original range state not found for range ID %d", originalRangeID)
	}

	// The range ID should not change for LHS since it's the same range
	if int(split.LeftDesc.RangeID) != originalRangeID {
		t.Fatalf("LHS range ID changed from %d to %d", originalRangeID, split.LeftDesc.RangeID)
	}
	// Update LHS by just updating the descriptor
	originalRangeState.desc = split.LeftDesc
	tc.ranges[int(split.LeftDesc.RangeID)] = originalRangeState

	// Create RHS range state by reading from the batch
	rhsRangeState := newRangeState(split.RightDesc)
	rhsSl := stateloader.Make(split.RightDesc.RangeID)

	// Read lease from batch
	rhsLease, err := rhsSl.LoadLease(ctx, batch)
	if err == nil {
		rhsRangeState.lease = rhsLease
	}

	// Read GC threshold from batch
	rhsGCThreshold, err := rhsSl.LoadGCThreshold(ctx, batch)
	if err == nil && rhsGCThreshold != nil {
		rhsRangeState.gcThreshold = *rhsGCThreshold
	}

	// Read GC hint from batch
	rhsGCHint, err := rhsSl.LoadGCHint(ctx, batch)
	if err == nil && rhsGCHint != nil {
		rhsRangeState.gcHint = *rhsGCHint
	}

	// Read version from batch
	rhsVersion, err := rhsSl.LoadVersion(ctx, batch)
	if err == nil {
		rhsRangeState.version = rhsVersion
	}

	tc.ranges[int(split.RightDesc.RangeID)] = rhsRangeState
}

func parseReplicas(val string) []int {
	var replicaIDs []int
	if len(val) >= 2 && val[0] == '[' && val[len(val)-1] == ']' {
		val = val[1 : len(val)-1]
		if val != "" {
			for _, s := range strings.Split(val, ",") {
				var id int
				fmt.Sscanf(strings.TrimSpace(s), "%d", &id)
				replicaIDs = append(replicaIDs, id)
			}
		}
	}
	return replicaIDs
}
