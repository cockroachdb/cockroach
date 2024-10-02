// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package kvserver

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestReplicaRaftOverload_computeExpendableOverloadedFollowers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, datapathutils.TestDataPath(t, "replica_raft_overload"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var buf strings.Builder
			tr := tracing.NewTracer()
			ctx, finishAndGet := tracing.ContextWithRecordingSpan(context.Background(), tr, path)
			defer func() {
				if t.Failed() {
					t.Logf("%s", finishAndGet())
				}
			}()
			require.Equal(t, "run", d.Cmd)
			var seed uint64
			var replDescs roachpb.ReplicaSet
			var self roachpb.ReplicaID
			ioOverloadMap := &ioThresholdMap{threshold: 1.0, m: map[roachpb.StoreID]*admissionpb.IOThreshold{}}
			snapshotMap := map[roachpb.ReplicaID]struct{}{}
			downMap := map[roachpb.ReplicaID]struct{}{}
			match := map[roachpb.ReplicaID]uint64{}
			minLiveMatchIndex := kvpb.RaftIndex(0) // accept all live followers by default
			for _, arg := range d.CmdArgs {
				for i := range arg.Vals {
					sl := strings.SplitN(arg.Vals[i], "@", 2)
					if len(sl) == 1 {
						sl = append(sl, "")
					}
					idS, matchS := sl[0], sl[1]
					arg.Vals[i] = idS

					var id uint64
					arg.Scan(t, i, &id)
					switch arg.Key {
					case "min-live-match-index":
						minLiveMatchIndex = kvpb.RaftIndex(id)
					case "self":
						self = roachpb.ReplicaID(id)
					case "voters", "learners":
						replicaID := roachpb.ReplicaID(id)
						if matchS != "" {
							var err error
							match[replicaID], err = strconv.ParseUint(matchS, 10, 64)
							require.NoError(t, err)
						}
						typ := roachpb.VOTER_FULL
						if arg.Key == "learners" {
							typ = roachpb.NON_VOTER
						}
						replDesc := roachpb.ReplicaDescriptor{
							NodeID:    roachpb.NodeID(id),
							StoreID:   roachpb.StoreID(id),
							ReplicaID: replicaID,
							Type:      typ,
						}
						replDescs.AddReplica(replDesc)
					case "overloaded":
						ioOverloadMap.m[roachpb.StoreID(id)] = &admissionpb.IOThreshold{
							L0NumSubLevels:          1000,
							L0NumSubLevelsThreshold: 20,
							L0NumFiles:              1,
							L0NumFilesThreshold:     1,
						}
					case "snapshot":
						snapshotMap[roachpb.ReplicaID(id)] = struct{}{}
					case "down":
						downMap[roachpb.ReplicaID(id)] = struct{}{}
					case "seed":
						d.ScanArgs(t, "seed", &seed)
					default:
						t.Fatalf("unknown: %s", arg.Key)
					}
				}
			}

			getProgressMap := func(ctx context.Context) map[raftpb.PeerID]tracker.Progress {
				log.Eventf(ctx, "getProgressMap was called")

				// First, set up a progress map in which all replicas are tracked and are live.
				m := map[raftpb.PeerID]tracker.Progress{}
				for _, replDesc := range replDescs.Descriptors() {
					pr := tracker.Progress{
						State:        tracker.StateReplicate,
						Match:        match[replDesc.ReplicaID],
						RecentActive: true,
						IsLearner:    replDesc.Type == roachpb.LEARNER || replDesc.Type == roachpb.NON_VOTER,
						Inflights:    tracker.NewInflights(1, 0), // avoid NPE
					}
					m[raftpb.PeerID(replDesc.ReplicaID)] = pr
				}
				// Mark replicas as down or needing snapshot as configured.
				for replicaID := range downMap {
					id := raftpb.PeerID(replicaID)
					pr := m[id]
					pr.RecentActive = false
					m[id] = pr
				}
				for replicaID := range snapshotMap {
					id := raftpb.PeerID(replicaID)
					pr := m[id]
					pr.State = tracker.StateSnapshot
					m[id] = pr
				}
				return m
			}

			m, _ := computeExpendableOverloadedFollowers(ctx, computeExpendableOverloadedFollowersInput{
				self:              self,
				replDescs:         replDescs,
				ioOverloadMap:     ioOverloadMap,
				getProgressMap:    getProgressMap,
				seed:              int64(seed),
				minLiveMatchIndex: minLiveMatchIndex,
			})
			{
				var sl []roachpb.ReplicaID
				for replID := range m {
					sl = append(sl, replID)
				}
				sort.Slice(sl, func(i, j int) bool { return sl[i] < sl[j] })
				fmt.Fprintln(&buf, sl)
			}
			return buf.String()
		})
	})
}

func TestIoThresholdMap_SafeFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	m := ioThresholdMap{threshold: 0.8, seq: 1, m: map[roachpb.StoreID]*admissionpb.IOThreshold{
		1: { // score 0.7
			L0NumSubLevels:          100,
			L0NumSubLevelsThreshold: 1000,
			L0NumFiles:              700,
			L0NumFilesThreshold:     1000,
		},
		7: { // score 0.9
			L0NumSubLevels:          90,
			L0NumSubLevelsThreshold: 100,
			L0NumFiles:              100,
			L0NumFilesThreshold:     1000,
		},
		9: { // score 1.1
			L0NumSubLevels:          110,
			L0NumSubLevelsThreshold: 100,
			L0NumFiles:              100,
			L0NumFilesThreshold:     1000,
		},
	}}
	echotest.Require(t, string(redact.Sprint(m)), datapathutils.TestDataPath(t, "io_threshold_map.txt"))
}
