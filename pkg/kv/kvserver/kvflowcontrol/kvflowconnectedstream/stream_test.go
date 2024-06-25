// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowconnectedstream

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func scanReplicaDescriptor(t *testing.T, line string) roachpb.ReplicaDescriptor {
	var storeID, replicaID int
	var replicaType roachpb.ReplicaType
	var desc roachpb.ReplicaDescriptor
	var err error

	parts := strings.Fields(line)
	parts[0] = strings.TrimSpace(parts[0])

	require.True(t, strings.HasPrefix(parts[0], "store_id="))
	parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "store_id=")
	storeID, err = strconv.Atoi(parts[0])
	require.NoError(t, err)

	parts[1] = strings.TrimSpace(parts[1])
	require.True(t, strings.HasPrefix(parts[1], "replica_id="))
	parts[1] = strings.TrimPrefix(strings.TrimSpace(parts[1]), "replica_id=")
	replicaID, err = strconv.Atoi(parts[1])
	require.NoError(t, err)

	parts[2] = strings.TrimSpace(parts[2])
	require.True(t, strings.HasPrefix(parts[2], "type="))
	parts[2] = strings.TrimPrefix(strings.TrimSpace(parts[2]), "type=")
	switch parts[2] {
	case "VOTER_FULL":
		replicaType = roachpb.VOTER_FULL
	case "VOTER_INCOMING":
		replicaType = roachpb.VOTER_INCOMING
	case "VOTER_DEMOTING_LEARNER":
		replicaType = roachpb.VOTER_DEMOTING_LEARNER
	case "LEARNER":
		replicaType = roachpb.LEARNER
	case "NON_VOTER":
		replicaType = roachpb.NON_VOTER
	case "VOTER_DEMOTING_NON_VOTER":
		replicaType = roachpb.VOTER_DEMOTING_NON_VOTER
	default:
		panic("unknown replica type")
	}

	desc = roachpb.ReplicaDescriptor{
		NodeID:    roachpb.NodeID(storeID),
		StoreID:   roachpb.StoreID(storeID),
		ReplicaID: roachpb.ReplicaID(replicaID),
		Type:      replicaType,
	}

	return desc
}

func scanRanges(t *testing.T, input string) []testingRange {
	replicas := []testingRange{}

	for _, line := range strings.Split(input, "\n") {
		parts := strings.Fields(line)
		parts[0] = strings.TrimSpace(parts[0])
		if strings.HasPrefix(parts[0], "range_id=") {
			// Create a new range, any replicas which follow until the next range_id
			// line will be added to this replica set.
			var rangeID, tenantID, localReplicaID int
			var err error

			require.True(t, strings.HasPrefix(parts[0], "range_id="))
			parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "range_id=")
			rangeID, err = strconv.Atoi(parts[0])
			require.NoError(t, err)

			parts[1] = strings.TrimSpace(parts[1])
			require.True(t, strings.HasPrefix(parts[1], "tenant_id="))
			parts[1] = strings.TrimPrefix(strings.TrimSpace(parts[1]), "tenant_id=")
			tenantID, err = strconv.Atoi(parts[1])
			require.NoError(t, err)

			parts[2] = strings.TrimSpace(parts[2])
			require.True(t, strings.HasPrefix(parts[2], "local_replica_id="))
			parts[2] = strings.TrimPrefix(strings.TrimSpace(parts[2]), "local_replica_id=")
			localReplicaID, err = strconv.Atoi(parts[2])
			require.NoError(t, err)

			replicas = append(replicas, testingRange{
				rangeID:        roachpb.RangeID(rangeID),
				tenantID:       roachpb.MustMakeTenantID(uint64(tenantID)),
				localReplicaID: roachpb.ReplicaID(localReplicaID),
				replicaSet:     map[roachpb.ReplicaID]roachpb.ReplicaDescriptor{},
			})
		} else {
			// Otherwise, add the replica to the last replica set created.
			desc := scanReplicaDescriptor(t, line)
			replicas[len(replicas)-1].replicaSet[desc.ReplicaID] = desc
		}
	}

	return replicas
}

func scanPriority(t *testing.T, input string) admissionpb.WorkPriority {
	require.True(t, strings.HasPrefix(input, "pri="))
	input = strings.TrimPrefix(strings.TrimSpace(input), "pri=")
	switch input {
	case "LowPri":
		return admissionpb.LowPri
	case "NormalPri":
		return admissionpb.NormalPri
	case "HighPri":
		return admissionpb.HighPri
	default:
		panic("unknown work class")
	}
}

// TestRangeController is a datadriven test that exercises the RangeController.
// The commands available are:
//
//   - init
//     range_id=<range_id> tenant_id=<tenant_id> local_replica_id=<local_replica_id>
//     store_id=<store_id> replica_id=<replica_id> type=<type>
//     ...
//     ...
//
//   - set_replicas
//     range_id=<range_id> tenant_id=<tenant_id> local_replica_id=<local_replica_id>
//     store_id=<store_id> replica_id=<replica_id> type=<type>
//     ...
//     ...
//
//   - send
//     range_id=<range_id> size=<size> pri=<pri>
//     ...
//
//   - admit
//     range_id=<range_id>
//     store_id=<store_id> to=<to> pri=<pri>
//     ...
//     ...
//
//   - set_leader range_id=<range_id> replica_id=<replica_id>
//
// TODO(kvoli):
//   - test replica set changes
//   - full voter transition [VOTER_FULL -> VOTER_DEMOTING_LEARNER -> LEARNER]
//   - force flushes
//   - test state transition from <- probe <-> replicate <-> snapshot ->.
//     These could be set via set_replicas, which would be updated to take a
//     connection state, in addition to the replica type? A downside of this is
//     that it would call into set replicas.
//   - test leaseholder changes
//   - test multi-tenant
func TestRangeController(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		mtime := timeutil.NewManualTime(time.Time{})
		clock := hlc.NewClockForTesting(mtime)
		settings := cluster.MakeTestingClusterSettings()
		stopper := stop.NewStopper()
		defer stopper.Stop(context.Background())

		var (
			raftImpls         map[roachpb.RangeID]*testingRaft
			controllers       map[roachpb.RangeID]RangeController
			tokenCounter      StoreStreamsTokenCounter
			sendTokensWatcher StoreStreamSendTokensWatcher
		)

		stateString := func() string {
			var buf strings.Builder

			// Sort the rangeIDs for deterministic output.
			rangeIDs := make([]roachpb.RangeID, 0, len(raftImpls))
			for rangeID := range raftImpls {
				rangeIDs = append(rangeIDs, rangeID)
			}
			sort.Slice(rangeIDs, func(i, j int) bool {
				return rangeIDs[i] < rangeIDs[j]
			})

			for _, rangeID := range rangeIDs {
				raftImpl := raftImpls[rangeID]
				fmt.Fprintf(&buf, "range_id=%d\n", rangeID)
				replicaIDs := make([]roachpb.ReplicaID, 0, len(raftImpl.replicas))
				for replicaID := range raftImpl.replicas {
					replicaIDs = append(replicaIDs, replicaID)
				}

				sort.Slice(replicaIDs, func(i, j int) bool {
					return replicaIDs[i] < replicaIDs[j]
				})

				// Grab out the controllerImpl from the controller interface in order to
				// inspect the send queue state.
				controllerImpl := controllers[rangeID].(*RangeControllerImpl)
				for _, replicaID := range replicaIDs {
					replica := raftImpl.replicas[replicaID]
					controllerRepl := controllerImpl.replicaMap[replicaID]
					fmt.Fprintf(&buf, "\t%v: %v eval=(%v) send=(%v)",
						replica.desc, replica.info, controllerRepl.evalTokenCounter, controllerRepl.sendTokenCounter)
					// Only include the send queue state if non-empty.
					if controllerRepl.replicaSendStream.queueSize() > 0 {
						fmt.Fprintf(&buf, " queue=[%v,%v) size=%v pri=%v",
							controllerRepl.replicaSendStream.sendQueue.indexToSend,
							controllerRepl.replicaSendStream.sendQueue.nextRaftIndex,
							controllerRepl.replicaSendStream.queueSize(),
							controllerRepl.replicaSendStream.queuePriority(),
						)
					}
					buf.WriteString("\n")
				}
			}
			return buf.String()
		}

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				raftImpls = make(map[roachpb.RangeID]*testingRaft)
				controllers = make(map[roachpb.RangeID]RangeController)
				sendTokensWatcher = NewStoreStreamSendTokensWatcher(stopper)
				tokenCounter = NewStoreStreamsTokenCounter(settings, clock)

				for _, r := range scanRanges(t, d.Input) {
					raftImpls[r.rangeID] = &testingRaft{}
					controllers[r.rangeID] = nil

					options := RangeControllerOptions{
						RangeID:           r.rangeID,
						TenantID:          r.tenantID,
						LocalReplicaID:    r.localReplicaID,
						SSTokenCounter:    tokenCounter,
						SendTokensWatcher: sendTokensWatcher,
						RaftInterface:     raftImpls[r.rangeID],
						MessageSender:     raftImpls[r.rangeID],
						Scheduler:         raftImpls[r.rangeID],
					}

					raftImpls[r.rangeID].localReplicaID = roachpb.ReplicaID(r.localReplicaID)
					raftImpls[r.rangeID].setReplicas(r.replicaSet)

					init := RangeControllerInitState{
						ReplicaSet:  r.replicaSet,
						Leaseholder: r.localReplicaID,
					}
					controllers[r.rangeID] = NewRangeControllerImpl(options, init)
					raftImpls[r.rangeID].controller = controllers[r.rangeID]
				}
			case "set_replicas":
				for _, r := range scanRanges(t, d.Input) {
					raftImpls[r.rangeID].setReplicas(r.replicaSet)
					controllers[r.rangeID].SetReplicas(r.replicaSet)
				}
			case "set_leader":
				var rangeID, leader int
				d.ScanArgs(t, "range_id=%d replica_id=%d", &rangeID, &leader)
				controllers[roachpb.RangeID(rangeID)].SetLeaseholder(roachpb.ReplicaID(leader))
			case "admit":
				var lastRangeID roachpb.RangeID
				for _, line := range strings.Split(d.Input, "\n") {
					var (
						rangeID int
						storeID int
						to      int
						err     error
					)
					parts := strings.Fields(line)
					parts[0] = strings.TrimSpace(parts[0])

					if strings.HasPrefix(parts[0], "range_id=") {
						parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "range_id=")
						rangeID, err = strconv.Atoi(parts[0])
						require.NoError(t, err)
						lastRangeID = roachpb.RangeID(rangeID)
					} else {
						parts[0] = strings.TrimSpace(parts[0])
						require.True(t, strings.HasPrefix(parts[0], "store_id="))
						parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "store_id=")
						storeID, err = strconv.Atoi(parts[0])
						require.NoError(t, err)

						parts[1] = strings.TrimSpace(parts[1])
						require.True(t, strings.HasPrefix(parts[1], "to="))
						parts[1] = strings.TrimPrefix(strings.TrimSpace(parts[1]), "to=")
						to, err = strconv.Atoi(parts[1])
						require.NoError(t, err)

						workPriority := scanPriority(t, parts[2])

						raftImpls[lastRangeID].admit(roachpb.StoreID(storeID), uint64(to), workPriority)
					}
				}
			case "send":
				for _, line := range strings.Split(d.Input, "\n") {
					var (
						rangeID int
						size    int64
					)

					parts := strings.Fields(line)
					parts[0] = strings.TrimSpace(parts[0])
					require.True(t, strings.HasPrefix(parts[0], "range_id="))
					parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "range_id=")
					rangeID, err := strconv.Atoi(parts[0])
					require.NoError(t, err)

					workPriority := scanPriority(t, parts[1])

					parts[2] = strings.TrimSpace(parts[2])
					require.True(t, strings.HasPrefix(parts[2], "size="))
					parts[2] = strings.TrimPrefix(strings.TrimSpace(parts[2]), "size=")
					size, err = humanizeutil.ParseBytes(parts[2])
					require.NoError(t, err)

					log.Infof(context.Background(), "rangeID: %d, prio: %v, size: %v", rangeID, workPriority, size)
					raftImpls[roachpb.RangeID(rangeID)].prop(AdmissionPriorityToRaftPriority(workPriority), uint64(size))
				}
			}
			return stateString()
		})
	})
}

// testingRaft is a mock implementation of the RaftInterface that is used to
// coordinate entry proposal, sending and admission. The two main methods used
// by the test are prop and admit. The prop method is used to propose a new
// entry to the RaftInterface, which will then be processed by the
// RangeController. The admit method is used to simulate the admission of a
// stream to a store.
//
// TODO(kvoli): This implementation is scrappy, consider improving and
// separating out the responsibilities for sending/admitting entries.
type testingRaft struct {
	localReplicaID roachpb.ReplicaID
	replicas       map[roachpb.ReplicaID]testingReplica
	lastReadyIndex uint64
	lastEntryIndex uint64
	entries        []raftpb.Entry
	controller     RangeController
}

type testingReplica struct {
	desc roachpb.ReplicaDescriptor
	info FollowerStateInfo
}

type testingRange struct {
	rangeID        roachpb.RangeID
	tenantID       roachpb.TenantID
	localReplicaID roachpb.ReplicaID
	replicaSet     map[roachpb.ReplicaID]roachpb.ReplicaDescriptor
}

func (t *testingRaft) prop(pri RaftPriority, size uint64) {
	t.lastEntryIndex++
	index := t.lastEntryIndex

	entry := encodeRaftFlowControlState(index, true /* usesFlowControl */, pri, size)
	t.entries = append(t.entries, entry)
	log.Infof(context.Background(), "prop %v", getFlowControlState(entry))
	t.controller.HandleRaftEvent(t.ready())

	// TODO(kvoli): We automatically bump the proposer (local replica) to match
	// the index. Perhaps this should be done elsewhere.
	localReplica := t.replicas[t.localReplicaID]
	localReplica.info.Next = index + 1
	localReplica.info.Match = index
	t.replicas[t.localReplicaID] = localReplica
}

func (t *testingRaft) admit(storeID roachpb.StoreID, to uint64, pri admissionpb.WorkPriority) {
	var replicaID roachpb.ReplicaID = -1
	for _, replica := range t.replicas {
		if replica.desc.StoreID == storeID {
			replicaID = replica.desc.ReplicaID
			break
		}
	}
	if replicaID == -1 {
		panic("store not found")
	}

	// We admit everything at or above the given priority.
	replica := t.replicas[replicaID]
	raftPrio := AdmissionPriorityToRaftPriority(pri)
	for rp := raftPrio; rp < NumRaftPriorities; rp++ {
		replica.info.Admitted[rp] = to
	}
	t.replicas[replicaID] = replica
	log.Infof(context.Background(), "admit store=%v to=%v pri=%v(%v) (%v)", storeID, to, pri, raftPrio, replica.info)
	t.controller.HandleRaftEvent(t.ready())
	// There may be some number of Notify() calls that result from the
	// HandleRaftEvent call, so we wait a short duration to ensure they finish
	// before proceeding.
	// TODO(kvoli): This is a hack, we should have a better way to wait on
	// potential async notify calls.
	time.Sleep(1 * time.Millisecond)
}

// setReplicas updates the replica set tracked by testingRaft. New replicas are
// assigned match and admitted equal to the last entry index.
func (t *testingRaft) setReplicas(replicaSet ReplicaSet) {
	if t.replicas == nil {
		t.replicas = make(map[roachpb.ReplicaID]testingReplica)
	}

	for _, rdesc := range replicaSet {
		repl := testingReplica{
			info: FollowerStateInfo{State: tracker.StateReplicate, Next: t.lastEntryIndex + 1, Match: t.lastEntryIndex},
			desc: rdesc,
		}
		for admitIdx := RaftPriority(0); admitIdx < NumRaftPriorities; admitIdx++ {
			repl.info.Admitted[admitIdx] = t.lastEntryIndex
		}
		if _, ok := t.replicas[rdesc.ReplicaID]; ok {
			repl.info = t.replicas[rdesc.ReplicaID].info
		}
		t.replicas[rdesc.ReplicaID] = repl
	}

	for replicaID := range t.replicas {
		if _, ok := replicaSet[replicaID]; !ok {
			delete(t.replicas, replicaID)
		}
	}
}

func (t *testingRaft) ready() testingRaftEvent {
	event := testingRaftEvent{entries: t.entries[t.lastReadyIndex:t.lastEntryIndex]}
	t.lastReadyIndex = t.lastEntryIndex
	return event
}

func (t *testingRaft) FollowerState(replicaID roachpb.ReplicaID) FollowerStateInfo {
	return t.replicas[replicaID].info
}

func (t *testingRaft) LastEntryIndex() uint64 {
	return t.lastEntryIndex
}

func (t *testingRaft) MakeMsgApp(
	replicaID roachpb.ReplicaID, start, end uint64, maxSize int64,
) (raftpb.Message, error) {
	entries := []raftpb.Entry{}
	var maxIndex uint64
	for _, entry := range t.entries {
		if entry.Index >= start && entry.Index < end {
			entries = append(entries, entry)
			maxIndex = max(maxIndex, entry.Index)
		}
	}

	log.Infof(context.Background(), "make_msg (from=%v to=%v) index=%v start=%v end=%v max_size=%d",
		t.localReplicaID, replicaID, maxIndex, start, end, maxSize)
	return raftpb.Message{
		Index:   maxIndex,
		To:      uint64(replicaID),
		From:    uint64(t.localReplicaID),
		Entries: entries,
	}, nil
}

func (t *testingRaft) SendRaftMessage(
	ctx context.Context, priorityInherited RaftPriority, msg raftpb.Message,
) {
	recvReplica := t.replicas[roachpb.ReplicaID(msg.To)]
	recvReplica.info.Match = msg.Index
	recvReplica.info.Next = msg.Index + 1
	t.replicas[roachpb.ReplicaID(msg.To)] = recvReplica
	log.Infof(context.Background(), "send_msg (from=%v to=%v) index=%v updated_state=(%v)",
		roachpb.ReplicaID(msg.From), roachpb.ReplicaID(msg.To), msg.Index, recvReplica.info)
}

func (t *testingRaft) ScheduleControllerEvent(rangeID roachpb.RangeID) {
	if t.controller == nil {
		// TODO(kvoli): Currently we may call ScheduleControllerEvent when
		// initializing the controller, in which case the mock testingRaft will not
		// have an associated controller.
		return
	}
	log.Infof(context.Background(), "schedule rangeID=%d", rangeID)
	if err := t.controller.HandleControllerSchedulerEvent(); err != nil {
		panic(err)
	}
}

type testingRaftEvent struct {
	entries []raftpb.Entry
}

func (t testingRaftEvent) Ready() Ready {
	return t
}

func (t testingRaftEvent) GetEntries() []raftpb.Entry {
	return t.entries
}
