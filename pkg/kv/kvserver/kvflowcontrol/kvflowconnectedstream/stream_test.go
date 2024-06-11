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

func scanReplicas(t *testing.T, input string) ReplicaSet {
	replicaSet := ReplicaSet{}
	for _, line := range strings.Split(input, "\n") {

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
		replicaSet[roachpb.ReplicaID(replicaID)] = desc
	}
	return replicaSet
}

func scanPriority(t *testing.T, input string) admissionpb.WorkPriority {
	require.True(t, strings.HasPrefix(input, "pri="))
	input = strings.TrimPrefix(strings.TrimSpace(input), "pri=")
	switch input {
	case "LowPri":
		return admissionpb.LowPri
	case "NormalPri":
		return admissionpb.NormalPri
	case "LockingNormalPri":
		return admissionpb.LockingNormalPri
	case "UserHighPri":
		return admissionpb.UserHighPri
	default:
		panic("unknown work class")
	}
}

// TestRangeController is a datadriven test that exercises the RangeController.
// The commands available are:
//
//   - init range_id=<range_id> tenant_id=<tenant_id> local_replica_id=<local_replica_id>
//     store_id=<store_id> replica_id=<replica_id> type=<type>
//     ...
//
//   - set_replicas
//     store_id=<store_id> replica_id=<replica_id> type=<type>
//     ...
//
//   - send
//     range_id=<range_id> size=<size> pri=<pri>
//     ...
//
//   - admit
//     store_id=<store_id> to=<to> pri=<pri>
//     ...
//
// TODO(kvoli):
//   - add multi-tenant throughout commands, this is currently mostly ignored.
//   - add multi-range support, this test currently assumes only a single range
//     controller.
//   - add stringer functions to print the tracker state.
//   - print the appropriate state after each command, rather than all the
//     state.
//   - test replica set changes
//   - test leaseholder changes
func TestRangeController(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		raftImpl := testingRaft{}
		mtime := timeutil.NewManualTime(time.Time{})
		clock := hlc.NewClockForTesting(mtime)
		settings := cluster.MakeTestingClusterSettings()
		stopper := stop.NewStopper()
		defer stopper.Stop(context.Background())

		var (
			controller        RangeController
			tokenCounter      StoreStreamsTokenCounter
			sendTokensWatcher StoreStreamSendTokensWatcher
			raftInterface     RaftInterface
			messageSender     MessageSender
			scheduler         Scheduler
		)

		stateString := func() string {
			var buf strings.Builder
			replicaIDs := make([]roachpb.ReplicaID, 0, len(raftImpl.replicas))
			for replicaID := range raftImpl.replicas {
				replicaIDs = append(replicaIDs, replicaID)
			}

			sort.Slice(replicaIDs, func(i, j int) bool {
				return replicaIDs[i] < replicaIDs[j]
			})

			// Grab out the controllerImpl from the controller interface in order to
			// inspect the send queue state.
			controllerImpl := controller.(*RangeControllerImpl)
			for _, replicaID := range replicaIDs {
				replica := raftImpl.replicas[replicaID]
				controllerRepl := controllerImpl.replicaMap[replicaID]
				fmt.Fprintf(&buf, "%v: %v eval=(%v) send=(%v)",
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
			return buf.String()
		}

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				var (
					rangeID        int = 1
					tenantID       int = 1
					localReplicaID int = 1
				)

				d.MaybeScanArgs(t, "range_id=%d tenant_id=%d local_replica_id=%d", &rangeID, &tenantID, &localReplicaID)

				raftInterface = &raftImpl
				messageSender = &raftImpl
				scheduler = &raftImpl
				sendTokensWatcher = NewStoreStreamSendTokensWatcher(stopper)
				tokenCounter = NewStoreStreamsTokenCounter(settings, clock)
				replicaSet := scanReplicas(t, d.Input)

				options := RangeControllerOptions{
					RangeID:           roachpb.RangeID(rangeID),
					TenantID:          roachpb.MustMakeTenantID(uint64(tenantID)),
					LocalReplicaID:    roachpb.ReplicaID(localReplicaID),
					SSTokenCounter:    tokenCounter,
					SendTokensWatcher: sendTokensWatcher,
					RaftInterface:     raftInterface,
					MessageSender:     messageSender,
					Scheduler:         scheduler,
				}

				raftImpl.localReplicaID = roachpb.ReplicaID(localReplicaID)
				raftImpl.replicas = make(map[roachpb.ReplicaID]testingReplica)
				for _, rdesc := range replicaSet {
					raftImpl.replicas[rdesc.ReplicaID] = testingReplica{
						info: FollowerStateInfo{
							State: tracker.StateReplicate,
							Next:  1,
							Match: 0,
						},
						desc: rdesc,
					}
				}

				init := RangeControllerInitState{
					ReplicaSet:  replicaSet,
					Leaseholder: roachpb.ReplicaID(localReplicaID),
				}
				controller = NewRangeControllerImpl(options, init)
				raftImpl.controller = controller
			case "set_replicas":
				replicaSet := scanReplicas(t, d.Input)
				controller.SetReplicas(replicaSet)
			case "set_leader":
				var leader int
				d.ScanArgs(t, "replica_id=%d", &leader)
				controller.SetLeaseholder(roachpb.ReplicaID(leader))
			case "admit":
				for _, line := range strings.Split(d.Input, "\n") {
					var (
						storeID int
						to      int
					)

					parts := strings.Fields(line)
					parts[0] = strings.TrimSpace(parts[0])
					require.True(t, strings.HasPrefix(parts[0], "store_id="))
					parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "store_id=")
					storeID, err := strconv.Atoi(parts[0])
					require.NoError(t, err)

					parts[1] = strings.TrimSpace(parts[1])
					require.True(t, strings.HasPrefix(parts[1], "to="))
					parts[1] = strings.TrimPrefix(strings.TrimSpace(parts[1]), "to=")
					to, err = strconv.Atoi(parts[1])
					require.NoError(t, err)

					workPriority := scanPriority(t, parts[2])

					raftImpl.admit(roachpb.StoreID(storeID), uint64(to), workPriority)
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
					raftImpl.prop(AdmissionPriorityToRaftPriority(workPriority), uint64(size))
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

	replica := t.replicas[replicaID]
	raftPrio := AdmissionPriorityToRaftPriority(pri)
	replica.info.Admitted[raftPrio] = to
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
