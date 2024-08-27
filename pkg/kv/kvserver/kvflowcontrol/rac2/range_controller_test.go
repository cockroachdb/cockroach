// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rac2

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

type testingRCEval struct {
	pri       admissionpb.WorkPriority
	done      bool
	waited    bool
	err       error
	cancel    context.CancelFunc
	refreshCh chan struct{}
}

type testingRCRange struct {
	rc *rangeController

	mu struct {
		syncutil.Mutex
		r     testingRange
		evals map[string]*testingRCEval
	}
}

func (r *testingRCRange) FollowerState(replicaID roachpb.ReplicaID) FollowerStateInfo {
	r.mu.Lock()
	defer r.mu.Unlock()

	replica, ok := r.mu.r.replicaSet[replicaID]
	if !ok {
		return FollowerStateInfo{}
	}
	return replica.info
}

func (r *testingRCRange) startWaitForEval(name string, pri admissionpb.WorkPriority) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	refreshCh := make(chan struct{})
	r.mu.evals[name] = &testingRCEval{
		err:       nil,
		cancel:    cancel,
		refreshCh: refreshCh,
		pri:       pri,
	}

	go func() {
		waited, err := r.rc.WaitForEval(ctx, pri)

		r.mu.Lock()
		defer r.mu.Unlock()
		r.mu.evals[name].waited = waited
		r.mu.evals[name].err = err
		r.mu.evals[name].done = true
	}()
}

func (r *testingRCRange) admit(
	ctx context.Context,
	storeID roachpb.StoreID,
	term uint64,
	toIndex uint64,
	pri admissionpb.WorkPriority,
) {
	r.mu.Lock()

	for _, replica := range r.mu.r.replicaSet {
		if replica.desc.StoreID == storeID {
			replica := replica
			replica.info.Admitted[AdmissionToRaftPriority(pri)] = toIndex
			replica.info.Term = term
			r.mu.r.replicaSet[replica.desc.ReplicaID] = replica
			break
		}
	}

	r.mu.Unlock()
	// Send an empty raft event in order to trigger potential token return.
	r.rc.HandleRaftEventRaftMuLocked(ctx, RaftEvent{})
}

type testingRange struct {
	rangeID        roachpb.RangeID
	tenantID       roachpb.TenantID
	localReplicaID roachpb.ReplicaID
	replicaSet     map[roachpb.ReplicaID]testingReplica
}

func (t testingRange) replicas() ReplicaSet {
	replicas := make(ReplicaSet, len(t.replicaSet))
	for i, replica := range t.replicaSet {
		replicas[i] = replica.desc
	}
	return replicas
}

const invalidTrackerState = tracker.StateSnapshot + 1

type testingReplica struct {
	desc roachpb.ReplicaDescriptor
	info FollowerStateInfo
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
				replicaSet:     make(map[roachpb.ReplicaID]testingReplica),
			})
		} else {
			// Otherwise, add the replica to the last replica set created.
			replica := scanReplica(t, line)
			replicas[len(replicas)-1].replicaSet[replica.desc.ReplicaID] = replica
		}
	}

	return replicas
}

func scanReplica(t *testing.T, line string) testingReplica {
	var storeID, replicaID int
	var replicaType roachpb.ReplicaType
	// Default to an invalid state when no state is specified, this will be
	// converted to the prior state or StateReplicate if the replica doesn't yet
	// exist.
	state := invalidTrackerState
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

	// The fourth field is optional, if set it contains the tracker state of the
	// replica on the leader replica (localReplicaID). The valid states are
	// Probe, Replicate, and Snapshot.
	if len(parts) > 3 {
		parts[3] = strings.TrimSpace(parts[3])
		require.True(t, strings.HasPrefix(parts[3], "state="))
		parts[3] = strings.TrimPrefix(strings.TrimSpace(parts[3]), "state=")
		switch parts[3] {
		case "StateProbe":
			state = tracker.StateProbe
		case "StateReplicate":
			state = tracker.StateReplicate
		case "StateSnapshot":
			state = tracker.StateSnapshot
		default:
			panic("unknown replica state")
		}
	}

	return testingReplica{
		desc: roachpb.ReplicaDescriptor{
			NodeID:    roachpb.NodeID(storeID),
			StoreID:   roachpb.StoreID(storeID),
			ReplicaID: roachpb.ReplicaID(replicaID),
			Type:      replicaType,
		},
		info: FollowerStateInfo{State: state},
	}
}

func parsePriority(t *testing.T, input string) admissionpb.WorkPriority {
	switch input {
	case "LowPri":
		return admissionpb.LowPri
	case "NormalPri":
		return admissionpb.NormalPri
	case "HighPri":
		return admissionpb.HighPri
	default:
		require.Failf(t, "unknown work class", "%v", input)
		return admissionpb.WorkPriority(-1)
	}
}

func testingFCStateToEntry(t *testing.T, state entryFCState) raftpb.Entry {
	cmdID := kvserverbase.CmdIDKey("11111111")
	var metaBuf []byte
	if state.usesFlowControl {
		meta := kvflowcontrolpb.RaftAdmissionMeta{
			AdmissionPriority: int32(state.pri),
		}
		var err error
		metaBuf, err = protoutil.Marshal(&meta)
		require.NoError(t, err)
	}
	cmdBufPrefix := raftlog.EncodeCommandBytes(raftlog.EntryEncodingStandardWithACAndPriority, cmdID, nil, state.pri)
	paddingLen := int(state.tokens) - len(cmdBufPrefix) - len(metaBuf)
	// Padding also needs to decode as part of the RaftCommand proto, so we
	// abuse the WriteBatch.Data field which is a byte slice. Since it is a
	// nested field it consumes two tags plus two lengths. We'll approximate
	// this as needing a maximum of 15 bytes, to be on the safe side.
	require.LessOrEqual(t, 15, paddingLen)
	cmd := kvserverpb.RaftCommand{
		WriteBatch: &kvserverpb.WriteBatch{Data: make([]byte, paddingLen)}}
	// Shrink by 1 on each iteration. This doesn't give us a guarantee that we
	// will get exactly paddingLen since the length of data affects the encoded
	// lengths, but it should usually work, and cause fewer questions when
	// looking at the testdata file.
	for cmd.Size() > paddingLen {
		cmd.WriteBatch.Data = cmd.WriteBatch.Data[:len(cmd.WriteBatch.Data)-1]
	}
	cmdBuf, err := protoutil.Marshal(&cmd)
	require.NoError(t, err)
	data := append(cmdBufPrefix, metaBuf...)
	data = append(data, cmdBuf...)
	return raftpb.Entry{
		Term:  state.term,
		Index: state.index,
		Type:  raftpb.EntryNormal,
		Data:  data,
	}
}

// TestRangeControllerWaitForEval tests the RangeController WaitForEval method.
//
//   - init: Initializes the range controller with the given ranges.
//     range_id=<range_id> tenant_id=<tenant_id> local_replica_id=<local_replica_id>
//     store_id=<store_id> replica_id=<replica_id> type=<type> [state=<state>]
//     ...
//
//   - wait_for_eval: Starts a WaitForEval call on the given range.
//     range_id=<range_id> name=<name> pri=<pri>
//
//   - check_state: Prints the current state of all ranges.
//
//   - adjust_tokens: Adjusts the token count for the given store and priority.
//     store_id=<store_id> pri=<pri> tokens=<tokens>
//     ...
//
//   - cancel_context: Cancels the context for the given range.
//     range_id=<range_id> name=<name>
//
//   - set_replicas: Sets the replicas for the given range.
//     range_id=<range_id> tenant_id=<tenant_id> local_replica_id=<local_replica_id>
//     store_id=<store_id> replica_id=<replica_id> type=<type> [state=<state>]
//     ...
//
//   - set_leaseholder: Sets the leaseholder for the given range.
//     range_id=<range_id> replica_id=<replica_id>
func TestRangeControllerWaitForEval(t *testing.T) {
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	ranges := make(map[roachpb.RangeID]*testingRCRange)
	ssTokenCounter := NewStreamTokenCounterProvider(settings)
	// By default we will initialize the token counters to 0 tokens.
	shouldZeroTCs := true

	// Eval will only wait on a positive token amount, set the limit to 1 in
	// order to simplify testing.
	// kvflowcontrol.RegularTokensPerStream.Override(ctx, &settings.SV, 1)
	// kvflowcontrol.ElasticTokensPerStream.Override(ctx, &settings.SV, 1)
	// We will initialize each token counter to 0 tokens initially. The map is
	// used to do so exactly once per stream.
	zeroedTokenCounters := make(map[kvflowcontrol.Stream]struct{})

	rangeStateString := func() string {

		var b strings.Builder

		// Sort the ranges by rangeID to ensure deterministic output.
		sortedRanges := make([]*testingRCRange, 0, len(ranges))
		for _, testRC := range ranges {
			sortedRanges = append(sortedRanges, testRC)
			// We retain the lock until the end of the function call.
			testRC.mu.Lock()
			defer testRC.mu.Unlock()
		}
		sort.Slice(sortedRanges, func(i, j int) bool {
			return sortedRanges[i].mu.r.rangeID < sortedRanges[j].mu.r.rangeID
		})

		for _, testRC := range sortedRanges {
			replicaIDs := make([]int, 0, len(testRC.mu.r.replicaSet))
			for replicaID := range testRC.mu.r.replicaSet {
				replicaIDs = append(replicaIDs, int(replicaID))
			}
			sort.Ints(replicaIDs)

			fmt.Fprintf(&b, "r%d: [", testRC.mu.r.rangeID)
			for i, replicaID := range replicaIDs {
				replica := testRC.mu.r.replicaSet[roachpb.ReplicaID(replicaID)]
				if i > 0 {
					fmt.Fprintf(&b, ",")
				}
				fmt.Fprintf(&b, "%v", replica.desc)
				if replica.desc.ReplicaID == testRC.rc.leaseholder {
					fmt.Fprint(&b, "*")
				}
			}
			fmt.Fprintf(&b, "]\n")
		}
		return b.String()
	}

	tokenCountsString := func() string {
		var b strings.Builder
		streams := make([]kvflowcontrol.Stream, 0, len(ssTokenCounter.mu.evalCounters))
		for stream := range ssTokenCounter.mu.evalCounters {
			streams = append(streams, stream)
		}
		sort.Slice(streams, func(i, j int) bool {
			return streams[i].StoreID < streams[j].StoreID
		})
		for _, stream := range streams {
			fmt.Fprintf(&b, "%v: %v\n", stream, ssTokenCounter.Eval(stream))
		}

		return b.String()
	}

	evalStateString := func() string {
		time.Sleep(100 * time.Millisecond)
		var b strings.Builder

		// Sort the ranges by rangeID to ensure deterministic output.
		sortedRanges := make([]*testingRCRange, 0, len(ranges))
		for _, testRC := range ranges {
			sortedRanges = append(sortedRanges, testRC)
			// We retain the lock until the end of the function call.
			testRC.mu.Lock()
			defer testRC.mu.Unlock()
		}
		sort.Slice(sortedRanges, func(i, j int) bool {
			return sortedRanges[i].mu.r.rangeID < sortedRanges[j].mu.r.rangeID
		})

		for _, testRC := range sortedRanges {
			fmt.Fprintf(&b, "range_id=%d tenant_id=%d local_replica_id=%d\n",
				testRC.mu.r.rangeID, testRC.mu.r.tenantID, testRC.mu.r.localReplicaID)
			// Sort the evals by name to ensure deterministic output.
			evals := make([]string, 0, len(testRC.mu.evals))
			for name := range testRC.mu.evals {
				evals = append(evals, name)
			}
			sort.Strings(evals)
			for _, name := range evals {
				eval := testRC.mu.evals[name]
				fmt.Fprintf(&b, "  name=%s pri=%-8v done=%-5t waited=%-5t err=%v\n", name, eval.pri,
					eval.done, eval.waited, eval.err)
			}
		}
		return b.String()
	}

	maybeZeroTokenCounters := func(r testingRange) {
		if !shouldZeroTCs {
			return
		}
		for _, replica := range r.replicaSet {
			stream := kvflowcontrol.Stream{
				StoreID:  replica.desc.StoreID,
				TenantID: r.tenantID,
			}
			if _, ok := zeroedTokenCounters[stream]; !ok {
				zeroedTokenCounters[stream] = struct{}{}
				ssTokenCounter.Eval(stream).(*tokenCounter).adjust(ctx, admissionpb.RegularWorkClass, -1)
			}
		}
	}

	getOrInitRange := func(r testingRange) *testingRCRange {
		testRC, ok := ranges[r.rangeID]
		if !ok {
			testRC = &testingRCRange{}
			testRC.mu.r = r
			testRC.mu.evals = make(map[string]*testingRCEval)
			options := RangeControllerOptions{
				RangeID:        r.rangeID,
				TenantID:       r.tenantID,
				LocalReplicaID: r.localReplicaID,
				SSTokenCounter: ssTokenCounter,
				RaftInterface:  testRC,
			}

			init := RangeControllerInitState{
				ReplicaSet:  r.replicas(),
				Leaseholder: r.localReplicaID,
			}
			testRC.rc = NewRangeController(ctx, options, init)
			ranges[r.rangeID] = testRC
		}
		maybeZeroTokenCounters(r)
		return testRC
	}

	datadriven.RunTest(t, "testdata/range_controller_raft_event", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init":
			var zeroTokens bool
			d.MaybeScanArgs(t, "zero_tokens", &zeroTokens)
			for _, r := range scanRanges(t, d.Input) {
				getOrInitRange(r)
			}
			return rangeStateString() + tokenCountsString()

		case "wait_for_eval":
			var rangeID int
			var name, priString string
			d.ScanArgs(t, "range_id", &rangeID)
			d.ScanArgs(t, "name", &name)
			d.ScanArgs(t, "pri", &priString)
			testRC := ranges[roachpb.RangeID(rangeID)]
			testRC.startWaitForEval(name, parsePriority(t, priString))
			return evalStateString()

		case "check_state":
			return evalStateString()

		case "adjust_tokens":
			for _, line := range strings.Split(d.Input, "\n") {
				parts := strings.Fields(line)
				parts[0] = strings.TrimSpace(parts[0])
				require.True(t, strings.HasPrefix(parts[0], "store_id="))
				parts[0] = strings.TrimPrefix(parts[0], "store_id=")
				store, err := strconv.Atoi(parts[0])
				require.NoError(t, err)

				parts[1] = strings.TrimSpace(parts[1])
				require.True(t, strings.HasPrefix(parts[1], "pri="))
				pri := parsePriority(t, strings.TrimPrefix(parts[1], "pri="))

				parts[2] = strings.TrimSpace(parts[2])
				require.True(t, strings.HasPrefix(parts[2], "tokens="))
				tokenString := strings.TrimPrefix(parts[2], "tokens=")
				tokens, err := humanizeutil.ParseBytes(tokenString)
				require.NoError(t, err)

				ssTokenCounter.Eval(kvflowcontrol.Stream{
					StoreID:  roachpb.StoreID(store),
					TenantID: roachpb.SystemTenantID,
				}).(*tokenCounter).adjust(ctx,
					admissionpb.WorkClassFromPri(pri),
					kvflowcontrol.Tokens(tokens))
			}

			return tokenCountsString()

		case "cancel_context":
			var rangeID int
			var name string

			d.ScanArgs(t, "range_id", &rangeID)
			d.ScanArgs(t, "name", &name)
			testRC := ranges[roachpb.RangeID(rangeID)]
			func() {
				testRC.mu.Lock()
				defer testRC.mu.Unlock()
				testRC.mu.evals[name].cancel()
			}()

			return evalStateString()

		case "set_replicas":
			for _, r := range scanRanges(t, d.Input) {
				testRC := getOrInitRange(r)
				func() {
					testRC.mu.Lock()
					defer testRC.mu.Unlock()
					testRC.mu.r = r
				}()
				err := testRC.rc.SetReplicasRaftMuLocked(ctx, r.replicas())
				require.NoError(t, err)
			}
			return rangeStateString()

		case "set_leaseholder":
			var rangeID, replicaID int
			d.ScanArgs(t, "range_id", &rangeID)
			d.ScanArgs(t, "replica_id", &replicaID)
			testRC := ranges[roachpb.RangeID(rangeID)]
			testRC.rc.SetLeaseholderRaftMuLocked(ctx, roachpb.ReplicaID(replicaID))
			return rangeStateString()

		case "close_rcs":
			for _, r := range ranges {
				r.rc.CloseRaftMuLocked(ctx)
			}
			evalStr := evalStateString()
			for k := range ranges {
				delete(ranges, k)
			}
			return evalStr

		case "admit":
			var lastRangeID roachpb.RangeID
			for _, line := range strings.Split(d.Input, "\n") {
				var (
					rangeID  int
					storeID  int
					term     int
					to_index int
					err      error
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
					require.True(t, strings.HasPrefix(parts[1], "term="))
					parts[1] = strings.TrimPrefix(strings.TrimSpace(parts[1]), "term=")
					term, err = strconv.Atoi(parts[1])
					require.NoError(t, err)

					parts[2] = strings.TrimSpace(parts[2])
					require.True(t, strings.HasPrefix(parts[2], "to_index="))
					parts[2] = strings.TrimPrefix(strings.TrimSpace(parts[2]), "to_index=")
					to_index, err = strconv.Atoi(parts[2])
					require.NoError(t, err)

					parts[3] = strings.TrimSpace(parts[3])
					require.True(t, strings.HasPrefix(parts[3], "pri="))
					parts[3] = strings.TrimPrefix(strings.TrimSpace(parts[3]), "pri=")
					pri := parsePriority(t, parts[3])
					ranges[lastRangeID].admit(ctx, roachpb.StoreID(storeID), uint64(term), uint64(to_index), pri)
				}
			}
			return tokenCountsString()

		case "raft_event":
			var lastRangeID roachpb.RangeID
			init := false
			var buf []entryFCState

			propRangeEntries := func() {
				event := RaftEvent{
					Entries: make([]raftpb.Entry, len(buf)),
				}
				for i, state := range buf {
					event.Entries[i] = testingFCStateToEntry(t, state)
				}
				ranges[lastRangeID].rc.HandleRaftEventRaftMuLocked(ctx, event)
			}

			for _, line := range strings.Split(d.Input, "\n") {
				var (
					rangeID, term, index int
					size                 int64
					err                  error
					pri                  admissionpb.WorkPriority
				)

				parts := strings.Fields(line)
				parts[0] = strings.TrimSpace(parts[0])
				if strings.HasPrefix(parts[0], "range_id=") {
					if init {
						// We are moving to another range, if a previous range has entries
						// created then create the raft event and call handle raft ready
						// using all the entries added so far.
						propRangeEntries()
						init = false
					}

					parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "range_id=")
					rangeID, err = strconv.Atoi(parts[0])
					require.NoError(t, err)
					lastRangeID = roachpb.RangeID(rangeID)
				} else {
					require.True(t, strings.HasPrefix(parts[0], "term="))
					parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "term=")
					term, err = strconv.Atoi(parts[0])
					require.NoError(t, err)

					parts[1] = strings.TrimSpace(parts[1])
					require.True(t, strings.HasPrefix(parts[1], "index="))
					parts[1] = strings.TrimPrefix(strings.TrimSpace(parts[1]), "index=")
					index, err = strconv.Atoi(parts[1])
					require.NoError(t, err)

					parts[2] = strings.TrimSpace(parts[2])
					require.True(t, strings.HasPrefix(parts[2], "pri="))
					parts[2] = strings.TrimPrefix(strings.TrimSpace(parts[2]), "pri=")
					pri = parsePriority(t, parts[2])

					parts[3] = strings.TrimSpace(parts[3])
					require.True(t, strings.HasPrefix(parts[3], "size="))
					parts[3] = strings.TrimPrefix(strings.TrimSpace(parts[3]), "size=")
					size, err = humanizeutil.ParseBytes(parts[3])
					require.NoError(t, err)

					init = true
					buf = append(buf, entryFCState{
						term:            uint64(term),
						index:           uint64(index),
						usesFlowControl: true,
						tokens:          kvflowcontrol.Tokens(size),
						pri:             AdmissionToRaftPriority(pri),
					})
				}
			}
			if init {
				propRangeEntries()
			}

			return tokenCountsString()

		case "tracker_state":
			var rangeID int
			d.ScanArgs(t, "range_id", &rangeID)

			var buf strings.Builder
			testRC := ranges[roachpb.RangeID(rangeID)]

			var replicaIDs []int
			for replicaID := range testRC.mu.r.replicaSet {
				replicaIDs = append(replicaIDs, int(replicaID))
			}
			sort.Ints(replicaIDs)

			for _, replicaID := range replicaIDs {
				replica := testRC.rc.replicaMap[roachpb.ReplicaID(replicaID)]
				buf.WriteString(fmt.Sprintf("%v\n", replica.desc))
				buf.WriteString(formatTrackerState(replica.tracker))
				buf.WriteString("++++\n")
			}

			return buf.String()

		default:
			panic(fmt.Sprintf("unknown command: %s", d.Cmd))
		}
	})
}
