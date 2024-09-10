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
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// testingRCState is a test state used in TestRangeController. It contains the
// necessary fields to construct RangeControllers and utility methods for
// generating strings representing the state of the RangeControllers.
type testingRCState struct {
	t                     *testing.T
	testCtx               context.Context
	settings              *cluster.Settings
	stopper               *stop.Stopper
	ts                    *timeutil.ManualTime
	clock                 *hlc.Clock
	ssTokenCounter        *StreamTokenCounterProvider
	probeToCloseScheduler ProbeToCloseTimerScheduler
	evalMetrics           *EvalWaitMetrics
	// ranges contains the controllers for each range. It is the main state being
	// tested.
	ranges map[roachpb.RangeID]*testingRCRange
	// setTokenCounters is used to ensure that we only set the initial token
	// counts once per counter.
	setTokenCounters     map[kvflowcontrol.Stream]struct{}
	initialRegularTokens kvflowcontrol.Tokens
	initialElasticTokens kvflowcontrol.Tokens
}

func (s *testingRCState) init(t *testing.T, ctx context.Context) {
	s.t = t
	s.testCtx = ctx
	s.settings = cluster.MakeTestingClusterSettings()
	s.stopper = stop.NewStopper()
	s.ts = timeutil.NewManualTime(timeutil.UnixEpoch)
	s.clock = hlc.NewClockForTesting(s.ts)
	s.ssTokenCounter = NewStreamTokenCounterProvider(s.settings, s.clock)
	s.probeToCloseScheduler = &testingProbeToCloseTimerScheduler{state: s}
	s.evalMetrics = NewEvalWaitMetrics()
	s.ranges = make(map[roachpb.RangeID]*testingRCRange)
	s.setTokenCounters = make(map[kvflowcontrol.Stream]struct{})
	s.initialRegularTokens = kvflowcontrol.Tokens(-1)
	s.initialElasticTokens = kvflowcontrol.Tokens(-1)
}

func sortReplicas(r *testingRCRange) []roachpb.ReplicaDescriptor {
	r.mu.Lock()
	defer r.mu.Unlock()

	sorted := make([]roachpb.ReplicaDescriptor, 0, len(r.mu.r.replicaSet))
	for _, replica := range r.mu.r.replicaSet {
		sorted = append(sorted, replica.desc)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ReplicaID < sorted[j].ReplicaID
	})
	return sorted
}

func (s *testingRCState) sortRanges() []*testingRCRange {
	sorted := make([]*testingRCRange, 0, len(s.ranges))
	for _, testRC := range s.ranges {
		sorted = append(sorted, testRC)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].mu.r.rangeID < sorted[j].mu.r.rangeID
	})
	return sorted
}

func (s *testingRCState) rangeStateString() string {
	var b strings.Builder

	for _, testRC := range s.sortRanges() {
		// We retain the lock until the end of the function call. We also ensure
		// that locking is done in order of rangeID, to avoid inconsistent lock
		// ordering leading to deadlocks.
		testRC.mu.Lock()
		defer testRC.mu.Unlock()

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
func (s *testingRCState) tokenCountsString() string {
	var b strings.Builder
	var streams []kvflowcontrol.Stream
	s.ssTokenCounter.evalCounters.Range(func(k kvflowcontrol.Stream, v *tokenCounter) bool {
		streams = append(streams, k)
		return true
	})
	sort.Slice(streams, func(i, j int) bool {
		return streams[i].StoreID < streams[j].StoreID
	})
	for _, stream := range streams {
		fmt.Fprintf(&b, "%v: %v\n", stream, s.ssTokenCounter.Eval(stream))
	}
	return b.String()
}

func (s *testingRCState) evalStateString() string {
	var b strings.Builder

	time.Sleep(20 * time.Millisecond)
	for _, testRC := range s.sortRanges() {
		// We retain the lock until the end of the function call, similar to
		// above.
		testRC.mu.Lock()
		defer testRC.mu.Unlock()

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

func (s *testingRCState) sendStreamString(rangeID roachpb.RangeID) string {
	var b strings.Builder

	for _, desc := range sortReplicas(s.ranges[rangeID]) {
		replica := s.ranges[rangeID].rc.replicaMap[desc.ReplicaID]
		fmt.Fprintf(&b, "%v: ", desc)
		if replica.sendStream == nil {
			fmt.Fprintf(&b, "closed\n")
			continue
		}
		replica.sendStream.mu.Lock()
		defer replica.sendStream.mu.Unlock()

		fmt.Fprintf(&b, "state=%v closed=%v\n",
			replica.sendStream.mu.connectedState, replica.sendStream.mu.closed)
		b.WriteString(formatTrackerState(&replica.sendStream.mu.tracker))
		b.WriteString("++++\n")
	}
	return b.String()
}

func (s *testingRCState) maybeSetInitialTokens(r testingRange) {
	for _, replica := range r.replicaSet {
		stream := kvflowcontrol.Stream{
			StoreID:  replica.desc.StoreID,
			TenantID: r.tenantID,
		}
		if _, ok := s.setTokenCounters[stream]; !ok {
			s.setTokenCounters[stream] = struct{}{}
			if s.initialRegularTokens != -1 {
				s.ssTokenCounter.Eval(stream).testingSetTokens(s.testCtx,
					admissionpb.RegularWorkClass, s.initialRegularTokens)
				s.ssTokenCounter.Send(stream).testingSetTokens(s.testCtx,
					admissionpb.RegularWorkClass, s.initialRegularTokens)
			}
			if s.initialElasticTokens != -1 {
				s.ssTokenCounter.Eval(stream).testingSetTokens(s.testCtx,
					admissionpb.ElasticWorkClass, s.initialElasticTokens)
				s.ssTokenCounter.Send(stream).testingSetTokens(s.testCtx,
					admissionpb.ElasticWorkClass, s.initialElasticTokens)
			}
		}
	}
}

func (s *testingRCState) getOrInitRange(r testingRange) *testingRCRange {
	testRC, ok := s.ranges[r.rangeID]
	if !ok {
		testRC = &testingRCRange{}
		testRC.mu.r = r
		testRC.mu.evals = make(map[string]*testingRCEval)
		options := RangeControllerOptions{
			RangeID:             r.rangeID,
			TenantID:            r.tenantID,
			LocalReplicaID:      r.localReplicaID,
			SSTokenCounter:      s.ssTokenCounter,
			RaftInterface:       testRC,
			Clock:               s.clock,
			CloseTimerScheduler: s.probeToCloseScheduler,
			EvalWaitMetrics:     s.evalMetrics,
		}

		init := RangeControllerInitState{
			ReplicaSet:  r.replicas(),
			Leaseholder: r.localReplicaID,
		}
		testRC.rc = NewRangeController(s.testCtx, options, init)
		s.ranges[r.rangeID] = testRC
	}
	s.maybeSetInitialTokens(r)
	return testRC
}

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

func (r *testingRCRange) FollowerStateRaftMuLocked(replicaID roachpb.ReplicaID) FollowerStateInfo {
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

func (r *testingRCRange) admit(ctx context.Context, storeID roachpb.StoreID, av AdmittedVector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, replica := range r.mu.r.replicaSet {
		if replica.desc.StoreID == storeID {
			r.rc.AdmitRaftMuLocked(ctx, replica.desc.ReplicaID, av)
			return
		}
	}
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
	av   AdmittedVector
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

type entryInfo struct {
	term   uint64
	index  uint64
	enc    raftlog.EntryEncoding
	pri    raftpb.Priority
	tokens kvflowcontrol.Tokens
}

func testingCreateEntry(t *testing.T, info entryInfo) raftpb.Entry {
	cmdID := kvserverbase.CmdIDKey("11111111")
	var metaBuf []byte
	if info.enc.UsesAdmissionControl() {
		meta := kvflowcontrolpb.RaftAdmissionMeta{
			AdmissionPriority: int32(info.pri),
		}
		var err error
		metaBuf, err = protoutil.Marshal(&meta)
		require.NoError(t, err)
	}
	cmdBufPrefix := raftlog.EncodeCommandBytes(info.enc, cmdID, nil, info.pri)
	paddingLen := int(info.tokens) - len(cmdBufPrefix) - len(metaBuf)
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
		Term:  info.term,
		Index: info.index,
		Type:  raftpb.EntryNormal,
		Data:  data,
	}
}

type testingProbeToCloseTimerScheduler struct {
	state *testingRCState
}

// testingProbeToCloseTimerScheduler implements the ProbeToCloseTimerScheduler
// interface.
var _ ProbeToCloseTimerScheduler = &testingProbeToCloseTimerScheduler{}

func (t *testingProbeToCloseTimerScheduler) ScheduleSendStreamCloseRaftMuLocked(
	ctx context.Context, rangeID roachpb.RangeID, delay time.Duration,
) {
	// TODO(kvoli): We likely want to test the transition delay using the actual
	// implementation, but we need to refactor out the close scheduler into a
	// separate pkg, or bring it into this package. For now, just do something
	// simple, which is to send raft events to each range on a tick.
	go func() {
		timer := t.state.ts.NewTimer()
		defer timer.Stop()
		timer.Reset(delay)

		select {
		case <-t.state.stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		case <-timer.Ch():
		}
		timer.MarkRead()
		require.NoError(t.state.t,
			t.state.ranges[rangeID].rc.HandleRaftEventRaftMuLocked(ctx, RaftEvent{}))
	}()
}

// TestRangeController tests the RangeController's various methods.
//
//   - init: Initializes the range controller with the given ranges.
//     range_id=<range_id> tenant_id=<tenant_id> local_replica_id=<local_replica_id>
//     store_id=<store_id> replica_id=<replica_id> type=<type> [state=<state>]
//     ...
//
//   - tick: Advances the manual time by the given duration.
//     duration=<duration>
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
//
//   - close_rcs: Closes all range controllers.
//
//   - admit: Admits the given store to the given range.
//     range_id=<range_id>
//     store_id=<store_id> term=<term> to_index=<to_index> pri=<pri>
//     ...
//
//   - raft_event: Simulates a raft event on the given rangeStateProbe, calling
//     HandleRaftEvent.
//     range_id=<range_id>
//     term=<term> index=<index> pri=<pri> size=<size>
//     ...
//
//   - stream_state: Prints the state of the stream(s) for the given range's
//     replicas.
//     range_id=<range_id>
//
//   - metrics: Prints the current state of the eval metrics.
func TestRangeController(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	datadriven.Walk(t, datapathutils.TestDataPath(t, "range_controller"), func(t *testing.T, path string) {
		state := &testingRCState{}
		state.init(t, ctx)
		defer state.stopper.Stop(ctx)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				var regularInitString, elasticInitString string
				var regularLimitString, elasticLimitString string
				d.MaybeScanArgs(t, "regular_init", &regularInitString)
				d.MaybeScanArgs(t, "elastic_init", &elasticInitString)
				d.MaybeScanArgs(t, "regular_limit", &regularLimitString)
				d.MaybeScanArgs(t, "elastic_limit", &elasticLimitString)
				// If the test specifies different token limits or initial token counts
				// (default is the limit), then we override the default limit and also
				// store the initial token count. tokenCounters are created
				// dynamically, so we update them on the fly as well.
				if regularLimitString != "" {
					regularLimit, err := humanizeutil.ParseBytes(regularLimitString)
					require.NoError(t, err)
					kvflowcontrol.RegularTokensPerStream.Override(ctx, &state.settings.SV, regularLimit)
				}
				if elasticLimitString != "" {
					elasticLimit, err := humanizeutil.ParseBytes(elasticLimitString)
					require.NoError(t, err)
					kvflowcontrol.ElasticTokensPerStream.Override(ctx, &state.settings.SV, elasticLimit)
				}
				if regularInitString != "" {
					regularInit, err := humanizeutil.ParseBytes(regularInitString)
					require.NoError(t, err)
					state.initialRegularTokens = kvflowcontrol.Tokens(regularInit)
				}
				if elasticInitString != "" {
					elasticInit, err := humanizeutil.ParseBytes(elasticInitString)
					require.NoError(t, err)
					state.initialElasticTokens = kvflowcontrol.Tokens(elasticInit)
				}

				for _, r := range scanRanges(t, d.Input) {
					state.getOrInitRange(r)
				}
				return state.rangeStateString() + state.tokenCountsString()

			case "tick":
				var durationStr string
				d.ScanArgs(t, "duration", &durationStr)
				duration, err := time.ParseDuration(durationStr)
				require.NoError(t, err)
				state.ts.Advance(duration)
				// Sleep for a bit to allow any timers to fire.
				time.Sleep(20 * time.Millisecond)
				return fmt.Sprintf("now=%v", humanizeutil.Duration(
					state.ts.Now().Sub(timeutil.UnixEpoch)))

			case "wait_for_eval":
				var rangeID int
				var name, priString string
				d.ScanArgs(t, "range_id", &rangeID)
				d.ScanArgs(t, "name", &name)
				d.ScanArgs(t, "pri", &priString)
				testRC := state.ranges[roachpb.RangeID(rangeID)]
				testRC.startWaitForEval(name, parsePriority(t, priString))
				return state.evalStateString()

			case "check_state":
				return state.evalStateString()

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

					state.ssTokenCounter.Eval(kvflowcontrol.Stream{
						StoreID:  roachpb.StoreID(store),
						TenantID: roachpb.SystemTenantID,
					}).adjust(ctx,
						admissionpb.WorkClassFromPri(pri),
						kvflowcontrol.Tokens(tokens))
				}

				return state.tokenCountsString()

			case "cancel_context":
				var rangeID int
				var name string
				d.ScanArgs(t, "range_id", &rangeID)
				d.ScanArgs(t, "name", &name)
				testRC := state.ranges[roachpb.RangeID(rangeID)]
				func() {
					testRC.mu.Lock()
					defer testRC.mu.Unlock()
					testRC.mu.evals[name].cancel()
				}()

				return state.evalStateString()

			case "set_replicas":
				for _, r := range scanRanges(t, d.Input) {
					testRC := state.getOrInitRange(r)
					func() {
						testRC.mu.Lock()
						defer testRC.mu.Unlock()
						testRC.mu.r = r
					}()
					require.NoError(t, testRC.rc.SetReplicasRaftMuLocked(ctx, r.replicas()))
					// Send an empty raft event in order to trigger any potential
					// connectedState changes.
					require.NoError(t, testRC.rc.HandleRaftEventRaftMuLocked(ctx, RaftEvent{}))
				}
				// Sleep for a bit to allow any timers to fire.
				time.Sleep(20 * time.Millisecond)
				return state.rangeStateString()

			case "set_leaseholder":
				var rangeID, replicaID int
				d.ScanArgs(t, "range_id", &rangeID)
				d.ScanArgs(t, "replica_id", &replicaID)
				testRC := state.ranges[roachpb.RangeID(rangeID)]
				testRC.rc.SetLeaseholderRaftMuLocked(ctx, roachpb.ReplicaID(replicaID))
				return state.rangeStateString()

			case "close_rcs":
				for _, r := range state.ranges {
					r.rc.CloseRaftMuLocked(ctx)
				}
				evalStr := state.evalStateString()
				for k := range state.ranges {
					delete(state.ranges, k)
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

						// TODO(sumeer): the test input only specifies an
						// incremental change to the admitted vector, for a
						// single priority. However, in practice, the whole
						// vector will be updated, which also cleanly handles
						// the case of an advancing term. Consider changing
						// this to accept a non-incremental update.
						parts[2] = strings.TrimSpace(parts[2])
						require.True(t, strings.HasPrefix(parts[2], "to_index="))
						parts[2] = strings.TrimPrefix(strings.TrimSpace(parts[2]), "to_index=")
						to_index, err = strconv.Atoi(parts[2])
						require.NoError(t, err)

						parts[3] = strings.TrimSpace(parts[3])
						require.True(t, strings.HasPrefix(parts[3], "pri="))
						parts[3] = strings.TrimPrefix(strings.TrimSpace(parts[3]), "pri=")
						pri := parsePriority(t, parts[3])

						av := AdmittedVector{Term: uint64(term)}
						av.Admitted[AdmissionToRaftPriority(pri)] = uint64(to_index)
						state.ranges[lastRangeID].admit(ctx, roachpb.StoreID(storeID), av)
					}
				}
				return state.tokenCountsString()

			case "raft_event":
				var lastRangeID roachpb.RangeID
				init := false
				var buf []entryInfo

				propRangeEntries := func() {
					event := RaftEvent{
						Entries: make([]raftpb.Entry, len(buf)),
					}
					for i, state := range buf {
						event.Entries[i] = testingCreateEntry(t, state)
					}
					err := state.ranges[lastRangeID].rc.HandleRaftEventRaftMuLocked(ctx, event)
					require.NoError(t, err)
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
						buf = append(buf, entryInfo{
							term:   uint64(term),
							index:  uint64(index),
							enc:    raftlog.EntryEncodingStandardWithACAndPriority,
							tokens: kvflowcontrol.Tokens(size),
							pri:    AdmissionToRaftPriority(pri),
						})
					}
				}
				if init {
					propRangeEntries()
				}
				return state.tokenCountsString()

			case "stream_state":
				var rangeID int
				d.ScanArgs(t, "range_id", &rangeID)
				return state.sendStreamString(roachpb.RangeID(rangeID))

			case "metrics":
				var buf strings.Builder
				evalMetrics := state.evalMetrics

				for _, wc := range []admissionpb.WorkClass{
					admissionpb.RegularWorkClass,
					admissionpb.ElasticWorkClass,
				} {
					fmt.Fprintf(&buf, "%-50v: %v\n", evalMetrics.Waiting[wc].GetName(), evalMetrics.Waiting[wc].Value())
					fmt.Fprintf(&buf, "%-50v: %v\n", evalMetrics.Admitted[wc].GetName(), evalMetrics.Admitted[wc].Count())
					fmt.Fprintf(&buf, "%-50v: %v\n", evalMetrics.Errored[wc].GetName(), evalMetrics.Errored[wc].Count())
					fmt.Fprintf(&buf, "%-50v: %v\n", evalMetrics.Bypassed[wc].GetName(), evalMetrics.Bypassed[wc].Count())
					// We only print the number of recorded durations, instead of any
					// percentiles or cumulative wait times as these are
					// non-deterministic in the test.
					fmt.Fprintf(&buf, "%-50v: %v\n",
						fmt.Sprintf("%v.count", evalMetrics.Duration[wc].GetName()),
						testingFirst(evalMetrics.Duration[wc].CumulativeSnapshot().Total()))
				}
				return buf.String()

			default:
				panic(fmt.Sprintf("unknown command: %s", d.Cmd))
			}
		})
	})
}

func TestGetEntryFCState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	for _, tc := range []struct {
		name            string
		entryInfo       entryInfo
		expectedFCState entryFCState
	}{
		{
			// V1 encoded entries with AC should end up with LowPri and otherwise
			// matching entry information.
			name: "v1_entry_with_ac",
			entryInfo: entryInfo{
				term:   1,
				index:  1,
				enc:    raftlog.EntryEncodingStandardWithAC,
				pri:    raftpb.NormalPri,
				tokens: 100,
			},
			expectedFCState: entryFCState{
				term:            1,
				index:           1,
				pri:             raftpb.LowPri,
				usesFlowControl: true,
				tokens:          100,
			},
		},
		{
			// Likewise for V1 sideloaded entries with AC enabled.
			name: "v1_entry_with_ac_sideloaded",
			entryInfo: entryInfo{
				term:   2,
				index:  2,
				enc:    raftlog.EntryEncodingSideloadedWithAC,
				pri:    raftpb.HighPri,
				tokens: 200,
			},
			expectedFCState: entryFCState{
				term:            2,
				index:           2,
				pri:             raftpb.LowPri,
				usesFlowControl: true,
				tokens:          200,
			},
		},
		{
			name: "entry_without_ac",
			entryInfo: entryInfo{
				term:   3,
				index:  3,
				enc:    raftlog.EntryEncodingStandardWithoutAC,
				tokens: 300,
			},
			expectedFCState: entryFCState{
				term:            3,
				index:           3,
				usesFlowControl: false,
				tokens:          300,
			},
		},
		{
			// V2 encoded entries with AC should end up with their original priority.
			name: "v2_entry_with_ac",
			entryInfo: entryInfo{
				term:   4,
				index:  4,
				enc:    raftlog.EntryEncodingStandardWithACAndPriority,
				pri:    raftpb.NormalPri,
				tokens: 400,
			},
			expectedFCState: entryFCState{
				term:            4,
				index:           4,
				pri:             raftpb.NormalPri,
				usesFlowControl: true,
				tokens:          400,
			},
		},
		{
			// Likewise for V2 sideloaded entries with AC enabled.
			name: "v2_entry_with_ac",
			entryInfo: entryInfo{
				term:   5,
				index:  5,
				enc:    raftlog.EntryEncodingSideloadedWithACAndPriority,
				pri:    raftpb.AboveNormalPri,
				tokens: 500,
			},
			expectedFCState: entryFCState{
				term:            5,
				index:           5,
				pri:             raftpb.AboveNormalPri,
				usesFlowControl: true,
				tokens:          500,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := testingCreateEntry(t, tc.entryInfo)
			fcState := getEntryFCStateOrFatal(ctx, entry)
			require.Equal(t, tc.expectedFCState, fcState)
		})
	}
}

func testingFirst(args ...interface{}) interface{} {
	if len(args) > 0 {
		return args[0]
	}
	return nil
}
