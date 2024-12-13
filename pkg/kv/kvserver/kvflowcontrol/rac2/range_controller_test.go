// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft"
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
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
	"github.com/gogo/protobuf/jsonpb"
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
	sendTokenWatcher      *SendTokenWatcher
	probeToCloseScheduler ProbeToCloseTimerScheduler
	waitForEvalConfig     *WaitForEvalConfig
	evalMetrics           *EvalWaitMetrics
	rcMetrics             *RangeControllerMetrics
	// ranges contains the controllers for each range. It is the main state being
	// tested.
	ranges map[roachpb.RangeID]*testingRCRange
	// setTokenCounters is used to ensure that we only set the initial token
	// counts once per counter.
	setTokenCounters     map[kvflowcontrol.Stream]struct{}
	initialRegularTokens kvflowcontrol.Tokens
	initialElasticTokens kvflowcontrol.Tokens
	maxInflightBytes     uint64
}

func (s *testingRCState) init(t *testing.T, ctx context.Context) {
	s.t = t
	s.testCtx = ctx
	s.settings = cluster.MakeTestingClusterSettings()
	kvflowcontrol.Enabled.Override(ctx, &s.settings.SV, true)
	kvflowcontrol.Mode.Override(ctx, &s.settings.SV, kvflowcontrol.ApplyToAll)
	s.stopper = stop.NewStopper()
	s.ts = timeutil.NewManualTime(timeutil.UnixEpoch)
	s.clock = hlc.NewClockForTesting(s.ts)
	s.ssTokenCounter = NewStreamTokenCounterProvider(s.settings, s.clock)
	s.sendTokenWatcher = NewSendTokenWatcher(s.stopper, s.ts)
	s.probeToCloseScheduler = &testingProbeToCloseTimerScheduler{state: s}
	s.waitForEvalConfig = NewWaitForEvalConfig(s.settings)
	s.evalMetrics = NewEvalWaitMetrics()
	s.rcMetrics = NewRangeControllerMetrics()
	s.ranges = make(map[roachpb.RangeID]*testingRCRange)
	s.setTokenCounters = make(map[kvflowcontrol.Stream]struct{})
	s.initialRegularTokens = kvflowcontrol.Tokens(-1)
	s.initialElasticTokens = kvflowcontrol.Tokens(-1)
}

func sortReplicas(r *testingRCRange) []roachpb.ReplicaDescriptor {
	r.mu.Lock()
	defer r.mu.Unlock()
	return sortReplicasLocked(r)
}

func sortReplicasLocked(r *testingRCRange) []roachpb.ReplicaDescriptor {
	return sortReplicaSet(r.mu.r.replicaSet)
}

func sortReplicaSet(replicas map[roachpb.ReplicaID]testingReplica) []roachpb.ReplicaDescriptor {
	sorted := make([]roachpb.ReplicaDescriptor, 0, len(replicas))
	for _, replica := range replicas {
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
		fmt.Fprintf(&b, "%v: eval %v\n", stream, s.ssTokenCounter.Eval(stream))
		fmt.Fprintf(&b, "       send %v\n", s.ssTokenCounter.Send(stream))
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

	r := s.ranges[rangeID]
	var tr testingRange
	var sendMsgAppCalls []sendMsgAppCall
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		tr = r.mu.r
		sendMsgAppCalls, r.mu.sendMsgAppCalls = r.mu.sendMsgAppCalls, sendMsgAppCalls
	}()
	rc := r.rc

	var scheduledReplicas []roachpb.ReplicaID
	func() {
		rc.scheduledMu.Lock()
		defer rc.scheduledMu.Unlock()
		for replica := range rc.scheduledMu.replicas {
			scheduledReplicas = append(scheduledReplicas, replica)
		}
	}()

	for _, desc := range sortReplicaSet(tr.replicaSet) {
		testRepl := tr.replicaSet[desc.ReplicaID]
		rs := rc.replicaMap[desc.ReplicaID]
		fmt.Fprintf(&b, "%v: ", desc)
		rss := rs.sendStream
		if rss == nil {
			fmt.Fprintf(&b, "closed\n")
			b.WriteString("++++\n")
			continue
		}
		rss.mu.Lock()
		defer rss.mu.Unlock()

		var inflightBytesStr string
		// NB: inflightBytes is retrieved from the state of replicaSendStream,
		// while the starting index of inflight (match + 1) is retrieved from
		// testRepl. If the change in testRepl state has not been communicated to
		// replicaSendStream, these can look inconsistent, which is completely
		// explainable. Typically, test code that advances match (via admit)
		// should also send an empty raft event so that replicaSendStream is aware
		// of the change in match.
		if rss.mu.inflightBytes != 0 {
			inflightBytesStr = fmt.Sprintf(" (%s)", humanize.IBytes(rss.mu.inflightBytes))
		}
		fmt.Fprintf(&b,
			"state=%v closed=%v inflight=[%v,%v)%s send_queue=[%v,%v) precise_q_size=%v",
			rss.mu.connectedState, rss.mu.closed,
			testRepl.info.Match+1, rss.mu.sendQueue.indexToSend, inflightBytesStr,
			rss.mu.sendQueue.indexToSend,
			rss.mu.sendQueue.nextRaftIndex, rss.mu.sendQueue.preciseSizeSum)
		if rss.mu.sendQueue.forceFlushStopIndex.active() {
			var stopStr string
			if !rss.mu.sendQueue.forceFlushStopIndex.untilInfinity() {
				stopStr = fmt.Sprintf(" (stop=%d)", rss.mu.sendQueue.forceFlushStopIndex)
			}
			fmt.Fprintf(&b, " force-flushing%s", stopStr)
		}
		if rss.mu.sendQueue.deductedForSchedulerTokens > 0 {
			fmt.Fprintf(&b, " deducted=%v", rss.mu.sendQueue.deductedForSchedulerTokens)
		}
		if rss.mu.sendQueue.tokenWatcherHandle != (SendTokenWatcherHandle{}) {
			fmt.Fprintf(&b, " watching-for-tokens")
		}
		fmt.Fprintf(&b, "\n")
		fmt.Fprintf(&b, "eval deducted: reg=%v ela=%v\n",
			rss.mu.eval.tokensDeducted[admissionpb.RegularWorkClass],
			rss.mu.eval.tokensDeducted[admissionpb.ElasticWorkClass])
		fmt.Fprintf(&b, "eval original in send-q: reg=%v ela=%v\n",
			rss.mu.sendQueue.originalEvalTokens[admissionpb.RegularWorkClass],
			rss.mu.sendQueue.originalEvalTokens[admissionpb.ElasticWorkClass])
		b.WriteString(formatTrackerState(&rss.raftMu.tracker))
		b.WriteString("++++\n")
	}

	if len(sendMsgAppCalls) != 0 {
		fmt.Fprintf(&b, "MsgApps sent in pull mode:\n")
		slices.SortStableFunc(sendMsgAppCalls, func(a, b sendMsgAppCall) int {
			return cmp.Compare(a.msg.To, b.msg.To)
		})
		for _, e := range sendMsgAppCalls {
			fmt.Fprintf(&b, " to: %d, lowPri: %t entries: [", e.msg.To, e.lowPriorityOverride)
			for j := range e.msg.Entries {
				var prefix string
				if j > 0 {
					prefix = " "
				}
				fmt.Fprintf(&b, "%s%d", prefix, e.msg.Entries[j].Index)
			}
			fmt.Fprintf(&b, "]\n")
		}
		b.WriteString("++++\n")
	}
	if events := r.scheduleControllerEventCount.Load(); events > 0 {
		fmt.Fprintf(&b, "schedule-controller-event-count: %d\n", events)
	}

	if len(rc.scheduledMu.replicas) != 0 {
		slices.Sort(scheduledReplicas)
		fmt.Fprintf(&b, "scheduled-replicas:")
		for _, r := range scheduledReplicas {
			fmt.Fprintf(&b, " %d", r)
		}
		fmt.Fprintf(&b, "\n")
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

func makeTestMutexAsserter() ReplicaMutexAsserter {
	var raftMu syncutil.Mutex
	var replicaMu syncutil.RWMutex
	return MakeReplicaMutexAsserter(&raftMu, &replicaMu)
}

func (s *testingRCState) getOrInitRange(
	t *testing.T, r testingRange, mode RaftMsgAppMode,
) *testingRCRange {
	testRC, ok := s.ranges[r.rangeID]
	if !ok {
		testRC = &testingRCRange{}
		testRC.mu.r = r
		testRC.mu.evals = make(map[string]*testingRCEval)
		testRC.mu.outstandingReturns = make(map[roachpb.ReplicaID]kvflowcontrol.Tokens)
		testRC.mu.quorumPosition = kvflowcontrolpb.RaftLogPosition{Term: 1, Index: 0}
		_ = testRC.raftLog.ApplySnapshot(raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{Index: r.nextRaftIndex - 1},
		})
		options := RangeControllerOptions{
			RangeID:                r.rangeID,
			TenantID:               r.tenantID,
			LocalReplicaID:         r.localReplicaID,
			SSTokenCounter:         s.ssTokenCounter,
			RaftInterface:          testRC,
			MsgAppSender:           testRC,
			Clock:                  s.clock,
			CloseTimerScheduler:    s.probeToCloseScheduler,
			Scheduler:              testRC,
			SendTokenWatcher:       s.sendTokenWatcher,
			EvalWaitMetrics:        s.evalMetrics,
			RangeControllerMetrics: s.rcMetrics,
			WaitForEvalConfig:      s.waitForEvalConfig,
			RaftMaxInflightBytes:   s.maxInflightBytes,
			ReplicaMutexAsserter:   makeTestMutexAsserter(),
			Knobs:                  &kvflowcontrol.TestingKnobs{},
		}

		init := RangeControllerInitState{
			Term:            1,
			ReplicaSet:      r.replicas(),
			Leaseholder:     r.localReplicaID,
			NextRaftIndex:   r.nextRaftIndex,
			ForceFlushIndex: r.forceFlushIndex,
		}
		options.ReplicaMutexAsserter.RaftMu.Lock()
		testRC.rc = NewRangeController(s.testCtx, options, init)
		options.ReplicaMutexAsserter.RaftMu.Unlock()
		s.ranges[r.rangeID] = testRC
	}
	s.maybeSetInitialTokens(r)
	if !ok {
		// Send through an empty raft event to trigger creating necessary replica
		// send streams for the range.
		event := testRC.makeRaftEventWithReplicasState()
		event.MsgAppMode = mode
		func() {
			testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
			defer testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
			require.NoError(t, testRC.rc.HandleRaftEventRaftMuLocked(s.testCtx, event))
		}()
	}
	// Else, this is an existing testingRCRange. The caller may want to send an
	// empty raft event too, to enact some changes.

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
	// snapshots contain snapshots of the tracker state for different replicas,
	// at various points in time. It is used in TestUsingSimulation.
	snapshots []testingTrackerSnapshot
	raftLog   raft.MemoryStorage

	// mu is ordered after RaftMu.
	//
	// This is because we hold RaftMu when calling into the RangeController,
	// which in turn may call back out to the testingRCRange for state
	// information, as it mocks the dependencies of the RangeController.
	mu struct {
		syncutil.Mutex
		r testingRange
		// outstandingReturns is used in TestUsingSimulation to track token
		// returns. Likewise, for quorumPosition. It is not used in
		// TestRangeController.
		outstandingReturns map[roachpb.ReplicaID]kvflowcontrol.Tokens
		quorumPosition     kvflowcontrolpb.RaftLogPosition
		// evals is used in TestRangeController for WaitForEval goroutine
		// callbacks. It is not used in TestUsingSimulation, as the simulation test
		// requires determinism on a smaller timescale than calling WaitForEval via
		// multiple goroutines would allow. See testingNonBlockingAdmit to see how
		// WaitForEval is done in simulation tests.
		evals           map[string]*testingRCEval
		sendMsgAppCalls []sendMsgAppCall
	}
	scheduleControllerEventCount atomic.Uint64
}

type sendMsgAppCall struct {
	msg                 raftpb.Message
	lowPriorityOverride bool
}

func (r *testingRCRange) makeRaftEventWithReplicasState() RaftEvent {
	return RaftEvent{
		ReplicasStateInfo: r.replicasStateInfo(),
	}
}

func (r *testingRCRange) replicasStateInfo() map[roachpb.ReplicaID]ReplicaStateInfo {
	r.mu.Lock()
	defer r.mu.Unlock()

	replicasStateInfo := map[roachpb.ReplicaID]ReplicaStateInfo{}
	for _, replica := range r.mu.r.replicaSet {
		replicasStateInfo[replica.desc.ReplicaID] = replica.info
	}
	return replicasStateInfo
}

func (r *testingRCRange) SendPingRaftMuLocked(roachpb.ReplicaID) bool {
	// TODO(sumeer): record this in the datadriven test output.
	return false
}

func (r *testingRCRange) SendMsgApp(
	ctx context.Context, msg raftpb.Message, lowPriorityOverride bool,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.sendMsgAppCalls = append(r.mu.sendMsgAppCalls, sendMsgAppCall{
		msg:                 msg,
		lowPriorityOverride: lowPriorityOverride,
	})
}

func (r *testingRCRange) ScheduleControllerEvent(rangeID roachpb.RangeID) {
	r.scheduleControllerEventCount.Add(1)
}

func (r *testingRCRange) logSnapshot() raft.LogSnapshot {
	return raft.MakeLogSnapshot(&r.raftLog)
}

func (r *testingRCRange) SendMsgAppRaftMuLocked(
	replicaID roachpb.ReplicaID, ls raft.LogSlice,
) (raftpb.Message, bool) {
	// TODO(pav-kv): populate the message correctly.
	// TODO(pav-kv): use the real RawNode instead of fakes.
	msg := raftpb.Message{
		Type:    raftpb.MsgApp,
		To:      raftpb.PeerID(replicaID),
		Entries: ls.Entries(),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	testR, ok := r.mu.r.replicaSet[replicaID]
	if !ok {
		panic("unknown replica")
	}
	testR = updateNext(r, testR, msg.Entries[len(msg.Entries)-1].Index+1)
	r.mu.r.replicaSet[replicaID] = testR
	return msg, true
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
	var replicaID roachpb.ReplicaID
	var found bool
	func() {
		// We need to ensure that r.mu isn't held before (and while) holding
		// RaftMu, in order to order the locks correctly (RaftMu before
		// testingRCRange.mu).
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, replDesc := range sortReplicasLocked(r) {
			replica := r.mu.r.replicaSet[replDesc.ReplicaID]
			if replica.desc.StoreID == storeID {
				for _, v := range av.Admitted {
					// Ensure that Match doesn't lag behind the highest index in the
					// AdmittedVector.
					replica = tryUpdateMatch(r, replica, v)
				}
				replicaID = replica.desc.ReplicaID
				r.mu.r.replicaSet[replicaID] = replica
				found = true
				break
			}
		}
	}()

	if !found {
		panic("replica not found")
	}

	r.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
	defer r.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
	r.rc.AdmitRaftMuLocked(ctx, replicaID, av)
}

func (r *testingRCRange) updateReplicas(t *testing.T, tr testingRange) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for replicaID, replica := range tr.replicaSet {
		require.Equal(t, uint64(0), replica.info.InflightBytes)
		if replica.info.Match+1 < replica.info.Next {
			entries, err := r.raftLog.Entries(replica.info.Match+1, replica.info.Next, math.MaxUint64)
			require.NoError(t, err)
			for i := range entries {
				replica.info.InflightBytes += uint64(len(entries[i].Data))
			}
			tr.replicaSet[replicaID] = replica
		}
	}
	r.mu.r = tr
}

type testingRange struct {
	rangeID         roachpb.RangeID
	tenantID        roachpb.TenantID
	localReplicaID  roachpb.ReplicaID
	nextRaftIndex   uint64
	forceFlushIndex uint64
	replicaSet      map[roachpb.ReplicaID]testingReplica
}

// Used by simulation test.
func makeSingleVoterTestingRange(
	rangeID roachpb.RangeID,
	tenantID roachpb.TenantID,
	localNodeID roachpb.NodeID,
	localStoreID roachpb.StoreID,
) testingRange {
	return testingRange{
		rangeID:        rangeID,
		tenantID:       tenantID,
		localReplicaID: 1,
		// Set to 1, since simulation test starts appending at index 1.
		nextRaftIndex: 1,
		replicaSet: map[roachpb.ReplicaID]testingReplica{
			1: {
				desc: roachpb.ReplicaDescriptor{
					NodeID:    localNodeID,
					StoreID:   localStoreID,
					ReplicaID: 1,
					Type:      roachpb.VOTER_FULL,
				},
			},
		},
	}
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
	info ReplicaStateInfo
}

func tryUpdateMatch(r *testingRCRange, replica testingReplica, match uint64) testingReplica {
	if match <= replica.info.Match {
		return replica
	}
	entries, err := r.raftLog.Entries(replica.info.Match+1, match+1, math.MaxUint64)
	if err != nil {
		panic(err)
	}
	for i := range entries {
		entrySize := uint64(len(entries[i].Data))
		if replica.info.InflightBytes < entrySize {
			panic(errors.Errorf(
				"inflight-bytes %d < entrySize %d at index %d", replica.info.InflightBytes, entrySize, i))
		}
		replica.info.InflightBytes -= entrySize
	}
	replica.info.Match = match
	return replica
}

func updateNext(r *testingRCRange, replica testingReplica, next uint64) testingReplica {
	if next < replica.info.Next {
		panic(errors.Errorf("next %d < replica.info.Next %d", next, replica.info.Next))
	}
	if next == replica.info.Next {
		return replica
	}
	entries, err := r.raftLog.Entries(replica.info.Next, next, math.MaxUint64)
	if err != nil {
		panic(err)
	}
	for i := range entries {
		entrySize := uint64(len(entries[i].Data))
		replica.info.InflightBytes += entrySize
	}
	replica.info.Next = next
	return replica
}

func scanRanges(t *testing.T, input string) []testingRange {
	replicas := []testingRange{}

	for _, line := range strings.Split(input, "\n") {
		parts := strings.Fields(line)
		parts[0] = strings.TrimSpace(parts[0])
		if strings.HasPrefix(parts[0], "range_id=") {
			// Create a new range, any replicas which follow until the next range_id
			// line will be added to this replica set.
			var rangeID, tenantID, localReplicaID, nextRaftIndex int
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

			parts[3] = strings.TrimSpace(parts[3])
			require.True(t, strings.HasPrefix(parts[3], "next_raft_index="))
			parts[3] = strings.TrimPrefix(strings.TrimSpace(parts[3]), "next_raft_index=")
			nextRaftIndex, err = strconv.Atoi(parts[3])
			require.NoError(t, err)

			replicas = append(replicas, testingRange{
				rangeID:        roachpb.RangeID(rangeID),
				tenantID:       roachpb.MustMakeTenantID(uint64(tenantID)),
				localReplicaID: roachpb.ReplicaID(localReplicaID),
				nextRaftIndex:  uint64(nextRaftIndex),
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

	next := uint64(0)
	match := uint64(0)
	// The fourth field is optional, if set it contains the tracker state of the
	// replica on the leader replica (localReplicaID). The valid states are
	// Probe, Replicate, and Snapshot.
	if len(parts) > 3 {
		require.LessOrEqual(t, 5, len(parts))
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
		parts[4] = strings.TrimSpace(parts[4])
		require.True(t, strings.HasPrefix(parts[4], "next="))
		parts[4] = strings.TrimPrefix(strings.TrimSpace(parts[4]), "next=")
		nextInt, err := strconv.Atoi(parts[4])
		require.NoError(t, err)
		next = uint64(nextInt)
		if len(parts) > 5 {
			require.Equal(t, 6, len(parts))
			parts[5] = strings.TrimSpace(parts[5])
			require.True(t, strings.HasPrefix(parts[5], "match="))
			parts[5] = strings.TrimPrefix(strings.TrimSpace(parts[5]), "match=")
			matchInt, err := strconv.Atoi(parts[5])
			require.NoError(t, err)
			match = uint64(matchInt)
		}
	}

	return testingReplica{
		desc: roachpb.ReplicaDescriptor{
			NodeID:    roachpb.NodeID(storeID),
			StoreID:   roachpb.StoreID(storeID),
			ReplicaID: roachpb.ReplicaID(replicaID),
			Type:      replicaType,
		},
		info: ReplicaStateInfo{State: state, Match: match, Next: next},
	}
}

// parsePriorityOrNone returns either a valid admissionpb.WorkPriority or
// !subjectToRAC, where the latter represents an entry not subject to
// replication admission control.
func parsePriorityOrNone(
	t *testing.T, input string,
) (subjectToRAC bool, pri admissionpb.WorkPriority) {
	switch input {
	case "LowPri":
		return true, admissionpb.LowPri
	case "NormalPri":
		return true, admissionpb.NormalPri
	case "HighPri":
		return true, admissionpb.HighPri
	case "None":
		return false, 0
	default:
		require.Failf(t, "unknown work class", "%v", input)
		return false, admissionpb.WorkPriority(-1)
	}
}

func parsePriority(t *testing.T, input string) admissionpb.WorkPriority {
	subjectToRAC, pri := parsePriorityOrNone(t, input)
	require.Truef(t, subjectToRAC, "not a valid priority")
	return pri
}

type testingEntryRange struct {
	fromIndex, toIndex uint64
}

type testingRaftEvent struct {
	rangeID           roachpb.RangeID
	entries           []entryInfo
	sendingEntryRange map[roachpb.ReplicaID]testingEntryRange
}

func parseRaftEvents(t *testing.T, input string) []testingRaftEvent {
	var eventBuf []testingRaftEvent
	n := 0
	for _, line := range strings.Split(input, "\n") {
		var (
			rangeID, term, index              int
			replicaID, fromIndex, toIndexExcl int
			size                              int64
			err                               error
			pri                               admissionpb.WorkPriority
		)

		parts := strings.Fields(line)
		parts[0] = strings.TrimSpace(parts[0])
		switch {
		case strings.HasPrefix(parts[0], "range_id="):
			parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "range_id=")
			rangeID, err = strconv.Atoi(parts[0])
			require.NoError(t, err)
			eventBuf = append(eventBuf, testingRaftEvent{
				rangeID: roachpb.RangeID(rangeID),
			})
			n++
		case strings.HasPrefix(parts[0], "entries"):
			// Skip the first line, which is the header.
			eventBuf[n-1].entries = make([]entryInfo, 0)
			continue
		case strings.HasPrefix(parts[0], "term="):
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
			var subjectToRAC bool
			subjectToRAC, pri = parsePriorityOrNone(t, parts[2])
			enc := raftlog.EntryEncodingStandardWithACAndPriority
			if !subjectToRAC {
				enc = raftlog.EntryEncodingStandardWithoutAC
			}

			parts[3] = strings.TrimSpace(parts[3])
			require.True(t, strings.HasPrefix(parts[3], "size="))
			parts[3] = strings.TrimPrefix(strings.TrimSpace(parts[3]), "size=")
			size, err = humanizeutil.ParseBytes(parts[3])
			require.NoError(t, err)

			eventBuf[n-1].entries = append(eventBuf[n-1].entries, entryInfo{
				term:   uint64(term),
				index:  uint64(index),
				enc:    enc,
				tokens: kvflowcontrol.Tokens(size),
				pri:    AdmissionToRaftPriority(pri),
			})
		case strings.HasPrefix(parts[0], "sending"):
			// Skip the first line, which is the header.
			eventBuf[n-1].sendingEntryRange = make(map[roachpb.ReplicaID]testingEntryRange)
			continue
		case strings.HasPrefix(parts[0], "replica_id="):
			require.True(t, strings.HasPrefix(parts[0], "replica_id="))
			parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "replica_id=")
			replicaID, err = strconv.Atoi(parts[0])
			require.NoError(t, err)

			// Line has format: [%d,%d) corresponding to [from,to).
			parts[1] = strings.TrimSpace(parts[1])
			bounds := strings.Split(parts[1], ",")
			require.Len(t, bounds, 2)

			bounds[0] = strings.TrimSpace(bounds[0])
			require.True(t, strings.HasPrefix(bounds[0], "["))
			bounds[0] = strings.TrimPrefix(strings.TrimSpace(bounds[0]), "[")
			fromIndex, err = strconv.Atoi(bounds[0])
			require.NoError(t, err)

			bounds[1] = strings.TrimSpace(bounds[1])
			require.True(t, strings.HasSuffix(bounds[1], ")"))
			bounds[1] = strings.TrimSuffix(strings.TrimSpace(bounds[1]), ")")
			toIndexExcl, err = strconv.Atoi(bounds[1])
			require.NoError(t, err)

			eventBuf[n-1].sendingEntryRange[roachpb.ReplicaID(replicaID)] = testingEntryRange{
				fromIndex: uint64(fromIndex),
				// Subtract 1 to make the range exclusive.
				toIndex: uint64(toIndexExcl - 1),
			}
		default:
			require.Failf(t, "unknown line", "%v", line)
		}
	}
	return eventBuf
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
		func() {
			r := t.state.ranges[rangeID]
			event := r.makeRaftEventWithReplicasState()

			r.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
			defer r.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()

			require.NoError(t.state.t,
				r.rc.HandleRaftEventRaftMuLocked(ctx, event))
		}()
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
//   - admit: Admits up to to_index for the given store, part of the given
//     range.
//     range_id=<range_id>
//     store_id=<store_id> term=<term> to_index=<to_index> pri=<pri>
//     ...
//
//   - raft_event: Simulates a raft event on the given range, calling
//     HandleRaftEvent.
//     range_id=<range_id>
//     entries
//     term=<term> index=<index> pri=<pri> size=<size>
//     ...
//     sending
//     replica_id=<replica_id> [from,to)
//     ...
//     ...
//
//   - stream_state: Prints the state of the stream(s) for the given range's
//     replicas.
//     range_id=<range_id>
//
//   - metrics: Prints the current state of the eval metrics.
//     [type=(eval|send_queue|range_controller)] default eval.
//
//   - inspect: Prints the result of an Inspect() call to the RangeController
//     for a range.
//     range_id=<range_id>
func TestRangeController(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Used to marshal the output of the Inspect() method into a human-readable
	// formatted JSON string. See case "inspect" below.
	marshaller := jsonpb.Marshaler{
		Indent:       "  ",
		EmitDefaults: true,
		OrigName:     true,
	}

	datadriven.Walk(t, datapathutils.TestDataPath(t, "range_controller"), func(t *testing.T, path string) {
		state := &testingRCState{}
		state.init(t, ctx)
		defer state.stopper.Stop(ctx)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				var (
					regularInitString, elasticInitString   string
					regularLimitString, elasticLimitString string
				)
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
				var maxInflightBytesString string
				d.MaybeScanArgs(t, "max_inflight_bytes", &maxInflightBytesString)
				if maxInflightBytesString != "" {
					maxInflightBytes, err := humanizeutil.ParseBytes(maxInflightBytesString)
					require.NoError(t, err)
					state.maxInflightBytes = uint64(maxInflightBytes)
				}

				for _, r := range scanRanges(t, d.Input) {
					state.getOrInitRange(t, r, MsgAppPush)
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
				eval := true
				if d.HasArg("send") {
					eval = false
				}
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

					var tc *tokenCounter
					if eval {
						tc = state.ssTokenCounter.Eval(kvflowcontrol.Stream{
							StoreID:  roachpb.StoreID(store),
							TenantID: roachpb.SystemTenantID,
						})
					} else {
						tc = state.ssTokenCounter.Send(kvflowcontrol.Stream{
							StoreID:  roachpb.StoreID(store),
							TenantID: roachpb.SystemTenantID,
						})
					}
					tc.adjust(ctx,
						admissionpb.WorkClassFromPri(pri),
						kvflowcontrol.Tokens(tokens),
						AdjNormal,
					)
				}
				// Sleep for a bit to allow any timers to fire.
				time.Sleep(20 * time.Millisecond)
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
				mode := MsgAppPush
				if d.HasArg("pull-mode") {
					mode = MsgAppPull
				}
				for _, r := range scanRanges(t, d.Input) {
					testRC := state.getOrInitRange(t, r, mode)
					testRC.updateReplicas(t, r)
					func() {
						testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
						defer testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
						require.NoError(t, testRC.rc.SetReplicasRaftMuLocked(ctx, r.replicas()))
					}()
					// Send an empty raft event in order to trigger any potential
					// connectedState changes.
					event := testRC.makeRaftEventWithReplicasState()
					event.MsgAppMode = mode
					func() {
						testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
						defer testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
						require.NoError(t, testRC.rc.HandleRaftEventRaftMuLocked(ctx, event))
					}()
				}
				// Sleep for a bit to allow any timers to fire.
				time.Sleep(20 * time.Millisecond)
				return state.rangeStateString()

			case "set_leaseholder":
				var rangeID, replicaID int
				d.ScanArgs(t, "range_id", &rangeID)
				d.ScanArgs(t, "replica_id", &replicaID)
				testRC := state.ranges[roachpb.RangeID(rangeID)]
				func() {
					testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
					defer testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
					testRC.rc.SetLeaseholderRaftMuLocked(ctx, roachpb.ReplicaID(replicaID))
				}()
				return state.rangeStateString()

			case "set_force_flush_index":
				var rangeID int
				d.ScanArgs(t, "range_id", &rangeID)
				var index int
				d.ScanArgs(t, "index", &index)
				mode := MsgAppPull
				if d.HasArg("push-mode") {
					mode = MsgAppPush
				}
				testRC := state.ranges[roachpb.RangeID(rangeID)]
				func() {
					testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
					defer testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
					testRC.rc.opts.ReplicaMutexAsserter.ReplicaMu.Lock()
					defer testRC.rc.opts.ReplicaMutexAsserter.ReplicaMu.Unlock()
					testRC.rc.ForceFlushIndexChangedLocked(ctx, uint64(index))
				}()
				// Send an empty raft event in order to trigger any potential changes.
				event := testRC.makeRaftEventWithReplicasState()
				event.MsgAppMode = mode
				func() {
					testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
					defer testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
					require.NoError(t, testRC.rc.HandleRaftEventRaftMuLocked(ctx, event))
				}()
				// Sleep for a bit to allow any timers to fire.
				time.Sleep(20 * time.Millisecond)
				return state.sendStreamString(roachpb.RangeID(rangeID))

			case "close_rcs":
				for _, r := range state.ranges {
					func() {
						r.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
						defer r.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
						r.rc.CloseRaftMuLocked(ctx)
					}()
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
				// Sleep for a bit to allow any timers to fire.
				time.Sleep(20 * time.Millisecond)
				return state.tokenCountsString()

			case "raft_event":
				mode := MsgAppPush
				if d.HasArg("pull-mode") {
					mode = MsgAppPull
				}
				for _, event := range parseRaftEvents(t, d.Input) {
					entries := make([]raftpb.Entry, len(event.entries))
					for i, entry := range event.entries {
						entries[i] = testingCreateEntry(t, entry)
					}
					testRC := state.ranges[event.rangeID]
					require.NoError(t, testRC.raftLog.Append(entries))

					raftEvent := RaftEvent{
						MsgAppMode: mode,
						// Term is set to 1, to be consistent with what we use when
						// calling NewRangeController in this test.
						Term:              1,
						Entries:           entries,
						MsgApps:           map[roachpb.ReplicaID][]raftpb.Message{},
						LogSnapshot:       testRC.logSnapshot(),
						ReplicasStateInfo: state.ranges[event.rangeID].replicasStateInfo(),
					}
					msgApp := raftpb.Message{
						Type: raftpb.MsgApp,
						To:   0,
						// We will selectively include a prefix of the new entries, and a
						// suffix of entries that were previously appended, down below.
						Entries: nil,
					}
					func() {
						testRC.mu.Lock()
						defer testRC.mu.Unlock()

						for replicaID, testR := range testRC.mu.r.replicaSet {
							msgApp.Entries = nil
							msgApp.To = raftpb.PeerID(replicaID)
							if mode == MsgAppPush {
								if event.sendingEntryRange == nil ||
									testingFirst(0, event.sendingEntryRange[replicaID]) == nil {
									// When sendingEntryRange is not specified, include all
									// entries for this replica.
									msgApp.Entries = raftEvent.Entries
								} else {
									fromIndex := event.sendingEntryRange[replicaID].fromIndex
									toIndex := event.sendingEntryRange[replicaID].toIndex
									entries, err := testRC.raftLog.Entries(fromIndex, toIndex+1, infinityEntryIndex)
									require.NoError(t, err)
									msgApp.Entries = entries
								}
								raftEvent.MsgApps[replicaID] = append([]raftpb.Message(nil), msgApp)
							}
							// Else MsgAppPull mode, so raftEvent.MsgApps is unpopulated.

							if len(msgApp.Entries) > 0 {
								// Bump the Next field for replicas that have MsgApps being
								// sent to them.
								testR = updateNext(testRC, testR, msgApp.Entries[len(msgApp.Entries)-1].Index+1)
								testRC.mu.r.replicaSet[replicaID] = testR
							} else if testR.desc.ReplicaID == testRC.mu.r.localReplicaID &&
								len(raftEvent.Entries) > 0 {
								// Leader does not see MsgApps, but the Next needs to be bumped.
								//
								// TODO(sumeer): many of the test cases are sending MsgApps to
								// the leader. Stop doing it.
								testR = updateNext(
									testRC, testR, raftEvent.Entries[len(raftEvent.Entries)-1].Index+1)
								testRC.mu.r.replicaSet[replicaID] = testR
							}
						}
					}()
					raftEvent.ReplicasStateInfo = testRC.replicasStateInfo()
					func() {
						testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
						defer testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
						require.NoError(t, testRC.rc.HandleRaftEventRaftMuLocked(ctx, raftEvent))
					}()
				}
				// Sleep for a bit to allow any timers to fire.
				time.Sleep(20 * time.Millisecond)
				return state.tokenCountsString()

			case "internal_schedule_replica":
				// scheduleReplica is called internally by replicaSendStream. Calling
				// this here is artificial.
				var rangeID, replicaID int
				d.ScanArgs(t, "range_id", &rangeID)
				d.ScanArgs(t, "replica_id", &replicaID)
				testRC := state.ranges[roachpb.RangeID(rangeID)]
				func() {
					testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
					defer testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
					testRC.rc.scheduleReplica(roachpb.ReplicaID(replicaID))
				}()
				return state.sendStreamString(roachpb.RangeID(rangeID))

			case "handle_scheduler_event":
				var rangeID int
				d.ScanArgs(t, "range_id", &rangeID)
				testRC := state.ranges[roachpb.RangeID(rangeID)]
				mode := MsgAppPull
				if d.HasArg("push-mode") {
					mode = MsgAppPush
				}
				func() {
					testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
					defer testRC.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
					testRC.rc.HandleSchedulerEventRaftMuLocked(ctx, mode, testRC.logSnapshot())
				}()
				// Sleep for a bit to allow any timers to fire.
				time.Sleep(20 * time.Millisecond)
				return state.sendStreamString(roachpb.RangeID(rangeID))

			case "stream_state":
				var rangeID int
				d.ScanArgs(t, "range_id", &rangeID)
				// Sleep for a bit to allow any timers to fire.
				time.Sleep(20 * time.Millisecond)
				return state.sendStreamString(roachpb.RangeID(rangeID))

			case "metrics":
				typ := "eval"
				d.MaybeScanArgs(t, "type", &typ)
				var buf strings.Builder

				switch typ {
				case "eval":
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
				case "range_controller":
					rcMetrics := state.rcMetrics
					fmt.Fprintf(&buf, "%v: %v\n", rcMetrics.Count.GetName(), rcMetrics.Count.Value())
				case "send_queue":
					sendQueueMetrics := state.rcMetrics.SendQueue
					sendQueueTokenMetrics := state.ssTokenCounter.tokenMetrics.CounterMetrics[SendToken].SendQueue[0]
					// We need to aggregate these ourselves, since this is normally done
					// in kvserver.updateReplicationGauges.
					var sizeCount, sizeBytes int64
					for _, rcState := range state.ranges {
						stats := RangeSendStreamStats{}
						func() {
							rcState.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
							defer rcState.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
							rcState.rc.updateSendQueueStatsRaftMuLocked(state.ts.Now())
						}()
						rcState.rc.SendStreamStats(&stats)
						count, bytes := stats.SumSendQueues()
						sizeCount += count
						sizeBytes += bytes
					}
					sendQueueMetrics.SizeBytes.Update(sizeBytes)
					sendQueueMetrics.SizeCount.Update(sizeCount)
					fmt.Fprintf(&buf, "%-66v: %v\n", sendQueueMetrics.SizeCount.GetName(), sendQueueMetrics.SizeCount.Value())
					fmt.Fprintf(&buf, "%-66v: %v\n", sendQueueMetrics.SizeBytes.GetName(), sendQueueMetrics.SizeBytes.Value())
					fmt.Fprintf(&buf, "%-66v: %v\n", sendQueueMetrics.ForceFlushedScheduledCount.GetName(), sendQueueMetrics.ForceFlushedScheduledCount.Value())
					fmt.Fprintf(&buf, "%-66v: %v\n", sendQueueMetrics.DeductedForSchedulerBytes.GetName(), sendQueueMetrics.DeductedForSchedulerBytes.Value())
					fmt.Fprintf(&buf, "%-66v: %v\n", sendQueueMetrics.PreventionCount.GetName(), sendQueueMetrics.PreventionCount.Count())
					fmt.Fprintf(&buf, "%-66v: %v\n", sendQueueTokenMetrics.ForceFlushDeducted.GetName(), sendQueueTokenMetrics.ForceFlushDeducted.Count())
					for _, wc := range []admissionpb.WorkClass{
						admissionpb.RegularWorkClass,
						admissionpb.ElasticWorkClass,
					} {
						fmt.Fprintf(&buf, "%-66v: %v\n", sendQueueTokenMetrics.PreventionDeducted[wc].GetName(), sendQueueTokenMetrics.PreventionDeducted[wc].Count())
					}

				default:
					panic(fmt.Sprintf("unknown metrics type: %s", typ))
				}
				return buf.String()

			case "inspect":
				var rangeID int
				d.ScanArgs(t, "range_id", &rangeID)
				var handle kvflowinspectpb.Handle
				func() {
					rc := state.ranges[roachpb.RangeID(rangeID)].rc
					rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
					defer rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
					handle = rc.InspectRaftMuLocked(ctx)
				}()
				marshaled, err := marshaller.MarshalToString(&handle)
				require.NoError(t, err)
				return fmt.Sprintf("%v", marshaled)

			case "send_stream_stats":
				var (
					rangeID int
					refresh = true
				)
				d.ScanArgs(t, "range_id", &rangeID)
				d.MaybeScanArgs(t, "refresh", &refresh)

				r := state.ranges[roachpb.RangeID(rangeID)]
				if refresh {
					func() {
						r.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
						defer r.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
						r.rc.updateSendQueueStatsRaftMuLocked(state.ts.Now())
					}()
				}
				stats := RangeSendStreamStats{}
				r.rc.SendStreamStats(&stats)
				log.Infof(ctx, "stats: %v", stats)
				var buf strings.Builder
				for _, repl := range sortReplicas(r) {
					replStats, ok := stats.ReplicaSendStreamStats(repl.ReplicaID)
					require.True(t, ok)
					buf.WriteString(fmt.Sprintf("%v: is_state_replicate=%-5v has_send_queue=%-5v send_queue_size=%v / %v entries\n",
						repl,
						replStats.IsStateReplicate,
						replStats.HasSendQueue,
						// Cast for formatting.
						kvflowcontrol.Tokens(replStats.SendQueueBytes),
						replStats.SendQueueCount,
					))
				}
				return buf.String()

			case "set_flow_control_config":
				if d.HasArg("enabled") {
					var enabled bool
					d.ScanArgs(t, "enabled", &enabled)
					kvflowcontrol.Enabled.Override(ctx, &state.settings.SV, enabled)
				}
				if d.HasArg("mode") {
					var mode string
					d.ScanArgs(t, "mode", &mode)
					var m kvflowcontrol.ModeT
					switch mode {
					case "apply_to_all":
						m = kvflowcontrol.ApplyToAll
					case "apply_to_elastic":
						m = kvflowcontrol.ApplyToElastic
					default:
						panic(fmt.Sprintf("unknown mode %s", mode))
					}
					kvflowcontrol.Mode.Override(ctx, &state.settings.SV, m)
				}
				// Sleep for a bit to allow any timers to fire.
				time.Sleep(20 * time.Millisecond)
				return fmt.Sprintf("enabled: %t mode: %v",
					kvflowcontrol.Enabled.Get(&state.settings.SV),
					kvflowcontrol.Mode.Get(&state.settings.SV))

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
				id:              entryID{index: 1, term: 1},
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
				id:              entryID{index: 2, term: 2},
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
				id:              entryID{index: 3, term: 3},
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
				id:              entryID{index: 4, term: 4},
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
				id:              entryID{index: 5, term: 5},
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

func TestRaftEventFromMsgStorageAppendAndMsgAppsBasic(t *testing.T) {
	// raftpb.Entry and raftpb.Message are only partially populated below, which
	// could be improved in the future.
	appendMsg := raftpb.Message{
		Type:     raftpb.MsgStorageAppend,
		LogTerm:  10,
		Snapshot: &raftpb.Snapshot{},
		Entries: []raftpb.Entry{
			{
				Term: 9,
			},
		},
	}
	outboundMsgs := []raftpb.Message{
		{
			Type: raftpb.MsgApp,
			To:   20,
			Entries: []raftpb.Entry{
				{
					Term: 9,
				},
			},
		},
		{
			Type: raftpb.MsgBeat,
			To:   22,
		},
		{
			Type: raftpb.MsgApp,
			To:   21,
		},
		{
			Type: raftpb.MsgApp,
			To:   20,
			Entries: []raftpb.Entry{
				{
					Term: 10,
				},
			},
		},
	}
	msgAppScratch := map[roachpb.ReplicaID][]raftpb.Message{}
	logSnap := raft.LogSnapshot{}
	infoMap := map[roachpb.ReplicaID]ReplicaStateInfo{}
	checkSnapAndMap := func(event RaftEvent) {
		require.Equal(t, logSnap, event.LogSnapshot)
		require.Equal(t, infoMap, event.ReplicasStateInfo)
	}

	// No outbound msgs.
	event := RaftEventFromMsgStorageAppendAndMsgApps(
		MsgAppPush, 20, appendMsg, nil, logSnap, msgAppScratch, infoMap)
	require.Equal(t, uint64(10), event.Term)
	require.Equal(t, appendMsg.Snapshot, event.Snap)
	require.Equal(t, appendMsg.Entries, event.Entries)
	require.Nil(t, event.MsgApps)
	checkSnapAndMap(event)
	// Only LogSnapshot and ReplicasStateInfo set.
	event = RaftEventFromMsgStorageAppendAndMsgApps(
		MsgAppPush, 20, raftpb.Message{}, nil, logSnap, msgAppScratch, infoMap)
	checkSnapAndMap(event)
	event.LogSnapshot = raft.LogSnapshot{}
	event.ReplicasStateInfo = nil
	require.Equal(t, RaftEvent{}, event)
	// Outbound msgs contains no MsgApps for a follower, since the only MsgApp
	// is for the leader.
	event = RaftEventFromMsgStorageAppendAndMsgApps(
		MsgAppPush, 20, appendMsg, outboundMsgs[:2], logSnap, msgAppScratch, infoMap)
	require.Equal(t, uint64(10), event.Term)
	require.Equal(t, appendMsg.Snapshot, event.Snap)
	require.Equal(t, appendMsg.Entries, event.Entries)
	require.Nil(t, event.MsgApps)
	checkSnapAndMap(event)
	// Outbound msgs contains MsgApps for followers. We call this twice to
	// ensure msgAppScratch is cleared before reuse.
	for i := 0; i < 2; i++ {
		event = RaftEventFromMsgStorageAppendAndMsgApps(
			MsgAppPush, 19, appendMsg, outboundMsgs, logSnap, msgAppScratch, infoMap)
		require.Equal(t, uint64(10), event.Term)
		require.Equal(t, appendMsg.Snapshot, event.Snap)
		require.Equal(t, appendMsg.Entries, event.Entries)
		var msgApps []raftpb.Message
		for _, v := range msgAppScratch {
			msgApps = append(msgApps, v...)
		}
		slices.SortStableFunc(msgApps, func(a, b raftpb.Message) int {
			return cmp.Compare(a.To, b.To)
		})
		require.Equal(t, []raftpb.Message{outboundMsgs[0], outboundMsgs[3], outboundMsgs[2]}, msgApps)
		checkSnapAndMap(event)
		// Outbound msgs contains MsgApps for followers, but they are ignored
		// since in pull mode.
		event = RaftEventFromMsgStorageAppendAndMsgApps(
			MsgAppPull, 19, appendMsg, outboundMsgs, logSnap, msgAppScratch, infoMap)
		require.Equal(t, uint64(10), event.Term)
		require.Equal(t, appendMsg.Snapshot, event.Snap)
		require.Equal(t, appendMsg.Entries, event.Entries)
		require.Nil(t, event.MsgApps)
		checkSnapAndMap(event)
	}
}

func TestConstructRaftEventForReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		startingIndex uint64 = 10
		startingTerm  uint64 = 1
		tokenMult            = 100
		defaultPri           = raftpb.NormalPri
		defaultUseFC         = true
	)

	makeEntry := func(t *testing.T, info entryInfo) raftpb.Entry {
		if info.pri == 0 {
			info.pri = raftpb.NormalPri
		}
		if info.enc == raftlog.EntryEncodingEmpty {
			info.enc = raftlog.EntryEncodingStandardWithACAndPriority
		}
		return testingCreateEntry(t, info)
	}

	makeEntries := func(t *testing.T, n int) []raftpb.Entry {
		var entries []raftpb.Entry
		for i := 0; i < n; i++ {
			entries = append(entries, makeEntry(t, entryInfo{
				index:  startingIndex + uint64(i),
				term:   startingTerm,
				tokens: kvflowcontrol.Tokens((i + 1) * tokenMult)}))
		}
		return entries
	}

	makeEntryFCStates := func(t *testing.T, n int) []entryFCState {
		var entries []entryFCState
		for i := 0; i < n; i++ {
			entries = append(entries, entryFCState{
				id:              entryID{index: startingIndex + uint64(i), term: startingTerm},
				usesFlowControl: defaultUseFC,
				tokens:          kvflowcontrol.Tokens((i + 1) * tokenMult),
				pri:             defaultPri,
			})
		}
		return entries
	}

	ctx := context.Background()
	testCases := []struct {
		name                     string
		raftEventAppendState     raftEventAppendState
		latestReplicaStateInfo   ReplicaStateInfo
		existingSendStreamState  existingSendStreamState
		msgApps                  []raftpb.Message
		scratchSendingEntries    []entryFCState
		expectedRaftEventReplica raftEventForReplica
		expectedScratchEntries   []entryFCState
		expectFatal              bool
	}{
		{
			name: "new entries existing send stream",
			raftEventAppendState: raftEventAppendState{
				newEntries:           makeEntryFCStates(t, 2),
				rewoundNextRaftIndex: 10,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  12,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: true,
				indexToSend:               10,
			},
			msgApps: []raftpb.Message{
				{
					Type:    raftpb.MsgApp,
					Entries: makeEntries(t, 2),
				},
			},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex:      10,
				newEntries:         makeEntryFCStates(t, 2),
				sendingEntries:     makeEntryFCStates(t, 2),
				recreateSendStream: false,
			},
			expectedScratchEntries: []entryFCState{},
		},
		{
			name: "no new entries create send stream",
			raftEventAppendState: raftEventAppendState{
				newEntries:           []entryFCState{},
				rewoundNextRaftIndex: 10,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  10,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: false,
			},
			msgApps:               []raftpb.Message{},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex:      10,
				newEntries:         []entryFCState{},
				sendingEntries:     nil,
				recreateSendStream: true,
			},
			expectedScratchEntries: []entryFCState{},
		},
		{
			name: "inconsistent msgapps recreate send stream",
			raftEventAppendState: raftEventAppendState{
				newEntries:           makeEntryFCStates(t, 2),
				rewoundNextRaftIndex: 10,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  12,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: true,
				indexToSend:               10,
			},
			msgApps: []raftpb.Message{
				{
					Type: raftpb.MsgApp,
					Entries: []raftpb.Entry{
						makeEntry(t, entryInfo{index: 10, term: 1, tokens: 100}),
						// Inconsistent index.
						makeEntry(t, entryInfo{index: 12, term: 1, tokens: 200}),
					},
				},
			},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex:      10,
				newEntries:         makeEntryFCStates(t, 2),
				sendingEntries:     makeEntryFCStates(t, 2),
				recreateSendStream: true,
			},
			expectedScratchEntries: []entryFCState{},
		},
		{
			name: "no new entries, next in the past, create send stream",
			raftEventAppendState: raftEventAppendState{
				newEntries:           []entryFCState{},
				rewoundNextRaftIndex: 15,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  10,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: false,
			},
			msgApps:               []raftpb.Message{},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex:      15,
				recreateSendStream: true,
				newEntries:         []entryFCState{},
				sendingEntries:     nil,
			},
			expectedScratchEntries: []entryFCState{},
		},
		{
			name: "existing send stream with no msgapps",
			raftEventAppendState: raftEventAppendState{
				newEntries:           []entryFCState{},
				rewoundNextRaftIndex: 10,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  10,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: true,
				indexToSend:               10,
			},
			msgApps:               []raftpb.Message{},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex:      10,
				recreateSendStream: false,
				newEntries:         []entryFCState{},
				sendingEntries:     nil,
			},
			expectedScratchEntries: []entryFCState{},
		},
		{
			name: "existing send stream with no msgapps and next in the past",
			raftEventAppendState: raftEventAppendState{
				newEntries:           []entryFCState{},
				rewoundNextRaftIndex: 12,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  10,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: true,
				indexToSend:               10,
			},
			msgApps:               []raftpb.Message{},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex:      12,
				recreateSendStream: false,
				newEntries:         []entryFCState{},
				sendingEntries:     nil,
			},
			expectedScratchEntries: []entryFCState{},
		},
		{
			name: "msgapps with entries before new entries",
			raftEventAppendState: raftEventAppendState{
				newEntries: []entryFCState{
					{id: entryID{index: 12, term: 1}, usesFlowControl: true, tokens: 300, pri: raftpb.NormalPri},
					{id: entryID{index: 13, term: 1}, usesFlowControl: true, tokens: 400, pri: raftpb.NormalPri},
				},
				rewoundNextRaftIndex: 12,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  14,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: true,
				indexToSend:               10,
			},
			msgApps: []raftpb.Message{
				{

					Type:    raftpb.MsgApp,
					Entries: makeEntries(t, 4),
				},
			},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex: 12,
				newEntries: []entryFCState{
					{id: entryID{index: 12, term: 1}, usesFlowControl: true, tokens: 300, pri: raftpb.NormalPri},
					{id: entryID{index: 13, term: 1}, usesFlowControl: true, tokens: 400, pri: raftpb.NormalPri},
				},
				sendingEntries:     makeEntryFCStates(t, 4),
				recreateSendStream: false,
			},
			expectedScratchEntries: makeEntryFCStates(t, 4),
		},
		{
			name: "msgapps with entries before new entries and send-queue after sending",
			raftEventAppendState: raftEventAppendState{
				newEntries: []entryFCState{
					{id: entryID{index: 12, term: 1}, usesFlowControl: true, tokens: 300, pri: raftpb.NormalPri},
					{id: entryID{index: 13, term: 1}, usesFlowControl: true, tokens: 400, pri: raftpb.NormalPri},
					{id: entryID{index: 14, term: 1}, usesFlowControl: true, tokens: 400, pri: raftpb.NormalPri},
				},
				rewoundNextRaftIndex: 12,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  14,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: true,
				indexToSend:               10,
			},
			msgApps: []raftpb.Message{
				{

					Type:    raftpb.MsgApp,
					Entries: makeEntries(t, 4),
				},
			},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex: 12,
				newEntries: []entryFCState{
					{id: entryID{index: 12, term: 1}, usesFlowControl: true, tokens: 300, pri: raftpb.NormalPri},
					{id: entryID{index: 13, term: 1}, usesFlowControl: true, tokens: 400, pri: raftpb.NormalPri},
					{id: entryID{index: 14, term: 1}, usesFlowControl: true, tokens: 400, pri: raftpb.NormalPri},
				},
				sendingEntries:     makeEntryFCStates(t, 4),
				recreateSendStream: false,
			},
			expectedScratchEntries: makeEntryFCStates(t, 4),
		},
		{
			name: "regression in send-queue",
			raftEventAppendState: raftEventAppendState{
				newEntries:           makeEntryFCStates(t, 2),
				rewoundNextRaftIndex: 10,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  12,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: true,
				indexToSend:               11, // Regression to 10.
			},
			msgApps: []raftpb.Message{
				{
					Type:    raftpb.MsgApp,
					Entries: makeEntries(t, 2),
				},
			},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex:      10,
				newEntries:         makeEntryFCStates(t, 2),
				sendingEntries:     makeEntryFCStates(t, 2),
				recreateSendStream: true,
			},
			expectedScratchEntries: []entryFCState{},
		},
		{
			name: "forward jump in send-queue",
			raftEventAppendState: raftEventAppendState{
				newEntries:           makeEntryFCStates(t, 2),
				rewoundNextRaftIndex: 10,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  12,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: true,
				// Forward jump.
				indexToSend: 9,
			},
			msgApps: []raftpb.Message{
				{
					Type:    raftpb.MsgApp,
					Entries: makeEntries(t, 2),
				},
			},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex:      10,
				newEntries:         makeEntryFCStates(t, 2),
				sendingEntries:     makeEntryFCStates(t, 2),
				recreateSendStream: true,
			},
			expectedScratchEntries: []entryFCState{},
		},
		{
			name: "msgapp upper bound is lagging next, so recreate",
			raftEventAppendState: raftEventAppendState{
				newEntries:           makeEntryFCStates(t, 2),
				rewoundNextRaftIndex: 10,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  12,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: true,
				indexToSend:               10,
			},
			msgApps: []raftpb.Message{
				{
					Type:    raftpb.MsgApp,
					Entries: makeEntries(t, 1),
				},
			},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex:      10,
				newEntries:         makeEntryFCStates(t, 2),
				sendingEntries:     makeEntryFCStates(t, 2),
				recreateSendStream: true,
			},
			expectedScratchEntries: []entryFCState{},
		},
		{
			name: "msgapp upper bound is ahead of appended entries, so recreate",
			raftEventAppendState: raftEventAppendState{
				newEntries:           makeEntryFCStates(t, 2),
				rewoundNextRaftIndex: 10,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  12,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: true,
				indexToSend:               10,
			},
			msgApps: []raftpb.Message{
				{
					Type:    raftpb.MsgApp,
					Entries: makeEntries(t, 3),
				},
			},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex:      10,
				newEntries:         makeEntryFCStates(t, 2),
				sendingEntries:     makeEntryFCStates(t, 2),
				recreateSendStream: true,
			},
			expectedScratchEntries: []entryFCState{},
		},
		{
			name: "msgapp first index is ahead of append, so recreate",
			raftEventAppendState: raftEventAppendState{
				newEntries:           makeEntryFCStates(t, 2),
				rewoundNextRaftIndex: 10,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  12,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: true,
				indexToSend:               10,
			},
			msgApps: []raftpb.Message{
				{
					Type: raftpb.MsgApp,
					Entries: []raftpb.Entry{
						makeEntry(t, entryInfo{index: 11, term: 1, tokens: 100}),
					},
				},
			},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex:      10,
				newEntries:         makeEntryFCStates(t, 2),
				sendingEntries:     makeEntryFCStates(t, 2),
				recreateSendStream: true,
			},
			expectedScratchEntries: []entryFCState{},
		},
		{
			name: "next not equal to index to send",
			raftEventAppendState: raftEventAppendState{
				newEntries:           []entryFCState{},
				rewoundNextRaftIndex: 10,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  10,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: true,
				indexToSend:               9,
			},
			msgApps:               []raftpb.Message{},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex:      10,
				newEntries:         []entryFCState{},
				sendingEntries:     nil,
				recreateSendStream: true,
			},
			expectedScratchEntries: []entryFCState{},
		},
		{
			name: "next greater than rewound next raft index",
			raftEventAppendState: raftEventAppendState{
				newEntries:           []entryFCState{},
				rewoundNextRaftIndex: 10,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  12,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: true,
				indexToSend:               10,
			},
			msgApps:               []raftpb.Message{},
			scratchSendingEntries: []entryFCState{},
			expectFatal:           true,
		},
		{
			name: "multiple msgapps",
			raftEventAppendState: raftEventAppendState{
				newEntries:           makeEntryFCStates(t, 3),
				rewoundNextRaftIndex: 10,
			},
			latestReplicaStateInfo: ReplicaStateInfo{
				State: tracker.StateReplicate,
				Match: 9,
				Next:  13,
			},
			existingSendStreamState: existingSendStreamState{
				existsAndInStateReplicate: true,
				indexToSend:               10,
			},
			msgApps: []raftpb.Message{
				{
					Type: raftpb.MsgApp,
					Entries: []raftpb.Entry{
						makeEntry(t, entryInfo{index: 10, term: 1, tokens: 100}),
					},
				},
				{
					Type: raftpb.MsgApp,
					Entries: []raftpb.Entry{
						makeEntry(t, entryInfo{index: 11, term: 1, tokens: 200}),
						makeEntry(t, entryInfo{index: 12, term: 1, tokens: 300}),
					},
				},
			},
			scratchSendingEntries: []entryFCState{},
			expectedRaftEventReplica: raftEventForReplica{
				replicaStateInfo: ReplicaStateInfo{
					State: tracker.StateReplicate,
					Match: 9,
					Next:  10,
				},
				nextRaftIndex:      10,
				newEntries:         makeEntryFCStates(t, 3),
				sendingEntries:     makeEntryFCStates(t, 3),
				recreateSendStream: false,
			},
			expectedScratchEntries: []entryFCState{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectFatal {
				require.Panics(t, func() {
					constructRaftEventForReplica(
						ctx,
						MsgAppPush,
						tc.raftEventAppendState,
						tc.latestReplicaStateInfo,
						tc.existingSendStreamState,
						tc.msgApps,
						raft.LogSnapshot{},
						tc.scratchSendingEntries,
					)
				})
			} else {
				gotRaftEventReplica, gotScratch := constructRaftEventForReplica(
					ctx,
					MsgAppPush,
					tc.raftEventAppendState,
					tc.latestReplicaStateInfo,
					tc.existingSendStreamState,
					tc.msgApps,
					raft.LogSnapshot{},
					tc.scratchSendingEntries,
				)
				require.Equal(t, tc.expectedRaftEventReplica, gotRaftEventReplica)
				require.Equal(t, tc.expectedScratchEntries, gotScratch)
			}
		})
	}
}

func TestRangeSendStreamStatsString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stats := RangeSendStreamStats{
		internal: []ReplicaSendStreamStats{
			{
				IsStateReplicate: false,
				HasSendQueue:     true,
				ReplicaSendQueueStats: ReplicaSendQueueStats{
					ReplicaID:      1,
					SendQueueCount: 10,
					SendQueueBytes: 100,
				},
			},
			{
				IsStateReplicate: true,
				HasSendQueue:     false,
				ReplicaSendQueueStats: ReplicaSendQueueStats{
					ReplicaID:      2,
					SendQueueCount: 0,
					SendQueueBytes: 0,
				},
			},
		},
	}

	require.Equal(t,
		"[r1=(is_state_replicate=false has_send_queue=true send_queue_size=100 B / 10 entries), "+
			"r2=(is_state_replicate=true has_send_queue=false send_queue_size=0 B / 0 entries)]",
		stats.String())
}
