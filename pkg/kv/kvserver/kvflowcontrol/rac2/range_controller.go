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
	"cmp"
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// RangeController provides flow control for replication traffic in KV, for a
// range at the leader.
//
// None of the methods are called with Replica.mu held. The caller should
// typically order its mutexes before Replica.mu.
type RangeController interface {
	// WaitForEval seeks admission to evaluate a request at the given priority.
	// This blocks until there are positive tokens available for the request to
	// be admitted for evaluation, or the context is canceled (which returns an
	// error). Note the number of tokens required by the request is not
	// considered, only the priority of the request, as the number of tokens is
	// not known until eval.
	//
	// In the non-error case, the waited return value is true if the
	// RangeController was not closed during the execution of WaitForEval. If
	// closed, a (false, nil) will be returned -- this is important for the
	// caller to fall back to waiting on the local store.
	//
	// No mutexes should be held.
	WaitForEval(ctx context.Context, pri admissionpb.WorkPriority) (waited bool, err error)
	// HandleRaftEventRaftMuLocked handles the provided raft event for the range.
	//
	// Requires replica.raftMu to be held.
	HandleRaftEventRaftMuLocked(ctx context.Context, e RaftEvent) error
	// HandleSchedulerEventRaftMuLocked processes an event scheduled by the
	// controller.
	//
	// Requires replica.raftMu to be held.
	HandleSchedulerEventRaftMuLocked(ctx context.Context) error
	// SetReplicasRaftMuLocked sets the replicas of the range. The caller will
	// never mutate replicas, and neither should the callee.
	//
	// Requires replica.raftMu to be held.
	SetReplicasRaftMuLocked(ctx context.Context, replicas ReplicaSet) error
	// SetLeaseholderRaftMuLocked sets the leaseholder of the range.
	//
	// Requires raftMu to be held.
	SetLeaseholderRaftMuLocked(ctx context.Context, replica roachpb.ReplicaID)
	// CloseRaftMuLocked closes the range controller.
	//
	// Requires replica.raftMu to be held.
	CloseRaftMuLocked(ctx context.Context)
}

// TODO(pav-kv): This interface a placeholder for the interface containing raft
// methods. Replace this as part of #128019.
type RaftInterface interface {
	// FollowerState returns the current state of a follower. The value of
	// Match, Next, Admitted are populated iff in StateReplicate. All entries >=
	// Next have not had MsgApps constructed during the lifetime of this
	// StateReplicate (they may have been constructed previously).
	//
	// When a follower transitions from {StateProbe,StateSnapshot} =>
	// StateReplicate, we start trying to send MsgApps. We should
	// notice such transitions both in HandleRaftEvent and SetReplicasLocked.
	//
	// RACv1 also cared about three other cases where the follower behaved as if
	// it were disconnected (a) paused follower, (b) follower is behind, (c)
	// follower is inactive (see
	// replicaFlowControlIntegrationImpl.notActivelyReplicatingTo). (b) and (c)
	// were needed since it paced at rate of slowest replica, while for regular
	// work we will in v2 pace at slowest in quorum (and we don't care about
	// elastic experiencing a hiccup, given it paces at rate of slowest). For
	// (a), we plan to remove follower pausing. So the v2 code will be
	// simplified.
	//
	// Requires Replica.raftMu to be held, Replica.mu is not held.
	FollowerState(replicaID roachpb.ReplicaID) FollowerStateInfo
}

type FollowerStateInfo struct {
	State tracker.StateType

	// Remaining only populated in StateReplicate.
	// (Match, Next) is in-flight.
	Match uint64
	Next  uint64
	// TODO(kvoli): Find a better home for this, we need it for token return.
	Term uint64
	// Invariant: Admitted[i] <= Match.
	Admitted [raftpb.NumPriorities]uint64
}

// RaftEvent carries a RACv2-relevant subset of raft state sent to storage.
type RaftEvent struct {
	// Term is the leader term on whose behalf the entries or snapshot are
	// written. Note that it may be behind the raft node's current term.
	Term uint64
	// Snap contains the snapshot to be written to storage.
	Snap *raftpb.Snapshot
	// Entries contains the log entries to be written to storage.
	Entries []raftpb.Entry
	// Only populated on leader when operating in MsgAppPush mode. This is
	// informational, for bookkeeping in the callee. These are MsgApps to
	// followers.
	//
	// NB: these MsgApps can be for entries in Entries, or for earlier ones.
	MsgApps map[roachpb.ReplicaID]raftpb.Message
}

// RaftEventFromMsgStorageAppendAndMsgApps constructs a RaftEvent from the
// given raft MsgStorageAppend message. Returns zero value if the message is
// empty.
func RaftEventFromMsgStorageAppendAndMsgApps(
	replicaID roachpb.ReplicaID,
	append raftpb.Message,
	outboundMsgs []raftpb.Message,
	msgAppScratch map[roachpb.ReplicaID]raftpb.Message,
) RaftEvent {
	var event RaftEvent
	if append.Type == raftpb.MsgStorageAppend {
		event = RaftEvent{
			Term:    append.LogTerm,
			Snap:    append.Snapshot,
			Entries: append.Entries,
		}
	}
	if len(outboundMsgs) == 0 {
		return event
	}
	for k := range msgAppScratch {
		delete(msgAppScratch, k)
	}
	for _, msg := range outboundMsgs {
		if msg.Type != raftpb.MsgApp || roachpb.ReplicaID(msg.To) == replicaID {
			continue
		}
		_, ok := msgAppScratch[roachpb.ReplicaID(msg.To)]
		if ok {
			panic(errors.AssertionFailedf("more than one MsgApp for same peer %d", msg.To))
		}
		msgAppScratch[roachpb.ReplicaID(msg.To)] = msg
	}
	event.MsgApps = msgAppScratch
	return event
}

// NoReplicaID is a special value of roachpb.ReplicaID, which can never be a
// valid ID.
const NoReplicaID roachpb.ReplicaID = 0

// ReplicaSet is a map, unlike roachpb.ReplicaSet, for convenient lookup by
// ReplicaID.
type ReplicaSet map[roachpb.ReplicaID]roachpb.ReplicaDescriptor

// SafeFormat implements the redact.SafeFormatter interface.
func (rs ReplicaSet) SafeFormat(w redact.SafePrinter, _ rune) {
	// If <= 7 replicas, no need to allocate.
	var buf [7]roachpb.ReplicaDescriptor
	replicas := buf[0:0:len(buf)]
	for _, desc := range rs {
		replicas = append(replicas, desc)
	}
	slices.SortFunc(replicas, func(a, b roachpb.ReplicaDescriptor) int {
		return cmp.Compare(a.ReplicaID, b.ReplicaID)
	})
	w.Printf("[")
	i := 0
	for _, desc := range replicas {
		if i > 0 {
			w.Printf(",")
		}
		w.Printf("%v", desc)
		i++
	}
	w.Printf("]")
}

func (rs ReplicaSet) String() string {
	return redact.StringWithoutMarkers(rs)
}

// ProbeToCloseTimerScheduler is an interface for scheduling the closing of a
// replica send stream.
type ProbeToCloseTimerScheduler interface {
	// ScheduleSendStreamCloseRaftMuLocked schedules a callback with a raft event
	// after the given delay. This function may be used to handle send stream
	// state transition, usually to close a send stream after the given delay.
	// e.g.,
	//
	//   HandleRaftEventRaftMuLocked(ctx, RaftEvent{})
	//
	// Which will trigger handleReadyState to close the send stream if it hasn't
	// transitioned to StateReplicate.
	//
	// Requires replica.raftMu to be held.
	ScheduleSendStreamCloseRaftMuLocked(
		ctx context.Context, rangeID roachpb.RangeID, delay time.Duration)
}

type RangeControllerOptions struct {
	RangeID  roachpb.RangeID
	TenantID roachpb.TenantID
	// LocalReplicaID is the ReplicaID of the local replica, which is the
	// leader.
	LocalReplicaID roachpb.ReplicaID
	// SSTokenCounter provides access to all the TokenCounters that will be
	// needed (keyed by (tenantID, storeID)).
	SSTokenCounter      *StreamTokenCounterProvider
	RaftInterface       RaftInterface
	Clock               *hlc.Clock
	CloseTimerScheduler ProbeToCloseTimerScheduler
	EvalWaitMetrics     *EvalWaitMetrics
}

// RangeControllerInitState is the initial state at the time of creation.
type RangeControllerInitState struct {
	// Must include RangeControllerOptions.ReplicaID.
	ReplicaSet ReplicaSet
	// Leaseholder may be set to NoReplicaID, in which case the leaseholder is
	// unknown.
	Leaseholder roachpb.ReplicaID

	NextRaftIndex uint64
}

// The rangeController exists for a single leader term, when this replica is
// the leader.
type rangeController struct {
	opts       RangeControllerOptions
	replicaSet ReplicaSet
	// leaseholder can be NoReplicaID or not be in ReplicaSet, i.e., it is
	// eventually consistent with the set of replicas.
	leaseholder   roachpb.ReplicaID
	nextRaftIndex uint64

	mu struct {
		syncutil.Mutex

		// State for waiters. When anything in voterSets changes, voterSetRefreshCh
		// is closed, and replaced with a new channel. The voterSets is
		// copy-on-write, so waiters make a shallow copy.
		voterSets         []voterSet
		voterSetRefreshCh chan struct{}
	}

	replicaMap map[roachpb.ReplicaID]*replicaState
}

// voterStateForWaiters informs whether WaitForEval is required to wait for
// eval-tokens for a voter.
type voterStateForWaiters struct {
	replicaID        roachpb.ReplicaID
	isLeader         bool
	isLeaseHolder    bool
	isStateReplicate bool
	evalTokenCounter *tokenCounter
}

type voterSet []voterStateForWaiters

var _ RangeController = &rangeController{}

func NewRangeController(
	ctx context.Context, o RangeControllerOptions, init RangeControllerInitState,
) *rangeController {
	rc := &rangeController{
		opts:        o,
		leaseholder: init.Leaseholder,
		replicaMap:  make(map[roachpb.ReplicaID]*replicaState),
	}
	rc.mu.voterSetRefreshCh = make(chan struct{})
	rc.updateReplicaSet(ctx, init.ReplicaSet)
	rc.updateVoterSets()
	return rc
}

// WaitForEval blocks until there are positive tokens available for the
// request to be admitted for evaluation. Note the number of tokens required
// by the request is not considered, only the priority of the request, as the
// number of tokens is not known until eval.
//
// No mutexes should be held.
func (rc *rangeController) WaitForEval(
	ctx context.Context, pri admissionpb.WorkPriority,
) (waited bool, err error) {
	wc := admissionpb.WorkClassFromPri(pri)
	waitForAllReplicateHandles := false
	if wc == admissionpb.ElasticWorkClass {
		waitForAllReplicateHandles = true
	}
	var handles []tokenWaitingHandleInfo
	var scratch []reflect.SelectCase

	rc.opts.EvalWaitMetrics.onWaiting(wc)
	start := rc.opts.Clock.PhysicalTime()
retry:
	// Snapshot the voterSets and voterSetRefreshCh.
	rc.mu.Lock()
	vss := rc.mu.voterSets
	vssRefreshCh := rc.mu.voterSetRefreshCh
	rc.mu.Unlock()

	if vssRefreshCh == nil {
		// RangeControllerImpl is closed.
		// TODO(kvoli): We also need to do this in the replica_rac2.Processor,
		// which will allow requests to bypass when a replica is not the leader and
		// therefore the controller is closed.
		rc.opts.EvalWaitMetrics.onBypassed(wc, rc.opts.Clock.PhysicalTime().Sub(start))
		return false, nil
	}
	for _, vs := range vss {
		quorumCount := (len(vs) + 2) / 2
		haveEvalTokensCount := 0
		handles = handles[:0]
		requiredWait := false
		for _, v := range vs {
			available, handle := v.evalTokenCounter.TokensAvailable(wc)
			if available {
				haveEvalTokensCount++
				continue
			}

			// Don't have eval tokens, and have a handle.
			handleInfo := tokenWaitingHandleInfo{
				handle: handle,
				requiredWait: v.isLeader || v.isLeaseHolder ||
					(waitForAllReplicateHandles && v.isStateReplicate),
			}
			handles = append(handles, handleInfo)
			if !requiredWait && handleInfo.requiredWait {
				requiredWait = true
			}
		}
		remainingForQuorum := quorumCount - haveEvalTokensCount
		if remainingForQuorum < 0 {
			remainingForQuorum = 0
		}
		if remainingForQuorum > 0 || requiredWait {
			var state WaitEndState
			state, scratch = WaitForEval(ctx, vssRefreshCh, handles, remainingForQuorum, scratch)
			switch state {
			case WaitSuccess:
				continue
			case ContextCanceled:
				rc.opts.EvalWaitMetrics.onErrored(wc, rc.opts.Clock.PhysicalTime().Sub(start))
				return false, ctx.Err()
			case RefreshWaitSignaled:
				goto retry
			}
		}
	}
	rc.opts.EvalWaitMetrics.onAdmitted(wc, rc.opts.Clock.PhysicalTime().Sub(start))
	return true, nil
}

type raftEventForReplica struct {
	// Match and Next represent the state preceding this raft event.
	FollowerStateInfo
	nextRaftIndex  uint64
	newEntries     []entryFCState
	sendingEntries []entryFCState
}

func constructSendingEntries(
	ctx context.Context, msgApp raftpb.Message, newEntries []entryFCState,
) []entryFCState {
	n := len(msgApp.Entries)
	if n == 0 {
		return nil
	}
	firstNewEntryIndex := uint64(math.MaxUint64)
	if len(newEntries) > 0 {
		firstNewEntryIndex = newEntries[0].index
	}
	if msgApp.Entries[0].Index > firstNewEntryIndex {
		panic("")
	}
	if msgApp.Entries[0].Index == firstNewEntryIndex {
		// Common case. Sub-slice
		if n > len(newEntries) {
			panic("")
		}
		return newEntries[:n]
	}
	// Starts before the first new entry.
	sendingEntries := make([]entryFCState, 0, len(msgApp.Entries))
	for _, entry := range msgApp.Entries {
		if entry.Index >= firstNewEntryIndex {
			sendingEntries = append(sendingEntries, newEntries[entry.Index-firstNewEntryIndex])
		} else {
			// Need to decode.
			sendingEntries = append(sendingEntries, getEntryFCStateOrFatal(ctx, entry))
		}
	}
	return sendingEntries
}

// HandleRaftEventRaftMuLocked handles the provided raft event for the range.
//
// Requires replica.raftMu to be held.
func (rc *rangeController) HandleRaftEventRaftMuLocked(ctx context.Context, e RaftEvent) error {
	// Compute the flow control state for each new entry. We do this once
	// here, instead of decoding each entry multiple times for all replicas.
	newEntries := make([]entryFCState, len(e.Entries))
	for i, entry := range e.Entries {
		newEntries[i] = getEntryFCStateOrFatal(ctx, entry)
	}
	nextRaftIndex := rc.nextRaftIndex
	if n := len(e.Entries); n > 0 {
		rc.nextRaftIndex = e.Entries[n-1].Index
	}
	shouldWaitChange := false
	for r, rs := range rc.replicaMap {
		info := rc.opts.RaftInterface.FollowerState(r)
		eventForReplica := raftEventForReplica{
			FollowerStateInfo: info,
			nextRaftIndex:     nextRaftIndex,
			newEntries:        newEntries,
		}
		if r == rc.opts.LocalReplicaID {
			// Everything is considered sent.
			eventForReplica.sendingEntries = newEntries
		} else {
			msgApp, ok := e.MsgApps[r]
			if ok {
				if eventForReplica.FollowerStateInfo.State != tracker.StateReplicate {
					panic("")
				}
				eventForReplica.sendingEntries = constructSendingEntries(ctx, msgApp, newEntries)
			}
		}
		if n := len(eventForReplica.sendingEntries); n > 0 {
			next := eventForReplica.sendingEntries[0].index
			expectedNext := eventForReplica.sendingEntries[n-1].index + 1
			if expectedNext != eventForReplica.Next {
				panic("")
			}
			// Rewind Next.
			eventForReplica.Next = next
		}
		shouldWaitChange = rs.handleReadyState(
			ctx, nextRaftIndex, eventForReplica.FollowerStateInfo) || shouldWaitChange
		rs.handleReadyEntries(ctx, eventForReplica)
	}
	// If there was a quorum change, update the voter sets, triggering the
	// refresh channel for any requests waiting for eval tokens.
	if shouldWaitChange {
		rc.updateVoterSets()
	}
	return nil
}

// HandleSchedulerEventRaftMuLocked processes an event scheduled by the
// controller.
//
// Requires replica.raftMu to be held.
func (rc *rangeController) HandleSchedulerEventRaftMuLocked(ctx context.Context) error {
	panic("unimplemented")
}

// SetReplicasRaftMuLocked sets the replicas of the range. The caller will
// never mutate replicas, and neither should the callee.
//
// Requires replica.raftMu to be held.
func (rc *rangeController) SetReplicasRaftMuLocked(ctx context.Context, replicas ReplicaSet) error {
	rc.updateReplicaSet(ctx, replicas)
	rc.updateVoterSets()
	return nil
}

// SetLeaseholderRaftMuLocked sets the leaseholder of the range.
//
// Requires raftMu to be held.
func (rc *rangeController) SetLeaseholderRaftMuLocked(
	ctx context.Context, replica roachpb.ReplicaID,
) {
	if replica == rc.leaseholder {
		return
	}
	rc.leaseholder = replica
	rc.updateVoterSets()
}

// CloseRaftMuLocked closes the range controller.
//
// Requires replica.raftMu to be held.
func (rc *rangeController) CloseRaftMuLocked(ctx context.Context) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.mu.voterSets = nil
	close(rc.mu.voterSetRefreshCh)
	rc.mu.voterSetRefreshCh = nil
}

func (rc *rangeController) updateReplicaSet(ctx context.Context, newSet ReplicaSet) {
	prevSet := rc.replicaSet
	for r := range prevSet {
		desc, ok := newSet[r]
		if !ok {
			delete(rc.replicaMap, r)
		} else {
			rs := rc.replicaMap[r]
			rs.desc = desc
		}
	}
	for r, desc := range newSet {
		_, ok := prevSet[r]
		if ok {
			// Already handled above.
			continue
		}
		rc.replicaMap[r] = NewReplicaState(ctx, rc, desc)
	}
	rc.replicaSet = newSet
}

func (rc *rangeController) updateVoterSets() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	setCount := 1
	for _, r := range rc.replicaSet {
		isOld := r.IsVoterOldConfig()
		isNew := r.IsVoterNewConfig()
		if !isOld && !isNew {
			continue
		}
		if !isOld && isNew {
			setCount++
			break
		}
	}
	var voterSets []voterSet
	for len(voterSets) < setCount {
		voterSets = append(voterSets, voterSet{})
	}
	for _, r := range rc.replicaSet {
		isOld := r.IsVoterOldConfig()
		isNew := r.IsVoterNewConfig()
		if !isOld && !isNew {
			continue
		}
		// Is a voter.
		rs := rc.replicaMap[r.ReplicaID]
		vsfw := voterStateForWaiters{
			replicaID:        r.ReplicaID,
			isLeader:         r.ReplicaID == rc.opts.LocalReplicaID,
			isLeaseHolder:    r.ReplicaID == rc.leaseholder,
			isStateReplicate: rs.isStateReplicate(),
			evalTokenCounter: rs.evalTokenCounter,
		}
		if isOld {
			voterSets[0] = append(voterSets[0], vsfw)
		}
		if isNew && setCount == 2 {
			voterSets[1] = append(voterSets[1], vsfw)
		}
	}
	rc.mu.voterSets = voterSets
	close(rc.mu.voterSetRefreshCh)
	rc.mu.voterSetRefreshCh = make(chan struct{})
}

type replicaState struct {
	parent *rangeController
	// stream aggregates across the streams for the same (tenant, store). This
	// is the identity that is used to deduct tokens or wait for tokens to be
	// positive.
	stream                             kvflowcontrol.Stream
	evalTokenCounter, sendTokenCounter *tokenCounter
	desc                               roachpb.ReplicaDescriptor

	sendStream *replicaSendStream
}

func NewReplicaState(
	ctx context.Context, parent *rangeController, desc roachpb.ReplicaDescriptor,
) *replicaState {
	stream := kvflowcontrol.Stream{TenantID: parent.opts.TenantID, StoreID: desc.StoreID}
	rs := &replicaState{
		parent:           parent,
		stream:           stream,
		evalTokenCounter: parent.opts.SSTokenCounter.Eval(stream),
		sendTokenCounter: parent.opts.SSTokenCounter.Send(stream),
		desc:             desc,
	}
	// Don't bother creating the replicaSendStream here. We will do this in
	// the next Ready which will be called immediately after. This centralizes
	// the logic of replicaSendStream creation.
	return rs
}

// replicaSendStream exists only in StateReplicate, and briefly in StateProbe.
type replicaSendStream struct {
	parent *replicaState

	mu struct {
		syncutil.Mutex
		// connectedStateStart is the time when the connectedState was last
		// transitioned from one state to another e.g., from replicate to
		// probeRecentlyReplicate or vice versa.
		connectedState      connectedState
		connectedStateStart time.Time

		// nextRaftIndexInitial is the value of nextRaftIndex when this
		// replicaSendStream was created, or transitioned into replicate.
		nextRaftIndexInitial uint64

		// tracker contains entries that have been sent, and have had
		// send-tokens deducted (and will have had eval-tokens deducted iff
		// index >= nextRaftIndexInitial).
		//
		// Contains no entries in probeRecentlyReplicate.
		tracker Tracker

		// Eval state.
		//
		// Contains no tokens in probeRecentlyReplicate.
		eval struct {
			// Only for indices >= nextRaftIndexInitial. These are either in
			// the send-queue, or in the tracker.
			tokensDeducted [admissionpb.NumWorkClasses]kvflowcontrol.Tokens
		}
		// When the presence of a sendQueue is due to Raft flow control, which
		// does not take store overload into account, we consider that the delay
		// in reaching quorum due to the send-queue is acceptable. The state is
		// maintained here so that we can transition between Raft flow control caused
		// send-queue and replication flow control caused send-queue, and vice versa.
		//
		// Say we pretended that there was no send-queue when using Raft flow
		// control. Then send tokens would have been deducted and entries
		// placed in tracker at eval time. When transitioning from replication
		// flow control to Raft flow control we would need to iterate over all
		// entries in the send-queue, read them from storage, and place them
		// in the tracker.
		//
		// Not updated in state probeRecentlyReplicate.
		sendQueue struct {
			// State of send-queue. [indexToSend, nextRaftIndex) have not been
			// sent. indexToSend == FollowerStateInfo.Next. nextRaftIndex is
			// the current value of NextUnstableIndex at the leader. The
			// send-queue is always empty for the leader.
			indexToSend   uint64
			nextRaftIndex uint64
		}
		closed bool
	}
}

func (rss *replicaSendStream) changeConnectedStateLocked(state connectedState, now time.Time) {
	rss.mu.connectedState = state
	rss.mu.connectedStateStart = now
}

func (rs *replicaState) createReplicaSendStream(indexToSend uint64, nextRaftIndex uint64) {
	// Must be in StateReplicate on creation.
	rs.sendStream = &replicaSendStream{
		parent: rs,
	}
	rs.sendStream.changeConnectedStateLocked(
		replicate, rs.parent.opts.Clock.PhysicalTime())
	rs.sendStream.mu.nextRaftIndexInitial = nextRaftIndex
	rs.sendStream.mu.tracker.Init(rs.stream)
	rs.sendStream.mu.sendQueue.indexToSend = indexToSend
	rs.sendStream.mu.sendQueue.nextRaftIndex = nextRaftIndex
	rs.sendStream.mu.closed = false
}

func (rs *replicaState) isStateReplicate() bool {
	// probeRecentlyReplicate is also included in this state.
	return rs.sendStream != nil
}

type entryFCState struct {
	term, index     uint64
	usesFlowControl bool
	tokens          kvflowcontrol.Tokens
	pri             raftpb.Priority
}

// getEntryFCStateOrFatal returns the given entry's flow control state. If the
// entry encoding cannot be determined, a fatal is logged.
func getEntryFCStateOrFatal(ctx context.Context, entry raftpb.Entry) entryFCState {
	enc, pri, err := raftlog.EncodingOf(entry)
	if err != nil {
		log.Fatalf(ctx, "error getting encoding of entry: %v", err)
	}

	if enc == raftlog.EntryEncodingStandardWithAC || enc == raftlog.EntryEncodingSideloadedWithAC {
		// When the entry is encoded with the v1 encoding, we don't have access to
		// the priority via the priority bit and would need to decode the admission
		// metadata. Instead, assume the priority is low priority, which is the
		// only sane flow control priority enforcement level in v1 (elastic only).
		pri = raftpb.LowPri
	}

	return entryFCState{
		index:           entry.Index,
		term:            entry.Term,
		usesFlowControl: enc.UsesAdmissionControl(),
		tokens:          kvflowcontrol.Tokens(len(entry.Data)),
		pri:             pri,
	}
}

func (rs *replicaState) handleReadyEntries(ctx context.Context, event raftEventForReplica) {
	if rs.sendStream == nil {
		return
	}
	rs.sendStream.mu.Lock()
	defer rs.sendStream.mu.Unlock()
	if rs.sendStream.mu.connectedState != replicate {
		return
	}
	rs.sendStream.handleReadyEntriesLocked(ctx, event)
}

// handleReadyState handles state management for the replica based on the
// provided follower state information. If the state changes in a way that
// affects requests waiting for evaluation, returns true.
func (rs *replicaState) handleReadyState(
	ctx context.Context, nextRaftIndex uint64, info FollowerStateInfo,
) (shouldWaitChange bool) {
	switch info.State {
	case tracker.StateProbe:
		if rs.sendStream == nil {
			// We have already closed the stream, nothing to do.
			return false
		}
		if shouldClose := func() (should bool) {
			now := rs.parent.opts.Clock.PhysicalTime()
			rs.sendStream.mu.Lock()
			defer rs.sendStream.mu.Unlock()

			if state := rs.sendStream.mu.connectedState; state == probeRecentlyReplicate &&
				now.Sub(rs.sendStream.mu.connectedStateStart) >= probeRecentlyReplicateDuration() {
				// The replica has been in StateProbe for at least
				// probeRecentlyReplicateDuration (default 1s) second, close the
				// stream.
				should = true
			} else if state != probeRecentlyReplicate {
				rs.sendStream.changeToProbeLocked(ctx, now)
			}
			return should
		}(); shouldClose {
			rs.closeSendStream(ctx)
			shouldWaitChange = true
		}

	case tracker.StateReplicate:
		if rs.sendStream == nil {
			rs.createReplicaSendStream(info.Next, nextRaftIndex)
			shouldWaitChange = true
		} else {
			rs.sendStream.makeConsistentInStateReplicate(ctx, info, nextRaftIndex)
		}

	case tracker.StateSnapshot:
		if rs.sendStream != nil {
			rs.closeSendStream(ctx)
			shouldWaitChange = true
		}
	}
	return shouldWaitChange
}

func (rss *replicaState) closeSendStream(ctx context.Context) {
	rss.sendStream.mu.Lock()
	defer rss.sendStream.mu.Unlock()

	rss.sendStream.closeLocked(ctx)
	rss.sendStream = nil
}

// Next can move forward because of the push. Need to handle that too!!
func (rss *replicaSendStream) makeConsistentInStateReplicate(
	ctx context.Context, info FollowerStateInfo, nextRaftIndex uint64,
) {
	rss.mu.Lock()
	defer rss.mu.Unlock()
	defer func() {
		returnedSend, returnedEval :=
			rss.mu.tracker.Untrack(info.Term, info.Admitted, rss.mu.nextRaftIndexInitial)
		rss.returnSendTokens(ctx, returnedSend)
		rss.returnEvalTokens(ctx, returnedEval)
	}()

	// The leader is always in state replicate.
	if rss.parent.parent.opts.LocalReplicaID == rss.parent.desc.ReplicaID {
		if rss.mu.connectedState != replicate {
			log.Fatalf(ctx, "%v", errors.AssertionFailedf(
				"leader should always be in state replicate but found in %v",
				rss.mu.connectedState))
		}
	}

	// Follower replica case. Update the connected state.
	switch rss.mu.connectedState {
	case replicate:
		if info.Match >= rss.mu.sendQueue.indexToSend {
			// Some things got popped without us sending. Must be old
			// MsgAppResp. Next cannot have moved past Match, since Next used
			// to be equal to indexToSend.
			if info.Next != info.Match+1 {
				log.Fatalf(ctx, "%v", errors.AssertionFailedf(
					"next=%d != match+1=%d [info=%v send_stream=<TODO>]",
					info.Next, info.Match+1, info))
			}
			rss.makeConsistentWhenUnexpectedPopLocked(ctx, info.Next)
		} else if info.Next == rss.mu.sendQueue.indexToSend {
			// Everything is as expected.
		} else if info.Next > rss.mu.sendQueue.indexToSend {
			// We've already covered the case where Next moves ahead, along
			// with Match earlier. This can never happen.
			log.Fatalf(ctx, "%v", errors.AssertionFailedf(
				"next=%d > index_to_send=%d [info=%v send_stream=<TODO>]",
				info.Next, rss.mu.sendQueue.indexToSend, info))
		} else {
			// info.Next < rss.sendQueue.indexToSend.
			//
			// Must have transitioned to StateProbe and back, and we did not
			// observe it.
			if info.Next != info.Match+1 {
				log.Fatalf(ctx, "%v", errors.AssertionFailedf(
					"next=%d != match+1=%d [info=%v send_stream=<TODO>]",
					info.Next, info.Match+1, info))
			}
			rss.makeConsistentWhenUnexpectedProbeToReplicateLocked(ctx, info.Next)
		}

	case probeRecentlyReplicate:
		// Returned from StateProbe => StateReplicate.
		if info.Next != info.Match+1 {
			log.Fatalf(ctx, "%v", errors.AssertionFailedf(
				"next=%d != match+1=%d [info=%v send_stream=<TODO>]",
				info.Next, info.Match+1, info))
		}
		rss.makeConsistentWhenProbeToReplicateLocked(ctx, info.Next, nextRaftIndex)
	}
}

func (rss *replicaSendStream) makeConsistentWhenProbeToReplicateLocked(
	ctx context.Context, indexToSend uint64, nextRaftIndex uint64,
) {
	rss.mu.AssertHeld()
	rss.mu.sendQueue.indexToSend = indexToSend
	rss.mu.sendQueue.nextRaftIndex = nextRaftIndex
	rss.mu.nextRaftIndexInitial = rss.mu.sendQueue.nextRaftIndex

	// NB: We could re-use the current time and acquire it outside of the
	// mutex, but we expect transitions to replicate to be rarer than replicas
	// remaining in replicate.
	rss.changeConnectedStateLocked(replicate, rss.parent.parent.opts.Clock.PhysicalTime())
}

// REQUIRES: indexToSend < sendQueue.indexToSend.
func (rss *replicaSendStream) makeConsistentWhenUnexpectedProbeToReplicateLocked(
	ctx context.Context, indexToSend uint64,
) {
	// A regression in indexToSend necessarily means a MsgApp constructed by
	// this replicaSendStream was dropped.
	//
	// The messages in [indexToSend, rss.sendQueue.indexToSend) must be in
	// the tracker. They can't have been removed since Match < indexToSend.
	// We will be resending these, so we should return the send tokens for
	// them. We don't need to adjust eval.tokensDeducted since even though we
	// are returning these send tokens, all of them are now in the send-queue,
	// and the eval.tokensDeducted includes the send-queue.
	returnedSend := rss.mu.tracker.UntrackGE(indexToSend)
	rss.returnSendTokens(ctx, returnedSend)
	rss.mu.sendQueue.indexToSend = indexToSend
	rss.changeConnectedStateLocked(replicate, rss.parent.parent.opts.Clock.PhysicalTime())
}

// While in StateReplicate, send-queue could have some elements popped
// (indexToSend advanced) because there could have been some inflight MsgApps
// that we forgot about due to a transition out of StateReplicate and back
// into StateReplicate, and we got acks for them (Match advanced past
// indexToSend).
func (rss *replicaSendStream) makeConsistentWhenUnexpectedPopLocked(
	ctx context.Context, indexToSend uint64,
) {
	rss.mu.AssertHeld()

	// When we start maintaining send-queue stats we will have accurate stats
	// for indices that are both >= nextRaftIndexInitial and >= indexToSend.
	// Also, we have eval tokens deducted for indices >= nextRaftIndexInitial.
	//
	// This unexpected pop should typically not happen for any index >=
	// nextRaftIndexInitial since these were proposed after this
	// replicaSendStream was created. However, it may be rarely possible for
	// this to happen: we advance nextRaftIndexInitial on snapshot
	// application, and if an old (pre-snapshot) message that comes after the
	// snapshot shows up at the follower, it could advance Match.
	//
	// In that case, the popped entries have had eval tokens deducted, and
	// were never in the tracker and are no longer in the send-queue. We don't
	// know their size, so the simplest thing is to return all eval tokens.
	rss.returnAllEvalTokens(ctx)
	// We don't need to touch the tracker since we still expect admission for
	// entries that were inflight.

	rss.mu.sendQueue.indexToSend = indexToSend
	rss.mu.nextRaftIndexInitial = rss.mu.sendQueue.nextRaftIndex
}

// close returns all tokens and closes the replicaSendStream.
func (rss *replicaSendStream) closeLocked(ctx context.Context) {
	rss.mu.Lock()
	defer rss.mu.Unlock()
	// Return all tokens.
	returnedSend := rss.mu.tracker.UntrackAll()
	rss.returnSendTokens(ctx, returnedSend)
	rss.returnAllEvalTokens(ctx)
	rss.mu.closed = true
}

func (rss *replicaSendStream) changeToProbeLocked(ctx context.Context, now time.Time) {
	// This is the first time we've seen the replica change to StateProbe,
	// update the connected state and start time. If the state doesn't
	// change within probeRecentlyReplicateDuration, we will close the
	// stream. Also schedule an event, so that even if there are no
	// entries, we will still reliably close the stream if still in
	// StateProbe.
	rss.changeConnectedStateLocked(probeRecentlyReplicate, now)
	rss.parent.parent.opts.CloseTimerScheduler.ScheduleSendStreamCloseRaftMuLocked(
		ctx, rss.parent.parent.opts.RangeID, probeRecentlyReplicateDuration())
	// Return all tokens since other ranges may need them, and it may be some
	// time before this replica transitions back to StateReplicate.
	returnedSend := rss.mu.tracker.UntrackAll()
	rss.returnSendTokens(ctx, returnedSend)
	rss.returnAllEvalTokens(ctx)
}

// returnSendTokens returns tokens to the send token counters.
func (rss *replicaSendStream) returnSendTokens(
	ctx context.Context, returnedSend [raftpb.NumPriorities]kvflowcontrol.Tokens,
) {
	for pri, tokens := range returnedSend {
		pri := raftpb.Priority(pri)
		if tokens > 0 {
			rss.parent.sendTokenCounter.Return(ctx, WorkClassFromRaftPriority(pri), tokens)
		}
	}
}

func (rss *replicaSendStream) returnAllEvalTokens(ctx context.Context) {
	for wc, tokens := range rss.mu.eval.tokensDeducted {
		if tokens > 0 {
			rss.parent.evalTokenCounter.Return(ctx, admissionpb.WorkClass(wc), tokens)
		}
		rss.mu.eval.tokensDeducted[wc] = 0
	}
}

// returnEvalTokens returns tokens to the eval token counters.
func (rss *replicaSendStream) returnEvalTokens(
	ctx context.Context, returnedEval [raftpb.NumPriorities]kvflowcontrol.Tokens,
) {
	for pri, tokens := range returnedEval {
		rpri := raftpb.Priority(pri)
		wc := WorkClassFromRaftPriority(rpri)
		if tokens > 0 {
			rss.parent.evalTokenCounter.Return(ctx, wc, tokens)
			rss.mu.eval.tokensDeducted[wc] -= tokens
			if rss.mu.eval.tokensDeducted[wc] < 0 {
				panic("")
			}
		}
	}
}

// REQUIRES: rss.mu.connectedState = replicate.
func (rss *replicaSendStream) handleReadyEntriesLocked(
	ctx context.Context, event raftEventForReplica,
) {
	if n := len(event.sendingEntries); n > 0 {
		rss.mu.sendQueue.indexToSend = event.sendingEntries[n-1].index
		for _, entry := range event.sendingEntries {
			if !entry.usesFlowControl {
				continue
			}
			var pri raftpb.Priority
			if entry.index >= rss.mu.sendQueue.nextRaftIndex {
				// Was never in the send-queue.
				pri = entry.pri

			} else {
				// Was in the send-queue.
				pri = raftpb.LowPri
			}
			rss.parent.sendTokenCounter.Deduct(ctx, WorkClassFromRaftPriority(pri), entry.tokens)
			rss.mu.tracker.Track(ctx, entry.term, entry.index, pri, entry.tokens)
		}
	}
	if n := len(event.newEntries); n > 0 {
		if event.newEntries[0].index != rss.mu.sendQueue.nextRaftIndex {
			panic("")
		}
		rss.mu.sendQueue.nextRaftIndex = event.newEntries[n-1].index + 1
		for _, entry := range event.newEntries {
			if !entry.usesFlowControl {
				continue
			}
			var pri raftpb.Priority
			if entry.index >= rss.mu.sendQueue.indexToSend {
				// Being added to the send-queue.
				//
				// NB: we deduct elastic eval tokens, but we are not yet
				// taking into account whether the replica has a send-queue or
				// not in deciding whether it can be part of the quorum in
				// WaitForEval. This is ok, since in this mode only elastic
				// work will be subject to replication flow control.
				pri = raftpb.LowPri
			} else {
				pri = entry.pri
			}
			rss.parent.evalTokenCounter.Deduct(ctx, WorkClassFromRaftPriority(pri), entry.tokens)
		}
	}
}

// probeRecentlyReplicateDuration is the duration the controller will wait
// after observing a replica in StateProbe before closing the send stream if
// the replica remains in StateProbe.
//
// TODO(kvoli): We will want to make this a cluster setting eventually.
func probeRecentlyReplicateDuration() time.Duration {
	return time.Second
}

type connectedState uint32

// Local replicas are always in state replicate.
//
// Initial state for a replicaSendStream is always replicate, since it is
// created in StateReplicate. We don't care about whether the transport is
// connected or disconnected, since there is buffering capacity in the
// RaftTransport, which allows for some buffering and immediate sending when
// the RaftTransport stream reconnects (which may happen before the next
// HandleRaftEvent), which is desirable.
//
// The first false return value from SendRaftMessage will trigger a
// notification to Raft that the replica is unreachable (see
// Replica.sendRaftMessage calling Replica.addUnreachableRemoteReplica), and
// that raftpb.MsgUnreachable will cause the transition out of StateReplicate
// to StateProbe. The false return value happens either when the (generous)
// RaftTransport buffer is full, or when the circuit breaker opens. The
// circuit breaker opens 3-6s after no more TCP packets are flowing.
//
// A single transient message drop, and nack, can also cause a transition to
// StateProbe. At this layer we don't bother distinguishing on why this
// transition happened and first transition to probeRecentlyReplicate. We stay
// in this state for 1 second, and then close the replicaSendStream.
//
// The only difference in behavior between replicate and
// probeRecentlyReplicate is that we don't try to construct MsgApps in the
// latter.
//
// Initial states: replicate
// State transitions: replicate <=> probeRecentlyReplicate
const (
	replicate connectedState = iota
	probeRecentlyReplicate
)

func (cs connectedState) String() string {
	return redact.StringWithoutMarkers(cs)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (cs connectedState) SafeFormat(w redact.SafePrinter, _ rune) {
	switch cs {
	case replicate:
		w.SafeString("replicate")
	case probeRecentlyReplicate:
		w.SafeString("probeRecentlyReplicate")
	default:
		panic(fmt.Sprintf("unknown connectedState %v", cs))
	}
}
