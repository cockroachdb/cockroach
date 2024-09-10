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
	// FollowerStateRaftMuLocked returns the current state of a follower. The
	// value of Match, Next are populated iff in StateReplicate. All entries >=
	// Next have not had MsgApps constructed during the lifetime of this
	// StateReplicate (they may have been constructed previously).
	//
	// When a follower transitions from {StateProbe,StateSnapshot} =>
	// StateReplicate, we start trying to send MsgApps. We should
	// notice such transitions both in HandleRaftEvent and SetReplicasLocked.
	//
	// Requires Replica.raftMu to be held, Replica.mu is not held.
	FollowerStateRaftMuLocked(replicaID roachpb.ReplicaID) FollowerStateInfo
}

type FollowerStateInfo struct {
	State tracker.StateType

	// Remaining only populated in StateReplicate.
	// (Match, Next) is in-flight.
	Match uint64
	Next  uint64
}

// AdmittedTracker is used to retrieve the latest admitted vector for a
// replica (including the leader).
type AdmittedTracker interface {
	// GetAdmitted returns the latest AdmittedVector for replicaID. It returns
	// an empty struct if the replicaID is not known. NB: the
	// AdmittedVector.Admitted[i] value can transiently advance past
	// FollowerStateInfo.Match, since the admitted tracking subsystem is
	// separate from Raft.
	GetAdmitted(replicaID roachpb.ReplicaID) AdmittedVector
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
}

// RaftEventFromMsgStorageAppend constructs a RaftEvent from the given raft
// MsgStorageAppend message. Returns zero value if the message is empty.
func RaftEventFromMsgStorageAppend(msg raftpb.Message) RaftEvent {
	if msg.Type != raftpb.MsgStorageAppend {
		return RaftEvent{}
	}
	return RaftEvent{
		Term:    msg.LogTerm,
		Snap:    msg.Snapshot,
		Entries: msg.Entries,
	}
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
	AdmittedTracker     AdmittedTracker
	EvalWaitMetrics     *EvalWaitMetrics
}

// RangeControllerInitState is the initial state at the time of creation.
type RangeControllerInitState struct {
	// Must include RangeControllerOptions.ReplicaID.
	ReplicaSet ReplicaSet
	// Leaseholder may be set to NoReplicaID, in which case the leaseholder is
	// unknown.
	Leaseholder roachpb.ReplicaID
}

type rangeController struct {
	opts       RangeControllerOptions
	replicaSet ReplicaSet
	// leaseholder can be NoReplicaID or not be in ReplicaSet, i.e., it is
	// eventually consistent with the set of replicas.
	leaseholder roachpb.ReplicaID

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

	rc.opts.EvalWaitMetrics.OnWaiting(wc)
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
		rc.opts.EvalWaitMetrics.OnBypassed(wc, rc.opts.Clock.PhysicalTime().Sub(start))
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
				rc.opts.EvalWaitMetrics.OnErrored(wc, rc.opts.Clock.PhysicalTime().Sub(start))
				return false, ctx.Err()
			case RefreshWaitSignaled:
				goto retry
			}
		}
	}
	rc.opts.EvalWaitMetrics.OnAdmitted(wc, rc.opts.Clock.PhysicalTime().Sub(start))
	return true, nil
}

// HandleRaftEventRaftMuLocked handles the provided raft event for the range.
//
// Requires replica.raftMu to be held.
func (rc *rangeController) HandleRaftEventRaftMuLocked(ctx context.Context, e RaftEvent) error {
	shouldWaitChange := false
	for r, rs := range rc.replicaMap {
		info := rc.opts.RaftInterface.FollowerStateRaftMuLocked(r)
		shouldWaitChange = rs.handleReadyState(ctx, info) || shouldWaitChange
	}
	// If there was a quorum change, update the voter sets, triggering the
	// refresh channel for any requests waiting for eval tokens.
	if shouldWaitChange {
		rc.updateVoterSets()
	}

	// Compute the flow control state for each entry. We do this once here,
	// instead of decoding each entry multiple times for all replicas.
	entryStates := make([]entryFCState, len(e.Entries))
	for i, entry := range e.Entries {
		entryStates[i] = getEntryFCStateOrFatal(ctx, entry)
	}
	for _, rs := range rc.replicaMap {
		rs.handleReadyEntries(ctx, entryStates)
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
	state := parent.opts.RaftInterface.FollowerStateRaftMuLocked(desc.ReplicaID)
	if state.State == tracker.StateReplicate {
		rs.createReplicaSendStream()
	}

	return rs
}

type replicaSendStream struct {
	parent *replicaState

	mu struct {
		syncutil.Mutex
		// connectedStateStart is the time when the connectedState was last
		// transitioned from one state to another e.g., from replicate to
		// probeRecentlyReplicate or snapshot to replicate.
		connectedState      connectedState
		connectedStateStart time.Time
		tracker             Tracker
		closed              bool
	}
}

func (rss *replicaSendStream) changeConnectedStateLocked(state connectedState, now time.Time) {
	rss.mu.connectedState = state
	rss.mu.connectedStateStart = now
}

func (rs *replicaState) createReplicaSendStream() {
	// Must be in StateReplicate on creation.
	rs.sendStream = &replicaSendStream{
		parent: rs,
	}
	rs.sendStream.mu.tracker.Init(rs.stream)
	rs.sendStream.mu.closed = false
	rs.sendStream.changeConnectedStateLocked(
		replicate, rs.parent.opts.Clock.PhysicalTime())
}

func (rs *replicaState) isStateReplicate() bool {
	if rs.sendStream == nil {
		return false
	}
	rs.sendStream.mu.Lock()
	defer rs.sendStream.mu.Unlock()

	return rs.sendStream.mu.connectedState.shouldWaitForElasticEvalTokens()
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

func (rs *replicaState) handleReadyEntries(ctx context.Context, entries []entryFCState) {
	if rs.sendStream == nil {
		return
	}

	rs.sendStream.mu.Lock()
	defer rs.sendStream.mu.Unlock()

	for _, entry := range entries {
		if !entry.usesFlowControl {
			continue
		}
		rs.sendStream.mu.tracker.Track(ctx, entry.term, entry.index, entry.pri, entry.tokens)
		rs.evalTokenCounter.Deduct(
			ctx, WorkClassFromRaftPriority(entry.pri), entry.tokens)
		rs.sendTokenCounter.Deduct(
			ctx, WorkClassFromRaftPriority(entry.pri), entry.tokens)
	}
}

// handleReadyState handles state management for the replica based on the
// provided follower state information. If the state changes in a way that
// affects requests waiting for evaluation, returns true.
func (rs *replicaState) handleReadyState(
	ctx context.Context, info FollowerStateInfo,
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
				// This is the first time we've seen the replica change to StateProbe,
				// update the connected state and start time. If the state doesn't
				// change within probeRecentlyReplicateDuration, we will close the
				// stream. Also schedule an event, so that even if there are no
				// entries, we will still reliably close the stream if still in
				// StateProbe.
				//
				// TODO(sumeer): think through whether we should actually be returning
				// tokens immediately here. Currently we are not. e.g.,
				// probeRecentlyReplicate only affects whether to wait on this replica
				// for eval, and otherwise it behaves like a closed replicaSendStream.
				rs.sendStream.changeConnectedStateLocked(probeRecentlyReplicate, now)
				rs.parent.opts.CloseTimerScheduler.ScheduleSendStreamCloseRaftMuLocked(
					ctx, rs.parent.opts.RangeID, probeRecentlyReplicateDuration())
			}
			return should
		}(); shouldClose {
			rs.closeSendStream(ctx)
			shouldWaitChange = true
		}

	case tracker.StateReplicate:
		if rs.sendStream == nil {
			rs.createReplicaSendStream()
			shouldWaitChange = true
		} else {
			shouldWaitChange = rs.sendStream.makeConsistentInStateReplicate(ctx)
		}

	case tracker.StateSnapshot:
		if rs.sendStream != nil {
			switch func() connectedState {
				rs.sendStream.mu.Lock()
				defer rs.sendStream.mu.Unlock()
				return rs.sendStream.mu.connectedState
			}() {
			case replicate:
				rs.sendStream.changeToStateSnapshot(ctx)
				shouldWaitChange = true
			case probeRecentlyReplicate:
				rs.closeSendStream(ctx)
				shouldWaitChange = true
			case snapshot:
			}
		}
	}
	return shouldWaitChange
}

func (rss *replicaState) closeSendStream(ctx context.Context) {
	rss.sendStream.mu.Lock()
	defer rss.sendStream.mu.Unlock()

	if rss.sendStream.mu.connectedState != snapshot {
		// changeToStateSnapshot returns all tokens, as we have no liveness
		// guarantee of their return with the send stream now closed.
		rss.sendStream.changeToStateSnapshotLocked(ctx)
	}
	rss.sendStream.mu.closed = true
	rss.sendStream = nil
}

func (rss *replicaSendStream) makeConsistentInStateReplicate(
	ctx context.Context,
) (shouldWaitChange bool) {
	av := rss.parent.parent.opts.AdmittedTracker.GetAdmitted(rss.parent.desc.ReplicaID)
	rss.mu.Lock()
	defer rss.mu.Unlock()
	defer rss.returnTokens(ctx, rss.mu.tracker.Untrack(av.Term, av.Admitted))

	// The leader is always in state replicate.
	if rss.parent.parent.opts.LocalReplicaID == rss.parent.desc.ReplicaID {
		if rss.mu.connectedState != replicate {
			log.Fatalf(ctx, "%v", errors.AssertionFailedf(
				"leader should always be in state replicate but found in %v",
				rss.mu.connectedState))
		}
		return false
	}

	// Follower replica case. Update the connected state.
	switch rss.mu.connectedState {
	case replicate:
	case probeRecentlyReplicate:
		// NB: We could re-use the current time and acquire it outside of the
		// mutex, but we expect transitions to replicate to be rarer than replicas
		// remaining in replicate.
		rss.changeConnectedStateLocked(replicate, rss.parent.parent.opts.Clock.PhysicalTime())
	case snapshot:
		rss.changeConnectedStateLocked(replicate, rss.parent.parent.opts.Clock.PhysicalTime())
		shouldWaitChange = true
	}
	return shouldWaitChange
}

// changeToStateSnapshot changes the connected state to snapshot and returns
// all tracked entries' tokens.
func (rss *replicaSendStream) changeToStateSnapshot(ctx context.Context) {
	rss.mu.Lock()
	defer rss.mu.Unlock()

	rss.changeToStateSnapshotLocked(ctx)
}

// changeToStateSnapshot changes the connected state to snapshot and returns
// all tracked entries' tokens.
//
// Requires rs.mu to be held.
func (rss *replicaSendStream) changeToStateSnapshotLocked(ctx context.Context) {
	rss.changeConnectedStateLocked(snapshot, rss.parent.parent.opts.Clock.PhysicalTime())
	// Since the replica is now in StateSnapshot, there is no need for Raft to
	// send MsgApp pings to discover what has been missed. So there is no
	// liveness guarantee on when these tokens will be returned, and therefore we
	// return all tokens in the tracker.
	rss.returnTokens(ctx, rss.mu.tracker.UntrackAll())
}

// returnTokens takes the tokens untracked by the tracker and returns them to
// the eval and send token counters.
func (rss *replicaSendStream) returnTokens(
	ctx context.Context, returned [raftpb.NumPriorities]kvflowcontrol.Tokens,
) {
	for pri, tokens := range returned {
		if tokens > 0 {
			pri := WorkClassFromRaftPriority(raftpb.Priority(pri))
			rss.parent.evalTokenCounter.Return(ctx, pri, tokens)
			rss.parent.sendTokenCounter.Return(ctx, pri, tokens)
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
// State transitions:
//
//	replicate <=> {probeRecentlyReplicate, snapshot}
//	snapshot => replicaSendStream closed (when observe StateProbe)
//	probeRecentlyReplicate => replicaSendStream closed (after short delay)
const (
	replicate connectedState = iota
	probeRecentlyReplicate
	snapshot
)

func (cs connectedState) shouldWaitForElasticEvalTokens() bool {
	return cs == replicate || cs == probeRecentlyReplicate
}

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
	case snapshot:
		w.SafeString("snapshot")
	default:
		panic(fmt.Sprintf("unknown connectedState %v", cs))
	}
}
