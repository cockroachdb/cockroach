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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// RangeController provides flow control for replication traffic in KV, for a
// range at the leader. It must be created for a particular leader term, and
// closed if the term changes.
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
	// AdmitRaftMuLocked handles the notification about the given replica's
	// admitted vector change. No-op if the replica is not known, or the admitted
	// vector is stale (either in Term, or the indices).
	//
	// Requires replica.raftMu to be held.
	AdmitRaftMuLocked(context.Context, roachpb.ReplicaID, AdmittedVector)
	// MaybeSendPingsRaftMuLocked sends a MsgApp ping to each raft peer in
	// StateReplicate whose admitted vector is lagging, and there wasn't a recent
	// MsgApp to this peer.
	//
	// Requires replica.raftMu to be held.
	MaybeSendPingsRaftMuLocked()
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
	// InspectRaftMuLocked returns a handle containing the state of the range
	// controller. It's used to power /inspectz-style debugging pages.
	InspectRaftMuLocked(ctx context.Context) kvflowinspectpb.Handle
}

// TODO(pav-kv): This interface a placeholder for the interface containing raft
// methods. Replace this as part of #128019.
type RaftInterface interface {
	// SendPingRaftMuLocked sends a MsgApp ping to the given raft peer if there
	// wasn't a recent MsgApp to this peer. The message is added to raft's message
	// queue, and will be extracted and sent during the next Ready processing.
	//
	// If the peer is not in StateReplicate, this call does nothing.
	//
	// Requires Replica.raftMu to be held.
	SendPingRaftMuLocked(roachpb.ReplicaID) bool
}

type ReplicaStateInfo struct {
	State tracker.StateType

	// Remaining only populated in StateReplicate.
	// (Match, Next) is in-flight.
	Match uint64
	Next  uint64
}

// RaftEvent carries a RACv2-relevant subset of raft state sent to storage.
type RaftEvent struct {
	// Term is the leader term on whose behalf the entries or snapshot are
	// written. Note that it may be behind the raft node's current term. Not
	// populated if Entries is empty and Snap is nil.
	Term uint64
	// Snap contains the snapshot to be written to storage.
	Snap *raftpb.Snapshot
	// Entries contains the log entries to be written to storage.
	Entries []raftpb.Entry
	// MsgApps to followers. Only populated on the leader, when operating in
	// MsgAppPush mode. This is informational, for bookkeeping in the callee.
	//
	// These MsgApps can be for entries in Entries, or for earlier ones.
	// Typically, the MsgApps are ordered by entry index, and are a sequence of
	// dense indices, starting at the previously observed (during the previous
	// RaftEvent) value of FollowerStateInfo.Next.
	//
	// But there can be exceptions. Indices can regress (on leader term change
	// or MsgAppResp reject) or jump (on receiving a MsgAppResp that indicates
	// that the follower is ahead of what the leader thinks). It is also
	// possible for there to be state transitions from StateReplicate =>
	// StateProbe/StateSnapshot => StateReplicate, between two consecutive
	// RaftEvents, i.e., RangeController would not observe the intermediate
	// state. Such missed transitions can also cause regressions or forward
	// jumps. Also, MsgApps can correspond to an older leader term, or prior to
	// the state transition to StateProbe/StateReplicate.
	//
	// Due to these exceptions, we only expect a single invariant: if the leader
	// term has not changed from the previous RaftEvent, all the MsgApps will be
	// from this leader term.
	//
	// A key can map to an empty slice, in order to reuse already allocated
	// slice memory.
	MsgApps map[roachpb.ReplicaID][]raftpb.Message
	// ReplicasStateInfo contains the state of all replicas. This is used to
	// determine if the state of a replica has changed, and if so, to update the
	// flow control state. It also informs the RangeController of a replica's
	// Match and Next.
	ReplicasStateInfo map[roachpb.ReplicaID]ReplicaStateInfo
}

// RaftEventFromMsgStorageAppendAndMsgApps constructs a RaftEvent from the
// given raft MsgStorageAppend message, and outboundMsgs. The replicaID is the
// local replica. The outboundMsgs will only contain MsgApps on the leader.
// msgAppScratch is used as the map in RaftEvent.MsgApps. Returns the zero
// value if the MsgStorageAppend is empty and there are no MsgApps.
func RaftEventFromMsgStorageAppendAndMsgApps(
	replicaID roachpb.ReplicaID,
	appendMsg raftpb.Message,
	outboundMsgs []raftpb.Message,
	msgAppScratch map[roachpb.ReplicaID][]raftpb.Message,
) RaftEvent {
	var event RaftEvent
	if appendMsg.Type == raftpb.MsgStorageAppend {
		event = RaftEvent{
			Term:    appendMsg.LogTerm,
			Snap:    appendMsg.Snapshot,
			Entries: appendMsg.Entries,
		}
	}
	if len(outboundMsgs) == 0 {
		return event
	}
	// Clear the slices, to reuse slice allocations.
	for k := range msgAppScratch {
		msgAppScratch[k] = msgAppScratch[k][:0]
	}
	if len(msgAppScratch) > 10 {
		// Clear all memory, in case we have a long-lived leader while other
		// replicas keep changing.
		clear(msgAppScratch)
	}
	added := false
	for _, msg := range outboundMsgs {
		if msg.Type != raftpb.MsgApp || roachpb.ReplicaID(msg.To) == replicaID {
			continue
		}
		added = true
		msgs := msgAppScratch[roachpb.ReplicaID(msg.To)]
		msgs = append(msgs, msg)
		msgAppScratch[roachpb.ReplicaID(msg.To)] = msgs
	}
	if added {
		event.MsgApps = msgAppScratch
	}
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
	Knobs               *kvflowcontrol.TestingKnobs
}

// RangeControllerInitState is the initial state at the time of creation.
type RangeControllerInitState struct {
	// Must include RangeControllerOptions.ReplicaID.
	ReplicaSet ReplicaSet
	// Leaseholder may be set to NoReplicaID, in which case the leaseholder is
	// unknown.
	Leaseholder roachpb.ReplicaID
	// NextRaftIndex is the first index that will appear in the next non-empty
	// RaftEvent.Entries handled by this RangeController.
	NextRaftIndex uint64
}

// rangeController is tied to a single leader term.
type rangeController struct {
	opts       RangeControllerOptions
	replicaSet ReplicaSet
	// leaseholder can be NoReplicaID or not be in ReplicaSet, i.e., it is
	// eventually consistent with the set of replicas.
	leaseholder   roachpb.ReplicaID
	nextRaftIndex uint64

	mu struct {
		syncutil.RWMutex

		// State for waiters. When anything in voterSets or nonVoterSets changes,
		// waiterSetRefreshCh is closed, and replaced with a new channel. The
		// voterSets and nonVoterSets is copy-on-write, so waiters make a shallow
		// copy.
		voterSets          []voterSet
		nonVoterSet        []stateForWaiters
		waiterSetRefreshCh chan struct{}
	}

	replicaMap map[roachpb.ReplicaID]*replicaState
}

// voterStateForWaiters informs whether WaitForEval is required to wait for
// eval-tokens for a voter.
type voterStateForWaiters struct {
	stateForWaiters
	isLeader      bool
	isLeaseHolder bool
}

// stateForWaiters informs whether WaitForEval is required to wait for
// eval-tokens for a replica.
type stateForWaiters struct {
	replicaID        roachpb.ReplicaID
	isStateReplicate bool
	evalTokenCounter *tokenCounter
}

type voterSet []voterStateForWaiters

var _ RangeController = &rangeController{}

func NewRangeController(
	ctx context.Context, o RangeControllerOptions, init RangeControllerInitState,
) *rangeController {
	log.VInfof(ctx, 1, "r%v creating range controller", o.RangeID)
	rc := &rangeController{
		opts:          o,
		leaseholder:   init.Leaseholder,
		nextRaftIndex: init.NextRaftIndex,
		replicaMap:    make(map[roachpb.ReplicaID]*replicaState),
	}
	rc.mu.waiterSetRefreshCh = make(chan struct{})
	rc.updateReplicaSet(ctx, init.ReplicaSet)
	rc.updateWaiterSets()
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
	// Snapshot the waiter sets and the refresh channel.
	rc.mu.RLock()
	vss := rc.mu.voterSets
	nvs := rc.mu.nonVoterSet
	refreshCh := rc.mu.waiterSetRefreshCh
	rc.mu.RUnlock()

	if refreshCh == nil {
		// RangeControllerImpl is closed.
		rc.opts.EvalWaitMetrics.OnBypassed(wc, rc.opts.Clock.PhysicalTime().Sub(start))
		return false, nil
	}
	for _, vs := range vss {
		quorumCount := (len(vs) + 2) / 2
		votersHaveEvalTokensCount := 0
		handles = handles[:0]
		requiredWait := false
		// First check the voter set, which participate in quorum.
		for _, v := range vs {
			available, handle := v.evalTokenCounter.TokensAvailable(wc)
			if available {
				votersHaveEvalTokensCount++
				continue
			}

			// Don't have eval tokens, and have a handle.
			handleInfo := tokenWaitingHandleInfo{
				handle: handle,
				requiredWait: v.isLeader || v.isLeaseHolder ||
					(waitForAllReplicateHandles && v.isStateReplicate),
				partOfQuorum: true,
			}
			handles = append(handles, handleInfo)
			if !requiredWait && handleInfo.requiredWait {
				requiredWait = true
			}
		}
		// If we don't need to wait for all replicate handles, then we will never
		// wait the non-voter streams having positive tokens, so we can skip
		// checking them.
		if waitForAllReplicateHandles {
			for _, nv := range nvs {
				available, handle := nv.evalTokenCounter.TokensAvailable(wc)
				if available || !nv.isStateReplicate {
					// Ignore non-voters without tokens which are not in StateReplicate,
					// as we won't be waiting for them.
					continue
				}
				// Don't have eval tokens, and have a handle for the non-voter.
				handleInfo := tokenWaitingHandleInfo{
					handle:       handle,
					requiredWait: true,
					partOfQuorum: false,
				}
				handles = append(handles, handleInfo)
				if !requiredWait {
					// NB: requiredWait won't always be true before here, because it is
					// possible that the leaseholder and leader have tokens, but all
					// other (non-)voters are not in StateReplicate or have tokens
					// available.
					requiredWait = true
				}
			}
		}

		remainingForQuorum := quorumCount - votersHaveEvalTokensCount
		if remainingForQuorum < 0 {
			remainingForQuorum = 0
		}
		if remainingForQuorum > 0 || requiredWait {
			var state WaitEndState
			state, scratch = WaitForEval(ctx, refreshCh, handles, remainingForQuorum, scratch)
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
	waitDuration := rc.opts.Clock.PhysicalTime().Sub(start)
	log.VEventf(ctx, 2, "r%v/%v admitted request (pri=%v wait-duration=%s wait-for-all=%v)",
		rc.opts.RangeID, rc.opts.LocalReplicaID, pri, waitDuration, waitForAllReplicateHandles)
	rc.opts.EvalWaitMetrics.OnAdmitted(wc, waitDuration)
	return true, nil
}

// raftEventForReplica is constructed for a replica iff it is in
// StateReplicate.
type raftEventForReplica struct {
	// Reminder: (ReplicaStateInfo.Match, ReplicaStateInfo.Next) are in-flight.
	// nextRaftIndex is where the next entry will be added.
	//
	// ReplicaStateInfo.{State, Match} are the latest state.
	// ReplicaStateInfo.Next represents the state preceding this raft event,
	// i.e., it will be altered by sendingEntries. nextRaftIndex also represents
	// the state preceding this event, and will be altered by newEntries.
	//
	// createSendStream is set to true if the replicaSendStream should be
	// (re)created.
	replicaStateInfo   ReplicaStateInfo
	nextRaftIndex      uint64
	newEntries         []entryFCState
	sendingEntries     []entryFCState
	recreateSendStream bool
}

// raftEventAppendState is the general state computed from RaftEvent that is
// the same for all replicas. Used as input for computing raftEventForReplica.
type raftEventAppendState struct {
	// Computed from RaftEvent.Entries.
	newEntries []entryFCState
	// rewoundNextRaftIndex is the next raft index prior to newEntries. That is,
	// if newEntries is non-empty, this is equal to newEntries[0].Index.
	rewoundNextRaftIndex uint64
}

// existingSendStreamState is used as input in computing raftEventForReplica.
type existingSendStreamState struct {
	existsAndInStateReplicate bool
	// indexToSend is populated iff existsAndInStateReplicate.
	indexToSend uint64
}

// constructRaftEventForReplica is called iff latestFollowerStateInfo.State is
// StateReplicate.
//
// latestFollowerStateInfo includes the effect of RaftEvent.Entries and
// RaftEvent.MsgApps on followerStataInfo.Next. msgApps is the map entry in
// RaftEvent.MsgApps for this replica. For other parameters, see the struct
// declarations.
func constructRaftEventForReplica(
	ctx context.Context,
	raftEventAppendState raftEventAppendState,
	latestReplicaStateInfo ReplicaStateInfo,
	existingSendStreamState existingSendStreamState,
	msgApps []raftpb.Message,
	scratchSendingEntries []entryFCState,
) (_ raftEventForReplica, scratch []entryFCState) {
	firstNewEntryIndex, lastNewEntryIndex := uint64(math.MaxUint64), uint64(math.MaxUint64)
	if n := len(raftEventAppendState.newEntries); n > 0 {
		firstNewEntryIndex = raftEventAppendState.newEntries[0].index
		lastNewEntryIndex = raftEventAppendState.newEntries[n-1].index + 1
		// The rewoundNextRaftIndex + newEntries should never lag behind Next.
		if latestReplicaStateInfo.Next > lastNewEntryIndex {
			panic(errors.AssertionFailedf("unexpected next=%v > last_new_entry_index=%v",
				latestReplicaStateInfo.Next, lastNewEntryIndex))
		}
	} else {
		if latestReplicaStateInfo.Next > raftEventAppendState.rewoundNextRaftIndex {
			panic(errors.AssertionFailedf("unexpected next=%v > rewound_next_raft_index=%v",
				latestReplicaStateInfo.Next, raftEventAppendState.rewoundNextRaftIndex))
		}
	}

	// Note that latestFollowerStateInfo.Next reflects the effects of msgApps,
	// if any, but that msgApps may not be consistent. We use msgApps only if
	// the replicaSendStream already exists and the msgApps are consistent with
	// its indexToSend, and are internally consistent etc. Else, we (re)create
	// the replicaSendStream and base the computation of sendingEntries only on
	// the latest Next and the newEntries.
	var createSendStream bool
	// [msgAppFirstIndex, msgAppUBIndex) are being sent.
	var msgAppFirstIndex, msgAppUBIndex uint64
	if existingSendStreamState.existsAndInStateReplicate {
		initialized := false
		for i := range msgApps {
			for _, entry := range msgApps[i].Entries {
				if !initialized {
					msgAppFirstIndex = entry.Index
					msgAppUBIndex = msgAppFirstIndex + 1
					initialized = true
				} else {
					if entry.Index != msgAppUBIndex {
						createSendStream = true
					} else {
						msgAppUBIndex++
					}
				}
			}
		}
		if !createSendStream {
			if msgAppFirstIndex < msgAppUBIndex {
				// First disjunct is true if there is a regression or forward jump in
				// the send-queue.
				if existingSendStreamState.indexToSend != msgAppFirstIndex ||
					// Unclear if the following can ever occur, but we are being
					// defensive.
					latestReplicaStateInfo.Next != msgAppUBIndex ||
					msgAppFirstIndex > raftEventAppendState.rewoundNextRaftIndex ||
					msgAppUBIndex > lastNewEntryIndex {
					createSendStream = true
				}
			} else {
				// No MsgApp, but Next may not be as expected.
				if existingSendStreamState.indexToSend != latestReplicaStateInfo.Next {
					createSendStream = true
				}
			}
		}
	} else {
		createSendStream = true
	}
	next := latestReplicaStateInfo.Next
	if createSendStream {
		if next > raftEventAppendState.rewoundNextRaftIndex {
			// We initialize the send-queue to be empty.
			next = raftEventAppendState.rewoundNextRaftIndex
			// At least one entry is "sent".
			msgAppFirstIndex = next
			msgAppUBIndex = latestReplicaStateInfo.Next
		} else {
			// next is in the past. No need to change it. Nothing is "sent".
			msgAppFirstIndex = 0
			msgAppUBIndex = 0
		}
	} else {
		next = existingSendStreamState.indexToSend
	}
	scratch = scratchSendingEntries
	var sendingEntries []entryFCState
	if msgAppFirstIndex < msgAppUBIndex {
		if msgAppFirstIndex == firstNewEntryIndex {
			// Common case. Sub-slice and don't use scratch.
			// We've already ensured that msgAppUBIndex is <= lastNewEntryIndex.
			sendingEntries = raftEventAppendState.newEntries[0 : msgAppUBIndex-msgAppFirstIndex]
		} else {
			sendingEntries = scratchSendingEntries[:0]
			// We've already ensured that msgAppFirstIndex <= firstNewEntryIndex.
			for i := range msgApps {
				for _, entry := range msgApps[i].Entries {
					if entry.Index >= firstNewEntryIndex {
						sendingEntries = append(
							sendingEntries, raftEventAppendState.newEntries[entry.Index-firstNewEntryIndex])
					} else {
						// Need to decode.
						sendingEntries = append(sendingEntries, getEntryFCStateOrFatal(ctx, entry))
					}
				}
			}
			scratch = sendingEntries
		}
	}
	refr := raftEventForReplica{
		replicaStateInfo: ReplicaStateInfo{
			State: latestReplicaStateInfo.State,
			Match: latestReplicaStateInfo.Match,
			Next:  next,
		},
		nextRaftIndex:      raftEventAppendState.rewoundNextRaftIndex,
		newEntries:         raftEventAppendState.newEntries,
		sendingEntries:     sendingEntries,
		recreateSendStream: createSendStream,
	}
	return refr, scratch
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
	rewoundNextRaftIndex := rc.nextRaftIndex
	if n := len(e.Entries); n > 0 {
		rc.nextRaftIndex = e.Entries[n-1].Index + 1
	}
	appendState := raftEventAppendState{
		newEntries:           newEntries,
		rewoundNextRaftIndex: rewoundNextRaftIndex,
	}

	shouldWaitChange := false
	for r, rs := range rc.replicaMap {
		info := e.ReplicasStateInfo[r]
		var eventForReplica raftEventForReplica
		if info.State == tracker.StateReplicate {
			// The leader won't have a MsgApp for itself, so we need to construct a
			// MsgApp for the leader, containing all the entries.
			var msgApps []raftpb.Message
			if r != rc.opts.LocalReplicaID {
				msgApps = e.MsgApps[r]
			} else {
				msgAppVec := [1]raftpb.Message{
					{
						Entries: e.Entries,
					},
				}
				msgApps = msgAppVec[:]
			}
			existingSSState := rs.getExistingSendStreamState()
			eventForReplica, rs.scratchSendingEntries = constructRaftEventForReplica(
				ctx, appendState, info, existingSSState, msgApps, rs.scratchSendingEntries)
			info = eventForReplica.replicaStateInfo
		}
		shouldWaitChange = rs.handleReadyState(
			ctx, info, eventForReplica.nextRaftIndex, eventForReplica.recreateSendStream) || shouldWaitChange
		rs.handleReadyEntries(ctx, eventForReplica)
	}
	// If there was a quorum change, update the voter sets, triggering the
	// refresh channel for any requests waiting for eval tokens.
	if shouldWaitChange {
		rc.updateWaiterSets()
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

// AdmitRaftMuLocked handles the notification about the given replica's
// admitted vector change. No-op if the replica is not known, or the admitted
// vector is stale (either in Term, or the indices).
//
// Requires replica.raftMu to be held.
func (rc *rangeController) AdmitRaftMuLocked(
	ctx context.Context, replicaID roachpb.ReplicaID, av AdmittedVector,
) {
	if rs, ok := rc.replicaMap[replicaID]; ok {
		rs.admit(ctx, av)
	}
}

// MaybeSendPingsRaftMuLocked sends a MsgApp ping to each raft peer in
// StateReplicate whose admitted vector is lagging, and there wasn't a recent
// MsgApp to this peer.
//
// Requires replica.raftMu to be held.
func (rc *rangeController) MaybeSendPingsRaftMuLocked() {
	for id, state := range rc.replicaMap {
		if id == rc.opts.LocalReplicaID {
			continue
		}
		if s := state.sendStream; s != nil && s.shouldPing() {
			rc.opts.RaftInterface.SendPingRaftMuLocked(id)
		}
	}
}

// SetReplicasRaftMuLocked sets the replicas of the range. The caller will
// never mutate replicas, and neither should the callee.
//
// Requires replica.raftMu to be held.
func (rc *rangeController) SetReplicasRaftMuLocked(ctx context.Context, replicas ReplicaSet) error {
	rc.updateReplicaSet(ctx, replicas)
	rc.updateWaiterSets()
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
	log.VInfof(ctx, 1, "r%v setting range leaseholder replica_id=%v", rc.opts.RangeID, replica)
	rc.leaseholder = replica
	rc.updateWaiterSets()
}

// CloseRaftMuLocked closes the range controller.
//
// Requires replica.raftMu to be held.
func (rc *rangeController) CloseRaftMuLocked(ctx context.Context) {
	log.VInfof(ctx, 1, "r%v closing range controller", rc.opts.RangeID)
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.mu.voterSets = nil
	rc.mu.nonVoterSet = nil
	close(rc.mu.waiterSetRefreshCh)
	rc.mu.waiterSetRefreshCh = nil
	// Return any tracked token deductions, as we don't expect to receive more
	// AdmittedVector updates.
	for _, rs := range rc.replicaMap {
		if rs.sendStream != nil {
			rs.closeSendStream(ctx)
		}
	}
}

// InspectRaftMuLocked returns a handle containing the state of the range
// controller. It's used to power /inspectz-style debugging pages.
//
// Requires replica.raftMu to be held.
func (rc *rangeController) InspectRaftMuLocked(ctx context.Context) kvflowinspectpb.Handle {
	var streams []kvflowinspectpb.ConnectedStream
	for _, rs := range rc.replicaMap {
		if rs.sendStream == nil {
			continue
		}

		func() {
			rs.sendStream.mu.Lock()
			defer rs.sendStream.mu.Unlock()
			streams = append(streams, kvflowinspectpb.ConnectedStream{
				Stream:            rc.opts.SSTokenCounter.InspectStream(rs.stream),
				TrackedDeductions: rs.sendStream.mu.tracker.Inspect(),
			})
		}()
	}

	// Sort the connected streams for determinism, which some tests rely on.
	slices.SortFunc(streams, func(a, b kvflowinspectpb.ConnectedStream) int {
		return cmp.Or(
			cmp.Compare(a.Stream.TenantID.ToUint64(), b.Stream.TenantID.ToUint64()),
			cmp.Compare(a.Stream.StoreID, b.Stream.StoreID),
		)
	})

	return kvflowinspectpb.Handle{
		RangeID:          rc.opts.RangeID,
		ConnectedStreams: streams,
	}
}

func (rc *rangeController) updateReplicaSet(ctx context.Context, newSet ReplicaSet) {
	prevSet := rc.replicaSet
	for r := range prevSet {
		desc, ok := newSet[r]
		if !ok {
			if rs := rc.replicaMap[r]; rs.sendStream != nil {
				// The replica is no longer part of the range, so we don't expect any
				// tracked token deductions to be returned. Return them now.
				rs.closeSendStream(ctx)
			}
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

func (rc *rangeController) updateWaiterSets() {
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
	var nonVoterSet []stateForWaiters
	for len(voterSets) < setCount {
		voterSets = append(voterSets, voterSet{})
	}
	for _, r := range rc.replicaSet {
		isOld := r.IsVoterOldConfig()
		isNew := r.IsVoterNewConfig()

		rs := rc.replicaMap[r.ReplicaID]
		waiterState := stateForWaiters{
			replicaID:        r.ReplicaID,
			isStateReplicate: rs.isStateReplicate(),
			evalTokenCounter: rs.evalTokenCounter,
		}

		if r.IsNonVoter() {
			nonVoterSet = append(nonVoterSet, waiterState)
		}

		if !isOld && !isNew {
			continue
		}

		// Is a voter.
		vsfw := voterStateForWaiters{
			stateForWaiters: waiterState,
			isLeader:        r.ReplicaID == rc.opts.LocalReplicaID,
			isLeaseHolder:   r.ReplicaID == rc.leaseholder,
		}
		if isOld {
			voterSets[0] = append(voterSets[0], vsfw)
		}
		if isNew && setCount == 2 {
			voterSets[1] = append(voterSets[1], vsfw)
		}
	}
	rc.mu.voterSets = voterSets
	rc.mu.nonVoterSet = nonVoterSet
	close(rc.mu.waiterSetRefreshCh)
	rc.mu.waiterSetRefreshCh = make(chan struct{})
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

	// Scratch space used in constructRaftEventForReplica.
	scratchSendingEntries []entryFCState
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
		// tracker contains entries that have been sent, and have had send-tokens
		// deducted (and will have had eval-tokens deducted iff index >=
		// nextRaftIndexInitial).
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
		// When the presence of a sendQueue is due to Raft flow control (push
		// mode), which does not take store overload into account, we consider
		// that the delay in reaching quorum due to the send-queue is acceptable.
		// The state is maintained here so that we can transition between Raft
		// flow control caused send-queue and replication flow control caused
		// send-queue, and vice versa.
		//
		// Say we pretended that there was no send-queue when using Raft flow
		// control. Then send tokens would have been deducted and entries placed
		// in tracker at eval time. When transitioning from replication flow
		// control to raft flow control we would need to iterate over all entries
		// in the send-queue, read them from storage, and place them in the
		// tracker. Which is clearly not viable.
		//
		// We have another issue when maintaining the send-queue in push-mode. We
		// do not control generation of the MsgApp, or the RaftMessageRequest in
		// which it is wrapped. So we cannot set
		// RaftMessageRequest.LowPriorityOverride. Even if we somehow fixed the
		// code plumbing difficulties in setting LowPriorityOverride, the MsgApp
		// (generated wholly by Raft in push mode) may span some entries in the
		// send-queue and some not in the send-queue, so a single bool override
		// does not apply to the whole MsgApp. To work around this, when in push
		// mode (the only mode in the current code) we use the original priority
		// when deducting eval tokens (and eventually send tokens) for entries in
		// the send-queue.
		//
		// Not updated in state probeRecentlyReplicate.
		sendQueue struct {
			// State of send-queue. [indexToSend, nextRaftIndex) have not been sent.
			// indexToSend == FollowerStateInfo.Next. nextRaftIndex is the current
			// value of NextUnstableIndex at the leader. The send-queue is always
			// empty for the leader.
			indexToSend   uint64
			nextRaftIndex uint64

			// Tokens corresponding to items in the senq-queue that have had eval
			// tokens deducted, i.e., have indices >= nextRaftIndexInitial and are
			// subject to replication flow control.
			//
			// These are not yet necessary, since we only support push mode, but
			// will be necessary soon when we add pull mode and switching back and
			// forth between modes.
			//
			// In push mode, originalEvalTokens == actualEvalTokensDeducted.
			//
			// When switching from push to pull:
			//  evalTokenCounter.Deduct(ElasticWorkClass, actualEvalTokensDeducted[RegularWorkClass])
			//  actualEvalTokensDeducted[ElasticWorkClass] += actualEvalTokensDeducted[RegularWorkClass]
			//  evalTokenCounter.Deduct(RegularWorkClass, -actualEvalTokensDeducted[RegularWorkClass])
			//  actualEvalTokensDeducted[RegularWorkClass] = 0
			//
			// When switching from pull to push:
			//  evalTokenCounter.Deduct(ElasticWorkClass, -originalEvalTokens[RegularWorkClass])
			//  actualEvalTokensDeducted[ElasticWorkClass] = originalEvalTokens[ElasticWorkClass]
			//  evalTokenCounter.Deduct(RegularWorkClass, originalEvalTokens[RegularWorkClass])
			//  actualEvalTokensDeducted[RegularWorkClass] = originalEvalTokens[RegularWorkClass]
			//
			// Nothing in the send-queue is in the tracker, so that is unaffected.
			// When de-queuing from the send-queue and sending in push mode, we use
			// the original priority when adding to the tracker. In pull mode we use
			// LowPri.
			originalEvalTokens       [admissionpb.NumWorkClasses]kvflowcontrol.Tokens
			actualEvalTokensDeducted [admissionpb.NumWorkClasses]kvflowcontrol.Tokens
		}
		closed bool
	}
}

func (rss *replicaSendStream) changeConnectedStateLocked(state connectedState, now time.Time) {
	rss.mu.connectedState = state
	rss.mu.connectedStateStart = now
}

func (rss *replicaSendStream) shouldPing() bool {
	rss.mu.Lock() // TODO(pav-kv): should we make it RWMutex.RLock()?
	defer rss.mu.Unlock()
	return !rss.mu.tracker.Empty()
}

func (rss *replicaSendStream) admit(ctx context.Context, av AdmittedVector) {
	log.VInfof(ctx, 2, "r%v:%v stream %v admit %v",
		rss.parent.parent.opts.RangeID, rss.parent.desc, rss.parent.stream, av)
	rss.mu.Lock()
	defer rss.mu.Unlock()

	returnedSend, returnedEval := rss.mu.tracker.Untrack(
		av.Term, av.Admitted, rss.mu.nextRaftIndexInitial)
	rss.returnSendTokens(ctx, returnedSend, false /* disconnect */)
	rss.returnEvalTokens(ctx, returnedEval)
}

func (rs *replicaState) getExistingSendStreamState() existingSendStreamState {
	if rs.sendStream == nil {
		return existingSendStreamState{
			existsAndInStateReplicate: false,
		}
	}
	rs.sendStream.mu.Lock()
	defer rs.sendStream.mu.Unlock()
	if rs.sendStream.mu.connectedState != replicate {
		return existingSendStreamState{
			existsAndInStateReplicate: false,
		}
	}
	return existingSendStreamState{
		existsAndInStateReplicate: true,
		indexToSend:               rs.sendStream.mu.sendQueue.indexToSend,
	}
}

func (rs *replicaState) createReplicaSendStream(
	ctx context.Context, indexToSend uint64, nextRaftIndex uint64,
) {
	// Must be in StateReplicate on creation.
	log.VEventf(ctx, 1, "creating send stream %v for replica %v", rs.stream, rs.desc)
	rs.sendStream = &replicaSendStream{
		parent: rs,
	}
	rs.sendStream.mu.tracker.Init(rs.stream)
	rs.sendStream.mu.closed = false
	rs.sendStream.changeConnectedStateLocked(
		replicate, rs.parent.opts.Clock.PhysicalTime())
	rs.sendStream.mu.nextRaftIndexInitial = nextRaftIndex
	rs.sendStream.mu.sendQueue.indexToSend = indexToSend
	rs.sendStream.mu.sendQueue.nextRaftIndex = nextRaftIndex

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

func (rs *replicaState) handleReadyEntries(
	ctx context.Context, eventForReplica raftEventForReplica,
) {
	if rs.sendStream == nil {
		return
	}

	rs.sendStream.mu.Lock()
	defer rs.sendStream.mu.Unlock()
	if rs.sendStream.mu.connectedState != replicate {
		return
	}
	rs.sendStream.handleReadyEntriesLocked(ctx, eventForReplica)
}

// handleReadyState handles state management for the replica based on the
// provided follower state information. If the state changes in a way that
// affects requests waiting for evaluation, returns true. nextRaftIndex and
// recreateSendStream are only relevant when info.State is StateReplicate.
func (rs *replicaState) handleReadyState(
	ctx context.Context, info ReplicaStateInfo, nextRaftIndex uint64, recreateSendStream bool,
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
			if !recreateSendStream {
				panic(errors.AssertionFailedf("in StateReplica, but recreateSendStream is false"))
			}
			shouldWaitChange = true
		}
		if rs.sendStream != nil && recreateSendStream {
			// This includes both (a) inconsistencies, and (b) transition from
			// probeRecentlyReplicate => replicate.
			rs.closeSendStream(ctx)
		}
		if rs.sendStream == nil {
			rs.createReplicaSendStream(ctx, info.Next, nextRaftIndex)
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
	log.VEventf(ctx, 1, "closing send stream %v for replica %v", rss.stream, rss.desc)
	rss.sendStream.mu.Lock()
	defer rss.sendStream.mu.Unlock()

	rss.sendStream.closeLocked(ctx)
	rss.sendStream = nil
}

func (rs *replicaState) admit(ctx context.Context, av AdmittedVector) {
	if rss := rs.sendStream; rss != nil {
		rss.admit(ctx, av)
	}
}

func (rss *replicaSendStream) closeLocked(ctx context.Context) {
	rss.returnSendTokens(ctx, rss.mu.tracker.UntrackAll(), true /* disconnect */)
	rss.returnAllEvalTokens(ctx)
	rss.mu.closed = true
}

func (rss *replicaSendStream) handleReadyEntriesLocked(
	ctx context.Context, event raftEventForReplica,
) {
	if n := len(event.sendingEntries); n > 0 {
		if event.sendingEntries[0].index != rss.mu.sendQueue.indexToSend {
			panic(errors.AssertionFailedf("first send entry %d does not match indexToSend %d",
				event.sendingEntries[0].index, rss.mu.sendQueue.indexToSend))
		}
		rss.mu.sendQueue.indexToSend = event.sendingEntries[n-1].index + 1
		for _, entry := range event.sendingEntries {
			if !entry.usesFlowControl {
				continue
			}
			var pri raftpb.Priority
			inSendQueue := false
			if entry.index >= rss.mu.sendQueue.nextRaftIndex {
				// Was never in the send-queue.
				pri = entry.pri
			} else {
				// Was in the send-queue.
				inSendQueue = true
				// TODO(sumeer): in pull mode, we will set this to LowPri.
				pri = entry.pri
			}
			tokens := entry.tokens
			if fn := rss.parent.parent.opts.Knobs.OverrideTokenDeduction; fn != nil {
				tokens = fn(tokens)
			}
			if inSendQueue && entry.index >= rss.mu.nextRaftIndexInitial {
				// Was in send-queue and had eval tokens deducted for it.
				rss.mu.sendQueue.originalEvalTokens[WorkClassFromRaftPriority(entry.pri)] -= tokens
				rss.mu.sendQueue.actualEvalTokensDeducted[WorkClassFromRaftPriority(pri)] -= tokens
			}
			rss.parent.sendTokenCounter.Deduct(ctx, WorkClassFromRaftPriority(pri), tokens)
			rss.mu.tracker.Track(ctx, entry.term, entry.index, pri, tokens)
		}
	}
	if n := len(event.newEntries); n > 0 {
		if event.newEntries[0].index != rss.mu.sendQueue.nextRaftIndex {
			panic(errors.AssertionFailedf("append %d does not match nextRaftIndex %d",
				event.newEntries[0].index, rss.mu.sendQueue.nextRaftIndex))
		}
		rss.mu.sendQueue.nextRaftIndex = event.newEntries[n-1].index + 1
		for _, entry := range event.newEntries {
			if !entry.usesFlowControl {
				continue
			}
			var pri raftpb.Priority
			inSendQueue := false
			if entry.index >= rss.mu.sendQueue.indexToSend {
				// Being added to the send-queue.
				inSendQueue = true
				// TODO(sumeer): in pull mode, we will set this to LowPri.
				//
				// NB: we may deduct regular eval tokens, but raft's own flow control
				// may delay sending this, and cause harm to other ranges. That is ok,
				// since in push mode we only subject elastic work to replication flow
				// control (in WaitForEval). That does not mean we will not have
				// regular entries in the send-queue since these could have been
				// evaluated while in pull mode.
				pri = entry.pri
			} else {
				pri = entry.pri
			}
			wc := WorkClassFromRaftPriority(pri)
			tokens := entry.tokens
			if fn := rss.parent.parent.opts.Knobs.OverrideTokenDeduction; fn != nil {
				tokens = fn(tokens)
			}
			if inSendQueue && entry.index >= rss.mu.nextRaftIndexInitial {
				// Is in send-queue and will have eval tokens deducted for it.
				rss.mu.sendQueue.originalEvalTokens[WorkClassFromRaftPriority(entry.pri)] += tokens
				rss.mu.sendQueue.actualEvalTokensDeducted[wc] += tokens
			}
			rss.parent.evalTokenCounter.Deduct(ctx, wc, tokens)
			rss.mu.eval.tokensDeducted[wc] += tokens
		}
	}
}

func (rss *replicaSendStream) changeToProbeLocked(ctx context.Context, now time.Time) {
	log.VEventf(ctx, 1, "r%v:%v stream %v changing to probe",
		rss.parent.parent.opts.RangeID, rss.parent.desc, rss.parent.stream)
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
	rss.returnSendTokens(ctx, rss.mu.tracker.UntrackAll(), true /* disconnect */)
	rss.returnAllEvalTokens(ctx)
	rss.mu.sendQueue.originalEvalTokens = [admissionpb.NumWorkClasses]kvflowcontrol.Tokens{}
	rss.mu.sendQueue.actualEvalTokensDeducted = [admissionpb.NumWorkClasses]kvflowcontrol.Tokens{}
}

// returnSendTokens takes the tokens untracked by the tracker and returns them
// to the send token counters.
func (rss *replicaSendStream) returnSendTokens(
	ctx context.Context, returned [raftpb.NumPriorities]kvflowcontrol.Tokens, disconnect bool,
) {
	for pri, tokens := range returned {
		if tokens > 0 {
			pri := WorkClassFromRaftPriority(raftpb.Priority(pri))
			rss.parent.sendTokenCounter.Return(ctx, pri, tokens, disconnect)
		}
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
			rss.parent.evalTokenCounter.Return(ctx, wc, tokens, false /* disconnect */)
			rss.mu.eval.tokensDeducted[wc] -= tokens
			if rss.mu.eval.tokensDeducted[wc] < 0 {
				if buildutil.CrdbTestBuild {
					panic(errors.AssertionFailedf(
						"negative eval tokens %d for r%s/%s", rss.mu.eval.tokensDeducted[wc],
						rss.parent.parent.opts.RangeID.String(), rss.parent.desc.ReplicaID.String()))
				} else {
					rss.mu.eval.tokensDeducted[wc] = 0
				}
			}
		}
	}
}

func (rss *replicaSendStream) returnAllEvalTokens(ctx context.Context) {
	for wc, tokens := range rss.mu.eval.tokensDeducted {
		if tokens > 0 {
			rss.parent.evalTokenCounter.Return(ctx, admissionpb.WorkClass(wc), tokens, true /* disconnect */)
		}
		rss.mu.eval.tokensDeducted[wc] = 0
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
