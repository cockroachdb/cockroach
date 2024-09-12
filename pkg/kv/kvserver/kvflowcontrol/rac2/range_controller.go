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

	// MakeMsgApp is used to construct a MsgApp for entries in [start, end) and
	// must only be called in MsgAppPush mode.
	//
	// REQUIRES:
	// - replicaID i, is in StateReplicate.
	// - start == Next(i)
	// - end <= NextUnstableIndex
	// - maxSize > 0.
	//
	// If the sum of all entries in [start,end) are <= maxSize, all will be
	// returned. Else, entries will be returned until, and including, the first
	// entry that causes maxSize to be equaled or exceeded. This implies at
	// least one entry will be returned in the MsgApp on success.
	//
	// Returns raft.ErrCompacted error if log truncated. If no error, there is
	// at least one entry in the message, and nNxt is advanced to be equal to
	// the index+1 of the last entry in the returned message. If
	// raft.ErrCompacted is returned, the replica will transition to
	// StateSnapshot.
	//
	// TODO(pav-kv): the transition to StateSnapshot is not guaranteed, there
	// are some error conditions after which the flow stays in StateReplicate.
	// We should define or eliminate these cases.
	MakeMsgApp(replicaID roachpb.ReplicaID, start, end uint64, maxSize int64) (raftpb.Message, error)
}

// RaftMsgAppMode specifies how Raft (at the leader) generates MsgApps. In
// both modes, Raft knows that (Match(i), Next(i)) are in-flight for a
// follower i.
type RaftMsgAppMode uint8

const (
	// MsgAppPush is the classic way in which Raft operates, where the Ready
	// includes MsgApps. We want to preserve this mode for now due to confidence
	// in its performance characteristics: the MsgApps are eagerly generated in
	// Step, where there is a higher probability that the entries needed for the
	// MsgApp generation are in various caches. In this mode Raft is responsible
	// for flow control, i.e., deciding when to send the entries in
	// [Next(i),NextUnstableIndex), to follower i.
	MsgAppPush RaftMsgAppMode = iota
	// MsgAppPull is the way in which Raft operates when the RangeController is
	// allowed to have send-queues. The MsgApps are generated by calling a
	// method on RaftInterface, and Raft's flow control is disabled. That is,
	// the caller asks Raft to generate MsgApps for a prefix of
	// [Next(i),NextUnstableIndex), for follower i.
	MsgAppPull
)

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
	// MsgAppMode is the current mode.
	MsgAppMode RaftMsgAppMode
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
		//
		// Always modified while holding raftMu, and mu, so either is sufficient
		// for readers.
		voterSets         []voterSet
		voterSetRefreshCh chan struct{}
	}

	replicaMap map[roachpb.ReplicaID]*replicaState
}

// voterStateForWaiters informs whether WaitForEval is required to wait for
// eval-tokens for a voter.
type voterStateForWaiters struct {
	replicaID     roachpb.ReplicaID
	isLeader      bool
	isLeaseHolder bool
	// INVARIANT: !isStateReplicate => hasSendQ. So it won't be included as part
	// of quorum. We also don't wait for it for elastic work.
	isStateReplicate bool
	// When hasSendQ is true, the voter is not included as part of the quorum.
	hasSendQ         bool
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
				partOfQuorum: !v.hasSendQ,
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
			// We may call WaitForEval with a remainingForQuorum count higher than
			// the number of handles that have partOfQuorum set to true. This can
			// happen when not enough replicas are in StateReplicate, or not enough
			// have no send-queue. This is acceptable in that the callee will end up
			// waiting on vssRefreshCh.
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
	msgAppMode     RaftMsgAppMode
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
	// needsTokens tracks which classes need tokens for the new entries. This
	// informs first-pass decision making on replicas that don't have
	// send-queues, and therefore can potentially send the new entries.
	var needsTokens [admissionpb.NumWorkClasses]bool
	for i, entry := range e.Entries {
		newEntries[i] = getEntryFCStateOrFatal(ctx, entry)
		if newEntries[i].usesFlowControl {
			needsTokens[WorkClassFromRaftPriority(newEntries[i].pri)] = true
		}
	}
	nextRaftIndex := rc.nextRaftIndex
	if n := len(e.Entries); n > 0 {
		rc.nextRaftIndex = e.Entries[n-1].Index
	}
	shouldWaitChange := false
	voterSets := rc.mu.voterSets
	numSets := len(voterSets)
	var votersContributingToQuorum [2]int
	var numOptionalForceFlushes [2]int
	for r, rs := range rc.replicaMap {
		info := rc.opts.RaftInterface.FollowerState(r)
		rs.scratchEvent = raftEventForReplica{
			FollowerStateInfo: info,
			nextRaftIndex:     nextRaftIndex,
			newEntries:        newEntries,
			msgAppMode:        e.MsgAppMode,
		}
		if r == rc.opts.LocalReplicaID {
			// Everything is considered sent, regardless of whether in MsgAppPull or
			// MsgAppPush mode.
			rs.scratchEvent.sendingEntries = newEntries
		} else {
			// In MsgAppPull mode, there are no MsgApps, so this will be a noop.
			msgApp, ok := e.MsgApps[r]
			if ok {
				if rs.scratchEvent.FollowerStateInfo.State != tracker.StateReplicate {
					panic("")
				}
				rs.scratchEvent.sendingEntries = constructSendingEntries(ctx, msgApp, newEntries)
			}
		}
		if n := len(rs.scratchEvent.sendingEntries); n > 0 {
			// In MsgAppPull mode, we will only come here for the leader.
			next := rs.scratchEvent.sendingEntries[0].index
			expectedNext := rs.scratchEvent.sendingEntries[n-1].index + 1
			if expectedNext != rs.scratchEvent.Next {
				panic("")
			}
			// Rewind Next.
			rs.scratchEvent.Next = next
		}
		shouldWaitChange = rs.handleReadyState(
			ctx, nextRaftIndex, rs.scratchEvent.FollowerStateInfo) || shouldWaitChange
		if e.MsgAppMode == MsgAppPull && rs.desc.IsAnyVoter() {
			// Compute state and first-pass decision on force-flushing and sending
			// the new entries.
			rs.scratchVoterStreamState = rs.computeReplicaStreamState(ctx, needsTokens)
			if (rs.scratchVoterStreamState.noSendQ && rs.scratchVoterStreamState.hasSendTokens) ||
				rs.scratchVoterStreamState.forceFlushing {
				if rs.desc.IsVoterOldConfig() {
					votersContributingToQuorum[0]++
					if rs.scratchVoterStreamState.forceFlushing &&
						!rs.scratchVoterStreamState.forceFlushingBecauseLeaseholder {
						numOptionalForceFlushes[0]++
					}
				}
				if numSets > 1 && rs.desc.IsVoterNewConfig() {
					votersContributingToQuorum[1]++
					if rs.scratchVoterStreamState.forceFlushing &&
						!rs.scratchVoterStreamState.forceFlushingBecauseLeaseholder {
						// We never actually use numOptionalForceFlushes[1].
						numOptionalForceFlushes[1]++
					}
				}
			}
		}
	}

	if e.MsgAppMode == MsgAppPull {
		// Need to consider making force-flushing changes, or deny voters wanting
		// to form a send-queue.
		var quorumCounts [2]int
		noChangesNeeded := true
		for i := 0; i < numSets; i++ {
			quorumCounts[i] = (len(voterSets[0]) + 2) / 2
			if quorumCounts[i] > votersContributingToQuorum[i] {
				noChangesNeeded = false
			} else if quorumCounts[i] < votersContributingToQuorum[i] && numOptionalForceFlushes[i] > 0 &&
				numSets == 1 {
				// In a joint config (numSets == 2), config 0 may not need a replica
				// to force-flush, but the replica may also be in config 1 and be
				// force-flushing due to that (or vice versa). This complicates
				// computeVoterDirectives, so under the assumption that joint configs
				// are temporary, we don't bother stopping force flushes in that joint
				// configs.
				noChangesNeeded = false
			}
			// Else, common case.
		}
		if noChangesNeeded {
			// Common case.
		} else {
			rc.computeVoterDirectives(votersContributingToQuorum, quorumCounts)
		}
	}
	for _, rs := range rc.replicaMap {
		var rd replicaDirective
		if e.MsgAppMode == MsgAppPull {
			var ss replicaStreamState
			if rs.desc.IsAnyVoter() {
				// Have already computed this above.
				ss = rs.scratchVoterStreamState
			} else {
				// Only need to do first-pass computation for non-voters, since
				// there is no adjustment needed to ensure quorum.
				ss = rs.computeReplicaStreamState(ctx, needsTokens)
			}
			rd = replicaDirective{
				forceFlush:    ss.forceFlushing,
				hasSendTokens: ss.hasSendTokens,
			}
		}
		shouldWaitChange = rs.handleReadyEntries(ctx, rs.scratchEvent, rd) || shouldWaitChange
	}
	// If there was a quorum change, update the voter sets, triggering the
	// refresh channel for any requests waiting for eval tokens.
	if shouldWaitChange {
		rc.updateVoterSets()
	}
	return nil
}

// Score is tuple (bucketed tokens_send(elastic), tokens_eval(elastic)).
type replicaScore struct {
	replicaID          roachpb.ReplicaID
	bucketedTokensSend kvflowcontrol.Tokens
	tokensEval         kvflowcontrol.Tokens
}

// Second-pass decision-making.
func (rc *rangeController) computeVoterDirectives(
	votersContributingToQuorum [2]int, quorumCounts [2]int,
) {
	var scratchFFScores, scratchCandidateFFScores, scratchDenySendQScores [3]replicaScore
	// Used to stop force-flushes if no longer needed. Never includes the
	// leaseholder, even though it may be force-flushing.
	forceFlushingScores := scratchFFScores[:0:len(scratchFFScores)]
	// Used to start force-flushes if we cannot handle the situation with
	// denying formation of send-queue.
	candidateForceFlushingScores := scratchCandidateFFScores[:0:len(scratchCandidateFFScores)]
	// Candidates who want to form a send-queue, but we will consider denying.
	// This will never include the leader or leaseholder.
	candidateDenySendQScores := scratchDenySendQScores[:0:len(scratchDenySendQScores)]
	// Compute the scores.
	for _, rs := range rc.replicaMap {
		if !rs.scratchVoterStreamState.isReplicate || !rs.desc.IsAnyVoter() {
			continue
		}
		if rs.scratchVoterStreamState.noSendQ && rs.scratchVoterStreamState.hasSendTokens {
			// NB: this also includes probeRecentlyNoSendQ.
			continue
		}
		if rs.scratchVoterStreamState.forceFlushingBecauseLeaseholder {
			continue
		}
		// INVARIANTS:
		// Voter and not leaseholder and not leader.
		// isReplicate
		// !noSendQ || !hasSendTokens
		// NB: forceFlushing => !noSendQ
		sendPoolLimit := rs.sendTokenCounter.tokens(admissionpb.ElasticWorkClass)
		sendPoolBucket := sendPoolLimit / 10
		if sendPoolBucket == 0 {
			sendPoolBucket = 1
		}
		sendTokens := rs.sendTokenCounter.tokens(admissionpb.ElasticWorkClass)
		bucketedSendTokens := (sendTokens / sendPoolBucket) * sendPoolBucket
		score := replicaScore{
			replicaID:          rs.desc.ReplicaID,
			bucketedTokensSend: bucketedSendTokens,
			tokensEval:         rs.evalTokenCounter.tokens(admissionpb.ElasticWorkClass),
		}
		if rs.scratchVoterStreamState.forceFlushing {
			forceFlushingScores = append(forceFlushingScores, score)
		} else if rs.scratchVoterStreamState.noSendQ {
			candidateDenySendQScores = append(candidateDenySendQScores, score)
		} else {
			candidateForceFlushingScores = append(candidateForceFlushingScores, score)
		}
	}
	// Sort the scores.
	if len(forceFlushingScores) > 1 {
		slices.SortFunc(forceFlushingScores, func(a, b replicaScore) int {
			return cmp.Or(cmp.Compare(a.bucketedTokensSend, b.bucketedTokensSend),
				cmp.Compare(a.tokensEval, b.tokensEval))
		})
	}
	if len(candidateForceFlushingScores) > 1 {
		slices.SortFunc(candidateForceFlushingScores, func(a, b replicaScore) int {
			return cmp.Or(cmp.Compare(a.bucketedTokensSend, b.bucketedTokensSend),
				cmp.Compare(a.tokensEval, b.tokensEval))
		})
	}
	if len(candidateDenySendQScores) > 1 {
		slices.SortFunc(candidateDenySendQScores, func(a, b replicaScore) int {
			return cmp.Or(cmp.Compare(a.bucketedTokensSend, b.bucketedTokensSend),
				cmp.Compare(a.tokensEval, b.tokensEval))
		})
	}
	voterSets := rc.mu.voterSets
	for i := range voterSets {
		gap := quorumCounts[i] - votersContributingToQuorum[i]
		if gap < 0 {
			// Have more than we need for quorum.
			if len(voterSets) > 1 {
				// Complicated to decide who to stop force-flushing in joint config,
				// so we don't bother.
				continue
			}
			// Stop force-flushes. Most overloaded are earlier in the slice.
			for i := range forceFlushingScores {
				if gap == 0 {
					break
				}
				// Since there is a single set, this must be a member.
				rs := rc.replicaMap[forceFlushingScores[i].replicaID]
				rs.scratchVoterStreamState.forceFlushing = false
				gap++
			}
		} else if gap > 0 {
			// Tell someone to not form send-queue or start force-flushing.
			//
			// First try to prevent someone from forming a send-queue.
			n := len(candidateDenySendQScores)
			// Search from the back since sorted in decreasing order of overload.
			for j := n - 1; j >= 0 && gap > 0; j-- {
				rs := rc.replicaMap[forceFlushingScores[j].replicaID]
				var isSetMember bool
				if i == 0 {
					isSetMember = rs.desc.IsVoterOldConfig()
				} else {
					isSetMember = rs.desc.IsVoterNewConfig()
				}
				if !isSetMember {
					continue
				}
				rs.scratchVoterStreamState.hasSendTokens = true
				gap--
				if i == 0 && len(voterSets) > 1 && rs.desc.IsVoterNewConfig() {
					// By denying formation of a send-queue, we have also increased the
					// voters contributing to quorum for the other set.
					votersContributingToQuorum[1]++
				}
			}
			if gap > 0 {
				// Have not successfully closed the gap by stopping formation of
				// send-queue, so need to force-flush.
				n := len(candidateForceFlushingScores)
				for j := n - 1; j >= 0 && gap > 0; j-- {
					rs := rc.replicaMap[forceFlushingScores[j].replicaID]
					var isSetMember bool
					if i == 0 {
						isSetMember = rs.desc.IsVoterOldConfig()
					} else {
						isSetMember = rs.desc.IsVoterNewConfig()
					}
					if !isSetMember {
						continue
					}
					rs.scratchVoterStreamState.forceFlushing = true
					gap--
					if i == 0 && len(voterSets) > 1 && rs.desc.IsVoterNewConfig() {
						// By force-flushing, we have also increased the voters
						// contributing to quorum for the other set.
						votersContributingToQuorum[1]++
					}
				}
			}
		}
	}
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
		isStateReplicate, hasSendQ := rs.isStateReplicateAndSendQ()
		vsfw := voterStateForWaiters{
			replicaID:        r.ReplicaID,
			isLeader:         r.ReplicaID == rc.opts.LocalReplicaID,
			isLeaseHolder:    r.ReplicaID == rc.leaseholder,
			isStateReplicate: isStateReplicate,
			hasSendQ:         hasSendQ,
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

func (rc *rangeController) tryStartForceFlushRaftMuLocked(ctx context.Context) {

}

type replicaState struct {
	parent *rangeController
	// stream aggregates across the streams for the same (tenant, store). This
	// is the identity that is used to deduct tokens or wait for tokens to be
	// positive.
	stream                             kvflowcontrol.Stream
	evalTokenCounter, sendTokenCounter *tokenCounter
	desc                               roachpb.ReplicaDescriptor

	sendStream              *replicaSendStream
	scratchEvent            raftEventForReplica
	scratchVoterStreamState replicaStreamState
}

type replicaStreamState struct {
	isReplicate bool
	noSendQ     bool
	// The remaining fields serve as output from replicaState and subsequent
	// input into replicaState.

	// forceFlushing is true iff in StateReplicate and there is a send-queue and
	// is being force-flushed. When provided as subsequent input, it should be
	// interpreted as a directive that *may* change the current behavior, i.e.,
	// it may be asking the stream to start a force-flush or stop a force-flush.
	//
	// INVARIANT: forceFlushing => !noSendQ && !hasSendTokens.
	forceFlushing                   bool
	forceFlushingBecauseLeaseholder bool
	// True only if noSendQ (and !forceFlushing). When interpreted as a
	// directive in subsequent input, it may have been changed from false to
	// true to prevent formation of a send-queue.
	hasSendTokens bool
}

type replicaDirective struct {
	forceFlush    bool
	hasSendTokens bool
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
		// probeRecentlyNoSendQ or vice versa.
		connectedState      connectedState
		connectedStateStart time.Time

		// nextRaftIndexInitial is the value of nextRaftIndex when this
		// replicaSendStream was created, or transitioned into replicate.
		nextRaftIndexInitial uint64

		// tracker contains entries that have been sent, and have had
		// send-tokens deducted (and will have had eval-tokens deducted iff
		// index >= nextRaftIndexInitial).
		//
		// Contains no entries in probeRecentlyNoSendQ.
		tracker Tracker

		// Eval state.
		//
		// Contains no tokens in probeRecentlyNoSendQ.
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
		// Not updated in state probeRecentlyNoSendQ.
		sendQueue struct {
			// State of send-queue. [indexToSend, nextRaftIndex) have not been
			// sent. indexToSend == FollowerStateInfo.Next. nextRaftIndex is
			// the current value of NextUnstableIndex at the leader. The
			// send-queue is always empty for the leader.
			indexToSend   uint64
			nextRaftIndex uint64

			// Approximate stats for send-queue. For indices < nextRaftIndexInitial.
			//
			// approxMeanSizeBytes is useful since it guides how many bytes to grab
			// in deductedForScheduler.tokens. If each entry is 100 bytes, and half
			// the entries are subject to AC, this should be ~50.
			approxMeanSizeBytes kvflowcontrol.Tokens

			// preciseSizeSum is the total size of entries subject to AC, and have
			// an index >= nextRaftIndexInitial and >= indexToSend.
			preciseSizeSum kvflowcontrol.Tokens

			// We don't care for now how tokens are watched for etc. We have a good
			// idea how to do that from the prototype.
			// The goals for this prototype are:
			// - Switching between push and pull.
			//
			// - Eval wait considering send-queue.
			//
			// - Maintaining quorum with no send-queue via (a) preventing send-queue
			//   formation, (b) force-flush.
			//
			// - Hiding send-queue when in probeRecentlyNoSendQ.

			// INVARIANT: forceFlushScheduled => send-queue is not empty.
			forceFlushScheduled bool

			// watcherHandleID, deductedForScheduler, forceFlushScheduled are only
			// relevant when connectedState == replicate, and the send-queue is
			// non-empty.
			//
			// If watcherHandleID != InvalidStoreStreamSendTokenHandleID, i.e., we have
			// registered a handle to watch for send tokens to become available. In this
			// case deductedForScheduler.tokens == 0 and !forceFlushScheduled.
			//
			// If watcherHandleID == InvalidStoreStreamSendTokenHandleID, we have
			// either deducted some tokens that we have not used, i.e.,
			// deductedForScheduler.tokens > 0, or forceFlushScheduled (i.e., we
			// don't need tokens). Both cannot be true. In this case, we are
			// waiting to be scheduled in the raftScheduler to do the sending.

			// watcherHandleID StoreStreamSendTokenHandleID

			// Two cases for deductedForScheduler.wc.
			// - grabbed regular tokens: must have regular work waiting. Use the
			//   highest pri in priorityCount for the inherited priority.
			//
			// - grabbed elastic tokens: may have regular work that will be sent.
			//   Deduct regular tokens without waiting for those. The message is sent
			//   with no inherited priority. Since elastic tokens were available
			//   recently it is highly probable that regular tokens are also
			//   available.

			// deductedForScheduler struct {
			//   wc     admissionpb.WorkClass
			//	 tokens kvflowcontrol.Tokens
			// }
			// forceFlushScheduled bool

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

func (rs *replicaState) isStateReplicateAndSendQ() (isStateReplicate, hasSendQ bool) {
	if rs.sendStream == nil {
		return false, true
	}
	rs.sendStream.mu.Lock()
	defer rs.sendStream.mu.Unlock()
	isStateReplicate = rs.sendStream.mu.connectedState == replicate
	if isStateReplicate {
		hasSendQ = rs.sendStream.mu.sendQueue.indexToSend != rs.sendStream.mu.sendQueue.nextRaftIndex
	} else {
		// For WaitForEval, we treat probeRecentlyNoSendQ as having a send-queue
		// and not part of the quorum. We don't want to keep evaluating and pile
		// up work. Note, that this is the exact opposite of how
		// probeRecentlyNoSendQ behaves wrt contributing to the quorum when
		// deciding to force-flush.
		hasSendQ = true
	}
	return isStateReplicate, hasSendQ
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

// First-pass decision making.
func (rs *replicaState) computeReplicaStreamState(
	ctx context.Context, needsTokens [admissionpb.NumWorkClasses]bool,
) replicaStreamState {
	if rs.sendStream == nil {
		return replicaStreamState{
			isReplicate:   false,
			forceFlushing: false,
			noSendQ:       false,
			hasSendTokens: false,
		}
	}
	rss := rs.sendStream
	rss.mu.Lock()
	defer rss.mu.Unlock()
	if rss.mu.connectedState == probeRecentlyNoSendQ {
		return replicaStreamState{
			// Pretend.
			isReplicate:   true,
			forceFlushing: false,
			// Pretend has no send-queue and has tokens, to delay any other stream
			// from having to force-flush.
			//
			// NB: this pretense is helpful in delaying force-flush, and we don't
			// need this pretense in deciding whether to prevent another
			// replicaSendStream from forming a send-queue. But doing separate logic
			// for these two situations is more complicated, and we accept the
			// slight increase in latency when applying this behavior in the latter
			// situation.
			noSendQ:       true,
			hasSendTokens: true,
		}
	}
	vss := replicaStreamState{
		isReplicate:   true,
		forceFlushing: rss.mu.sendQueue.forceFlushScheduled,
		noSendQ:       rss.mu.sendQueue.indexToSend == rss.mu.sendQueue.nextRaftIndex,
	}
	if rs.desc.ReplicaID == rs.parent.leaseholder {
		if vss.noSendQ {
			// The first-pass itself decides that we need to send.
			vss.hasSendTokens = true
		} else {
			// The leaseholder may not be force-flushing yet, but this will start
			// force-flushing.
			vss.forceFlushing = true
			vss.forceFlushingBecauseLeaseholder = true
		}
		return vss
	}
	if rs.desc.ReplicaID == rs.parent.opts.LocalReplicaID {
		// Leader.
		vss.hasSendTokens = true
		return vss
	}
	// Non-leaseholder and non-leader voter.
	if vss.noSendQ && !vss.forceFlushing {
		vss.hasSendTokens = true
		// If tokens are available, that is > 0, we decide we can send all the new
		// entries. This allows for a burst, but it is too complicated to make a
		// tentative decision to send now, and reverse it later (the quorum
		// computation depends on not reversing this decision). To allow for
		// reversing the decision (since some other range could have taken these
		// tokens until we get to sending), we would need to iterate the decision
		// computation until we converge, which would be bad for performance.
		//
		// Alternatively, we could deduct the tokens now, but it introduces the
		// slight code complexity of sending only some of the new entries. We'd
		// rather send all or nothing. This admits a burst, in that we will get
		// into a deficit, and then because of that deficit, form a send-queue,
		// and will need to both (a) pay back the deficit, (b) have enough tokens
		// to empty the send-queue, before the send-queue disappears. The positive
		// side of this is that the frequency of flapping between send-queue and
		// no send-queue is reduced, which means the WaitForEval refreshCh needs
		// to be utilized less frequently.
		if needsTokens[admissionpb.ElasticWorkClass] {
			if available, _ := rs.sendTokenCounter.TokensAvailable(admissionpb.ElasticWorkClass); !available {
				vss.hasSendTokens = false
			}
		}
		if needsTokens[admissionpb.RegularWorkClass] {
			if available, _ := rs.sendTokenCounter.TokensAvailable(admissionpb.RegularWorkClass); !available {
				vss.hasSendTokens = false
			}
		}
	}
	return vss
}

// replicaDirective is only relevant in MsgAppPull mode.
func (rs *replicaState) handleReadyEntries(
	ctx context.Context, event raftEventForReplica, directive replicaDirective,
) (transitionedToNonEmptySendQAsVoter bool) {
	if rs.sendStream == nil {
		return false
	}
	rs.sendStream.mu.Lock()
	defer rs.sendStream.mu.Unlock()
	if rs.sendStream.mu.connectedState != replicate {
		return false
	}
	transitionedToNonEmptySendQ, err := rs.sendStream.handleReadyEntriesLocked(ctx, event, directive)
	if err != nil {
		// Transitioned to StateSnapshot, or some other error that Raft needs to deal with.
		rs.sendStream.closeLocked(ctx)
		rs.sendStream = nil
		transitionedToNonEmptySendQ = true
	}
	return transitionedToNonEmptySendQ && rs.desc.IsAnyVoter()
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
		if shouldClose := func() (shouldClose bool) {
			now := rs.parent.opts.Clock.PhysicalTime()
			rs.sendStream.mu.Lock()
			defer rs.sendStream.mu.Unlock()

			if state := rs.sendStream.mu.connectedState; state == probeRecentlyNoSendQ &&
				now.Sub(rs.sendStream.mu.connectedStateStart) >= probeRecentlyNoSendQDuration() {
				// The replica has been in StateProbe for at least
				// probeRecentlyNoSendQDuration (default 1s) second, close the
				// stream.
				shouldClose = true
			} else if state != probeRecentlyNoSendQ {
				if rs.sendStream.mu.sendQueue.indexToSend == rs.sendStream.mu.sendQueue.nextRaftIndex {
					// Empty send-q. We will transition to probeRecentlyNoSendQ, which
					// trades off not doing a force-flush with allowing for higher
					// latency to achieve quorum.
					rs.sendStream.changeToProbeLocked(ctx, now)
					shouldWaitChange = true
				} else {
					// Already had a send-q.
					shouldClose = true
				}
			}
			return shouldClose
		}(); shouldClose {
			rs.closeSendStream(ctx)
			shouldWaitChange = true
		}

	case tracker.StateReplicate:
		if rs.sendStream == nil {
			rs.createReplicaSendStream(info.Next, nextRaftIndex)
			shouldWaitChange = true
		} else {
			shouldWaitChange = rs.sendStream.makeConsistentInStateReplicate(ctx, info, nextRaftIndex) ||
				shouldWaitChange
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
) (queueEmptinessStateChanged bool) {
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
			queueEmptinessStateChanged = info.Next == rss.mu.sendQueue.nextRaftIndex
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
			// info.Next < rss.sendQueue.indexToSend. Some things got pushed
			// back.
			//
			// Must have transitioned to StateProbe and back, and we did not
			// observe it.
			if info.Next != info.Match+1 {
				log.Fatalf(ctx, "%v", errors.AssertionFailedf(
					"next=%d != match+1=%d [info=%v send_stream=<TODO>]",
					info.Next, info.Match+1, info))
			}
			queueEmptinessStateChanged = rss.mu.sendQueue.indexToSend == rss.mu.sendQueue.nextRaftIndex
			rss.makeConsistentWhenUnexpectedProbeToReplicateLocked(ctx, info.Next)
		}

	case probeRecentlyNoSendQ:
		// Returned from StateProbe => StateReplicate.
		if info.Next != info.Match+1 {
			log.Fatalf(ctx, "%v", errors.AssertionFailedf(
				"next=%d != match+1=%d [info=%v send_stream=<TODO>]",
				info.Next, info.Match+1, info))
		}
		rss.makeConsistentWhenProbeToReplicateLocked(ctx, info.Next, nextRaftIndex)
		queueEmptinessStateChanged = true
	}
	return queueEmptinessStateChanged
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
	// change within probeRecentlyNoSendQDuration, we will close the
	// stream. Also schedule an event, so that even if there are no
	// entries, we will still reliably close the stream if still in
	// StateProbe.
	rss.changeConnectedStateLocked(probeRecentlyNoSendQ, now)
	rss.parent.parent.opts.CloseTimerScheduler.ScheduleSendStreamCloseRaftMuLocked(
		ctx, rss.parent.parent.opts.RangeID, probeRecentlyNoSendQDuration())
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

func (rss *replicaSendStream) isEmptySendQueueLocked() bool {
	return rss.mu.sendQueue.indexToSend == rss.mu.sendQueue.nextRaftIndex
}

// REQUIRES: rss.mu.connectedState = replicate.
func (rss *replicaSendStream) handleReadyEntriesLocked(
	ctx context.Context, event raftEventForReplica, directive replicaDirective,
) (transitionedToNonEmptySendQ bool, err error) {
	wasEmptySendQ := rss.isEmptySendQueueLocked()
	if event.msgAppMode == MsgAppPush {
		if rss.mu.sendQueue.forceFlushScheduled {
			// Must be switching from MsgAppPull to MsgAppPush mode.
			rss.stopForceFlushLocked(ctx)
		}
	} else {
		// MsgAppPull mode. Populate sendingEntries.
		n := len(event.sendingEntries)
		if rss.parent.desc.ReplicaID == rss.parent.parent.opts.LocalReplicaID {
			// Leader.
			if n != len(event.newEntries) {
				panic("leader must be sending all entries.")
			}
		} else {
			// Non-leader. Also includes non-voting replicas.
			if n != 0 {
				panic("pull mode for non-leader must not have sending entries")
			}
			if directive.forceFlush {
				if !rss.mu.sendQueue.forceFlushScheduled {
					// Must have a send-queue, so sendingEntries should stay empty
					// (these will be queued).
					rss.startForceFlushLocked(ctx)
				}
			} else if rss.mu.sendQueue.forceFlushScheduled {
				// INVARIANT: !directive.forceFlush. Must have a send-queue, so
				// sendingEntries should stay empty (these will be queued).
				rss.stopForceFlushLocked(ctx)
			} else if directive.hasSendTokens {
				// Send everything that is being added.
				event.sendingEntries = event.newEntries
			}
		}
	}
	// Common behavior for updating using sendingEntries and newEntries for
	// MsgAppPush and MsgAppPull.
	if n := len(event.sendingEntries); n > 0 {
		rss.mu.sendQueue.indexToSend = event.sendingEntries[n-1].index + 1
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
				// Being added to the send-queue, so we deduct elastic eval tokens.
				pri = raftpb.LowPri
				rss.mu.sendQueue.preciseSizeSum += entry.tokens
			} else {
				pri = entry.pri
			}
			rss.parent.evalTokenCounter.Deduct(ctx, WorkClassFromRaftPriority(pri), entry.tokens)
		}
	}
	if n := len(event.sendingEntries); n > 0 && event.msgAppMode == MsgAppPull {
		_, err := rss.parent.parent.opts.RaftInterface.MakeMsgApp(
			rss.parent.desc.ReplicaID, event.sendingEntries[0].index, event.sendingEntries[n-1].index,
			math.MaxInt64)
		if err != nil {
			return false, err
		}
		// TODO: send msg
	}
	return wasEmptySendQ && !rss.isEmptySendQueueLocked(), nil
}

func (rss *replicaSendStream) startForceFlushLocked(ctx context.Context) {
	// TODO:
}

func (rss *replicaSendStream) stopForceFlushLocked(ctx context.Context) {
	// TODO:
}

// probeRecentlyNoSendQDuration is the duration the controller will wait
// after observing a replica in StateProbe before closing the send stream if
// the replica remains in StateProbe.
//
// TODO(kvoli): We will want to make this a cluster setting eventually.
func probeRecentlyNoSendQDuration() time.Duration {
	return 400 * time.Millisecond
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
// transition happened and may first transition to probeRecentlyNoSendQ. We
// stay in this state for a short time interval, and then close the
// replicaSendStream. No tokens are held in state probeRecentlyNoSendQ and no
// MsgApps are sent. We simply pretend that the replica has no send-queue.
//
// Initial states: replicate
// State transitions: replicate <=> probeRecentlyNoSendQ
const (
	replicate connectedState = iota
	probeRecentlyNoSendQ
)

func (cs connectedState) String() string {
	return redact.StringWithoutMarkers(cs)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (cs connectedState) SafeFormat(w redact.SafePrinter, _ rune) {
	switch cs {
	case replicate:
		w.SafeString("replicate")
	case probeRecentlyNoSendQ:
		w.SafeString("probeRecentlyNoSendQ")
	default:
		panic(fmt.Sprintf("unknown connectedState %v", cs))
	}
}
