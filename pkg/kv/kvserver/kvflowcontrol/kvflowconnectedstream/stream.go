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
	"math"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontroller"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowtokentracker"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TODO(sumeer): go through all recent slack threads and ensure they are
// incorporated.
// TODO(sumeer): undo random things.

// TODO: kvflowcontrol and the packages it contains are sliced and diced quite
// fine, with the benefit of multiple code iterations to get to that final
// structure. We don't yet have that benefit, so we just lump things together
// for now, until most of the code is written and we know how to abstract into
// packages. There are things here that don't belong in kvflowconnectedstream,
// especially all the interfaces.

// TODO: many of the comments here are to guide the implementation. They will
// need to be cleaned up.

// A RangeController exists when a local replica of the range is the raft
// leader. It does not have a goroutine of its own and reacts to events. No
// mutex inside RangeController should be ordered before Replica.raftMu, since
// some event notifications happen with Replica.raftMu already held. We
// consider the following events:
//
// - RaftEvent: This happens on the raftScheduler and already holds raftMu, so
//   the RangeController can be sure that any questions it asks of
//   RaftInterface are consistent with RaftEvent. A RaftEvent encompasses a
//   combination of a tick and ready -- it is possible that there is no
//   "Ready". We assume that as long as not all replicas are caught up,
//   RaftEvents are guaranteed to happen at some minimum frequency (e.g. the
//   500ms of COCKROACH_RAFT_TICK_INTERVAL).
//
// - ControllerSchedulerEvent: This happens on the raftScheduler and
//   represents some work that the RangeController had scheduled. If the
//   RangeController needs to call into RaftInterface, it must itself acquire
//   raftMu before doing so. Such events are used to dequeue raft entries from
//   the send-queue when tokens are available, or to force-flush.
//
// - SetReplicas: raftMu is already held. This ensures RaftEvent and
//   SetReplicas are serialized and the latest set of replicas provided by
//   SetReplicas is also what Raft is operating with. We will back this with a
//   data-structure under Replica.raftMu (and Replica.mu) that is updated in
//   setDescLockedRaftMuLocked. This consistency is important for multiple
//   reasons, including knowing which replicas to use when calling the various
//   methods in RaftInterface (i.e., it isn't only needed for quorum
//   calculaton as discussed on the slack thread
//   https://cockroachlabs.slack.com/archives/C06UFBJ743F/p1715692063606459?thread_ts=1715641995.372289&cid=C06UFBJ743F)
//
// - SetLeaseholder: This is not synchronized with raftMu. The callee should
//   be prepared to handle the case where the leaseholder is not even a known
//   replica, but will eventually be known. TODO: The comment may be incorrect.
//
// - ReplicaDisconnected: raftMu is not held. Informs that a replica has
//   its RaftTransport disconnected. This is necessary to prevent lossiness of
//   tokens. The aforementioned map under Replica.muu will be used to
//   ensure consistency. TODO: make it a narrower data-structure
//   mutex in Replica, so that Raft.mu is not held when calling RangeController.
//
//   Connection events are not communicated. The current state of connectivity
//   should be requested from RaftInterface when handling RaftEvent (in case
//   there are some disconnected replicas that have reconnected), SetReplicas
//   (to see if a new replica is connected). So we rely on liveness of
//   RaftEvent to notice reconnections.
//
// ======================================================================
// Aside on consistency of RangeDescriptor in Replica and the raft group conf:
//
// - handleDescResult and replicaStateMachine.maybeApplyConfChange both happen
// in ApplySideEffects which is called with raftMu held.
//
// - snapshot: processRaftSnapshotRequest is both calling RawNode.Step and
// then handleRaftReadyRaftMuLocked while holding raftMu, and the latter
// updates the desc and the former the raft group conf.
//
// - init after restart: initRaftMuLockedReplicaMuLocked does both
// initialization of the raft group and that of the descriptor.
//
// Since we are relying on this consistency, we should better document it
// ======================================================================
// Buffering and delayed transition to StateProbe:
// Replica.addUnreachableRemoteReplica causes that transition, and is caused by
// a drop in Replica.sendRaftMessageRequest. But since the
// RaftTransport has a queue, it should not be get filled up due to a very
// transient connection break and reestablishment. This is good since transitions
// to StateProbe can result in force flush of some other replica.
// ======================================================================
// Reproposal handling: We will no longer do any special handling of reproposals.
// v1 was accounting before an entry emerged in Ready, so there was a higher chance
// of lossiness (it may never emerge). With v2, there is lossiness too, but less,
// and since both the proposal and reproposal are going to be persisted in the raft
// log we count them both.
// ======================================================================

type RangeControllerOptions struct {
	// Immutable for the lifetime of the RangeController
	RaftMu    *syncutil.Mutex
	RangeID   roachpb.RangeID
	TenantID  roachpb.TenantID
	ReplicaID roachpb.ReplicaID

	StoreStreamsTokenCounter
	StoreStreamSendTokensWatcher
	RaftInterface
	MessageSender
	Scheduler
}

// RangeControllerInitState is the initial state at the time of creation.
type RangeControllerInitState struct {
	// Must include RangeControllerOptions.ReplicaID.
	Replicas ReplicaSet
	// Leaseholder may be set to NoReplicaID, in which case the leaseholder is
	// unknown.
	Leaseholder roachpb.ReplicaID
}

// NoReplicaID is a special value of roachpb.ReplicaID, which can never be a
// valid ID.
const NoReplicaID = 0

type ReplicaSet map[roachpb.ReplicaID]roachpb.ReplicaDescriptor

type RangeController interface {
	// WaitForEval is called concurrently by all requests wanting to evaluate.
	WaitForEval(ctx context.Context, pri admissionpb.WorkPriority) error
	// HandleRaftEvent will be called from handleRaftReadyRaftMuLocked, including
	// the case of snapshot application.
	HandleRaftEvent(e RaftEvent) error
	// HandleControllerSchedulerEvent will be called from the raftScheduler when
	// an event the controller scheduled can be processed.
	HandleControllerSchedulerEvent() error
	// SetReplicas will be called from setDescLockedRaftMuLocked.
	//
	// A new follower here may already be in StateReplicate and have a
	// send-queue, so we should schedule a ControllerSchedulerEvent without
	// waiting for the next Ready.
	SetReplicas(replicas ReplicaSet) error
	// SetLeaseholder is called from leasePostApplyLocked.
	// TODO: I suspect raftMu is held here too.
	SetLeaseholder(replica roachpb.ReplicaID)
	// TransportDisconnected originates in RaftTransport.RaftMessageBatch. To
	// demux to the relevant ranges, the latest set of replicas for a range must
	// be known. We don't want to acquire Replica.raftMu in this iteration from
	// RaftTransport, since Replica.raftMu is held for longer than Replica.mu
	// (and so this read to demultiplex can encounter contention). We will keep
	// a map of StoreID=>ReplicaID in the Replica struct that is updated when
	// the RangeDescriptor is set (which holds both Replica.raftMu and
	// Replica.mu), and so this map can be read with either of these mutexes
	// (and we will read it with Replica.mu).
	//
	// Unlike v1, where this destroyed the connected-stream data-structure for
	// this replica, we could just return the inflight send tokens and keep the
	// rest of the state (send-queue and stats, and eval token deductions) since
	// it may still be in StateReplicate. Though if this is actually down and we
	// stay in StateReplicate, we are penalizing elastic work which will still
	// pace at rate of slowest replica (and the rate of this replica will be 0).
	// Actually, we should keep the connected-stream for a couple of ticks. This
	// will also ensure that this stream can participate in the quorum, and we
	// don't force-flush. But this will also be building up a send-queue, so we
	// may have a quorum of connected-streams with tokens, but we don't have a
	// quorum with an empty send-queue -- figure out a reasonable heuristic
	// (perhaps wait to force-flush until the non-empty send-queue has existed
	// for more than a tick).
	TransportDisconnected(replica roachpb.ReplicaID)
	// Close the controller, since no longer the leader. Can be called concurrently
	// with other methods like WaitForEval. WaitForEval should unblock and return
	// without an error.
	Close()
}

// RaftEvent is an abstraction around raft.Ready, constructed in
// handleRaftReadyRaftMuLocked.
type RaftEvent interface {
	// Ready can return nil if there is no Ready.
	Ready() Ready
}

// Ready interface is an abstraction around raft.Ready, for the pull mode in
// which replication AC will operate. In this pull mode, the Raft leader will
// produce a Ready that has the new entries it knows about, but MsgApps will
// only be produced for a follower for (a) entries in (Match, Next), i.e.,
// entries that are already in-flight for that follower, and have been nacked,
// (b) empty MsgApps to ping the follower. Next is only advanced via pull,
// which is serviced via RaftInterface. In a sense, there is a NextUpperBound
// maintained by Raft, that never regresses in StateReplicate -- it advances
// whenever RaftInterface.MakeMsgApp is called. And everything in (Match,
// NextUpperBound) is the responsibility of Raft to retry. The existing notion
// of Next is protocol state internal to raft, regarding what to retry
// sending. NextUpperBound can regress if the replica transitions out of
// StateReplicate, and back into StateReplicate.
//
// The Ready described here is only the subset that is needed by replication
// AC -- heartbeats, appending to the local log etc. are not relevant.
//
// Ready must be called on every tick/ready of Raft since we cannot tolerate
// state transitions from
//
// StateReplicate => {state in all states: state != StateReplicate} =>
// StateReplicate
//
// that is not observed by RangeController. Since outgoing messages are sent
// in the tick/ready handling and a return from the intermediate state to
// StateReplicate relies on receiving responses to those outgoing messages, we
// should not miss any transitions. If stale messages received in a step can
// cause transitions out and back without observation, we can add a monotonic
// counter for each follower inside Raft (this is just local state at the
// leader), which will be incremented on every state transition and expose
// that via the RaftInterface.
type Ready interface {
	// Entries represents the new entries that are being added to the log.
	// This may be empty.
	//
	// The second byte of AC entries will encode the priority, so we will have
	// the priority, size, and index position, which is all that we need. Unlike
	// v1, where we are doing the accounting in
	// https://github.com/cockroachdb/cockroach/blob/f601b7b439ced71030bfdb0d9ba9cb4925420569/pkg/kv/kvserver/replica_proposal_buf.go#L1057-L1066
	// which is messy.
	Entries() []raftpb.Entry
	// RetransmitMsgApps returns the MsgApps that are being retransmitted, or
	// being used to ping the follower. These will never be queued by
	// replication AC, but it may adjust the priority before sending (due to
	// priority inheritance, when the Message has non-empty Entries).
	RetransmitMsgApps() []raftpb.Message
}

// RaftInterface abstracts what the RangeController needs from the raft
// package, when running at the leader. It also provides one piece of
// information that is not in the raft package -- the current connected state
// of the store, that we will use the RaftTransport to answer.
//
// NB: group membership and connectivity information is communicated to the
// RangeController via a separate channel, as in the v1 implementation, since
// it depends on information inside and outside Raft. The methods in Raft are
// the ones RangeController will call.
//
// The implementation should not need to acquire Replica.mu since we have not
// said anything about the relative lock ordering of Replica.mu and the
// internal mutexes in RangeController.
type RaftInterface interface {
	// FollowerState returns the current state of a follower. The value of
	// nextUpperBound is populated iff in StateReplicate. All entries >=
	// nextUpperBound have not yet had MsgApps constructed.
	//
	// When a follower transitions from {StateProbe,StateSnapshot} =>
	// StateReplicate, or was already in StateReplicate but was disconnected and
	// has now reconnected, we can now start trying to send MsgApps. We should
	// notice such transitions both in HandleRaftEvent and SetReplicas. We
	// should *not* construct MsgApps for a StateReplicate follower that is
	// disconnected -- there is no timeliness guarantee on how long a follower
	// will stay in StateReplicate despite it being down, and by sending such a
	// follower MsgApps that are not being received we are defeating flow
	// control (since we will have advanced nextUpperBound).
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
	FollowerState(replicaID roachpb.ReplicaID) (state tracker.StateType, nextUpperBound uint64)
	FollowerTransportConnected(storeID roachpb.StoreID) bool
	// HighestEntryIndex is the highest index assigned in the log, and produced
	// in Ready.Entries(). If there have been no entries produced, since this
	// replica became the leader, this is the commit index.
	HighestEntryIndex() uint64
	// MakeMsgApp is used to construct a MsgApp for entries in [start, end).
	// REQUIRES: start == nextUpperBound and replicaID is in StateReplicate.
	// REQUIRES: maxSize > 0.
	//
	// If the sum of all entries in [start,end) are <= maxSize, all will be
	// returned. Else, entries will be returned until, and including, the first
	// entry that causes maxSize to be equaled or exceeded. This implies at
	// least one entry will be returned in the MsgApp on success.
	//
	// Returns raft.ErrCompacted error if log truncated. If no error,
	// nextUpperBound is advanced to be equal to end. If raft.ErrCompacted is
	// returned, and the replica was in StateReplicate prior to this call, it
	// will transition to StateSnapshot.
	MakeMsgApp(replicaID roachpb.ReplicaID, start, end uint64, maxSize int64) (raftpb.Message, error)
}

// Scheduler abstracts the raftScheduler to allow the RangeController to
// schedule its own internal processing.
type Scheduler interface {
	ScheduleControllerEvent(rangeID roachpb.RangeID)
}

// MessageSender abstracts Replica.sendRaftMessage. The context used is always
// Replica.raftCtx, so we do not need to pass it.
//
// REQUIRES: msg is a MsgApp. The follower is a member and is in StateReplicate.
type MessageSender interface {
	// SendRaftMessage ...
	//
	// priorityOverride can be kvserverpb.AdmissionPriorityNotOverridden
	//
	// Implementation:
	//
	// On the receiver Replica.stepRaftGroup is called with
	// kvserverpb.RaftMessageRequest. And we do the AdmitRaftEntry call in
	// handleRaftReadyRaftMuLocked. By then we have no access to the wrapper
	// that is RaftMessageRequest.
	//
	// We will also be mediating Raft doing retries. If it is retrying and
	// previously we had sent index i with one priority override and now send
	// with a different priority override, we don't want to change the tracker.
	// Also, we don't know which of these messages made it to the other side, so
	// when the tokens are returned there is ambiguity.
	//
	// Solution:
	// The sender will only track the original priority in tracker. The receiver will
	// know both the original and overridden priority. Will replace the second byte in entry
	// with the overridden priority before handing to stepRaftGroup. RaftAdmissionMeta has the
	// original. Will use the overridden one to admit and then the original to return tokens.
	//
	// Indices 3E, 4E, 5E, 6R, where E is elastic and R is regular. 5E, 6R sent
	// with 5R', 6R. Wnen 5R' returned, will also return 3E, 4E. So there is
	// some early return here. But this is ok since we only have 8MB of elastic
	// outstanding (across all ranges) so the send-q will be the one with most
	// of the elastic bytes. Indices 3E, 4E, 5E, ..., 99E, 100R Send-q is 5E to
	// 100R. Then we can send all of these using R.
	SendRaftMessage(
		ctx context.Context, priorityOverride admissionpb.WorkPriority, msg raftpb.Message)
}

type RangeControllerImpl struct {
	opts        RangeControllerOptions
	replicas    ReplicaSet
	leaseholder roachpb.ReplicaID

	voterSets  []voterSet
	replicaMap map[roachpb.ReplicaID]*replicaStream

	// Scheduled work.
}

type voterSet []roachpb.ReplicaID

var _ RangeController = &RangeControllerImpl{}

func (rc *RangeControllerImpl) WaitForEval(
	ctx context.Context, pri admissionpb.WorkPriority,
) error {
	// TODO: optimize allocations by having arrays for scratch space that we use
	// for handles and slices.
	wc := admissionpb.WorkClassFromPri(pri)
	waitForAllNonStoppedHandles := false
	if wc == admissionpb.ElasticWorkClass {
		waitForAllNonStoppedHandles = true
	}
	// Need to gather all voters, even if disconnected.
	// Also gather the ones that ...
	for _, vs := range rc.voterSets {
		quorumCount := (len(vs) + 2) / 2
		haveEvalTokensCount := 0
		var handleAndSoftDisconnectedChSlice []kvflowcontroller.HandleAndStopCh
		for _, r := range vs {
			rs := rc.replicaMap[r]
			available, handle := rs.evalTokenCounter.TokensAvailable(wc)
			if available {
				haveEvalTokensCount++
				continue
			}
			// Don't have eval tokens, and have a handle.
			var softDisconnectedCh chan struct{}
			if rs.replicateStream != nil && rs.replicateStream.isConnected() {
				softDisconnectedCh = rs.replicateStream.softDisconnectedCh
			}
			handleAndSoftDisconnectedChSlice = append(handleAndSoftDisconnectedChSlice, kvflowcontroller.HandleAndStopCh{
				Handle:     handle,
				StopWaitCh: softDisconnectedCh,
			})
		}
		remainingForQuorum := quorumCount - haveEvalTokensCount
		if wc == admissionpb.RegularWorkClass && remainingForQuorum <= 0 {
			continue
		}
		if remainingForQuorum < 0 {
			remainingForQuorum = 0
		}
		waitEndState, _ := kvflowcontroller.WaitForHandlesAndChannels(
			ctx, nil /*TODO*/, remainingForQuorum, waitForAllNonStoppedHandles, handleAndSoftDisconnectedChSlice, nil)
		if waitEndState == kvflowcontroller.ContextCanceled {
			return ctx.Err()
		}
	}
	return nil
}

func (rc *RangeControllerImpl) HandleRaftEvent(e RaftEvent) error {
	// Refresh the replicaStreams.
	nextRaftIndex := rc.opts.RaftInterface.HighestEntryIndex() + 1
	for r, rs := range rc.replicaMap {
		if rs.replicateStream == nil {
			state, nextUB := rc.opts.RaftInterface.FollowerState(r)
			if state == tracker.StateReplicate {
				// Need to create a replicateStream
				indexToSend := nextUB
				rsInitState := replicateStreamInitState{
					isConnected:   rc.opts.RaftInterface.FollowerTransportConnected(rs.desc.StoreID),
					indexToSend:   indexToSend,
					nextRaftIndex: nextRaftIndex,
					// TODO: these need to be based on some history observed by RangeControllerImpl.
					approxMaxPriority:   admissionpb.NormalPri,
					approxMeanSizeBytes: 1000,
				}
				rs.replicateStream = newReplicateStream(rsInitState)
				// TODO: check if send-queue is non-empty and generate some MsgApps if
				// permitted. Queue up more work, or notification when send tokens are
				// available.
				//
				// TODO: Doing this can cause a transition to StateSnapshot. Need to handle it here.
			}
		} else if !rs.replicateStream.isConnected() &&
			rc.opts.RaftInterface.FollowerTransportConnected(rs.desc.StoreID) {
			rs.replicateStream.setConnectedState(true, false)
			// TODO: check if send-queue is non-empty and generate some MsgApps if
			// permitted. Queue up more work, or notification when send tokens are
			// available.
			//
			// TODO: Doing this can cause a transition to StateSnapshot. Need to handle it here.
		}
	}
	ready := e.Ready()
	if ready == nil {
		return nil
	}
	// Send the MsgApps we have been asked to send. Note that these may cause
	// queueing up in Replica.addUnreachableRemoteReplica, but those will be
	// handed to Raft later, so this act will not cause a transition from
	// StateReplicate => StateProbe.
	msgApps := ready.RetransmitMsgApps()
	for i := range msgApps {
		rc.opts.SendRaftMessage(
			context.TODO(), admissionpb.WorkPriority(kvserverpb.AdmissionPriorityNotOverridden), msgApps[i])
	}

	entries := ready.Entries()
	if len(entries) == 0 {
		return nil
	}
	// These are the only things we want to handle here. If someone has a
	// send-queue of existing entries, they are already trying to slowly
	// eliminate it...
	for r, rs := range rc.replicaMap {
		if r == rc.opts.ReplicaID {
			// Local replica, which is the leader.
			for i := range entries {
				entryFCState := getFlowControlState(entries[i])
				rs.replicateStream.advanceNextRaftIndexAndSent(entryFCState)
			}
			continue
		}
		if rs.replicateStream == nil {
			continue
		}
		if rs.replicateStream.isEmptySendQueue() {
			// Consider sending.
			// If leaseholder just send.
			isLeaseholder := r == rc.leaseholder
			from := entries[0].Index
			// [from, to) is what we will send.
			to := entries[0].Index
			toFinalized := false
			for i := range entries {
				entryFCState := getFlowControlState(entries[i])
				wc := admissionpb.WorkClassFromPri(entryFCState.priority)
				if toFinalized {
					rs.replicateStream.advanceNextRaftIndexAndQueued(entryFCState)
					continue
				}
				send := false
				if isLeaseholder {
					rs.sendTokenCounter.Deduct(context.TODO(), wc, entryFCState.tokens)
					send = true
				} else {
					tokens := rs.sendTokenCounter.TryDeduct(context.TODO(), wc, entryFCState.tokens)
					if tokens > 0 {
						send = true
						if tokens < entryFCState.tokens {
							toFinalized = true
							rs.sendTokenCounter.Deduct(context.TODO(), wc, entryFCState.tokens-tokens)
						}
					}
				}
				if send {
					to++
					rs.replicateStream.advanceNextRaftIndexAndSent(entryFCState)
				} else {
					toFinalized = true
					rs.replicateStream.advanceNextRaftIndexAndQueued(entryFCState)
				}
			}
			if to > from {
				// Have deducted the send tokens. Proceed to send.
				msg, err := rc.opts.RaftInterface.MakeMsgApp(r, from, to, math.MaxInt64)
				if err != nil {
					panic("in Ready.Entries, but unable to create MsgApp -- couldn't have been truncated")
				}
				rc.opts.SendRaftMessage(
					context.TODO(), admissionpb.WorkPriority(kvserverpb.AdmissionPriorityNotOverridden), msg)
			}
		}
	}
	return nil
}

type entryFlowControlState struct {
	index           uint64
	usesFlowControl bool
	priority        admissionpb.WorkPriority
	tokens          kvflowcontrol.Tokens
}

func getFlowControlState(entry raftpb.Entry) entryFlowControlState {
	// TODO:
	return entryFlowControlState{}
}

func (rc *RangeControllerImpl) HandleControllerSchedulerEvent() error {
	return nil
}
func (rc *RangeControllerImpl) SetReplicas(replicas ReplicaSet) error {
	return nil
}
func (rc *RangeControllerImpl) SetLeaseholder(replica roachpb.ReplicaID) {

}
func (rc *RangeControllerImpl) TransportDisconnected(replica roachpb.ReplicaID) {

}
func (rc *RangeControllerImpl) Close() {

}

// connectedStream is per replica to which we are replicating to. These are contained
// inside RangeController.
type replicaStreamOptions struct {
	// storeStream aggregates across the streams for the same (tenant, store).
	// This is the identity that is used to deduct tokens or wait for tokens to
	// be positive.
	storeStream kvflowcontrol.Stream
	// uint64(replicaID) is the id used in Raft.
	replicaID roachpb.ReplicaID
	// This will actually be the RangeControllerImpl, since the RangeController
	// interface is only for external integration.
	RangeController
}

type replicateStreamInitState struct {
	isConnected bool
	// [indexToSend, nextRaftIndex) are known to the (local) leader and need to be sent
	// to this replica. This is the initial send-queue.
	//
	// INVARIANT: isLocal => indexToSend == nextRaftIndex.
	indexToSend   uint64
	nextRaftIndex uint64

	// Approximate stats for the initial send-queue.
	approxMaxPriority   admissionpb.WorkPriority
	approxMeanSizeBytes int
}

type replicaStream struct {
	evalTokenCounter EvalTokenCounter
	sendTokenCounter SendTokenCounter
	desc             roachpb.ReplicaDescriptor
	replicateStream  *replicateStream
}

// replicateStream is the state for replicas in StateReplicate. We need to maintain
// a send-queue for them. We may return some tokens if a connection breaks but
// still want to keep them in StateReplicate? For elastic work this may not be
// great? Well, we can close the ch, and replace it with a new channel.
// We should keep the state of priority etc. in the send-queue. Yes, this is principled.
//
// If Entries pop out and we have not checked whether the stream is connected, we may
// not construct MsgApps, even though the RaftTransport has some buffering. And the
// RaftTransport may reconnect before the next ready/tick. So it is good to have some
// buffer. Consider sending until it returns false, even if the transport is closed.
// Then consider the stream truly disconnected.
//
// So this is a replica-stream. Soft-down when disconnect notified. Up when
// known to be connected. Hard-down if soft-down and send returns false. If we
// stop evaluating because have to wait for hard-down then we have a problem
// in that may never feed something to switch to hard-down.
// So keep feeding until hard-down. And remove from elastic wait when soft-down.
// If circuit-breaker does not trip too bad. When soft-down, those messages not
// subject to AC since not tracking them on the sender.
//
// **what is the channel situation in soft-down** Close and replace channel.
//
// TODO: replicateStream could also encompass StateSnapshot, when transitioned
// from StateReplicate to StateSnapshot. The problem is that indexToSend can
// will need to advance. We also want to eventually use the mediation of
// tracker on when to send snapshot. So overall we should encompass
// StateSnapshot.
//
// "breaker will open in 3-6 seconds after no more TCP packets are flowing. If
// we assume 6 seconds, then that is ~1600 messages/second."
//
// TODO: the fields are incomplete.
type replicateStream struct {

	// TODO: synchronization.
	mu syncutil.Mutex

	softDisconnectedCh chan struct{}
	connectedState     connectedState

	tracker kvflowtokentracker.Tracker

	// State of send-queue.
	indexToSend   uint64
	nextRaftIndex uint64

	// Stats for send-queue.
	approxMaxPriority   admissionpb.WorkPriority
	approxMeanSizeBytes int
	// The approx stats are only for index < approxLimitIndex. This is
	// initialized to streamOptions.nextRaftIndex.
	approxLimitIndex uint64

	// Eval state
	evalTokensDeducted [admissionpb.NumWorkClasses]int64
	// Initially -1. Set to index of first call to advanceNextRaftIndex.
	evalIndexDeductedLowerBound int64
}

func newReplicateStream(initState replicateStreamInitState) *replicateStream {
	// TODO
	return nil
}

func (rc *replicateStream) advanceNextRaftIndexAndSent(state entryFlowControlState) {
	// Will deduct eval tokens
	// Account for in tracker.
	// Deduct send tokens.
}

func (rc *replicateStream) advanceNextRaftIndexAndQueued(entryFlowControlState) {
	// Will deduct eval tokens.
	// If not have an outstanding notification, register one
}

func (rc *replicateStream) isEmptySendQueue() bool {
	// TODO
	return false
}

type connectedState uint32

// Additional connectivity state in StateReplicate.
//
// Local replicas are always in state connected.
//
// Initial state for a replicateStream is either connected or
// softDisconnected, depending on whether FollowerTransportConnected returns
// true or false. Transport stream closure is immediately notified
// (TransportDisconnected), but the reconnection is only learnt about when
// polling FollowerTransportConnected (which happens in a HandleRaftEvent).
// The transport stream closure transitions connected => softDisconnected.
// FollowerTransportConnected polling transitions from softDisconnected state
// to connected. In softDisconnected state, and if the send-queue was already
// empty, we continue to send MsgApps if send-tokens are available, since
// there is buffering capacity in the RaftTransport, which allows for some
// buffering and immediate sending when the RaftTransport reconnects (which
// may happen before the next HandleRaftEvent), which is desirable.
// Unfortunately we cannot do any flow token accounting in this state, since
// those tokens may be lossy. The first false return value from
// SendRaftMessage in softDisconnected will also trigger a notification to
// Raft that the replica is unreachable (see Replica.sendRaftMessage calling
// Replica.addUnreachableRemoteReplica), and that raftpb.MsgUnreachable will
// cause the transition out of StateReplicate to StateProbe. The false return
// value happens either when the (generous) RaftTransport buffer is full, or
// when the circuit breaker opens. The circuit breaker opens 3-6s after no
// more TCP packets are flowing, so we can send about 6s of messages without
// flow control accounting, which is considered acceptable.
//
// In state softDisconnected, we do not wait on eval tokens for this stream
// for elastic work. If we waited for eval tokens and the eval tokens are
// negative due to some other replica, we may not evaluate new work, which
// means we would not call SendRaftMessage hence never triggering the
// transition to StateProbe. That would unnecessarily stop elastic work when a
// node is down.
//
// We call this state softDisconnected since MsgApps are still being generated.
//
// Initial states: {connected, softDisconnected}
// State transitions:
//
//	connected => softDisconnected
//	softDisconnected => connected
//	* => replicateStream closed
const (
	connected connectedState = iota
	softDisconnected
)

func (rs *replicateStream) isConnected() bool {
	return rs.connectedState == connected
}

func (rs *replicateStream) setConnectedState(isConnected bool, init bool) {
	if isConnected {
		if init || rs.connectedState != connected {
			rs.softDisconnectedCh = make(chan struct{})
			rs.connectedState = connected
		}
	} else {
		if init || rs.connectedState == connected {
			close(rs.softDisconnectedCh)
			rs.softDisconnectedCh = nil
			rs.connectedState = softDisconnected
		}
	}
}

// TODO: will need the Disconnected channel like in kvflowcontrol.ConnectedStream, so
// that waiting for eval tokens can unblock.

// TODO: the following has not been revised after the stuff above was written.
//
// NB: the logic notes below are incomplete. Consult the design doc for details on
// the logic.
//
// cstream is an approximate interface for a connectedStream. We won't
// actually use am interface since the RangeControllerImpl and connectedStream
// are tightly integrated. But we should use method on connectedStream as much
// as possible. The following gives some hints at the things that need to
// happen.
type cstream interface {
	// Deduct eval tokens. The send-queue will be non-empty when this method returns.
	//
	// This will be done based on Ready.Entries, since MsgApp will not be
	// created for this until we ask for it. And this could also be the local
	// replica for which MsgApps are never created.
	advanceNextRaftIndex(index uint64, pri admissionpb.WorkPriority, tokens kvflowcontrol.Tokens)
	isEmptySendQ() bool
	// Uses a combination of approx and accurate stats.
	sendQByteSize() int64
	// REQUIRES: !isEmpty()
	// Uses priority inheritance.
	// Caller will use this to wait for store-stream send-tokens to be positive.
	getIndexToSendAndPriority() (uint64, admissionpb.WorkPriority)
	// REQUIRES: !isEmpty() && pos.Index == indexToSend.
	// Caller has decided to send the entry at pos. Deduct send tokens and start tracking.
	incrementIndexToSend(
		pos kvflowcontrolpb.RaftLogPosition, pri admissionpb.WorkPriority, tokens kvflowcontrol.Tokens)
	// Transitions from voter => non-voter or vice versa can happen for a
	// stream. This won't actually happen here -- we will track this information
	// in RangeControllerImpl.
	setIsVoter(isVoter bool)
	setIsLeaseholder(isLeaseholder bool)

	// Message is received to return flow tokens for pri, for all positions <= upto.
	// Return send-tokens and eval-tokens.
	flowTokensReturn(pri admissionpb.WorkPriority, upto kvflowcontrolpb.RaftLogPosition) kvflowcontrol.Tokens
	// sendFunc is called synchronously. If trackTokens is true,
	// incrementIndexToSend will get called from within sendFunc.
	forceFlush(firstKToTrackTokens int, sendFunc func(index uint64, trackTokens bool))

	// The replica has applied a snapshot at index snapIndex. Need to advance indexToSend to
	// snapIndex+1. Return all tokens in tracker > snapIndex, since we will send then again,
	// and don't want to confuse tracker state. Also evalIndexDeductedLowerBound should change to
	// -1 and return all evalTokensDeducted.
	snapshotApplied(snapIndex uint64)
	// The connected-stream is being closed. Return all tokens.
	close()
}

func replicaSetToVoterSets(replicaSet ReplicaSet) (voterSets []voterSet) {
	setCount := 1
	for _, r := range replicaSet {
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
	for len(voterSets) < setCount {
		voterSets = append(voterSets, voterSet{})
	}
	for _, r := range replicaSet {
		isOld := r.IsVoterOldConfig()
		isNew := r.IsVoterNewConfig()
		if !isOld && !isNew {
			continue
		}
		if isOld {
			voterSets[0] = append(voterSets[0], r.ReplicaID)
		}
		if isNew && setCount == 2 {
			voterSets[1] = append(voterSets[1], r.ReplicaID)
		}
	}
	return voterSets
}
