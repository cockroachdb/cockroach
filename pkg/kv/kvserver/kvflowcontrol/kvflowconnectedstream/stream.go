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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowtokentracker"
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
	raftMu    *syncutil.Mutex
	rangeID   roachpb.RangeID
	tenantID  roachpb.TenantID
	replicaID roachpb.ReplicaID

	StoreStreamsTokenAdjuster
	StoreStreamSendTokensWatcher
	RaftInterface
	MessageSender
	Scheduler

	// Initial state at the time of creation. Must include replicaID.
	replicas    ReplicaSet
	leaseholder roachpb.ReplicaID
}

type ReplicaSet map[roachpb.ReplicaID]roachpb.ReplicaDescriptor

type RangeController interface {
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
	// Close the controller, since no longer the leader.
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
// maintained by Raft, that never regresses -- it advances whenever
// RaftInterface.MakeMsgApp is called. And everything in (Match,
// NextUpperBound) is the responsibility of Raft to retry. The existing notion
// of Next is protocol state internal to raft, regarding what to retry
// sending.
//
// The Ready described here is only the subset that is needed by replication
// AC -- heartbeats, appending to the local log etc. are not relevant.
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
	FollowerConnected(storeID roachpb.StoreID) bool
	// HighestEntryIndex is the highest index assigned in the log, and produced
	// in Ready.Entries(). If there have been no entries produced, since this
	// replica became the leader, this is the commit index.
	HighestEntryIndex() uint64
	// MakeMsgApp is used to construct a MsgApp for entries in [start, end).
	// REQUIRES: start == nextUpperBound and replicaID is in StateReplicate.
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

// StoreStreamSendTokensWatcher implements a watcher interface that wlll use
// at most one goroutine per kvflowcontrol.Stream that has no send tokens.
// Replicas (from different ranges) waiting for those tokens will call
// NotifyWhenAvailable to queue up for those send tokens. When
type StoreStreamSendTokensWatcher interface {
	NotifyWhenAvailable(
		stream kvflowcontrol.Stream,
		bytesInQueue int64,
		priority admissionpb.WorkPriority,
		tokensGrantedNotification TokensGrantedNotification,
	) (handle struct{})
	UpdateHandle(handle struct{}, priority admissionpb.WorkPriority)
	CancelHandle(handle struct{})
}

type TokensGrantedNotification interface {
	Granted(tokens int64)
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

// StoreStreamsTokenAdjuster is one per node.
//
// TODO: modify kvflowcontroller.Controller to implement this.
type StoreStreamsTokenAdjuster interface {
	// deductTokens deducts (without blocking) flow tokens for the given
	// priority over the given stream.
	deductEvalTokens(
		context.Context, kvflowcontrol.Stream, admissionpb.WorkPriority, kvflowcontrol.Tokens)
	// ReturnTokens returns flow tokens for the given priority and stream.
	returnEvalTokens(
		context.Context, kvflowcontrol.Stream, admissionpb.WorkPriority, kvflowcontrol.Tokens)

	deductSendTokens(
		context.Context, kvflowcontrol.Stream, admissionpb.WorkPriority, kvflowcontrol.Tokens)
	returnSendTokens(
		context.Context, kvflowcontrol.Stream, admissionpb.WorkPriority, kvflowcontrol.Tokens)
}

// connectedStream is per replica to which we are replicating to. These are contained
// inside RangeController.
type connectedStreamOptions struct {
	// Immutable for the lifetime of the connectedStream.

	// storeStream aggregates across the streams for the same (tenant, store).
	// This is the identity that is used to deduct tokens or wait for tokens to
	// be positive.
	storeStream kvflowcontrol.Stream
	// uint64(replicaID) is the id used in Raft.
	replicaID roachpb.ReplicaID
	// This will actually be the RangeControllerImpl, since the RangeController
	// interface is only for external integration.
	RangeController
	// isLocal is true for the local replica.
	isLocal bool

	// Initial state at the time of creation.

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

// TODO: the fields are incomplete.
type connectedStream struct {
	stream       kvflowcontrol.Stream
	ch           chan struct{}
	disconnected int32
	// TODO: since streamOptions is a mix of immutable and mutable state, don't
	// keep a full copy, since it is error prone.
	o connectedStreamOptions

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
