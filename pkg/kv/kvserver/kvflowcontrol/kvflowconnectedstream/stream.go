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
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// NB: bare TODO: are more urgent. TODO(sumeer) are for later.

/*

TODO: Robust token return guarantee

We want a model like the following, given we there is buffering in raftTransport.raftSendQueue
that we want to utilize when processing Ready. That is, if the grpc-stream is transiently
down (say for 50ms), and there are send tokens available, we want to subtract tokens
and add to the raftSendQueue. If the grpc-stream reconnects those buffers will
get drained.
If that stream breaks, we will get TransportDisconnected to early return tokens.


                            (c) deducted here
                                 |
                                 v
<grpc-stream does not exist> <grpc-stream exists> <stream breaks>

  ^                                               ^
  |                                               |
                                                (b) and (c) tokens returned
tokens deducted here
added to raftTransport.raftSendQueue
(a) queue overflows (will transition to StateProbe and return tokens)
(b) did not overflow

We probably need to change the wire protocol to make this possible.

One other issue is that we can deduct tokens for the same index multiple
times, due to transitions out and into StateReplicate, each of which will
first cause early-return.
e.g.
  deduct send-tokens for index 10
  early-return index 10 tokens due to transition to StateSnapshot/StateProbe
  transition to StateReplicate and indexToSend < 10
  deduct send-tokens for index 10

After the second deduction, say there is no stream breakage or transition to
StateProbe. Which means the remote store will receive index 10. We need to ensure
it will call AdmitRaftEntry. Currently, that call happens in handRaftReadyRaftMuLocked,
using raftpb.MsgStorageAppend. This seems risky. If for some reason the remote store
has already appended an index >= 10, when it receives this MsgApp with index
10, this MsgApp will not pop out in handleRaftReadyRaftMuLocked.

If we instead called AdmitRaftEntry in Replica.stepRaftGroup, it may be
sufficient
to fix this problem.

*/

/*
Design for robust flow token return in RACv2

RACv1 has a lot of fragile moving parts for token return in the presence of
gRPC stream disconnects. It also is over-eager in early-returning tokens at
the sender, since it has to interpret the gRPC stream disconnect as the worst
case of tokens being lost. With RACv2 we have more requirements, due to the
repeated deduction of send-tokens for the same entry, which make the fragility
even worse (and we will need to make code changes to the already fragile
code). See
https://cockroachlabs.slack.com/archives/C06UFBJ743F/p1717076603877549 for a
long discussion with details.

Since we now control the Raft code, we have the opportunity to change Raft and
eliminate the existing fragility.

As part of this design, we reduce the number of priorities used in flow
control. We only have two token pools (elastic and regular work-class) with
all priorities < NormalPri mapping to elastic, which limits the performance
isolation guarantee we can make between different priorities that map to
regular work-class. There is still value in having more than two priorities
since on the receiving store we can better distinguish different priority
regular work on different ranges, whose leaders are on different nodes.
Reducing the number of priorities simplifies tracking data-structures both in
RACv2 (the counts of entries at each priority can be an array and not a map)
and in Raft.

The new priorities are:
pri < NormalPri : flowLowPri
NormalPri <= pri < LockingNormalPri : flowNormalPri
LockingNormalPri <= pri < UserHighPri : intent resolution typically flowAboveNormalPri
UserHighPri <= pri: flowHighPri

And numFlowPri is flowHighPri+1. 4 priorities will add 4 uint64's to the wire
protocol and data-structures. With varint encoding, the wire protocol cost
should not be significant.

Raft knows nothing about flow tokens, i.e., how admission is accomplished. But
it does track what has been admitted, and ensures liveness of this tracking.

Raft state at leader for each follower in StateReplicate:
match uint64, next uint64, admitted [numFlowPri]uint64
Invariants:
- (existing) match < next
- (new) for all i: admitted[i] <= match: That is, something cannot be admitted
   unless it is also persisted (this is slightly stronger than ACv1, but solves
   the OOM problem too).

Additional condition for pinging with MsgApps (which happen when match+1 <
next) and for not quiescing (which additionally requires match is equal to
lastEntryIndex):
- exists i: admitted[i] < match

When a follower transitions to StateReplicate, admitted[i] is initialized to
match, for all i.

Advancing Raft state at leader for the follower:

MsgAppResp currently contains Message.Index which becomes the new Match.
Additionally it will contain Admitted [numFlowPri]uint64. This is the latest
index up to which admission has happened. That is, this is not incremental
state, and just the latest state (which makes it easy to handle lossiness). It
will be used to advance admitted.

MsgApp from the leader contains the latest admitted[i] values it has.

Raft state at follower:

Existing state includes stableIndex, lastEntryIndex (I am not using the right
Raft package terms, and there could be other mistakes here)

- stableIndex is advanced based on MsgStorageAppendResp
- lastEntryIndex is advanced in RawNode.Step. This happens before Ready handling in kvserver.

- for async storage writes (which we enable in Raft): the MsgStorageAppend
  includes Responses which contain both MsgStorageAppendResp for the local node
  (this is delivered when the raftScheduler processes this range again), and
  MsgAppResp for the leader (which is sent immediately).
  - This early creation of MsgAppResp, before the entries to append, are
    persisted, is reasonable since that persistence is atomic for all the
    entries in MsgStorageAppend. In comparison, admission of the various entries
    in MsgStorageAppend is not atomic.

New raft state at follower: admitted [numFlowPri]uint64
Invariant: for all i: admitted[i] <= stableIndex.

Raft constructs MsgAppResp for (a) MsgApp pings, (b) for use when
MsgStorageAppend has persisted. These will piggyback the latest admitted
values known to it.

Additionally, RaftInterface.AdvanceAdmitted(admitted [numFlowPri]uint64) will
be called by kvserver in handleRaftReadyRaftMuLocked. And if advanced, this
method will return a MsgAppResp to be sent.

Life of admission:
- MsgApp construction and sending at leader:
  - (kvserver) deducts flow tokens corresponding to MsgApp it asks for by
    calling RaftInterface.MakeMsgApp. This also advances next.
  - (raft) constructs its own pinging MsgApps
  Both kinds include Admitted, representing the leader's current knowledge.

- MsgApp processing at follower:
  - (kvserver) calls RawNode.Step with the MsgApps. lastEntryIndex is advanced.
  - (kvserver) handleRaftReadyRaftMuLocked. Ready may contain:
     - MsgAppResp: this will be sent.
     - MsgStorageAppend: Processing:
       - async storage writes: The MsgStorageAppend includes Responses which
         contain both MsgStorageAppendResp for the local node (this is delivered
         when the raftScheduler processes this range again), and MsgAppResp
         which are sent when the write completes, without waiting for the
         raftScheduler (to minimize commit latency).
       - calls AdmitRaftEntry for the entries in raftpb.MsgStorageAppend. In
         addition to queueing in the StoreWorkQueue, this will track (on the
         Replica): notAdmitted [numFlowPri][]uint64, where the []uint64 is in
         increasing entry index order. We don't bother with the term in the
         tracking (unlike the RaftLogPosition struct in the current code). These
         slices are appended to here, based on the entries in MsgStorageAppend.

- (kvserver) Admission happens on follower:
  - The structs corresponding to the admitted entries are queued in the
    Replica akin to how we queue Replica.localMsgs and the raftScheduler will be
    told to do ready processing (there may not be a Raft ready, but this is akin
    to how handleRaftReadyRaftMuLocked does
    deliverLocalRaftMsgsRaftMuLockedReplicaMuLocked).
  - In handleRaftReadyRaftMuLocked, this queue is used to remove things from
    notAdmitted. Additionally RaftInterface is queried for stableIndex. If a
    notAdmitted[i] is empty, admitted[i] = stableIndex, else admitted[i] =
    notAdmitted[i][0]-1. RaftInterface.AdvanceAdmitted is called, which may
    return a MsgAppResp.
  - MsgAppResp is scheduled to be sent.

Optimization:

I don't think we can afford to construct a RaftMessageRequest containing a
single MsgAppResp just to advance admitted. A RaftMessageRequest includes
ReplicaDescriptors etc. that make it heavyweight.

RACv1 optimizes this by having
kvflowcontrolpb.AdmittedRaftLogEntries(range-id, pri, upto-raft-log-position,
store-id) and piggy-backing these on any RaftMessageRequest being sent to the
relevant node.

We can consider doing a similar piggy-backing:

- Say m is the raftpb.Message corresponding to the MsgAppResp that was
  returned from RaftInterface.AdvanceAdmitted
- Wrap it with the RangeID, and enqueue it for the leader's nodeid in the
  RaftTransport (this will actually be much simpler than the
  RaftTransport.kvflowControl plumbing).
- Every RaftMessageRequest for the node will piggy-back these.

Priority override:

Since the admitted[i] value for every priority advances up to stableIndex
based on the absence of entries with that priority waiting for admission
(unlike RACv1 advancement), we can simply use the overridden priorities at
admission time. TODO(sumeer): elaborate.

*/

// TODO:
// - force-flush when there isn't quorum.
//
// - delay force-flush slightly (say 1s) since there could be a transient
//   transition to StateProbe and back to StateReplicate due to a single
//   MsgApp drop. Should we delay the closing of the replicaSendStream since
//   it has all the interesting stats. It also has tokens that we should be
//   wary of returning -- we will have to return the tokens anyway since the
//   MsgApp drop must be due to a TransportDisconnected. This is a regression
//   in indexToSend. We won't have approx stats for this regression, but so be
//   it.
//
// - While in StateReplicate, send-queue could have some elements popped
//   (indexToSend advanced) because there could have been some inflight
//   MsgApps that we forgot about due to a transition out of StateReplicate
//   and back into StateReplicate, and we got acks for them (Match advanced
//   past indexToSend). To handle this we need to do the same approx stats
//   thing we do when transitioning from StateSnapshot => StateReplicate (we
//   are still holding tokens unlike StateSnapshot and we can keep some of
//   them since the transport did not break.
//
// - If we miss a transition out of StateReplicate and back, Match will not
//   regress, but Next can regress. This is similar to us ignoring a
//   transition into StateProbe to avoid force-flushing immediately. So should
//   handle it in a similar manner.
//
// - use NotSubjectToACForFlowControl for most of force-flush.
//
// - for the first 1MB of force-flush, deduct flow tokens. So these will need
//   to be in a separate MsgApp. Don't do any priority override for these.

// TODO(sumeer): kvflowcontrol and the packages it contains are sliced and
// diced quite fine, with the benefit of multiple code iterations to get to
// that final structure. We don't yet have that benefit, so we just lump
// things together for now, until most of the code is written and we know how
// to abstract into packages. There are things here that don't belong in
// kvflowconnectedstream, especially all the interfaces.

// TODO(sumeer): many of the comments here are to guide the implementation.
// They will need to be cleaned up.

// TODO(sumeer): Incorporate the following comment somewhere: we don't want
// force-flush entries to usually use up tokens since we don't want logical AC
// on the other side since it could cause an OOM. Also, a force-flush on range
// r1 to s3 because of a lack of quorum could consume all the tokens for s3
// that are needed by range r2 -- this is not a problem by itself since we are
// not trying to inter-range isolation.
//
// In ACv1, when a node rejoins, we send it entries saying they are subject to
// AC, but don't track them on the sender, so they will logically queue. So
// that OOM possibility is real -- we just haven't seen it happen.

// A RangeController exists when a local replica of the range is the raft
// leader. It does not have a goroutine of its own and reacts to events. No
// mutex inside RangeController should be ordered before Replica.raftMu (or
// Replica.mu), since most event notifications happen with Replica.raftMu
// already held. We consider the following events:
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
//   represents some work that the RangeController had scheduled. The Replica
//   will acquire raftMu before calling into RangeController. Such events are
//   used to dequeue raft entries from the send-queue when tokens are
//   available, or to force-flush.
//
// - SetReplicas: raftMu is already held (Replica.mu also happens to be held,
//   so SetReplicas should do a minimal amount of work). This ensures
//   RaftEvent and SetReplicas are serialized and the latest set of replicas
//   provided by SetReplicas is also what Raft is operating with. We will back
//   this with a data-structure under Replica.raftMu (and Replica.mu) that is
//   updated in setDescLockedRaftMuLocked. This consistency is important for
//   multiple reasons, including knowing which replicas to use when calling
//   the various methods in RaftInterface (i.e., it isn't only needed for
//   quorum calculation as discussed on the thread
//   https://cockroachlabs.slack.com/archives/C06UFBJ743F/p1715692063606459?thread_ts=1715641995.372289&cid=C06UFBJ743F)
//
// - SetLeaseholder: This is not synchronized with raftMu. The callee should
//   be prepared to handle the case where the leaseholder is not even a known
//   replica, but will eventually be known. TODO: The comment may be incorrect.
//
// - ReplicaDisconnected: raftMu is not held. Informs that a replica has
//   its RaftTransport disconnected. This is necessary to prevent lossiness of
//   tokens. The aforementioned map under Replica.mu will be used to
//   ensure consistency.
//   TODO: make it a narrower data-structure mutex in Replica, which will be
//   held in read mode. So that Raft.mu is not held when calling
//   RangeController.
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
// Since we are relying on this consistency, we should better document it.
// ======================================================================
// Buffering and delayed transition to StateProbe:
// Replica.addUnreachableRemoteReplica causes that transition, and is caused by
// a drop in Replica.sendRaftMessageRequest. But since the
// RaftTransport has a queue, it should not be get filled up due to a very
// transient connection break and reestablishment. This is good since transitions
// to StateProbe can result in force flush of some other replica.
//
// But a single MsgApp drop will also cause a transition to StateProbe, and
// that risk of force flush needs to be mitigated.
// ======================================================================
// Reproposal handling: We no longer do any special handling of reproposals.
// v1 was accounting before an entry emerged in Ready, so there was a higher
// chance of lossiness (it may never emerge). With v2, there is lossiness too,
// but less, and since both the proposal and reproposal are going to be
// persisted in the raft log we count them both.
// ======================================================================

type RangeController interface {
	// WaitForEval is called concurrently by all requests wanting to evaluate.
	WaitForEval(ctx context.Context, pri admissionpb.WorkPriority) error
	// HandleRaftEvent is called from handleRaftReadyRaftMuLocked, including the
	// case of snapshot application.
	HandleRaftEvent(e RaftEvent) error
	// HandleControllerSchedulerEvent ia called from the raftScheduler when an
	// event the controller scheduled can be processed.
	HandleControllerSchedulerEvent() error
	// SetReplicas is called from setDescLockedRaftMuLocked.
	//
	// NB: a new follower here may already be in StateReplicate and have a
	// send-queue.
	SetReplicas(replicas ReplicaSet) error
	// SetLeaseholder is called from leasePostApplyLocked.
	// TODO: I suspect raftMu is held here too.
	SetLeaseholder(replica roachpb.ReplicaID)
	// TransportDisconnected originates in RaftTransport.startProcessNewQueue.
	// To demux to the relevant ranges, the latest set of replicas for a range
	// must be known. We don't want to acquire Replica.raftMu in this iteration
	// (over ranges) from RaftTransport, since Replica.raftMu is held for longer
	// than Replica.mu (and so this read to demultiplex can encounter
	// contention). We will keep a map of StoreID=>ReplicaID in the Replica
	// struct that has its own narrow mutex (ordered after Replica.raftMu and
	// Replica.mu, for updates), and that narrow mutex will be held in read mode
	// when calling TransportDisconnected (which means that mutex is ordered
	// before RangeControllerImpl.mu). TransportDisconnected should do very
	// little work and return.
	TransportDisconnected(replica roachpb.ReplicaID)
	// Close the controller, since no longer the leader. Called from
	// handleRaftReadyRaftMuLocked. It can be called concurrently with
	// WaitForEval. WaitForEval should unblock and return without an error,
	// since there is no need to wait for tokens at this replica (which may
	// still be the leaseholder).
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
// entries that are already in-flight for that follower, that Raft may want to
// resend (b) empty MsgApps to ping the follower (it seems like (a) doesn't
// happen in our Raft implementation, but it is permitted).
//
// Next is typically only advanced via pull, which is serviced via
// RaftInterface. Next, never regresses in StateReplicate, and advances
// whenever RaftInterface.MakeMsgApp is called. And everything in (Match,
// Next) is the responsibility of Raft to maintain liveness for (via MsgApp
// pings). Next can regress if the replica transitions out of StateReplicate,
// and back into StateReplicate. Also, in the rare case, Next can advance
// within Raft there were old MsgApps forgotten about due to a transition to
// StateProbe and back to StateReplicate, and we receive acks for those e.g.
// if (Match, Next) was (5, 10), it is possible to receive acks up to 11, and
// so the state becomes (12, 12).
//
// The Ready described here is only the subset that is needed by replication
// AC -- heartbeats, appending to the local log etc. are not relevant.
//
// Ready must be called on every tick/ready of Raft.
//
// The RangeController may not see all state transitions involving
// StateProbe/StateReplicate/StateSnapshot for a replica, and must be prepared
// to adjust its internal state to reflect the current reality.
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
	// replication AC.
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
// The implementation can assume Replica.raftMu is already held. It should not
// need to acquire Replica.mu, since there is no promise made on whether it is
// already held.
type RaftInterface interface {
	// FollowerState returns the current state of a follower. The value of match
	// and next are populated iff in StateReplicate. All entries >= next have
	// not had MsgApps constructed during the lifetime of this StateReplicate
	// (they may have been constructed previously)
	//
	// TODO: **resume here**
	// When a follower transitions from {StateProbe,StateSnapshot} =>
	// StateReplicate, we start trying to send MsgApps. We should
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
	// priorityOverride can be kvserverpb.PriorityNotOverriddenForFlowControl
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
	//
	// We need to send the receiver the overridden-priority and not just the
	// original priority since if we consumed regular tokens for elastic work
	// due to override, we want that elastic work admitted with that overridden
	// priority so that the tokens are returned in a timely manner. If they are
	// not returned in a timely manner, another range could suffer which is
	// waiting on the regular tokens.
	SendRaftMessage(
		ctx context.Context, priorityOverride admissionpb.WorkPriority, msg raftpb.Message)
}

type RangeControllerOptions struct {
	// TODO: synchronization.
	// RaftMu    *syncutil.Mutex
	RangeID  roachpb.RangeID
	TenantID roachpb.TenantID
	// LocalReplicaID is the ReplicaID of the local replica, which is the
	// leader.
	LocalReplicaID roachpb.ReplicaID

	SSTokenCounter    StoreStreamsTokenCounter
	SendTokensWatcher StoreStreamSendTokensWatcher
	RaftInterface     RaftInterface
	MessageSender     MessageSender
	Scheduler         Scheduler
}

// RangeControllerInitState is the initial state at the time of creation.
type RangeControllerInitState struct {
	// Must include RangeControllerOptions.ReplicaID.
	ReplicaSet ReplicaSet
	// Leaseholder may be set to NoReplicaID, in which case the leaseholder is
	// unknown.
	Leaseholder roachpb.ReplicaID
}

// NoReplicaID is a special value of roachpb.ReplicaID, which can never be a
// valid ID.
const NoReplicaID roachpb.ReplicaID = 0

type ReplicaSet map[roachpb.ReplicaID]roachpb.ReplicaDescriptor

type RangeControllerImpl struct {
	opts        RangeControllerOptions
	replicaSet  ReplicaSet
	leaseholder roachpb.ReplicaID

	// State for waiters. When anything in voterSets changes, voterSetRefreshCh
	// is closed, and replaced with a new channel. The voterSets is
	// copy-on-write, so waiters make a shallow copy.
	voterSets         []voterSet
	voterSetRefreshCh chan struct{}

	replicaMap map[roachpb.ReplicaID]*replicaState

	scheduledReplicas map[roachpb.ReplicaID]struct{}
}

type voterSet []voterStateForWaiters

type voterStateForWaiters struct {
	replicaID        roachpb.ReplicaID
	isLeader         bool
	isLeaseHolder    bool
	isStateReplicate bool
	evalTokenCounter TokenCounter
}

var _ RangeController = &RangeControllerImpl{}

func NewRangeControllerImpl(
	o RangeControllerOptions, init RangeControllerInitState,
) *RangeControllerImpl {
	rc := &RangeControllerImpl{
		opts:              o,
		replicaSet:        ReplicaSet{},
		leaseholder:       init.Leaseholder,
		replicaMap:        map[roachpb.ReplicaID]*replicaState{},
		scheduledReplicas: make(map[roachpb.ReplicaID]struct{}),
	}
	rc.updateReplicaSetAndMap(init.ReplicaSet)
	rc.updateVoterSets()
	return rc
}

func (rc *RangeControllerImpl) updateReplicaSetAndMap(newSet ReplicaSet) {
	prevSet := rc.replicaSet
	for r := range prevSet {
		desc, ok := newSet[r]
		if !ok {
			rs := rc.replicaMap[r]
			rs.close()
			delete(rc.replicaMap, r)
		} else {
			// It does not matter if the replica has changed from voter to non-voter
			// or vice-versa, in that we still need to replicate to it.
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
		rc.replicaMap[r] = NewReplicaState(rc, desc)
	}
}

// replicaSet, replicaMap, leaseholder are up-to-date.
func (rc *RangeControllerImpl) updateVoterSets() {
	// TODO: some callers of updateVoterSets should first figure out if anything
	// has changed in the voters.

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
			isStateReplicate: rs.replicaSendStream != nil && rs.replicaSendStream.connectedState.isStateReplicate(),
			evalTokenCounter: rs.evalTokenCounter,
		}
		if isOld {
			voterSets[0] = append(voterSets[0], vsfw)
		}
		if isNew && setCount == 2 {
			voterSets[1] = append(voterSets[1], vsfw)
		}
	}
	rc.voterSets = voterSets
	close(rc.voterSetRefreshCh)
	rc.voterSetRefreshCh = make(chan struct{})

	// TODO: go through the voters and figure out if we need to force-flush
	// something. Account for already ongoing force-flushes.
}

func (rc *RangeControllerImpl) WaitForEval(
	ctx context.Context, pri admissionpb.WorkPriority,
) error {
	wc := admissionpb.WorkClassFromPri(pri)
	waitForAllReplicateHandles := false
	if wc == admissionpb.ElasticWorkClass {
		waitForAllReplicateHandles = true
	}
	var handles []tokenWaitingHandleInfo
	var scratch []reflect.SelectCase
retry:
	// Snapshot the voterSets and voterSetRefreshCh.
	// TODO: synchronization.
	vss := rc.voterSets
	vssRefreshCh := rc.voterSetRefreshCh
	if vssRefreshCh == nil {
		// RangeControllerImpl is closed.
		return nil
	}
	for _, vs := range vss {
		quorumCount := (len(vs) + 2) / 2
		haveEvalTokensCount := 0
		handles := handles[:0]
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
				return ctx.Err()
			case RefreshWaitSignaled:
				goto retry
			}
		}
	}
	return nil
}

func (rc *RangeControllerImpl) HandleRaftEvent(e RaftEvent) error {
	// Ensure that the replicaSendStreams are consistent with the current Raft
	// state.
	for r, rs := range rc.replicaMap {
		// The state may have changed due to events that are not observed by
		// RangeControllerImpl.
		//
		// - Transitions to StateProbe can happen via the circuit breaker, or a nack.
		//
		// - Transitions to StateReplicate can happen because a MsgAppResp was
		//   received by Raft.
		//
		// - Transitions from StateReplicate => StateSnapshot are caused by RangeControllerImpl,
		//   but there are also transitions from StateProbe => StateSnapshot (TODO: confirm) on
		//   receiving a MsgAppResp with a Match that is too far in the past.
		//
		// Transport connected => disconnected transitions will always be communicated via
		// TransportDisconnected, but the reverse transition is not.
		state, nextUB := rc.opts.RaftInterface.FollowerState(r)
		switch state {
		case tracker.StateProbe:
			if rs.replicaSendStream != nil {
				rs.replicaSendStream.close()
				rs.replicaSendStream = nil
			}
		case tracker.StateReplicate:
			if rs.replicaSendStream == nil {
				rs.createReplicaSendStream(nextUB)
			} else {
				// replicaSendStream already exists, but may be in state snapshot or
				// replicateSoftDisconnected
				switch rs.replicaSendStream.connectedState {
				case replicateConnected: // Nothing to do.
				case replicateSoftDisconnected:
					isConnected := rs.parent.opts.RaftInterface.FollowerTransportConnected(rs.desc.StoreID)
					if isConnected {
						rs.replicaSendStream.changeConnectedStateInStateReplicate(isConnected)
					}
				case snapshot:
					isConnected := rs.parent.opts.RaftInterface.FollowerTransportConnected(rs.desc.StoreID)
					rs.replicaSendStream.changeToStateReplicate(isConnected, nextUB)
				}
			}
		case tracker.StateSnapshot:
			if rs.replicaSendStream != nil && rs.replicaSendStream.connectedState != snapshot {
				rs.replicaSendStream.changeToStateSnapshot()
			}
		}
	}

	// Process ready.
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
		rc.opts.MessageSender.SendRaftMessage(
			context.TODO(), admissionpb.WorkPriority(kvserverpb.PriorityNotOverriddenForFlowControl), msgApps[i])
	}

	entries := ready.Entries()
	if len(entries) == 0 {
		return nil
	}
	// The entries are the only things we handle here for StateReplicate
	// replicas with empty send-queues. If a replica has a send-queue of
	// existing entries, they are already trying to eliminate it, and we simply
	// queue.
	for r, rs := range rc.replicaMap {
		if r == rc.opts.LocalReplicaID {
			// Local replica, which is the leader. These will have a
			// MsgStorageAppend that is not mediated here, but we do need to account
			// for tokens.
			for i := range entries {
				entryFCState := getFlowControlState(entries[i])
				if !entryFCState.usesFlowControl {
					continue
				}
				wc := admissionpb.WorkClassFromPri(entryFCState.priority)
				rs.sendTokenCounter.Deduct(context.TODO(), wc, entryFCState.tokens)
				rs.replicaSendStream.advanceNextRaftIndexAndSent(entryFCState)
			}
			continue
		}
		if rs.replicaSendStream == nil {
			continue
		}
		if rs.replicaSendStream.connectedState != snapshot && rs.replicaSendStream.isEmptySendQueue() {
			// Consider sending.
			// If leaseholder just send. If the leaseholder has a send-queue we won't be in this
			// path, but a force-flush must be ongoing.
			isLeaseholder := r == rc.leaseholder
			from := entries[0].Index
			// [from, to) is what we will send.
			to := entries[0].Index
			toIsFinalized := false
			for i := range entries {
				entryFCState := getFlowControlState(entries[i])
				wc := admissionpb.WorkClassFromPri(entryFCState.priority)
				if toIsFinalized {
					if entries[i].Index == to && !entryFCState.usesFlowControl {
						// Include additional entries that are not subject to AC, since we
						// always have tokens for those.
						to++
					} else {
						rs.replicaSendStream.advanceNextRaftIndexAndQueued(entryFCState)
					}
					continue
				}
				// INVARIANT: !toIsFinalized.
				send := false
				if isLeaseholder {
					if entryFCState.usesFlowControl {
						rs.sendTokenCounter.Deduct(context.TODO(), wc, entryFCState.tokens)
					}
					send = true
				} else {
					if entryFCState.usesFlowControl {
						tokens := rs.sendTokenCounter.TryDeduct(context.TODO(), wc, entryFCState.tokens)
						if tokens > 0 {
							send = true
							if tokens < entryFCState.tokens {
								toIsFinalized = true
								// Deduct the remaining for this entry.
								rs.sendTokenCounter.Deduct(context.TODO(), wc, entryFCState.tokens-tokens)
							}
						}
						// Else send stays false.
					} else {
						send = true
					}
				}
				if send {
					to++
					rs.replicaSendStream.advanceNextRaftIndexAndSent(entryFCState)
				} else {
					toIsFinalized = true
					rs.replicaSendStream.advanceNextRaftIndexAndQueued(entryFCState)
				}
			}
			if to > from {
				// Have deducted the send tokens. Proceed to send.
				msg, err := rc.opts.RaftInterface.MakeMsgApp(r, from, to, math.MaxInt64)
				if err != nil {
					panic("in Ready.Entries, but unable to create MsgApp -- couldn't have been truncated")
				}
				rc.opts.MessageSender.SendRaftMessage(
					context.TODO(), kvserverpb.PriorityNotOverriddenForFlowControl, msg)
			}
			// Else nothing to send.
		} else {
			// In StateSnapshot, or in StateReplicate with a queue. Need to queue.
			for i := range entries {
				entryFCState := getFlowControlState(entries[i])
				rs.replicaSendStream.advanceNextRaftIndexAndSent(entryFCState)
			}
		}
	}
	return nil
}

type entryFlowControlState struct {
	pos kvflowcontrolpb.RaftLogPosition
	// usesFlowControl can be false for entries that don't use flow control.
	// This can happen if RAC is partly disabled e.g. disabled for regular work,
	// or for conf changes. In the former case the send-queue will also be
	// disabled (i.e., equivalent to RACv1).
	usesFlowControl bool
	priority        admissionpb.WorkPriority
	tokens          kvflowcontrol.Tokens
}

func getFlowControlState(entry raftpb.Entry) entryFlowControlState {
	// TODO: change the payload encoding and parsing, and delegate the priority
	// parsing to that.
	return entryFlowControlState{
		pos: kvflowcontrolpb.RaftLogPosition{
			Term:  entry.Term,
			Index: entry.Index,
		},
		usesFlowControl: false,                 // TODO:
		priority:        admissionpb.NormalPri, // TODO:
		tokens:          kvflowcontrol.Tokens(len(entry.Data)),
	}
}

func (rc *RangeControllerImpl) HandleControllerSchedulerEvent() error {
	for r := range rc.scheduledReplicas {
		rs, ok := rc.replicaMap[r]
		scheduleAgain := false
		if ok && rs.replicaSendStream != nil {
			scheduleAgain = rs.replicaSendStream.scheduled()
		}
		if !scheduleAgain {
			delete(rc.scheduledReplicas, r)
		}
	}
	if len(rc.scheduledReplicas) > 0 {
		rc.opts.Scheduler.ScheduleControllerEvent(rc.opts.RangeID)
	}
	return nil
}

func (rc *RangeControllerImpl) scheduleReplica(r roachpb.ReplicaID) {
	rc.scheduledReplicas[r] = struct{}{}
	if len(rc.scheduledReplicas) == 1 {
		rc.opts.Scheduler.ScheduleControllerEvent(rc.opts.RangeID)
	}
}

func (rc *RangeControllerImpl) SetReplicas(replicas ReplicaSet) error {
	rc.updateReplicaSetAndMap(replicas)
	rc.updateVoterSets()
	return nil
}

func (rc *RangeControllerImpl) SetLeaseholder(replica roachpb.ReplicaID) {
	if replica == rc.leaseholder {
		return
	}
	rc.leaseholder = replica
	rc.updateVoterSets()
	rs, ok := rc.replicaMap[replica]
	if !ok {
		// Ignore. Should we panic?
	}
	if rs.replicaSendStream != nil && rs.replicaSendStream.connectedState != snapshot &&
		!rs.replicaSendStream.isEmptySendQueue() {
		rs.replicaSendStream.scheduleForceFlush()
	}
}

func (rc *RangeControllerImpl) TransportDisconnected(replica roachpb.ReplicaID) {
	rs, ok := rc.replicaMap[replica]
	if ok && rs.replicaSendStream != nil && rs.replicaSendStream.connectedState == replicateConnected {
		rs.replicaSendStream.changeConnectedStateInStateReplicate(false)
		// TODO: the callee isn't returning tokens in tracker. Fix it.
	}
}

func (rc *RangeControllerImpl) Close() {
	close(rc.voterSetRefreshCh)
	rc.voterSetRefreshCh = nil
	for _, rs := range rc.replicaMap {
		rs.close()
	}
}

type replicaState struct {
	parent *RangeControllerImpl
	// stream aggregates across the streams for the same (tenant, store). This
	// is the identity that is used to deduct tokens or wait for tokens to be
	// positive.
	stream            kvflowcontrol.Stream
	evalTokenCounter  TokenCounter
	sendTokenCounter  TokenCounter
	desc              roachpb.ReplicaDescriptor
	replicaSendStream *replicaSendStream
}

func NewReplicaState(parent *RangeControllerImpl, desc roachpb.ReplicaDescriptor) *replicaState {
	stream := kvflowcontrol.Stream{TenantID: parent.opts.TenantID, StoreID: desc.StoreID}
	rs := &replicaState{
		parent:            parent,
		stream:            stream,
		evalTokenCounter:  parent.opts.SSTokenCounter.EvalTokenCounterForStream(stream),
		sendTokenCounter:  parent.opts.SSTokenCounter.SendTokenCounterForStream(stream),
		desc:              desc,
		replicaSendStream: nil,
	}
	state, nextUB := parent.opts.RaftInterface.FollowerState(desc.ReplicaID)
	if state == tracker.StateReplicate {
		rs.createReplicaSendStream(nextUB)
	}
	return rs
}

func (rs *replicaState) createReplicaSendStream(nextUpperBound uint64) {
	isConnected := rs.parent.opts.RaftInterface.FollowerTransportConnected(rs.desc.StoreID)
	rss := newReplicaSendStream(rs, replicaSendStreamInitState{
		isConnected:   isConnected,
		indexToSend:   nextUpperBound,
		nextRaftIndex: rs.parent.opts.RaftInterface.HighestEntryIndex() + 1,
		// TODO: these need to be based on some history observed by RangeControllerImpl.
		approxMaxPriority:   admissionpb.NormalPri,
		approxMeanSizeBytes: 1000,
	})
	rs.replicaSendStream = rss
	if rs.parent.leaseholder == rs.desc.ReplicaID && !rss.isEmptySendQueue() {
		rss.scheduleForceFlush()
	}
	// TODO: need to do something here like changeToStateReplicate, in that we
	// should try to grab tokens, and then try to schedule on raftScheduler.
}

func (rs *replicaState) close() {
	if rs.replicaSendStream != nil {
		rs.replicaSendStream.close()
	}
}

// TODO: update.
//
// replicaSendStream is the state for replicas in StateReplicate. We need to maintain
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
// in that may never feed something to switch to hard-down. Circuit-breaker also exists!
//
// So keep feeding until hard-down. And remove from elastic wait when soft-down.
// If circuit-breaker does not trip too bad. When soft-down, those messages not
// subject to AC since not tracking them on the sender.
//
// **what is the channel situation in soft-down** Close and replace channel.
//
// TODO: replicaSendStream could also encompass StateSnapshot, when transitioned
// from StateReplicate to StateSnapshot. The problem is that indexToSend can
// will need to advance. We also want to eventually use the mediation of
// tracker on when to send snapshot. So overall we should encompass
// StateSnapshot.
//
// "breaker will open in 3-6 seconds after no more TCP packets are flowing. If
// we assume 6 seconds, then that is ~1600 messages/second."
// Why does soft-disconnected matter at all.
type replicaSendStream struct {
	parent *replicaState
	// TODO: synchronization.
	mu syncutil.Mutex

	connectedState connectedState

	// indexToSendInitial is the indexToSend when this transitioned to
	// StateReplicate. Only indices >= indexToSendInitial are tracked.
	indexToSendInitial uint64
	// Only the tracker works in terms of kvflowcontrolpb.RaftLogPosition, which
	// contains both the term and index, since it needs to be able to ignore
	// stale messages. The rest of the data-structures in replicaSendStream only
	// need to track the index and not the term.
	tracker Tracker

	// nextRaftIndexInitial is the value of nextRaftIndex when this transitioned
	// to StateReplicate.
	nextRaftIndexInitial uint64

	sendQueue struct {
		// State of send-queue. [indexToSend, nextRaftIndex) have not been sent.
		indexToSend   uint64
		nextRaftIndex uint64

		// Approximate stats for send-queue. For indices < nextRaftIndexInitial.
		approxMaxPriority   admissionpb.WorkPriority
		approxMeanSizeBytes kvflowcontrol.Tokens

		// Precise stats for send-queue. For indices >= nextRaftIndexInitial.
		priorityCount map[admissionpb.WorkPriority]int64
		// sizeSum is only for entries subject to AC.
		sizeSum kvflowcontrol.Tokens

		// watcherHandleID, deductedForScheduler, forceFlushScheduled are only
		// relevant when connectedState != snapshot, and the send-queue is
		// non-empty.
		//
		// If watcherHandleID != InvalidStoreStreamSendTokenHandleID, i.e., we have
		// registered a handle to watch for send tokens to become available. In this
		// case deductedForScheduler.tokens == 0 and !forceFlushScheduled.
		//
		// If watcherHandleID == InvalidStoreStreamSendTokenHandleID, we have
		// either deducted some tokens that we have not used, i.e.,
		// deductedForScheduler.tokens > 0, or forceFlushScheduled (i.e., we don't
		// need tokens). Both can be true, i.e. deductedForScheduler.tokens > 0
		// and forceFlushScheduled. In this case, we are waiting to be scheduled
		// in the raftScheduler to do the sending.
		watcherHandleID      StoreStreamSendTokenHandleID
		deductedForScheduler struct {
			pri    admissionpb.WorkPriority
			tokens kvflowcontrol.Tokens
		}
		// Only relevant when connectedState != snapshot.
		forceFlushScheduled bool
	}
	// Eval state.
	eval struct {
		// Only for indices >= nextRaftIndexInitial. These are either in the
		// send-queue, or in the tracker.
		tokensDeducted [admissionpb.NumWorkClasses]kvflowcontrol.Tokens
	}

	closed bool
}

// Initial state provided to constructor of replicaSendStream.
type replicaSendStreamInitState struct {
	isConnected bool
	// [indexToSend, nextRaftIndex) are known to the (local) leader and need to
	// be sent to this replica. This is the initial send-queue.
	//
	// INVARIANT: is-local-replica => indexToSend == nextRaftIndex, i.e., no
	// send-queue.
	indexToSend   uint64
	nextRaftIndex uint64

	// Approximate stats for the initial send-queue.
	approxMaxPriority   admissionpb.WorkPriority
	approxMeanSizeBytes kvflowcontrol.Tokens
}

func newReplicaSendStream(
	parent *replicaState, init replicaSendStreamInitState,
) *replicaSendStream {
	// Must be in StateReplicate on creation.
	connectedState := replicateConnected
	if !init.isConnected {
		connectedState = replicateSoftDisconnected
	}
	rss := &replicaSendStream{
		parent:               parent,
		connectedState:       connectedState,
		indexToSendInitial:   init.indexToSend,
		nextRaftIndexInitial: init.nextRaftIndex,
	}
	rss.tracker.Init(parent.stream)
	rss.sendQueue.indexToSend = init.indexToSend
	rss.sendQueue.nextRaftIndex = init.nextRaftIndex
	rss.sendQueue.approxMaxPriority = init.approxMaxPriority
	rss.sendQueue.approxMeanSizeBytes = init.approxMeanSizeBytes
	rss.sendQueue.watcherHandleID = InvalidStoreStreamSendTokenHandleID
	return rss
}

func (rss *replicaSendStream) close() {
	if rss.connectedState != snapshot {
		// Will cause all tokens to be returned etc.
		rss.changeToStateSnapshot()
	}
	rss.closed = true
}

// An entry is being sent that was never in the send-queue.
func (rss *replicaSendStream) advanceNextRaftIndexAndSent(state entryFlowControlState) {
	if rss.connectedState == snapshot {
		panic("")
	}
	if state.pos.Index != rss.sendQueue.indexToSend {
		panic("")
	}
	if state.pos.Index != rss.sendQueue.nextRaftIndex {
		panic("")
	}
	rss.sendQueue.indexToSend++
	rss.sendQueue.nextRaftIndex++
	if !state.usesFlowControl {
		return
	}
	rss.tracker.Track(context.TODO(), state.priority, admissionpb.WorkClassFromPri(state.priority),
		state.tokens, state.pos)
	rss.parent.evalTokenCounter.Deduct(
		context.TODO(), admissionpb.WorkClassFromPri(state.priority), state.tokens)
}

func (rss *replicaSendStream) scheduleForceFlush() {
	if rss.sendQueue.watcherHandleID != InvalidStoreStreamSendTokenHandleID {
		rss.parent.parent.opts.SendTokensWatcher.CancelHandle(rss.sendQueue.watcherHandleID)
		rss.sendQueue.watcherHandleID = InvalidStoreStreamSendTokenHandleID
	}
	rss.sendQueue.forceFlushScheduled = true
	rss.parent.parent.scheduleReplica(rss.parent.desc.ReplicaID)
}

func (rss *replicaSendStream) scheduled() (scheduleAgain bool) {
	// 5MB.
	const MaxBytesToSend kvflowcontrol.Tokens = 5 << 20
	bytesToSend := MaxBytesToSend
	if !rss.sendQueue.forceFlushScheduled {
		bytesToSend = rss.sendQueue.deductedForScheduler.tokens
	}
	if bytesToSend == 0 {
		return false
	}
	msg, err := rss.parent.parent.opts.RaftInterface.MakeMsgApp(
		rss.parent.desc.ReplicaID, rss.sendQueue.indexToSend, rss.sendQueue.nextRaftIndex,
		int64(bytesToSend))
	if err != nil {
		if !errors.Is(err, raft.ErrCompacted) {
			panic(err)
		}
		rss.changeToStateSnapshot()
		return
	}
	rss.dequeueFromQueueAndSend(msg)
	isEmpty := rss.isEmptySendQueue()
	if isEmpty {
		if rss.sendQueue.forceFlushScheduled {
			rss.sendQueue.forceFlushScheduled = false
		}
		rss.returnDeductedFromSchedulerTokens()
		return false
	}
	// INVARIANT: !isEmpty.
	if rss.sendQueue.forceFlushScheduled || rss.sendQueue.deductedForScheduler.tokens > 0 {
		return true
	} else {
		pri := rss.queuePriority()
		rss.sendQueue.watcherHandleID = rss.parent.parent.opts.SendTokensWatcher.NotifyWhenAvailable(
			rss.parent.sendTokenCounter, admissionpb.WorkClassFromPri(pri), rss)
		return false
	}
}

func (rss *replicaSendStream) dequeueFromQueueAndSend(msg raftpb.Message) {
	remainingTokens := rss.sendQueue.deductedForScheduler.tokens
	priAlreadyDeducted := kvserverpb.PriorityNotOverriddenForFlowControl
	noRemainingTokens := false
	if remainingTokens > 0 {
		priAlreadyDeducted = rss.sendQueue.deductedForScheduler.pri
	} else {
		noRemainingTokens = true
	}
	wcAlreadyDeducted := admissionpb.WorkClassFromPri(priAlreadyDeducted)
	// It is possible that the Notify raced with an enqueue (or scheduled with
	// an enqueue) and the send-queue has some regular work now, and the
	// priAlreadyDeducted was of a lower priority corresponding to elastic work.
	// We just consume these tokens and apply the override corresponding to
	// priAlreadyDeducted. It is considered harmless for regular work to consume
	// elastic tokens. If that delays their logical admission, it will not harm
	// later arriving regular work, that will use regular tokens.

	for _, entry := range msg.Entries {
		if rss.sendQueue.indexToSend != entry.Index {
			panic("")
		}
		rss.sendQueue.indexToSend++
		entryFCState := getFlowControlState(entry)
		if !entryFCState.usesFlowControl {
			continue
		}
		rss.sendQueue.sizeSum -= entryFCState.tokens
		rss.sendQueue.priorityCount[entryFCState.priority]--
		wc := wcAlreadyDeducted
		if priAlreadyDeducted == kvserverpb.PriorityNotOverriddenForFlowControl {
			wc = admissionpb.WorkClassFromPri(entryFCState.priority)
		}
		if noRemainingTokens {
			rss.parent.sendTokenCounter.Deduct(context.TODO(), wc, entryFCState.tokens)
		} else {
			remainingTokens -= entryFCState.tokens
			if remainingTokens <= 0 {
				noRemainingTokens = true
				rss.parent.sendTokenCounter.Deduct(context.TODO(), wc, -remainingTokens)
				remainingTokens = 0
			}
		}
		rss.tracker.Track(context.TODO(), entryFCState.priority, wc, entryFCState.tokens,
			kvflowcontrolpb.RaftLogPosition{Term: entry.Term, Index: entry.Index})
	}
	rss.parent.parent.opts.MessageSender.SendRaftMessage(context.TODO(), priAlreadyDeducted, msg)
	rss.sendQueue.deductedForScheduler.tokens = remainingTokens
	if remainingTokens == 0 {
		rss.sendQueue.deductedForScheduler.pri = kvserverpb.PriorityNotOverriddenForFlowControl
	}
}

func (rss *replicaSendStream) advanceNextRaftIndexAndQueued(entry entryFlowControlState) {
	if entry.pos.Index != rss.sendQueue.nextRaftIndex {
		panic("")
	}
	wasEmpty := rss.isEmptySendQueue()
	rss.sendQueue.nextRaftIndex++
	if entry.usesFlowControl {
		rss.sendQueue.sizeSum += entry.tokens
		var priority admissionpb.WorkPriority
		if rss.sendQueue.watcherHandleID != InvalidStoreStreamSendTokenHandleID {
			// May need to update it.
			priority = rss.queuePriority()
		}
		rss.sendQueue.priorityCount[entry.priority]++
		if rss.connectedState == snapshot {
			// Do not deduct eval-tokens in StateSnapshot, since there is no
			// guarantee these will be returned.
			return
		}
		wcChanged := false
		entryWC := admissionpb.WorkClassFromPri(entry.priority)
		if rss.sendQueue.watcherHandleID != InvalidStoreStreamSendTokenHandleID &&
			entry.priority > priority {
			existingWC := admissionpb.WorkClassFromPri(priority)
			if existingWC != entryWC {
				wcChanged = true
			}
		}
		rss.eval.tokensDeducted[entryWC] += entry.tokens
		rss.parent.evalTokenCounter.Deduct(context.TODO(), entryWC, entry.tokens)
		if wasEmpty {
			// Register notification.
			rss.sendQueue.watcherHandleID = rss.parent.parent.opts.SendTokensWatcher.NotifyWhenAvailable(
				rss.parent.sendTokenCounter, entryWC, rss)
		} else if wcChanged {
			// Update notification
			rss.parent.parent.opts.SendTokensWatcher.UpdateHandle(rss.sendQueue.watcherHandleID, entryWC)
		}
	}
}

// Notify implements TokenAvailableNotification.
func (rss *replicaSendStream) Notify() {
	// TODO: concurrency. raftMu is not held, and not being called from raftScheduler.
	if rss.closed || rss.connectedState == snapshot {
		// Must have canceled the handle and the cancellation raced with the
		// notification.
		return
	}
	pri := rss.queuePriority()
	wc := admissionpb.WorkClassFromPri(pri)
	queueSize := rss.queueSize()
	tokens := rss.parent.sendTokenCounter.TryDeduct(context.TODO(), wc, queueSize)
	if tokens > 0 {
		rss.parent.parent.opts.SendTokensWatcher.CancelHandle(rss.sendQueue.watcherHandleID)
		rss.sendQueue.watcherHandleID = InvalidStoreStreamSendTokenHandleID
		rss.sendQueue.deductedForScheduler.pri = pri
		rss.sendQueue.deductedForScheduler.tokens = tokens
		rss.parent.parent.scheduleReplica(rss.parent.desc.ReplicaID)
	}
}

func (rss *replicaSendStream) isEmptySendQueue() bool {
	return rss.sendQueue.indexToSend == rss.sendQueue.nextRaftIndex
}

// REQUIRES: send-queue is not empty.
func (rss *replicaSendStream) queuePriority() admissionpb.WorkPriority {
	initialized := false
	var maxPri admissionpb.WorkPriority
	if rss.sendQueue.indexToSend < rss.nextRaftIndexInitial {
		maxPri = rss.sendQueue.approxMaxPriority
		initialized = true
	}
	for pri, count := range rss.sendQueue.priorityCount {
		if count > 0 && (!initialized || pri > maxPri) {
			maxPri = pri
		}
	}
	return maxPri
}

// REQUIRES: send-queue is not empty.
func (rss *replicaSendStream) queueSize() kvflowcontrol.Tokens {
	var size kvflowcontrol.Tokens
	countWithApproxStats := rss.nextRaftIndexInitial - rss.sendQueue.indexToSend
	if countWithApproxStats > 0 {
		size = kvflowcontrol.Tokens(countWithApproxStats) * rss.sendQueue.approxMeanSizeBytes
	}
	size += rss.sendQueue.sizeSum
	return size
}

func (rss *replicaSendStream) changeConnectedStateInStateReplicate(isConnected bool) {
	if isConnected {
		if rss.connectedState != replicateSoftDisconnected {
			panic("")
		}
		rss.connectedState = replicateConnected
	}
	if !isConnected {
		if rss.connectedState != replicateConnected {
			panic("")
		}
		rss.connectedState = replicateSoftDisconnected
	}
}

func (rss *replicaSendStream) changeToStateSnapshot() {
	rss.connectedState = snapshot
	// The tracker must only contain entries in < rss.sendQueue.indexToSend.
	// These may not have been received by the replica and will not get resent
	// by Raft, so we have no guarantee those tokens will be returned. So return
	// all tokens in the tracker.
	rss.tracker.UntrackAll(context.TODO(), func(
		pri admissionpb.WorkPriority, sendTokenWC admissionpb.WorkClass, tokens kvflowcontrol.Tokens) {
		rss.parent.sendTokenCounter.Return(context.TODO(), sendTokenWC, tokens)
	})
	// For the same reason, return all eval tokens deducted.
	for wc := range rss.eval.tokensDeducted {
		if rss.eval.tokensDeducted[wc] > 0 {
			rss.parent.evalTokenCounter.Return(context.TODO(), admissionpb.WorkClass(wc), rss.eval.tokensDeducted[wc])
			rss.eval.tokensDeducted[wc] = 0
		}
	}
	if rss.sendQueue.watcherHandleID != InvalidStoreStreamSendTokenHandleID {
		rss.parent.parent.opts.SendTokensWatcher.CancelHandle(rss.sendQueue.watcherHandleID)
		rss.sendQueue.watcherHandleID = InvalidStoreStreamSendTokenHandleID
	}
	rss.returnDeductedFromSchedulerTokens()
	rss.sendQueue.forceFlushScheduled = false
}

func (rss *replicaSendStream) returnDeductedFromSchedulerTokens() {
	if rss.sendQueue.deductedForScheduler.tokens > 0 {
		wc := admissionpb.WorkClassFromPri(rss.sendQueue.deductedForScheduler.pri)
		rss.parent.sendTokenCounter.Return(context.TODO(), wc, rss.sendQueue.deductedForScheduler.tokens)
		rss.sendQueue.deductedForScheduler.tokens = 0
	}
}

func (rss *replicaSendStream) changeToStateReplicate(isConnected bool, indexToSend uint64) {
	if rss.sendQueue.nextRaftIndex < indexToSend {
		panic("")
	}
	if rss.sendQueue.indexToSend > indexToSend {
		panic("")
	}
	if rss.connectedState != snapshot {
		panic("")
	}
	state := replicateConnected
	if !isConnected {
		state = replicateSoftDisconnected
	}
	rss.connectedState = state
	// INVARIANT: rss.sendQueue.indexToSend <= indexToSend <=
	// rss.sendQueue.nextRaftIndex. Typically, both will be <, since we
	// transitioned to StateSnapshot since rss.sendQueue.indexToSend was
	// truncated, and there have likely been some proposed entries since the
	// snapshot was applied. So we will start off with some entries in the
	// send-queue.

	// NB: the tracker entries have already been returned in
	// changeToStateSnapshot. And so have the eval tokens. We have partially or
	// fully emptied the send-queue and we don't want to iterate over the
	// remaining members to precisely figure out what to deduct from
	// eval-tokens, since that may require reading from storage.

	rss.indexToSendInitial = indexToSend
	rss.sendQueue.indexToSend = indexToSend
	totalCount := int64(0)
	var maxPri admissionpb.WorkPriority
	for pri, count := range rss.sendQueue.priorityCount {
		if count > 0 {
			if totalCount == 0 || pri > maxPri {
				maxPri = pri
			}
			totalCount += count
			rss.sendQueue.priorityCount[pri] = 0
		}
	}
	meanSizeBytes := kvflowcontrol.Tokens(0)
	if totalCount > 0 {
		meanSizeBytes = rss.sendQueue.sizeSum / kvflowcontrol.Tokens(totalCount)
	}
	if rss.nextRaftIndexInitial > rss.sendQueue.indexToSend {
		// The approx stats are still relevant.
		if rss.sendQueue.approxMaxPriority > maxPri {
			maxPri = rss.sendQueue.approxMaxPriority
		}
		if totalCount == 0 {
			meanSizeBytes = rss.sendQueue.approxMeanSizeBytes
		} else {
			meanSizeBytes = kvflowcontrol.Tokens(0.9*float64(meanSizeBytes) + 0.1*float64(rss.sendQueue.approxMeanSizeBytes))
		}
	}
	rss.sendQueue.approxMaxPriority = maxPri
	rss.sendQueue.approxMeanSizeBytes = meanSizeBytes
	rss.sendQueue.sizeSum = 0
	rss.nextRaftIndexInitial = rss.sendQueue.nextRaftIndex
	if !rss.isEmptySendQueue() {
		rss.Notify()
		if rss.sendQueue.deductedForScheduler.tokens == 0 {
			// Weren't able to deduct any tokens.
			rss.parent.parent.opts.SendTokensWatcher.NotifyWhenAvailable(
				rss.parent.sendTokenCounter, admissionpb.WorkClassFromPri(maxPri), rss)
		} else {
			scheduleAgain := rss.scheduled()
			if scheduleAgain {
				rss.parent.parent.scheduleReplica(rss.parent.desc.ReplicaID)
			} else {
				rss.parent.parent.opts.SendTokensWatcher.NotifyWhenAvailable(
					rss.parent.sendTokenCounter, admissionpb.WorkClassFromPri(maxPri), rss)
			}
		}
	}
}

// Message is received to return flow tokens for pri, for all positions <= upto.
// Return send-tokens and eval-tokens.
func (rss *replicaSendStream) flowTokensReturn(
	pri admissionpb.WorkPriority, upto kvflowcontrolpb.RaftLogPosition,
) kvflowcontrol.Tokens {
	wc := admissionpb.WorkClassFromPri(pri)
	rss.tracker.Untrack(context.TODO(), pri, upto,
		func(index uint64, sendTokenWC admissionpb.WorkClass, tokens kvflowcontrol.Tokens) {
			if index >= rss.nextRaftIndexInitial {
				rss.eval.tokensDeducted[wc] -= tokens
				rss.parent.evalTokenCounter.Return(context.TODO(), wc, tokens)
			}
			rss.parent.sendTokenCounter.Return(context.TODO(), sendTokenWC, tokens)
		})
	return 0
}

type connectedState uint32

// Additional connectivity state in StateReplicate.
//
// Local replicas are always in state connected.
//
// Initial state for a replicaSendStream is either connected or
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
// node is down. The transition to StateProbe will happen anyway.
//
// We call this state softDisconnected since MsgApps are still being generated.
//
// Initial states: {connected, softDisconnected}
// State transitions:
//
//	connected => softDisconnected
//	softDisconnected => connected
//	* => replicaSendStream closed
//
// Why do we return tokens early?
const (
	replicateConnected connectedState = iota
	replicateSoftDisconnected
	snapshot
)

func (cs connectedState) isStateReplicate() bool {
	return cs == replicateConnected || cs == replicateSoftDisconnected
}
