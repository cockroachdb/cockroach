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
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
Additionally, it will contain Admitted [numFlowPri]uint64. This is the latest
index up to which admission has happened. That is, this is not incremental
state, and just the latest state (which makes it easy to tolerate lossiness). It
will be used to advance admitted at the leader.

[optimization]: MsgApp from the leader contains the latest admitted[i] values it
has, so that the follower doesn't bother with a MsgAppResp if the states are the
same at the leader and the follower.

Raft state at all replicas:

Existing state includes stableIndex, lastEntryIndex (I am not using the right
Raft package terms, and there could be other mistakes here)

- stableIndex is advanced based on MsgStorageAppendResp
- lastEntryIndex is advanced in RawNode.Step. This happens before Ready handling in kvserver.

- for async storage writes (which we enable in Raft): the MsgStorageAppend
  includes Responses to deliver when the write is done. These responses include
  both MsgStorageAppendResp for the local node (this is delivered when the
  raftScheduler processes this range again), and
  MsgAppResp for the leader, when the node is a follower (which is sent immediately).
  - This early creation of MsgAppResp, before the entries to append are
    persisted, is reasonable since that persistence is atomic for all the
    entries in MsgStorageAppend. In comparison, admission of the various entries
    in MsgStorageAppend is not atomic, which justifies the different behavior
    sketched below.

New raft state at all replicas regarding what they have admitted: admitted [numFlowPri]uint64
Invariant: for all i: admitted[i] <= stableIndex.

Raft constructs MsgAppResp for (a) MsgApp pings, (b) for use when
MsgStorageAppend has persisted. These will piggyback the latest admitted
values known to it.

Additionally, RaftInterface.AdvanceAdmitted(admitted [numFlowPri]uint64) will
be called by kvserver in handleRaftReadyRaftMuLocked. And if advanced, this
method will return a MsgAppResp to be sent.

Life of admission:
- (kvserver) Leader proposes by calling RawNode.Step with MsgProp.
- (kvserver) Leader Ready processing:
  - Raft produces MsgStorageAppend for leader. This includes MsgStorageAppendResp
    to send to the leader when the append is done.
  - MsgApp for followers, constructed by RAC once send tokens are available:
    - RAC deducts flow tokens corresponding to MsgApp it asks for by
      calling RaftInterface.MakeMsgApp. This also advances Next.
    - Raft constructs its own pinging MsgApps
    Both kinds include Admitted, representing the leader's current knowledge.

- MsgApp processing at follower:
  - (kvserver) calls RawNode.Step with the MsgApps. lastEntryIndex is advanced.
  - (kvserver) handleRaftReadyRaftMuLocked. Ready may contain:
     - MsgAppResp: this will be sent.
     - [MSA] MsgStorageAppend: Processing:
       - async storage writes: The MsgStorageAppend includes Responses which
         contain both MsgStorageAppendResp for the local node (this is delivered
         after the write completes and when the raftScheduler processes this
         range again), and MsgAppResp which are sent when the write completes,
         without waiting for the raftScheduler (to minimize commit latency).
       - calls AdmitRaftEntry for the entries in raftpb.MsgStorageAppend. In
         addition to queueing in the StoreWorkQueue, this will track (on the
         Replica): notAdmitted [numFlowPri][]uint64, where the []uint64 is in
         increasing entry index order. We don't bother with the term in the
         tracking (unlike the RaftLogPosition struct in the current code). These
         slices are appended to here, based on the entries in MsgStorageAppend.

- (kvserver) Admission happens on follower:
  - [AH1] The structs corresponding to the admitted entries are queued in the
    Replica akin to how we queue Replica.localMsgs and the raftScheduler will be
    told to do ready processing (there may not be a Raft ready, but this is akin
    to how handleRaftReadyRaftMuLocked does
    deliverLocalRaftMsgsRaftMuLockedReplicaMuLocked).
  - [AH2] In handleRaftReadyRaftMuLocked, this queue is used to remove things from
    notAdmitted. Additionally RaftInterface is queried for stableIndex. If a
    notAdmitted[i] is empty, admitted[i] = stableIndex, else admitted[i] =
    notAdmitted[i][0]-1. RaftInterface.AdvanceAdmitted is called, which may
    return a MsgAppResp.
  - MsgAppResp is sent.

MSA is also the behavior at the leader (which is just another replica that
must also advance Admitted for itself), which happens synchronously in the
handleRaftReadyRaftMuLocked which handles the MsgStorageAppend.

AH1 and AH2 can happen synchronously when the entry is immediately admitted
during the handleRaftReadyRaftMuLocked that called AdmitRaftEntry. These
entries are not stable yet, so we won't be able to advance admitted, but
when MsgStorageAppendResp is processed, that will be advanced.

Optimization:

I don't think we can afford to construct a RaftMessageRequest containing a
single MsgAppResp just to advance admitted. A RaftMessageRequest includes
ReplicaDescriptors etc. that make it heavyweight.

RACv1 optimizes this by having
kvflowcontrolpb.AdmittedRaftLogEntries(range-id, pri, upto-raft-log-position,
store-id) and piggy-backing these on any RaftMessageRequest being sent to the
relevant node. It didn't matter that the RaftMessageRequest was for a different
range.

We will do a similar piggy-backing:

- Say m is the raftpb.Message corresponding to the MsgAppResp that was
  returned from RaftInterface.AdvanceAdmitted
- Wrap it with the RangeID, and enqueue it for the leader's nodeid in the
  RaftTransport (this will actually be much simpler than the
  current RaftTransport.kvflowControl plumbing).
- Every RaftMessageRequest for the node will piggy-back these.

Priority inheritance:

See https://docs.google.com/document/d/1Qf6uteFRlbScLdWIrTfqgbrKRHfUsoMGatRLWYQgAi8/edit#bookmark=id.69inkdboirw0
for the high-level design.

Since the admitted[i] value for every priority advances up to stableIndex
based on the absence of entries with that priority waiting for admission
(unlike RACv1 advancement), there is no risk that a difference in belief in
the priority of index j at the leader and the follower will result in
permanent leakage of tokens corresponding to index j. Eventually admitted[i]
for all priorities i will advance to j.

We observe that the probability of priority mismatch is very low. The leader
when sending index j must be sending it for the first time since the
transition of this follower to StateReplicate, and given the ordered gRPC
stream used for communication, the likelihood that an old MsgApp containing
index j is received after whatever was needed to transition into
StateReplicate is very unlikely:

- if the follower was in StateProbe and transitioned to StateReplicate with a
  next index k < j, then k must have been lost, and the follower will discard
  everything until it sees index k again. This means any old j that was not lost
  will also be discarded.

- if the follower was in StateSnapshot and transitioned to StateReplicate with
  a next index k < j, then j must have never been sent earlier, since there was
  an index < k that had been truncated from the log.

Say such mismatch does happen. We consider two cases, via an example,
involving raft entry at index 3.

Case A: leader sent as low pri (elastic) and follower sees high pri (regular)

We will represent this as (3, L) at leader and (3, H) at follower.

Say the follower saw (2, L) and has not admitted it yet. But it admits (3, H).
It will have Admitted[H]=4, Admitted[L]=1. So the elastic tokens for 3 will
not be returned to the leader even though 3 has been admitted. This is
considered ok since the overload is severe enough to not yet admit (2, L), so
better not to return elastic tokens for (3, L).

Say the follower saw (4, L) and has admitted it but not yet admitted (3, H).
This could cause Admitted[H]=2, Admitted[L]=4, causing elastic tokens to be
returned for 3 before it has been admitted. This can't actually happen since
for the same raft group, (3, H) will be admitted before (4, L).

Case B: leader sent as high pri (regular) and follower sees low pri (elastic)

Say the follower admitted (2, H). So Admitted[H] >= 3. Even though (3, L) is
still waiting to be admitted, the regular tokens for 3 at the leader will be
returned. This is desirable since it is possible (3, H) would also have been
admitted, and it is better to be optimistic for regular work since it
corresponds to user facing work.

*/

// TODO:
//
// - force-flush when there isn't quorum with no send-queue but there is a
//   quorum with replicaSendStream in StateReplicate.
//
// - delay force-flush slightly (say 1s) since there could be a transient
//   transition to StateProbe and back to StateReplicate due to a single
//   MsgApp drop. Related to this, should we delay the closing of the
//   replicaSendStream since it has all the interesting stats? It also has
//   tokens that we don't really need to return due to a transient transition
//   to StateProbe and back. A regression in indexToSend necessarily means one
//   or more MsgApps were dropped that were sent during the latest
//   StateReplicate. Only the send tokens for these MsgApps that will be
//   resent need to be returned. We have already implemented
//   probeRecentlyReplicate, but not the transition into and out of
//   probeRecentlyReplicate.
//
// - for the first C bytes of force-flush, deduct flow tokens. So these will
//   need to be in a separate MsgApp. Don't do any priority inheritance for
//   these. This is motivated in
//   https://docs.google.com/document/d/1Qf6uteFRlbScLdWIrTfqgbrKRHfUsoMGatRLWYQgAi8/edit#bookmark=id.wcwgvtka9qr0
//   We will make C configurable, eventually.

// TODO(sumeer): non-voters; voter <=> non-voter state transition
//
// (not essential to fix this for the prototype, where we can just run with
// voting replicas)
//
// For a non-voter, we never wait for eval-tokens. But the code deducts
// eval-tokens for a non-voter too. Consider a store s2 with a voter follower
// replica for range R1 and a non-voter follower replica for range R2, and
// both have a leader at the same other node. R2 can keep evaluating even if
// the send-queue to s2 are back logged. R1 and R2 are fairly competing for
// the send-queues, but if s2 is needed for the quorum of R1, that range will
// not be able to evaluate since R2 keeps consuming all the eval-tokens. Note
// that this "unfairness" is within the same WorkClass (regular or elastic).
// Say we didn't deduct eval-tokens for R2. R2 can still keep evaluating. R1
// will evaluate if eval-tokens are available, which only it is consuming. If
// R1 and R2 compete equally for send-tokens, R1 will be able to get half the
// bandwidth.
//
// We know the latest state of a replica, in terms of whether it is a voter or
// a non-voter. We can maintain replicaSendStream.eval.tokensDeducted to be
// zero when a non-voter.

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
// not trying to do inter-range isolation.
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
	HandleRaftEvent(ctx context.Context, e RaftEvent) error
	// HandleControllerSchedulerEvent ia called from the raftScheduler when an
	// event the controller scheduled can be processed.
	HandleControllerSchedulerEvent(ctx context.Context) error
	// SetReplicas is called from setDescLockedRaftMuLocked.
	// Must not be called during Ready processing.
	//
	// NB: a new follower here may already be in StateReplicate and have a
	// send-queue.
	SetReplicas(ctx context.Context, replicas ReplicaSet) error
	// SetLeaseholder is called from leasePostApplyLocked.
	// TODO: I suspect raftMu is held here too.
	SetLeaseholder(ctx context.Context, replica roachpb.ReplicaID)
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
	//
	// TODO: remove this after "Design for robust flow token return in RACv2"
	// discussed above is finalized. All the complicated transport handling
	// will go away.
	// TransportDisconnected(replica roachpb.ReplicaID)

	// Close the controller, since no longer the leader. Called from
	// handleRaftReadyRaftMuLocked. It can be called concurrently with
	// WaitForEval. WaitForEval should unblock and return without an error,
	// since there is no need to wait for tokens at this replica (which may
	// still be the leaseholder).
	Close(ctx context.Context)
	// String returns a string representation of the RangeController.
	String() string
}

// RaftEvent is an abstraction around raft.Ready, constructed in
// handleRaftReadyRaftMuLocked.
//
// TODO: this is unnecessarily abstracted. Make this a struct with an Entries
// slice.
type RaftEvent interface {
	// Ready can return nil if there is no Ready.
	Ready() Ready
}

// Ready interface is an abstraction around raft.Ready, for the pull mode in
// which replication AC will operate. In this pull mode, the Raft leader will
// produce a Ready that has the new entries it knows about, but MsgApps will
// only be produced for a follower for (a) entries in (Match, Next), i.e.,
// entries that are already in-flight for that follower, that Raft may want to
// resend (b) empty MsgApps to ping the follower ((a) doesn't happen in our
// Raft implementation, but it is permitted).
//
// Next is typically only advanced via pull, which is serviced via
// RaftInterface. Next never regresses in StateReplicate, and advances
// whenever RaftInterface.MakeMsgApp is called. And everything in (Match,
// Next) is the responsibility of Raft to maintain liveness for (via MsgApp
// pings). Next can regress if the replica transitions out of StateReplicate,
// and back into StateReplicate. Also, in the rare case, Next can advance
// during StateReplicate without a pull, if there were old MsgApps forgotten
// about due to a transition to StateProbe and back to StateReplicate, and we
// receive acks for those e.g. if (Match, Next) was (5, 10), it is possible to
// receive acks up to 11, and so the state becomes (11, 12).
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
	// GetEntries represents the new entries that are being added to the log.
	// This may be empty.
	//
	// The second byte of AC entries will encode the priority, so we will have
	// the priority, size, and index position, which is all that we need. Unlike
	// v1, where we are doing the accounting in
	// https://github.com/cockroachdb/cockroach/blob/f601b7b439ced71030bfdb0d9ba9cb4925420569/pkg/kv/kvserver/replica_proposal_buf.go#L1057-L1066
	// which is messy.
	GetEntries() []raftpb.Entry
	// TODO: remove the following entirely. RACv2 is completely uninterested in
	// these messages.

	// RetransmitMsgApps returns the MsgApps that are being retransmitted, or
	// being used to ping the follower. These will never be queued by
	// replication AC, and will simply be sent.
	// RetransmitMsgApps() []raftpb.Message
}

// RaftInterface abstracts what the RangeController needs from the raft
// package, when running at the leader.
//
// NB: group membership is communicated to the RangeController via a separate
// path (SetReplicas) that kvserver already ensures is synchronized with the
// internal state of RaftInterface. The methods in RaftInterface are the ones
// RangeController will call.
//
// The implementation can assume Replica.raftMu is already held. It should not
// need to acquire Replica.mu, since there is no promise made on whether it is
// already held or not.
type RaftInterface interface {
	// FollowerState returns the current state of a follower. The value of
	// Match, Next, Admitted are populated iff in StateReplicate. All entries >=
	// Next have not had MsgApps constructed during the lifetime of this
	// StateReplicate (they may have been constructed previously).
	//
	// When a follower transitions from {StateProbe,StateSnapshot} =>
	// StateReplicate, we start trying to send MsgApps. We should
	// notice such transitions both in HandleRaftEvent and SetReplicas.
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
	FollowerState(replicaID roachpb.ReplicaID) FollowerStateInfo
	// LastEntryIndex is the highest index assigned in the log.
	// Only for debugging.
	LastEntryIndex() uint64
	// NextUnstableIndex returns the index of the next entry that will be sent to
	// local storage, if there are any. All entries < this index are either stored,
	// or have been sent to storage.
	NextUnstableIndex() uint64
	// MakeMsgApp is used to construct a MsgApp for entries in [start, end).
	// REQUIRES: start == FollowerStateInfo.Next and replicaID is in
	// StateReplicate.
	//
	// REQUIRES: maxSize > 0.
	//
	// If the sum of all entries in [start,end) are <= maxSize, all will be
	// returned. Else, entries will be returned until, and including, the first
	// entry that causes maxSize to be equaled or exceeded. This implies at
	// least one entry will be returned in the MsgApp on success.
	//
	// Returns raft.ErrCompacted error if log truncated. If no error, there is
	// at least one entry in the message, and next is advanced to be equal to
	// the index+1 of the last entry in the returned message. If
	// raft.ErrCompacted is returned, the replica will transition to
	// StateSnapshot.
	//
	// TODO: add back maxEntries.
	//	- [pav-kv]: Why? maxEntries == end-start, no need in an extra parameter.
	//
	// TODO: the transition to StateSnapshot is not guaranteed, there are some
	// error conditions after which the flow stays in StateReplicate. We should
	// define or eliminate these cases.
	MakeMsgApp(replicaID roachpb.ReplicaID, start, end uint64, maxSize int64) (raftpb.Message, error)
}

// Scheduler abstracts the raftScheduler to allow the RangeController to
// schedule its own internal processing. This internal processing is to pop
// some entries from the send queue and send them in a MsgApp.
type Scheduler interface {
	ScheduleControllerEvent(rangeID roachpb.RangeID)
}

type FollowerStateInfo struct {
	State tracker.StateType

	// Remaining only populated in StateReplicate.
	// (Match, Next) is in-flight.
	Match uint64
	Next  uint64
	// Invariant: Admitted[i] <= Match.
	Admitted [kvflowcontrolpb.NumRaftPriorities]uint64
}

func (f FollowerStateInfo) String() string {
	var buf strings.Builder
	// Avoid cluttering the message, only include the state if it is not
	// StateReplicate.
	if f.State != tracker.StateReplicate {
		fmt.Fprintf(&buf, "%s ", f.State)
	}
	fmt.Fprintf(&buf, "match=%d next=%d admitted=[", f.Match, f.Next)
	i := 0
	for pri, a := range f.Admitted {
		if a == 0 {
			continue
		}
		if i > 0 {
			fmt.Fprintf(&buf, ",")
		}
		fmt.Fprintf(&buf, "%v=%d", kvflowcontrolpb.RaftPriority(pri), a)
		i++
	}
	buf.WriteString("]")
	return buf.String()
}

// RaftAdmittedInterface is used to interact with Raft on all replicas, for
// the purposes of advancing Admitted for that replica. These methods will
// only be called for replicas that are deemed initialized (in kvserver
// terms).
//
// See the "Design for robust flow token return in RACv2" earlier in this
// file for details.
type RaftAdmittedInterface interface {
	// StableIndex is the index up to which the raft log is stable. The
	// Admitted values must be <= this index. It advances when Raft sees
	// MsgStorageAppendResp.
	StableIndex() uint64
	// GetAdmitted returns the admitted values known to Raft. Except for
	// snapshot application, this value will only advance via the caller
	// calling SetAdmitted. When a snapshot is applied, the snapshot index
	// becomes the value of admitted for all priorities.
	//
	// TODO: Get/SetAdmitted can be moved out of this interface, since all
	// admittance happens outside raft. Alternatively, move more things to raft
	// because all the tracking logic for indices duplicates the logic that async
	// log appends do for deciding when things become durable.
	//
	// TODO: admitted indices should be paired with the Term of the leader whose
	// log the indices pertain to. Between leadership terms, it can be truncated,
	// and an entry at the same index can change. It is easy to make a bug if
	// there is no explicit coupling with the leader term, because a term change
	// can slip through in between Get/SetAdmitted calls, by virtue of any Step().
	GetAdmitted() [kvflowcontrolpb.NumRaftPriorities]uint64
	// SetAdmitted sets the new value of admitted.
	// REQUIRES:
	//  The admitted[i] values in the parameter are >= the corresponding
	//  values returned by GetAdmitted.
	//
	//  admitted[i] <= stableIndex.
	//
	// Returns a MsgAppResp that contains these latest admitted values.
	SetAdmitted(admitted [kvflowcontrolpb.NumRaftPriorities]uint64) raftpb.Message
}

// MessageSender abstracts Replica.sendRaftMessage. The context used is always
// Replica.raftCtx, so we do not need to pass it.
//
// REQUIRES: msg is a MsgApp.
type MessageSender interface {
	// SendRaftMessage ...
	//
	// priorityInherited can be PriorityNotInheritedForFlowControl or
	// NotSubjectToACForFlowControl
	//
	// Implementation:
	//
	// On the receiver Replica.stepRaftGroup is called with
	// kvserverpb.RaftMessageRequest. And we do the AdmitRaftEntry call in
	// handleRaftReadyRaftMuLocked. By then we have no access to the wrapper
	// that is RaftMessageRequest. So before calling RawNode.Step we will pass
	// this information via a side-channel to the Replica. See the integration
	// code for more discussion.
	//
	// Due to multiple transitions into and out of StateReplicate, the same
	// entry could be sent to the follower with different inherited
	// priorities. Only the last send is being tracked in the tracker, with
	// the inherited priority used in that send. The follower will track based
	// on the inherited priority in the entry that it appended. These can
	// differ, but it does not matter. See discussion about priority
	// inheritance in "Design for robust flow token return in RACv2".
	SendRaftMessage(
		ctx context.Context, priorityInherited kvflowcontrolpb.RaftPriority, msg raftpb.Message)
}

type RangeControllerOptions struct {
	RangeID  roachpb.RangeID
	TenantID roachpb.TenantID
	// LocalReplicaID is the ReplicaID of the local replica, which is the
	// leader.
	LocalReplicaID roachpb.ReplicaID

	// SSTokenCounter provides access to all the TokenCounters that will be
	// needed (keyed by (tenantID, storeID)).
	SSTokenCounter StoreStreamsTokenCounter
	// SendTokensWatcher is for watching for send-token availability for any
	// TokenCounter.
	SendTokensWatcher StoreStreamSendTokensWatcher
	RaftInterface     RaftInterface
	// MessageSender is for sending MsgApps mediated by RACv2.
	MessageSender MessageSender
	// Scheduler is for scheduling popping from the send-queue.
	Scheduler Scheduler
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

func (rs ReplicaSet) String() string {
	var buf strings.Builder
	i := 0
	buf.WriteString("[")
	for _, desc := range rs {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "%v", desc)
		i++
	}
	buf.WriteString("]")
	return buf.String()
}

type RangeControllerImpl struct {
	opts       RangeControllerOptions
	replicaSet ReplicaSet
	// leaseholder can be NoReplicaID or not be in ReplicaSet, i.e., it is
	// eventually consistent with the set of replicas.
	leaseholder roachpb.ReplicaID

	mu struct {
		syncutil.Mutex
		// TODO: synchronization. Ensure all methods other than WaitForEval are
		// called with raftMu held. RangeControllerImpl needs its own mutex for
		// WaitForEval since it needs to sample voterSets*.

		// State for waiters. When anything in voterSets changes, voterSetRefreshCh
		// is closed, and replaced with a new channel. The voterSets is
		// copy-on-write, so waiters make a shallow copy.
		voterSets         []voterSet
		voterSetRefreshCh chan struct{}
	}

	replicaMap map[roachpb.ReplicaID]*replicaState

	// Demultiplexer. When HandleControllerSchedulerEvent is called, this
	// is used to call into the replicaSendStreams that have asked to be
	// scheduled.
	scheduledReplicas map[roachpb.ReplicaID]struct{}
}

type voterSet []voterStateForWaiters

// voterStateForWaiters informs whether WaitForEval is required to wait for
// eval-tokens for a voter.
type voterStateForWaiters struct {
	replicaID        roachpb.ReplicaID
	isLeader         bool
	isLeaseHolder    bool
	isStateReplicate bool
	evalTokenCounter TokenCounter
}

var _ RangeController = &RangeControllerImpl{}

func NewRangeControllerImpl(
	ctx context.Context,
	o RangeControllerOptions,
	init RangeControllerInitState,
	nextRaftIndex uint64,
) *RangeControllerImpl {
	rc := &RangeControllerImpl{
		opts:              o,
		replicaSet:        ReplicaSet{},
		leaseholder:       init.Leaseholder,
		replicaMap:        map[roachpb.ReplicaID]*replicaState{},
		scheduledReplicas: make(map[roachpb.ReplicaID]struct{}),
	}
	rc.mu.voterSetRefreshCh = make(chan struct{})
	rc.updateReplicaSetAndMap(ctx, init.ReplicaSet, nextRaftIndex)
	rc.updateVoterSets()
	return rc
}

func (rc *RangeControllerImpl) String() string {
	var buf strings.Builder

	fmt.Fprintf(&buf, "controller(%v): lh=%v scheduled=[", rc.opts.RangeID, rc.leaseholder)
	for i, r := range rc.scheduledReplicas {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "%v", r)
	}
	buf.WriteString("] ")

	i := 0
	buf.WriteString("replica_state=[")
	for _, r := range rc.replicaMap {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "(%v)", r)
		i++
	}
	buf.WriteString("] ")

	buf.WriteString("replica_info=[")
	i = 0
	for replicaID := range rc.replicaMap {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "%v=(%v)", replicaID, rc.opts.RaftInterface.FollowerState(replicaID))
		i++
	}
	buf.WriteString("]")

	return buf.String()
}

func (rc *RangeControllerImpl) updateReplicaSetAndMap(
	ctx context.Context, newSet ReplicaSet, nextRaftIndex uint64,
) {
	prevSet := rc.replicaSet
	for r := range prevSet {
		desc, ok := newSet[r]
		if !ok {
			rs := rc.replicaMap[r]
			rs.close(ctx)
			delete(rc.replicaMap, r)
		} else {
			// TODO: see the comment earlier about voter <=> non-voter transitions.
			//
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
		rc.replicaMap[r] = NewReplicaState(ctx, rc, desc, nextRaftIndex)
	}
	rc.replicaSet = newSet
}

// replicaSet, replicaMap, leaseholder are up-to-date.
func (rc *RangeControllerImpl) updateVoterSets() {
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
			replicaID:     r.ReplicaID,
			isLeader:      r.ReplicaID == rc.opts.LocalReplicaID,
			isLeaseHolder: r.ReplicaID == rc.leaseholder,
			isStateReplicate: rs.replicaSendStream != nil &&
				rs.replicaSendStream.state().shouldWaitForElasticEvalTokens(),
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
	rc.mu.Lock()
	vss := rc.mu.voterSets
	vssRefreshCh := rc.mu.voterSetRefreshCh
	rc.mu.Unlock()

	if vssRefreshCh == nil {
		// RangeControllerImpl is closed.
		return nil
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
				return ctx.Err()
			case RefreshWaitSignaled:
				goto retry
			}
		}
	}
	return nil
}

func (rc *RangeControllerImpl) HandleRaftEvent(ctx context.Context, e RaftEvent) error {
	ready := e.Ready()
	var entries []raftpb.Entry
	// nextRaftIndex is the next index that will be processed by all streams.
	nextRaftIndex := rc.opts.RaftInterface.NextUnstableIndex()
	if ready != nil {
		entries = ready.GetEntries()
		if len(entries) > 0 {
			// These entries will be processed next.
			nextRaftIndex = entries[0].Index
		}
	}

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
		// In general, we need to be prepared to deal with anything returned
		// by FollowerState.
		//
		// TODO: this should not be called FollowerState since this includes
		// the leader. It is more accurately the replica state as known to the
		// leader.
		info := rc.opts.RaftInterface.FollowerState(r)
		if r == rc.opts.LocalReplicaID {
			// Leader replica.
			if n := len(entries); n > 0 && entries[n-1].Index+1 != info.Next {
				// Leader is operating in push mode, so Next should have advanced.
				log.Warningf(ctx, "%v", errors.AssertionFailedf("last entry index %d + 1 != %d Next at leader [info=%v %v]",
					entries[n-1].Index, info.Next, info, rs).Error())
			}
		}
		rs.handleReadyState(ctx, info, nextRaftIndex)
	}

	// TODO: it is possible that we have a quorum with replicaSendStreams that
	// are not in state snapshot, but not a quorum with an empty send-queue, because
	// of some state transitions above. Accounting for pendingRecentlyReplicate,
	// we may need to initiate a force-flush. We should abstract out the logic
	// for deciding to force-flush since we need it in multiple places.

	// Process ready.
	// Send the MsgApps we have been asked to send. Note that these may cause
	// queueing up in Replica.addUnreachableRemoteReplica, but those will be
	// handed to Raft later, so this act will not cause a transition from
	// StateReplicate => StateProbe.
	// msgApps := ready.RetransmitMsgApps()
	// for i := range msgApps {
	// rc.opts.MessageSender.SendRaftMessage(
	//	context.TODO(), PriorityNotInheritedForFlowControl, msgApps[i])
	// }

	// entries := ready.GetEntries()
	if len(entries) == 0 {
		return nil
	}
	// The entries are the only things we handle here for StateReplicate
	// replicas with empty send-queues. If a replica has a send-queue of
	// existing entries, they are already trying to eliminate it, and we simply
	// queue.
	for _, rs := range rc.replicaMap {
		rs.handleReadyEntries(ctx, entries)
	}
	return nil
}

type entryFlowControlState struct {
	index uint64
	// usesFlowControl is false for entries that don't use flow control. This
	// can happen if RAC is partly disabled e.g. disabled for regular work, or
	// for conf changes. In the former case the send-queue will also be
	// disabled (i.e., equivalent to RACv1).
	usesFlowControl bool
	originalPri     kvflowcontrolpb.RaftPriority
	tokens          kvflowcontrol.Tokens
}

func (e entryFlowControlState) String() string {
	return fmt.Sprintf("index=%d usesFC=%t pri=%v tokens=%d",
		e.index, e.usesFlowControl, e.originalPri, e.tokens)
}

// testingEncodeRaftFlowControlState encodes the flow control state into a Raft entry,
// used only in testing.
func testingEncodeRaftFlowControlState(
	ctx context.Context,
	index uint64,
	usesFlowControl bool,
	pri kvflowcontrolpb.RaftPriority,
	tokens uint64,
	originNodeID roachpb.NodeID,
) raftpb.Entry {
	r := rand.New(rand.NewSource(123))
	createTime := timeutil.Now().UnixNano()
	cmd := &kvserverpb.RaftCommand{
		AdmissionPriority:   int32(pri),
		AdmissionCreateTime: createTime,
		AdmissionOriginNode: originNodeID,
	}
	idKey := raftlog.MakeCmdIDKey()
	admissionMeta := &kvflowcontrolpb.RaftAdmissionMeta{
		AdmissionPriority:   int32(pri),
		AdmissionOriginNode: originNodeID,
		AdmissionCreateTime: createTime,
	}

	// Deduct the size of the command not including the write batch data, this
	// makes the size of the command exactly tokens long.
	cmd.WriteBatch = &kvserverpb.WriteBatch{Data: randutil.RandBytes(r, int(tokens)-60)}

	data, err := raftlog.EncodeCommand(ctx, cmd, idKey, admissionMeta, usesFlowControl)
	if err != nil {
		log.Fatalf(ctx, "%v", err.Error())
	}

	entry := raftpb.Entry{
		Index: index,
		Data:  data,
	}
	return entry
}

func getFlowControlState(ctx context.Context, entry raftpb.Entry) entryFlowControlState {
	typ, priBits, err := raftlog.EncodingOf(entry)
	if err != nil {
		log.Fatalf(ctx, "%v", err)
	}

	entryFCState := entryFlowControlState{
		index:           entry.Index,
		usesFlowControl: typ.UsesAdmissionControl(),
	}

	if typ.UsesAdmissionControl() {
		if typ != raftlog.EntryEncodingStandardWithRaftPriority &&
			typ != raftlog.EntryEncodingSideloadedWithRaftPriority {
			log.Fatalf(ctx, "%v", errors.AssertionFailedf("expected a RACv2 encoding"))
		}

		entryFCState.originalPri = priBits
		entryFCState.tokens = kvflowcontrol.Tokens(len(entry.Data))

		if buildutil.CrdbTestBuild {
			// REMINDER: this decoding is for a costly assertion only for the prototype and for
			// CrdbTestBuild.
			meta, err := raftlog.DecodeRaftAdmissionMeta(entry.Data)
			if err != nil {
				log.Fatalf(ctx, "%v", errors.Wrap(err, "unable to decode raft command admission data"))
			}
			if kvflowcontrolpb.RaftPriority(meta.AdmissionPriority) != priBits {
				log.Fatalf(ctx, "%v", errors.AssertionFailedf("inconsistent priorities: pri bits=%v admission pri=%v",
					priBits, kvflowcontrolpb.RaftPriority(meta.AdmissionPriority)).Error())
			}
		}
	}

	return entryFCState
}

func (rc *RangeControllerImpl) HandleControllerSchedulerEvent(ctx context.Context) error {
	for r := range rc.scheduledReplicas {
		rs, ok := rc.replicaMap[r]
		scheduleAgain := false
		if ok && rs.replicaSendStream != nil {
			scheduleAgain = rs.replicaSendStream.scheduled(ctx)
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

func (rc *RangeControllerImpl) SetReplicas(ctx context.Context, replicas ReplicaSet) error {
	nextRaftIndex := rc.opts.RaftInterface.NextUnstableIndex()
	rc.updateReplicaSetAndMap(ctx, replicas, nextRaftIndex)
	rc.updateVoterSets()
	return nil
}

func (rc *RangeControllerImpl) SetLeaseholder(ctx context.Context, replica roachpb.ReplicaID) {
	if replica == rc.leaseholder {
		return
	}
	log.VInfof(ctx, 1, "leaseholder change: old=%v [info=%v] new=%v [info=%v]",
		rc.leaseholder, rc.opts.RaftInterface.FollowerState(rc.leaseholder),
		replica, rc.opts.RaftInterface.FollowerState(replica))

	rc.leaseholder = replica
	rc.updateVoterSets()
	rs := rc.replicaMap[replica]

	if rs.replicaSendStream != nil {
		rs.replicaSendStream.mu.Lock()
		defer rs.replicaSendStream.mu.Unlock()
		if rs.replicaSendStream.connectedState != snapshot && !rs.replicaSendStream.isEmptySendQueueLocked() {
			rs.replicaSendStream.scheduleForceFlushLocked(ctx)
		}
	}
}

func (rc *RangeControllerImpl) Close(ctx context.Context) {
	func() {
		rc.mu.Lock()
		defer rc.mu.Unlock()

		close(rc.mu.voterSetRefreshCh)
		rc.mu.voterSetRefreshCh = nil
	}()

	for _, rs := range rc.replicaMap {
		rs.close(ctx)
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

func (rs *replicaState) String() string {
	return fmt.Sprintf("%v: stream=%v send_stream=(%v) eval_tokens=(%v) send_tokens=(%v)",
		rs.desc, rs.stream, rs.replicaSendStream, rs.evalTokenCounter, rs.sendTokenCounter)
}

func NewReplicaState(
	ctx context.Context,
	parent *RangeControllerImpl,
	desc roachpb.ReplicaDescriptor,
	nextRaftIndex uint64,
) *replicaState {
	stream := kvflowcontrol.Stream{TenantID: parent.opts.TenantID, StoreID: desc.StoreID}
	rs := &replicaState{
		parent:            parent,
		stream:            stream,
		evalTokenCounter:  parent.opts.SSTokenCounter.EvalTokenCounterForStream(stream),
		sendTokenCounter:  parent.opts.SSTokenCounter.SendTokenCounterForStream(stream),
		desc:              desc,
		replicaSendStream: nil,
	}
	info := parent.opts.RaftInterface.FollowerState(desc.ReplicaID)
	if info.State == tracker.StateReplicate {
		rs.createReplicaSendStream(ctx, info.Next, nextRaftIndex)
	}
	return rs
}

func (rs *replicaState) createReplicaSendStream(
	ctx context.Context, infoNext uint64, nextRaftIndex uint64,
) {
	// [indexToSend, nextRaftIndex) is the initial send-queue.
	//
	// For followers, we are guaranteed that info.Next <= nextRaftIndex, due
	// to pull mode, and that it represents the indexToSend.
	//
	// For the leader, info.Next advances very early, so can be higher than nextRaftIndex. But since
	// a leader never has a send-queue it is safe to set indexToSend == nextRaftIndex.
	//
	// The min logic below accomplishes this without knowing whether we are a
	// leader or follower.
	indexToSend := min(infoNext, nextRaftIndex)
	if rs.desc.ReplicaID == rs.parent.opts.LocalReplicaID && indexToSend != nextRaftIndex {
		log.Fatalf(ctx, "%v", errors.AssertionFailedf(
			"leader should not have send-queue info.Next %d indexToSend %d nextRaftIndex %d",
			infoNext, indexToSend, nextRaftIndex))
	}
	rss := newReplicaSendStream(ctx, rs, replicaSendStreamInitState{
		indexToSend:   indexToSend,
		nextRaftIndex: nextRaftIndex,
		// TODO: these need to be based on some history observed by RangeControllerImpl.
		approxMeanSizeBytes: 1000,
	})
	rss.mu.Lock()
	defer rss.mu.Unlock()

	rs.replicaSendStream = rss
	if rs.parent.leaseholder == rs.desc.ReplicaID && !rss.isEmptySendQueueLocked() {
		rss.scheduleForceFlushLocked(ctx)
	}
	rss.startProcessingSendQueueLocked(ctx)
}

func (rs *replicaState) close(ctx context.Context) {
	if rs.replicaSendStream != nil {
		rs.replicaSendStream.closeLocked(ctx)
	}
}

func (rs *replicaState) handleReadyState(
	ctx context.Context, info FollowerStateInfo, nextRaftIndex uint64,
) {
	state := info.State
	if log.V(3) {
		log.Infof(ctx,
			"handle raft event replica_id=%d: info=(%v) state=(%v)", rs.desc.ReplicaID, info, rs)
	} else if log.V(2) {
		log.Infof(ctx,
			"handle raft event replica_id=%d: info=(%v)", rs.desc.ReplicaID, info)
	}

	switch state {
	case tracker.StateProbe:
		if rs.replicaSendStream != nil {
			// TODO: delay this by 1 sec.
			rs.replicaSendStream.close(ctx)
			rs.replicaSendStream = nil
		}
	case tracker.StateReplicate:
		if rs.replicaSendStream == nil {
			rs.createReplicaSendStream(ctx, info.Next, nextRaftIndex)
		} else {
			// replicaSendStream already exists.
			func() {
				rs.replicaSendStream.mu.Lock()
				defer rs.replicaSendStream.mu.Unlock()

				switch rs.replicaSendStream.connectedState {
				case replicate:
					rs.replicaSendStream.makeConsistentInStateReplicateLocked(ctx, info)
				case probeRecentlyReplicate:
					rs.replicaSendStream.makeConsistentWhenProbeToReplicateLocked(ctx, info.Next)
				case snapshot:
					rs.replicaSendStream.makeConsistentWhenSnapshotToReplicateLocked(ctx, info.Next)
				}
			}()
		}
	case tracker.StateSnapshot:
		if rs.replicaSendStream != nil {
			func() {
				rs.replicaSendStream.mu.Lock()
				defer rs.replicaSendStream.mu.Unlock()

				switch rs.replicaSendStream.connectedState {
				case replicate:
					rs.replicaSendStream.changeToStateSnapshotLocked(ctx)
				case probeRecentlyReplicate:
					rs.replicaSendStream.closeLocked(ctx)
					rs.replicaSendStream = nil
				case snapshot:
				}
			}()
		}
	}
	log.VInfof(ctx, 3,
		"post-handle raft event replica_id=%d: info=(%v) state=(%v)", rs.desc.ReplicaID, info, rs)
}

// handleReadyEntries is called when on every Ready event.
func (rs *replicaState) handleReadyEntries(ctx context.Context, entries []raftpb.Entry) {
	if rs.replicaSendStream == nil {
		return
	}

	rs.replicaSendStream.mu.Lock()
	defer rs.replicaSendStream.mu.Unlock()

	r := rs.desc.ReplicaID
	rc := rs.parent
	if r == rs.parent.opts.LocalReplicaID {
		// Local replica, which is the leader. These will have a
		// MsgStorageAppend that is not mediated here, but we do need to account
		// for tokens.
		for i := range entries {
			entryFCState := getFlowControlState(ctx, entries[i])
			wc := kvflowcontrolpb.WorkClassFromRaftPriority(entryFCState.originalPri)
			log.VInfof(ctx, 3, "leader handle_ready_entries(%v): entryFCState=%v info=%v",
				rs.replicaSendStream.stringLocked(), entryFCState, rs.parent.opts.RaftInterface.FollowerState(r))
			rs.replicaSendStream.advanceNextRaftIndexAndSentLocked(ctx, entryFCState)
			if entryFCState.usesFlowControl {
				rs.sendTokenCounter.Deduct(ctx, wc, entryFCState.tokens)
			}
		}
		return
	}

	if rs.replicaSendStream.stateLocked() == replicate && rs.replicaSendStream.isEmptySendQueueLocked() {
		// Consider sending.
		// If leaseholder just send. If the leaseholder has a send-queue we won't be in this
		// path, but a force-flush must be ongoing.
		isLeaseholder := r == rc.leaseholder
		from := entries[0].Index
		// [from, to) is what we will send.
		to := entries[0].Index
		toIsFinalized := false
		for i := range entries {
			entryFCState := getFlowControlState(ctx, entries[i])
			log.VInfof(ctx, 3, "follower handle_ready_entries(%v): entryFCState=%v",
				rs.replicaSendStream.stringLocked(), entryFCState)
			if toIsFinalized {
				if entries[i].Index == to && !entryFCState.usesFlowControl {
					// Include additional entries that are not subject to AC, since we
					// always have tokens for those.
					to++
				} else {
					rs.replicaSendStream.advanceNextRaftIndexAndQueuedLocked(ctx, entryFCState)
				}
				continue
			}
			// INVARIANT: !toIsFinalized.
			wc := kvflowcontrolpb.WorkClassFromRaftPriority(entryFCState.originalPri)
			send := false
			if isLeaseholder {
				if entryFCState.usesFlowControl {
					rs.sendTokenCounter.Deduct(ctx, wc, entryFCState.tokens)
				}
				send = true
			} else {
				if entryFCState.usesFlowControl {
					tokens := rs.sendTokenCounter.TryDeduct(ctx, wc, entryFCState.tokens)
					if tokens > 0 {
						send = true
						if tokens < entryFCState.tokens {
							toIsFinalized = true
							// Deduct the remaining for this entry.
							rs.sendTokenCounter.Deduct(ctx, wc, entryFCState.tokens-tokens)
						}
					}
					// Else send stays false.
				} else {
					send = true
				}
			}
			if send {
				to++
				rs.replicaSendStream.advanceNextRaftIndexAndSentLocked(ctx, entryFCState)
			} else {
				toIsFinalized = true
				rs.replicaSendStream.advanceNextRaftIndexAndQueuedLocked(ctx, entryFCState)
			}
		}
		if to > from {
			// Have deducted the send tokens. Proceed to send.
			//
			// TODO: use configuration of a limit on max inflight entries.
			msg, err := rc.opts.RaftInterface.MakeMsgApp(r, from, to, math.MaxInt64)
			if err != nil {
				log.Fatalf(ctx, "%v", errors.AssertionFailedf(
					"in Ready.Entries, but unable to create MsgApp -- couldn't have been truncated"))
			}
			// Check that the sender and receiver replica IDs are correct, if not
			// crash.
			if roachpb.ReplicaID(msg.To) != r {
				log.Fatalf(ctx, "%v", errors.AssertionFailedf(
					"created msg with incorrect recepient replica id expected %v got %v",
					r, msg.To))
			}
			if roachpb.ReplicaID(msg.From) != rc.opts.LocalReplicaID {
				log.Fatalf(ctx, "%v", errors.AssertionFailedf("created msg with incorrect sender replica id expected %v got %v",
					rc.opts.LocalReplicaID, msg.From))
			}

			log.VInfof(ctx, 2,
				"send raft message from=%d to=%d: [%d,%d) [info=%v send_stream=%v]",
				msg.From, msg.To, msg.Index+1, int(msg.Index)+len(msg.Entries)+1,
				rs.parent.opts.RaftInterface.FollowerState(rs.desc.ReplicaID),
				rs.replicaSendStream.stringLocked())
			rc.opts.MessageSender.SendRaftMessage(
				ctx, kvflowcontrolpb.PriorityNotInheritedForFlowControl, msg)
		}
		// Else nothing to send.
	} else {
		// Not in StateReplicate, or have an existing send-queue. Need to queue.
		for i := range entries {
			entryFCState := getFlowControlState(ctx, entries[i])
			rs.replicaSendStream.advanceNextRaftIndexAndQueuedLocked(ctx, entryFCState)
		}
	}
}

// replicaSendStream is the state for a replica in one of the connectedState
// states (see comment where connectedState is declared). We maintain a
// send-queue for the replica. We may track token deductions in states
// replicate and probeRecentlyReplicate. On a transition to state snapshot we
// immediately return all tokens since holding onto these can affect other
// ranges.
//
// The justification for not immediately returning all tokens in
// probeRecentlyReplicate is that such a transition will (a) typically happen
// due to lossiness that affects all ranges replicating to that store, (b)
// this is a very transient state. Any immediate returning of tokens can cause
// more overload, so we want to be narrow in the conditions when we resort to
// it.
type replicaSendStream struct {
	parent *replicaState
	// TODO: synchronization. Needs synchronization as callbacks corresponding
	// to TokenAvailableNotification come directly to replicaSendStream without
	// any external synchronization.
	mu syncutil.Mutex

	connectedState connectedState

	// indexToSendInitial is the indexToSend when this replica was observed to
	// transition to StateReplicate. Only indices >= indexToSendInitial can be
	// in tracker.
	//
	// This is equal to FollowerStateInfo.Match+1 when the replicaSendStream
	// is created, or transitions from snapshot => replicate.
	indexToSendInitial uint64
	// tracker contains entries that have been sent, and have had send-tokens
	// deducted (and will have had eval-tokens deducted iff index >=
	// nextRaftIndexInitial).
	//
	// All entries in (FollowerStateInfo.Match, indexToSend) must be in the
	// tracker, since FollowerStateInfo.Admitted[i] <=
	// FollowerStateInfo.Match.
	tracker Tracker

	// nextRaftIndexInitial is the value of nextRaftIndex when this
	// replicaSendStream was created, or transitioned from snapshot =>
	// replicate.
	//
	// INVARIANT: indexToSendInitial <= nextRaftIndexInitial
	nextRaftIndexInitial uint64

	sendQueue struct {
		// State of send-queue. [indexToSend, nextRaftIndex) have not been sent.
		// indexToSend == FollowerStateInfo.Next.
		indexToSend   uint64
		nextRaftIndex uint64

		// Approximate stats for send-queue. For indices < nextRaftIndexInitial.
		//
		// An approx-max-priority is not needed for priority inheritance,
		// since the entries in < nextRaftIndexInitial, are not consuming eval
		// tokens, and priority inheritance is a fairness device for eval
		// tokens. However, it could serve a rudimentary function when
		// nextRaftIndex == nextRaftIndexInitial, i.e., there are no precise
		// stats, and we need to grab some tokens for
		// deductedForScheduler.tokens. For now, we chose to only grab elastic
		// tokens in this case. If there is regular work waiting, it may have
		// to wait longer, but this is a backlog case, so it is fine (we are
		// not sacrificing quorum on the range, or preventing new regular work
		// from evaluating).
		//
		// approxMeanSizeBytes is useful since it guides how many bytes to
		// grab in deductedForScheduler.tokens.
		approxMeanSizeBytes kvflowcontrol.Tokens

		// Precise stats for send-queue. For indices >= nextRaftIndexInitial.
		priorityCount [kvflowcontrolpb.NumRaftPriorities]int64
		// sizeSum is only for entries subject to AC.
		sizeSum kvflowcontrol.Tokens

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
		watcherHandleID StoreStreamSendTokenHandleID
		// Two cases for deductedForScheduler.wc.
		// - grabbed regular tokens: must have regular work waiting. Use the
		//   highest pri in priorityCount for the inherited priority.
		//
		// - grabbed elastic tokens: may have regular work that will be sent.
		//   Deduct regular tokens without waiting for those. The message is sent
		//   with no inherited priority. Since elastic tokens were available
		//   recently it is highly probable that regular tokens are also
		//   available.
		deductedForScheduler struct {
			wc     admissionpb.WorkClass
			tokens kvflowcontrol.Tokens
		}
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

func (rss *replicaSendStream) String() string {
	rss.mu.Lock()
	defer rss.mu.Unlock()

	return rss.stringLocked()
}

func (rss *replicaSendStream) stringLocked() string {
	rss.mu.AssertHeld()
	return fmt.Sprintf("[%v,%v) conn=%v closed=%v handle_id=%d size=%v pri=%v force=%v tracker=%v",
		rss.sendQueue.indexToSend, rss.sendQueue.nextRaftIndex, rss.connectedState, rss.closed,
		rss.sendQueue.watcherHandleID, rss.queueSizeLocked(), rss.queuePriorityLocked(),
		rss.sendQueue.forceFlushScheduled, rss.tracker.String())
}

// Initial state provided to constructor of replicaSendStream.
type replicaSendStreamInitState struct {
	// [indexToSend, nextRaftIndex) are known to the (local) leader and need to
	// be sent to this replica. This is the initial send-queue.
	//
	// INVARIANT: is-local-replica => indexToSend == nextRaftIndex, i.e., no
	// send-queue.
	indexToSend   uint64
	nextRaftIndex uint64

	// Approximate stats for the initial send-queue.
	approxMeanSizeBytes kvflowcontrol.Tokens
}

func newReplicaSendStream(
	ctx context.Context, parent *replicaState, init replicaSendStreamInitState,
) *replicaSendStream {
	// Must be in StateReplicate on creation.
	rss := &replicaSendStream{
		parent:               parent,
		connectedState:       replicate,
		indexToSendInitial:   init.indexToSend,
		nextRaftIndexInitial: init.nextRaftIndex,
	}
	rss.tracker.Init(parent.stream)
	rss.sendQueue.indexToSend = init.indexToSend
	rss.sendQueue.nextRaftIndex = init.nextRaftIndex
	rss.sendQueue.approxMeanSizeBytes = init.approxMeanSizeBytes
	rss.sendQueue.watcherHandleID = InvalidStoreStreamSendTokenHandleID
	rss.mu.Lock()
	defer rss.mu.Unlock()

	log.VInfof(ctx, 1,
		"init replica send stream(%v): index_to_send=%v next_raft_index=%d "+
			"next_unstable_index=%d last_entry_index=%d [info=%v send_stream=%v]",
		rss.parent.desc.ReplicaID, rss.sendQueue.indexToSend,
		rss.sendQueue.nextRaftIndex,
		parent.parent.opts.RaftInterface.NextUnstableIndex(),
		parent.parent.opts.RaftInterface.LastEntryIndex(),
		parent.parent.opts.RaftInterface.FollowerState(parent.desc.ReplicaID),
		rss.stringLocked())
	return rss
}

func (rss *replicaSendStream) close(ctx context.Context) {
	rss.mu.Lock()
	defer rss.mu.Unlock()

	rss.closeLocked(ctx)
}

func (rss *replicaSendStream) closeLocked(ctx context.Context) {
	rss.mu.AssertHeld()
	if rss.connectedState != snapshot {
		// Will cause all tokens to be returned etc.
		rss.changeToStateSnapshotLocked(ctx)
	}
	rss.closed = true
}

// An entry is being sent that was never in the send-queue. So the send-queue
// must be empty.
func (rss *replicaSendStream) advanceNextRaftIndexAndSentLocked(
	ctx context.Context, state entryFlowControlState,
) {
	rss.mu.AssertHeld()
	if rss.connectedState != replicate {
		log.Fatalf(ctx, "%v", errors.AssertionFailedf(
			"connected_state=%v != replicate [info=%v send_stream=%v]",
			rss.connectedState,
			rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID),
			rss.stringLocked()))
	}
	if state.index != rss.sendQueue.indexToSend {
		log.Fatalf(ctx, "%v", errors.AssertionFailedf(
			"entry_index=%v != index_to_send=%v [entry=%v info=%v send_stream=%v]",
			state.index, rss.sendQueue.indexToSend, state,
			rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID),
			rss.stringLocked()))
	}
	if state.index != rss.sendQueue.nextRaftIndex {
		log.Fatalf(ctx, "%v", errors.AssertionFailedf(
			"entry_index=%v != next_raft_index=%v [entry=%v info=%v send_stream=%v]",
			state.index, rss.sendQueue.nextRaftIndex, state,
			rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID),
			rss.stringLocked()))
	}
	rss.sendQueue.indexToSend++
	rss.sendQueue.nextRaftIndex++
	if !state.usesFlowControl {
		return
	}
	// inheritedPri and originalPri are the same for an entry that was never
	// queued.

	rss.tracker.Track(ctx, state.index, state.originalPri, state.originalPri, state.tokens)
	rss.parent.evalTokenCounter.Deduct(
		ctx, kvflowcontrolpb.WorkClassFromRaftPriority(state.originalPri), state.tokens)
}

func (rss *replicaSendStream) scheduleForceFlushLocked(ctx context.Context) {
	rss.mu.AssertHeld()
	if rss.sendQueue.watcherHandleID != InvalidStoreStreamSendTokenHandleID {
		rss.parent.parent.opts.SendTokensWatcher.CancelHandle(rss.sendQueue.watcherHandleID)
		rss.sendQueue.watcherHandleID = InvalidStoreStreamSendTokenHandleID
	}
	rss.returnDeductedFromSchedulerTokensLocked(ctx)
	rss.sendQueue.forceFlushScheduled = true
	rss.parent.parent.scheduleReplica(rss.parent.desc.ReplicaID)
}

func (rss *replicaSendStream) scheduled(ctx context.Context) (scheduleAgain bool) {
	rss.mu.Lock()
	defer rss.mu.Unlock()

	return rss.scheduledLocked(ctx)
}

// REQUIRES: !rss.closed
func (rss *replicaSendStream) scheduledLocked(ctx context.Context) (scheduleAgain bool) {
	rss.mu.AssertHeld()

	if rss.connectedState != replicate {
		return false
	}
	info := rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID)
	switch info.State {
	case tracker.StateReplicate:
		rss.makeConsistentInStateReplicateLocked(ctx, info)
	default:
		rss.returnDeductedFromSchedulerTokensLocked(ctx)
		rss.sendQueue.forceFlushScheduled = false
		return false
	}
	// 4MB. Don't want to hog the scheduler thread for too long.
	//
	// Also, if have regular tokens and one of the early queued entries is the
	// one causing the inherited priority to become a priority that belongs in
	// regular work, and the rest after that are elastic, will bound how much
	// elastic we unnecessarily inherit to regular. Note, we could also handle
	// this algorithmically, by not sending the elastic entries, but we've
	// already pulled them out using MakeMsgApp, so we don't want to waste the
	// storage read.
	const MaxBytesToSend kvflowcontrol.Tokens = 4 << 20
	bytesToSend := MaxBytesToSend
	if !rss.sendQueue.forceFlushScheduled {
		bytesToSend = rss.sendQueue.deductedForScheduler.tokens
	}
	log.VInfof(ctx, 2, "scheduled replica=%v: bytes_to_send=%v [info=%v send_stream=%v]",
		rss.parent.desc.ReplicaID, bytesToSend,
		rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID),
		rss.stringLocked())
	if bytesToSend == 0 {
		return false
	}
	// TODO(sumeer): use configuration of a limit on max inflight entries.
	//
	// [pav-kv] The raft.Config.{MaxSizePerMsg, MaxInflightMsgs} fields are
	// relevant. I don't think we need MsgInflightMsgs, but the the bytesToSend
	// could be paginated MaxSizePerMsg per message, if we need it.
	//
	// When bytesToSend is decided by rss.sendQueue.deductedForScheduler.tokens,
	// it is only based on what is subject to AC. But Raft is unaware of this
	// linkage between admission control and flow tokens, and MakeMsgApp will
	// use this bytesToSend to compute across all entries. How harmful is this?
	// RACv2 will be configured in one of three modes (a) fully disabled, so
	// this is irrelevant, (b) flow tokens only for elastic work, and
	// send-queues disabled (analogous to RACv1), so this is irrelevant, (c)
	// flow tokens for regular and elastic work, and send-queues enabled (can't
	// have it disabled as that would result in overload based on pacing at
	// quorum for regular work), in which case the total size of entries not
	// subject to flow control will be tiny, and this is harmless.
	msg, err := rss.parent.parent.opts.RaftInterface.MakeMsgApp(
		rss.parent.desc.ReplicaID, rss.sendQueue.indexToSend, rss.sendQueue.nextRaftIndex,
		int64(bytesToSend))
	if err != nil {
		if !errors.Is(err, raft.ErrCompacted) {
			log.Fatalf(ctx, "%v", err)
		}
		rss.changeToStateSnapshotLocked(ctx)
		return
	}
	rss.dequeueFromQueueAndSendLocked(ctx, msg)
	isEmpty := rss.isEmptySendQueueLocked()
	if isEmpty {
		if rss.sendQueue.forceFlushScheduled {
			rss.sendQueue.forceFlushScheduled = false
		}
		rss.returnDeductedFromSchedulerTokensLocked(ctx)
		return false
	}
	// INVARIANT: !isEmpty.
	if rss.sendQueue.forceFlushScheduled || rss.sendQueue.deductedForScheduler.tokens > 0 {
		return true
	} else {
		pri := rss.queuePriorityLocked()
		rss.sendQueue.watcherHandleID = rss.parent.parent.opts.SendTokensWatcher.NotifyWhenAvailable(
			rss.parent.sendTokenCounter, kvflowcontrolpb.WorkClassFromRaftPriority(pri), rss)
		return false
	}
}

func (rss *replicaSendStream) dequeueFromQueueAndSendLocked(
	ctx context.Context, msg raftpb.Message,
) {
	rss.mu.AssertHeld()

	inheritedPri := kvflowcontrolpb.PriorityNotInheritedForFlowControl
	// These are the remaining send tokens, that we may want to return. Can be
	// negative, in which case we need to deduct some more.
	var remainingTokens [admissionpb.NumWorkClasses]kvflowcontrol.Tokens
	if rss.sendQueue.forceFlushScheduled {
		inheritedPri = kvflowcontrolpb.NotSubjectToACForFlowControl
	} else {
		// Only inherit the priority if have regular tokens. If have elastic
		// tokens, will not inherit.
		if rss.sendQueue.deductedForScheduler.tokens > 0 &&
			rss.sendQueue.deductedForScheduler.wc == admissionpb.RegularWorkClass {
			// Best guess at the inheritedPri based on the stats. We will correct
			// this before.
			inheritedPri = rss.queuePriorityLocked()
			if kvflowcontrolpb.WorkClassFromRaftPriority(inheritedPri) == admissionpb.ElasticWorkClass {
				log.Fatalf(ctx, "%v", errors.AssertionFailedf("inherited elastic work-class from regular work-class"))
			}
			remainingTokens[admissionpb.RegularWorkClass] = rss.sendQueue.deductedForScheduler.tokens
		} else {
			remainingTokens[admissionpb.ElasticWorkClass] = rss.sendQueue.deductedForScheduler.tokens
		}
		rss.sendQueue.deductedForScheduler.tokens = 0
	}
	// fcStates is only used when we are doing priority inheritance, since we
	// need to update the inheritedPri.
	var fcStates []entryFlowControlState
	for _, entry := range msg.Entries {
		if rss.sendQueue.indexToSend != entry.Index {
			log.Fatalf(ctx, "%v", errors.AssertionFailedf(
				"entry_index=%v != index_to_send=%v [entry=%v info=%v send_stream=%v]",
				entry.Index, rss.sendQueue.indexToSend, entry,
				rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID),
				rss.stringLocked()))
		}
		rss.sendQueue.indexToSend++
		entryFCState := getFlowControlState(ctx, entry)
		if !entryFCState.usesFlowControl {
			continue
		}
		if entryFCState.index >= rss.nextRaftIndexInitial {
			// Fix the stats since this is no longer in the send-queue. We only do
			// this for entries which after >= initial next raft index, as any prior
			// entries the controller wouldn't have seen or added tokens for
			// (estimate is used).
			rss.sendQueue.sizeSum -= entryFCState.tokens
			rss.sendQueue.priorityCount[entryFCState.originalPri]--
		}
		originalEntryWC := kvflowcontrolpb.WorkClassFromRaftPriority(entryFCState.originalPri)
		switch inheritedPri {
		case kvflowcontrolpb.NotSubjectToACForFlowControl:
			// Don't touch remaining tokens, and return the eval tokens.
			if entryFCState.index >= rss.nextRaftIndexInitial {
				rss.eval.tokensDeducted[originalEntryWC] -=
					entryFCState.tokens
				rss.parent.evalTokenCounter.Return(
					ctx, originalEntryWC, entryFCState.tokens)
			}
		case kvflowcontrolpb.PriorityNotInheritedForFlowControl:
			remainingTokens[originalEntryWC] -= entryFCState.tokens
			rss.tracker.Track(
				ctx, entryFCState.index, entryFCState.originalPri, entryFCState.originalPri, entryFCState.tokens)
		default:
			if entryFCState.originalPri > inheritedPri {
				// This can happen since the priorityCounts are not complete.
				// They only track >= nextRaftIndexInitial.
				inheritedPri = entryFCState.originalPri
			}
			fcStates = append(fcStates, entryFCState)
			remainingTokens[admissionpb.RegularWorkClass] -= entryFCState.tokens
		}
	}
	for _, e := range fcStates {
		rss.tracker.Track(
			ctx, e.index, inheritedPri, e.originalPri, e.tokens)
	}
	for i := range remainingTokens {
		if remainingTokens[i] > 0 {
			rss.sendQueue.deductedForScheduler.tokens = remainingTokens[i]
		} else if remainingTokens[i] < 0 {
			rss.parent.sendTokenCounter.Deduct(
				ctx, admissionpb.WorkClass(i), -remainingTokens[i])
		}
	}
	log.VInfof(ctx, 2,
		"send raft message from=%d to=%d: [%d,%d) inherited_pri=%v [info=%v send_stream=%v]",
		msg.From, msg.To, msg.Index+1, int(msg.Index)+len(msg.Entries)+1, inheritedPri,
		rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID),
		rss.stringLocked())
	rss.parent.parent.opts.MessageSender.SendRaftMessage(ctx, inheritedPri, msg)
}

func (rss *replicaSendStream) advanceNextRaftIndexAndQueuedLocked(
	ctx context.Context, entry entryFlowControlState,
) {
	rss.mu.AssertHeld()
	if entry.index != rss.sendQueue.nextRaftIndex {
		log.Fatalf(ctx, "%v", errors.AssertionFailedf(
			"entry.index=%v != next_raft_index=%v [entry=%v info=%v send_stream=%v]",
			entry.index, rss.sendQueue.nextRaftIndex, entry,
			rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID),
			rss.stringLocked()))
	}
	wasEmpty := rss.isEmptySendQueueLocked()
	// TODO: if wasEmpty, we may need to force-flush something. That decision needs to be
	// made in the caller.

	rss.sendQueue.nextRaftIndex++
	if entry.usesFlowControl {
		rss.sendQueue.sizeSum += entry.tokens
		var existingSendQWC admissionpb.WorkClass
		if rss.sendQueue.watcherHandleID != InvalidStoreStreamSendTokenHandleID {
			// May need to update it.
			existingSendQWC = kvflowcontrolpb.WorkClassFromRaftPriority(rss.queuePriorityLocked())
		}
		rss.sendQueue.priorityCount[entry.originalPri]++
		if rss.connectedState == snapshot {
			// Do not deduct eval-tokens in StateSnapshot, since there is no
			// guarantee when these will be returned. In
			// probeRecentlyReplicate, we continue deducting, since it is
			// short-lived.
			return
		}
		entryWC := kvflowcontrolpb.WorkClassFromRaftPriority(entry.originalPri)
		rss.eval.tokensDeducted[entryWC] += entry.tokens
		rss.parent.evalTokenCounter.Deduct(ctx, entryWC, entry.tokens)
		if wasEmpty {
			// Register notification.
			rss.sendQueue.watcherHandleID = rss.parent.parent.opts.SendTokensWatcher.NotifyWhenAvailable(
				rss.parent.sendTokenCounter, entryWC, rss)
		} else if entryWC != existingSendQWC {
			// Update notification
			rss.parent.parent.opts.SendTokensWatcher.UpdateHandle(rss.sendQueue.watcherHandleID, entryWC)
		}
	}
}

// Notify implements TokenAvailableNotification.
func (rss *replicaSendStream) Notify(ctx context.Context) {
	rss.mu.Lock()
	defer rss.mu.Unlock()

	rss.notifyLocked(ctx)
}

func (rss *replicaSendStream) notifyLocked(ctx context.Context) {
	rss.mu.AssertHeld()
	// TODO: concurrency. raftMu is not held, and not being called from raftScheduler.
	if rss.closed || rss.connectedState != replicate || rss.sendQueue.forceFlushScheduled {
		// Must have canceled the handle and the cancellation raced with the
		// notification.
		return
	}

	pri := rss.queuePriorityLocked()
	wc := kvflowcontrolpb.WorkClassFromRaftPriority(pri)
	queueSize := rss.queueSizeLocked()

	tokens := rss.parent.sendTokenCounter.TryDeduct(ctx, wc, queueSize)
	if tokens > 0 || queueSize == 0 {
		// Either deducted tokens successfully or the queue was already empty,
		// there's no tokens to wait on or deduct. Ensure that any outstanding
		// token handle is cancelled.
		if rss.sendQueue.watcherHandleID != InvalidStoreStreamSendTokenHandleID {
			rss.parent.parent.opts.SendTokensWatcher.CancelHandle(rss.sendQueue.watcherHandleID)
			rss.sendQueue.watcherHandleID = InvalidStoreStreamSendTokenHandleID
		}
		rss.sendQueue.deductedForScheduler.wc = wc
		rss.sendQueue.deductedForScheduler.tokens = tokens

		log.VInfof(ctx, 1,
			"Notify(%v): deduct=%v watcher=%v %v state=%v",
			rss.parent.desc.ReplicaID, tokens,
			rss.parent.parent.opts.SendTokensWatcher, rss.stringLocked(),
			rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID))

		rss.parent.parent.scheduleReplica(rss.parent.desc.ReplicaID)
	}
}

func (rss *replicaSendStream) state() connectedState {
	rss.mu.Lock()
	defer rss.mu.Unlock()

	return rss.stateLocked()
}

func (rss *replicaSendStream) stateLocked() connectedState {
	rss.mu.AssertHeld()
	return rss.connectedState
}

func (rss *replicaSendStream) isEmptySendQueue() bool {
	rss.mu.Lock()
	defer rss.mu.Unlock()

	return rss.isEmptySendQueueLocked()
}

func (rss *replicaSendStream) isEmptySendQueueLocked() bool {
	rss.mu.AssertHeld()
	return rss.sendQueue.indexToSend == rss.sendQueue.nextRaftIndex
}

// REQUIRES: send-queue is not empty.
func (rss *replicaSendStream) queuePriority() kvflowcontrolpb.RaftPriority {
	rss.mu.Lock()
	defer rss.mu.Unlock()

	return rss.queuePriorityLocked()
}

func (rss *replicaSendStream) queuePriorityLocked() kvflowcontrolpb.RaftPriority {
	rss.mu.AssertHeld()
	maxPri := kvflowcontrolpb.RaftLowPri
	for pri := kvflowcontrolpb.RaftLowPri + 1; pri < kvflowcontrolpb.NumRaftPriorities; pri++ {
		if rss.sendQueue.priorityCount[pri] > 0 {
			maxPri = pri
		}
	}
	return maxPri
}

// REQUIRES: send-queue is not empty.
func (rss *replicaSendStream) queueSize() kvflowcontrol.Tokens {
	rss.mu.Lock()
	defer rss.mu.Unlock()

	return rss.queueSizeLocked()
}

func (rss *replicaSendStream) queueSizeLocked() kvflowcontrol.Tokens {
	rss.mu.AssertHeld()
	var size kvflowcontrol.Tokens
	countWithApproxStats := int64(rss.nextRaftIndexInitial) - int64(rss.sendQueue.indexToSend)
	if countWithApproxStats > 0 {
		size = kvflowcontrol.Tokens(countWithApproxStats) * rss.sendQueue.approxMeanSizeBytes
	}
	size += rss.sendQueue.sizeSum

	if size < 0 {
		// The queue size should be bounded below by 0 in most cases, however it is
		// possible that as entries are dequeued and sent
		panic(fmt.Sprintf(
			"%v: negative send queue size %v [next_raft_index_initial=%v "+
				"next_raft_index=%v index_to_send=%v approx_mean_bytes=%v "+
				"send_queue_size_sum=%v info=%v]",
			rss.parent.desc.ReplicaID, size, rss.nextRaftIndexInitial,
			rss.sendQueue.nextRaftIndex, rss.sendQueue.indexToSend,
			rss.sendQueue.approxMeanSizeBytes, rss.sendQueue.sizeSum,
			rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID)))
	}

	return size
}

func (rss *replicaSendStream) changeToStateSnapshotLocked(ctx context.Context) {
	rss.mu.AssertHeld()
	rss.connectedState = snapshot
	// The tracker must only contain entries in < rss.sendQueue.indexToSend.
	// These may not have been received by the replica. Since the replica is
	// now in StateSnapshot, there is no need for Raft to send MsgApp pings to
	// discover what has been missed. So there is no liveness guarantee on
	// when these tokens will be returned, and therefore we return all tokens
	// in the tracker.
	rss.tracker.UntrackAll(func(
		index uint64, inheritedPri kvflowcontrolpb.RaftPriority, originalPri kvflowcontrolpb.RaftPriority, tokens kvflowcontrol.Tokens) {
		rss.parent.sendTokenCounter.Return(
			ctx, kvflowcontrolpb.WorkClassFromRaftPriority(inheritedPri), tokens)
	})
	// For the same reason, return all eval tokens deducted.
	for wc := range rss.eval.tokensDeducted {
		if rss.eval.tokensDeducted[wc] > 0 {
			rss.parent.evalTokenCounter.Return(
				ctx, admissionpb.WorkClass(wc), rss.eval.tokensDeducted[wc])
			rss.eval.tokensDeducted[wc] = 0
		}
	}
	if rss.sendQueue.watcherHandleID != InvalidStoreStreamSendTokenHandleID {
		rss.parent.parent.opts.SendTokensWatcher.CancelHandle(rss.sendQueue.watcherHandleID)
		rss.sendQueue.watcherHandleID = InvalidStoreStreamSendTokenHandleID
	}
	rss.returnDeductedFromSchedulerTokensLocked(ctx)
	rss.sendQueue.forceFlushScheduled = false
}

func (rss *replicaSendStream) returnDeductedFromSchedulerTokensLocked(ctx context.Context) {
	rss.mu.AssertHeld()
	if rss.sendQueue.deductedForScheduler.tokens > 0 {
		rss.parent.sendTokenCounter.Return(
			ctx, rss.sendQueue.deductedForScheduler.wc, rss.sendQueue.deductedForScheduler.tokens)
		rss.sendQueue.deductedForScheduler.tokens = 0
	}
}

// The replica is in StateReplicate, and has the provided info. It's previous state may
// have been any state (include replicate). Make the state consistent with info. Note that
// even if the previous state was replicate, we may not have seen all state transitions,
// or even if we see all state transitions, info.Match can jump ahead of indexToSend, because
// of old MsgApps arriving at the replica. So everything needs to be fixed up.
func (rss *replicaSendStream) makeConsistentInStateReplicateLocked(
	ctx context.Context, info FollowerStateInfo,
) {
	rss.mu.AssertHeld()
	defer rss.returnTokensUsingAdmittedLocked(ctx, info.Admitted)

	if rss.parent.parent.opts.LocalReplicaID == rss.parent.desc.ReplicaID {
		if rss.connectedState != replicate {
			log.Fatalf(ctx, "%v", errors.AssertionFailedf(
				"leader should always be in state replicate [send_stream=%v]",
				rss.stringLocked()))
		}
		if info.Match >= rss.sendQueue.indexToSend {
			log.Fatalf(ctx, "%v", errors.AssertionFailedf(
				"leader unexpectedly advanced Match [info=%v send_stream=%v]",
				info, rss.stringLocked()))
		}
		// info.Next can have advanced past rss.sendQueue.indexToSend since
		// leader operates in push mode for MsgStorageAppend.
		return
	}
	switch rss.connectedState {
	case replicate:
		if info.Match >= rss.sendQueue.indexToSend {
			// Some things got popped without us sending. Must be old
			// MsgAppResp. Next cannot have moved past Match, since Next used
			// to be equal to indexToSend.
			if info.Next != info.Match+1 {
				log.Fatalf(ctx, "%v", errors.AssertionFailedf(
					"next=%d != match+1=%d [info=%v send_stream=%v]",
					info.Next, info.Match+1, info, rss.stringLocked()))
			}
			rss.makeConsistentWhenUnexpectedPopLocked(ctx, info.Next)
		} else if info.Next == rss.sendQueue.indexToSend {
			// Everything is as expected.
		} else if info.Next > rss.sendQueue.indexToSend {
			// In pull-mode this can never happen (on a follower). We've
			// already covered the case where Next moves ahead, along with
			// Match earlier.

			log.Fatalf(ctx, "%v", errors.AssertionFailedf(
				"next=%d > index_to_send=%d [info=%v send_stream=%v]",
				info.Next, rss.sendQueue.indexToSend, info, rss.stringLocked()))
		} else {
			// info.Next < rss.sendQueue.indexToSend.
			//
			// Must have transitioned to StateProbe and back, and we did not
			// observe it.
			if info.Next != info.Match+1 {
				log.Fatalf(ctx, "%v", errors.AssertionFailedf(
					"next=%d != match+1=%d [info=%v send_stream=%v]",
					info.Next, info.Match+1, info, rss.stringLocked()))
			}
			rss.makeConsistentWhenProbeToReplicateLocked(ctx, info.Next)
		}
	case probeRecentlyReplicate:
		// Returned from StateProbe => StateReplicate.
		if info.Next != info.Match+1 {
			log.Fatalf(ctx, "%v", errors.AssertionFailedf(
				"next=%d != match+1=%d [info=%v send_stream=%v]",
				info.Next, info.Match+1, info, rss.stringLocked()))
		}
		rss.makeConsistentWhenProbeToReplicateLocked(ctx, info.Next)
	case snapshot:
		// Returned from StateSnapshot => StateReplicate
		if info.Next != info.Match+1 {
			log.Fatalf(ctx, "%v", errors.AssertionFailedf(
				"next=%d != match+1=%d [info=%v send_stream=%v]",
				info.Next, info.Match+1, info, rss.stringLocked()))
		}
		rss.makeConsistentWhenSnapshotToReplicateLocked(ctx, info.Next)
	}
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
	// Cancel watcher and return deductedForScheduler tokens. Will try again after
	// we've fixed up everything.
	if rss.sendQueue.watcherHandleID != InvalidStoreStreamSendTokenHandleID {
		rss.parent.parent.opts.SendTokensWatcher.CancelHandle(rss.sendQueue.watcherHandleID)
		rss.sendQueue.watcherHandleID = InvalidStoreStreamSendTokenHandleID
	}
	rss.returnDeductedFromSchedulerTokensLocked(ctx)
	rss.sendQueue.forceFlushScheduled = false

	// We have accurate stats for indices >= nextRaftIndexInitial. This unexpected pop
	// can't happen for any index >= nextRaftIndexInitial since these were proposed after
	// this replicaSendStream was created.
	if indexToSend > rss.nextRaftIndexInitial {
		log.Fatalf(ctx, "%v", errors.AssertionFailedf(
			"(arg)index_to_send=%d > next_raft_index_initial=%d [info=%v send_stream=%v]",
			indexToSend, rss.nextRaftIndexInitial,
			rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID),
			rss.stringLocked()))
	}
	// INVARIANT: indexToSend <= rss.nextRaftIndexInitial. Don't need to
	// change any stats.
	rss.sendQueue.indexToSend = indexToSend

	rss.startProcessingSendQueueLocked(ctx)
}

func (rss *replicaSendStream) makeConsistentWhenProbeToReplicate(
	ctx context.Context, indexToSend uint64,
) {
	rss.mu.Lock()
	defer rss.mu.Unlock()

	rss.makeConsistentWhenProbeToReplicateLocked(ctx, indexToSend)
}

func (rss *replicaSendStream) makeConsistentWhenProbeToReplicateLocked(
	ctx context.Context, indexToSend uint64,
) {
	rss.mu.AssertHeld()
	if rss.sendQueue.watcherHandleID != InvalidStoreStreamSendTokenHandleID {
		rss.parent.parent.opts.SendTokensWatcher.CancelHandle(rss.sendQueue.watcherHandleID)
		rss.sendQueue.watcherHandleID = InvalidStoreStreamSendTokenHandleID
	}
	rss.returnDeductedFromSchedulerTokensLocked(ctx)
	rss.sendQueue.forceFlushScheduled = false

	rss.connectedState = replicate
	if indexToSend == rss.sendQueue.indexToSend {
		// Queue state is already correct.
		rss.startProcessingSendQueueLocked(ctx)
		return
	}
	if indexToSend > rss.sendQueue.indexToSend {
		log.Fatalf(ctx, "%v", errors.AssertionFailedf(
			"(arg)index_to_send=%d > queue.index_to_send=%d [info=%v send_stream=%v]",
			indexToSend, rss.sendQueue.indexToSend,
			rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID),
			rss.stringLocked()))
	}
	// INVARIANT: indexToSend < rss.sendQueue.indexToSend.

	// A regression in indexToSend necessarily means a MsgApp constructed by
	// this replicaSendStream was dropped.
	if indexToSend < rss.indexToSendInitial {
		log.Fatalf(ctx, "%v", errors.AssertionFailedf(
			"did not construct MsgApp in this replicaSendStream"))
	}
	// The messages in [indexToSend, rss.sendQueue.indexToSend) must be in
	// the tracker. They can't have been removed since Match < indexToSend.
	// We will be resending these, so we should return the send tokens for
	// them. We don't need to adjust eval.tokensDeducted since even though we
	// are returning these send tokens, all of them are now in the send-queue,
	// and the eval.tokensDeducted includes the send-queue.
	rss.tracker.UntrackGE(indexToSend,
		func(index uint64, inheritedPri kvflowcontrolpb.RaftPriority, originalPri kvflowcontrolpb.RaftPriority,
			tokens kvflowcontrol.Tokens) {
			rss.sendQueue.priorityCount[originalPri]++
			rss.sendQueue.sizeSum += tokens
			inheritedWC := kvflowcontrolpb.WorkClassFromRaftPriority(inheritedPri)
			rss.parent.sendTokenCounter.Return(ctx, inheritedWC, tokens)

		})
	rss.sendQueue.indexToSend = indexToSend
	rss.startProcessingSendQueueLocked(ctx)
}

func (rss *replicaSendStream) makeConsistentWhenSnapshotToReplicateLocked(
	ctx context.Context, indexToSend uint64,
) {
	rss.mu.AssertHeld()
	if rss.sendQueue.nextRaftIndex < indexToSend {
		log.Fatalf(ctx, "%v", errors.AssertionFailedf(
			"next_raft_index=%d < (arg)index_to_send=%d [info=%v send_stream=%v]",
			rss.sendQueue.nextRaftIndex, indexToSend,
			rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID),
			rss.stringLocked()))
	}
	if rss.sendQueue.indexToSend > indexToSend {
		log.Fatalf(ctx, "%v", errors.AssertionFailedf(
			"index_to_send=%d > (arg)index_to_send=%d [info=%v send_stream=%v]",
			rss.sendQueue.indexToSend, indexToSend,
			rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID),
			rss.stringLocked()))
	}
	if rss.connectedState != snapshot {
		log.Fatalf(ctx, "%v", errors.AssertionFailedf(
			"connected_state=%v != snapshot [info=%v send_stream=%v]",
			rss.connectedState, rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID),
			rss.stringLocked()))
	}
	rss.connectedState = replicate
	// INVARIANT: rss.sendQueue.indexToSend <= indexToSend <=
	// rss.sendQueue.nextRaftIndex. Typically, both will be <, since we
	// transitioned to StateSnapshot since rss.sendQueue.indexToSend was
	// truncated, and there have likely been some proposed entries since the
	// snapshot was applied. So we will start off with some entries in the
	// send-queue.

	// NB: the tracker entries have already been returned in
	// changeToStateSnapshot. And so have the eval tokens. We have close to
	// fully emptied the send-queue and we don't want to iterate over the
	// remaining members to precisely figure out what to deduct from
	// eval-tokens, since that may require reading from storage.

	rss.indexToSendInitial = indexToSend
	rss.sendQueue.indexToSend = indexToSend
	totalCount := int64(0)
	for pri, count := range rss.sendQueue.priorityCount {
		if count > 0 {
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
		if totalCount == 0 {
			meanSizeBytes = rss.sendQueue.approxMeanSizeBytes
		} else {
			meanSizeBytes = kvflowcontrol.Tokens(0.9*float64(meanSizeBytes) + 0.1*float64(rss.sendQueue.approxMeanSizeBytes))
		}
	}
	rss.sendQueue.approxMeanSizeBytes = meanSizeBytes
	rss.sendQueue.sizeSum = 0
	rss.nextRaftIndexInitial = rss.sendQueue.nextRaftIndex
	rss.startProcessingSendQueueLocked(ctx)
}

func (rss *replicaSendStream) startProcessingSendQueueLocked(ctx context.Context) {
	rss.mu.AssertHeld()
	if rss.isEmptySendQueueLocked() {
		// Nothing to do.
		return
	}

	log.VInfof(ctx, 1,
		"replica=%d processing send_queue [info=%v send_stream=%v]",
		rss.parent.desc.ReplicaID,
		rss.parent.parent.opts.RaftInterface.FollowerState(rss.parent.desc.ReplicaID),
		rss.stringLocked())

	rss.notifyLocked(ctx)
	if rss.sendQueue.deductedForScheduler.tokens == 0 {
		// Weren't able to deduct any tokens.
		if rss.sendQueue.watcherHandleID == InvalidStoreStreamSendTokenHandleID {
			// This must be the first time we tried to process the send queue. We
			// weren't able to deduct any tokens so create a handle to wait on for
			// available send tokens. Otherwise, we already have a handle.
			rss.sendQueue.watcherHandleID = rss.parent.parent.opts.SendTokensWatcher.NotifyWhenAvailable(
				rss.parent.sendTokenCounter, admissionpb.ElasticWorkClass, rss)
		}
	} else if scheduleAgain := rss.scheduledLocked(ctx); scheduleAgain {
		rss.parent.parent.scheduleReplica(rss.parent.desc.ReplicaID)
	}
}

func (rss *replicaSendStream) returnTokensUsingAdmittedLocked(
	ctx context.Context, admitted [kvflowcontrolpb.NumRaftPriorities]uint64,
) {
	rss.mu.AssertHeld()
	for pri, uptoIndex := range admitted {
		rss.returnTokensForPri(ctx, kvflowcontrolpb.RaftPriority(pri), uptoIndex)
	}
}

func (rss *replicaSendStream) returnTokensForPri(
	ctx context.Context, pri kvflowcontrolpb.RaftPriority, uptoIndex uint64,
) {
	rss.mu.AssertHeld()
	rss.tracker.Untrack(pri, uptoIndex,
		func(index uint64, originalPri kvflowcontrolpb.RaftPriority, tokens kvflowcontrol.Tokens) {
			wc := kvflowcontrolpb.WorkClassFromRaftPriority(pri)
			originalWC := kvflowcontrolpb.WorkClassFromRaftPriority(originalPri)
			if index >= rss.nextRaftIndexInitial {
				rss.eval.tokensDeducted[originalWC] -= tokens
				rss.parent.evalTokenCounter.Return(ctx, originalWC, tokens)
			}
			log.VInfof(ctx, 2, "returnTokensForPri(%v): wc=%v original_wc=%v index=%v tokens=%v",
				rss.parent.desc.ReplicaID, wc, originalWC, index, tokens)
			rss.parent.sendTokenCounter.Return(ctx, wc, tokens)
		})
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

func (cs connectedState) String() string {
	switch cs {
	case replicate:
		return "replicate"
	case probeRecentlyReplicate:
		return "probe_recently_replicate"
	case snapshot:
		return "snapshot"
	default:
		panic(fmt.Sprintf("unknown connected_state %d", cs))
	}
}

func (cs connectedState) shouldWaitForElasticEvalTokens() bool {
	return cs == replicate || cs == probeRecentlyReplicate
}

// UseRACv2 is used to exercise integration. The integration in the prototype
// does not have any capability to switch a range from RACv1 to RACv2.
const UseRACv2 = true
