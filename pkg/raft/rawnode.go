// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/errors"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.trk for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// RawNode is a thread-unsafe Node.
// The methods of this struct correspond to the methods of Node and are described
// more fully there.
type RawNode struct {
	raft *raft
	// Mutable fields.
	prevSoftSt *SoftState
	prevHardSt pb.HardState
}

// NewRawNode instantiates a RawNode from the given configuration.
//
// See Bootstrap() for bootstrapping an initial state; this replaces the former
// 'peers' argument to this method (with identical behavior). However, It is
// recommended that instead of calling Bootstrap, applications bootstrap their
// state manually by setting up a Storage that has a first index > 1 and which
// stores the desired ConfState as its InitialState.
func NewRawNode(config *Config) (*RawNode, error) {
	r := newRaft(config)
	rn := &RawNode{
		raft: r,
	}
	ss := r.softState()
	rn.prevSoftSt = &ss
	rn.prevHardSt = r.hardState()
	return rn, nil
}

// Tick advances the internal logical clock by a single tick. Election timeouts
// and heartbeat timeouts are in units of ticks.
func (rn *RawNode) Tick() {
	rn.raft.tick()
}

// Campaign causes this RawNode to transition to candidate state and start
// campaigning to become leader.
func (rn *RawNode) Campaign() error {
	return rn.raft.Step(pb.Message{
		Type: pb.MsgHup,
	})
}

// Propose proposes an entry with the given data to be appended to the raft log.
//
// Returns ErrProposalDropped if the proposal could not be made. A few reasons
// why this can happen:
//
//   - the RawNode is not the leader, and DisableProposalForwarding is true
//   - the RawNode is transferring the leadership away
//   - the proposal overflows the internal size limits
//   - the proposal is incorrect for other reasons
//
// If the proposal is submitted, it can still be lost or not committed, e.g. due
// to a racing leader change. As such, the user may need to retry the proposal
// even if no error is returned here.
func (rn *RawNode) Propose(data []byte) error {
	return rn.raft.Step(pb.Message{
		Type: pb.MsgProp,
		From: rn.raft.id,
		Entries: []pb.Entry{
			{Data: data},
		}})
}

// ProposeConfChange proposes a config change. Like any proposal, the config
// change may be dropped with or without an error being returned (see the
// Propose() method). In addition, config changes are dropped unless the leader
// has certainty that there is no prior unapplied config change in its log.
//
// The method accepts either a pb.ConfChange (deprecated) or pb.ConfChangeV2
// message. The latter allows arbitrary config changes via joint consensus,
// notably including replacing a voter. Passing a ConfChangeV2 is only allowed
// if all nodes participating in the cluster run a version of this library aware
// of the V2 API. See pb.ConfChangeV2 for usage details and semantics.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChangeI) error {
	m, err := confChangeToMsg(cc)
	if err != nil {
		return err
	}
	return rn.raft.Step(m)
}

func confChangeToMsg(c pb.ConfChangeI) (pb.Message, error) {
	typ, data, err := pb.MarshalConfChange(c)
	if err != nil {
		return pb.Message{}, err
	}
	return pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: typ, Data: data}}}, nil
}

// ApplyConfChange applies a config change to this node. This must be called
// whenever a config change is observed in Ready.CommittedEntries, except when
// the app decides to reject / no-op this change.
//
// Returns an opaque non-nil ConfState protobuf which must be recorded in
// snapshots.
// TODO(pav-kv): we don't use the returned value, see if it can be removed.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState {
	cs := rn.raft.applyConfChange(cc.AsV2())
	return &cs
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// Ignore unexpected local messages receiving over network.
	if IsLocalMsg(m.Type) {
		return ErrStepLocalMsg
	}
	if IsResponseMsg(m.Type) && rn.raft.trk.Progress(m.From) == nil {
		return ErrStepPeerNotFound
	}
	return rn.raft.Step(m)
}

// SetLazyReplication enables or disables the lazy MsgApp replication for
// StateReplicate flows. See Config.LazyReplication which defines the initial
// value of this setting - the semantics are explained in its comment.
//
// When the lazy mode flips to enabled, there might be MsgApp messages in the
// RawNode's message queue which were previously sent eagerly from within the
// RawNode. These will be extracted with the next Ready handling cycle. If this
// call is placed immediately after the Ready() call, there are no outstanding
// MsgApp messages in the queue, and there won't be any in the future (except
// the probes).
//
// When the lazy mode flips to disabled, RawNode scans the peers and may put
// MsgApp messages into the queue immediately. These will be extracted with the
// next Ready handling cycle.
func (rn *RawNode) SetLazyReplication(lazy bool) {
	r := rn.raft
	if r.lazyReplication == lazy {
		return
	}
	r.lazyReplication = lazy
	if lazy {
		// The lazy replication mode was enabled. There is nothing to do. From now
		// on, MsgApp messages for StateReplicate peers are constructed using the
		// SendMsgApp method.
		return
	}
	// The lazy mode was disabled. We need to check whether any replication flows
	// are unblocked and can be saturated.
	if r.state == pb.StateLeader {
		// TODO(pav-kv): this sends at most one MsgApp message per peer. It may not
		// completely saturate the flow. Consider looping while maybeSendAppend()
		// returns true.
		r.bcastAppend()
	}
}

// LogSnapshot returns a point-in-time read-only state of the raft log.
//
// The returned snapshot can be read from while RawNode continues operation, as
// long as the application guarantees immutability of the underlying log storage
// snapshot (returned from the LogStorage.LogSnapshot method) while the snapshot
// is being used.
//
// One way the application can implement an immutable snapshot is by blocking
// the entire log storage for new writes. This also means the Ready() handling
// loop isn't able to hand over log writes to storage.
//
// A more advanced implementation can grab an immutable storage engine snapshot
// that does not block writes. Not blocking writes is beneficial for commit tail
// latency, since it doesn't prevent MsgApp/Resp exchange with the leader.
func (rn *RawNode) LogSnapshot() LogSnapshot {
	return rn.raft.raftLog.snap(rn.raft.raftLog.storage.LogSnapshot())
}

// SendMsgApp conditionally sends a MsgApp message containing the given log
// slice to the given peer. The message is returned to the caller, who is
// responsible for actually sending it. The RawNode only updates the internal
// state to reflect the fact that it was sent.
//
// The message can be sent only if all the conditions are true:
//   - this node is the leader of term to which the slice corresponds
//   - the given peer exists
//   - the replication flow to the given peer is in StateReplicate
//   - the first slice index matches the Next index to send to this peer
//
// Returns false if the message can not be sent.
func (rn *RawNode) SendMsgApp(to pb.PeerID, slice LeadSlice) (pb.Message, bool) {
	return rn.raft.maybePrepareMsgApp(to, slice)
}

// Ready returns the outstanding work that the application needs to handle. This
// includes appending entries to the log, applying committed entries or a
// snapshot, updating the HardState, and sending messages. See comments in the
// Ready struct for the specification on how the updates must be handled.
func (rn *RawNode) Ready() Ready {
	r := rn.raft
	var rd Ready
	rd.Messages, r.msgs = r.msgs, nil

	if softSt := r.softState(); !softSt.equal(rn.prevSoftSt) {
		// Allocate only when SoftState changes.
		escapingSoftSt := softSt
		rd.SoftState = &escapingSoftSt
		rn.prevSoftSt = &escapingSoftSt
	}
	// For async storage writes, enqueue messages to local storage threads.
	if rn.needStorageAppend() {
		rd.StorageAppend = rn.sendStorageAppend()
	}
	rd.Committed = r.raftLog.nextCommittedSpan(rn.applyUnstableEntries())

	return rd
}

func (rn *RawNode) needStorageAppend() bool {
	r := rn.raft
	// Return true if log entries, HardState, or a snapshot need to be written to
	// stable storage. Also return true if any messages are contingent on all
	// prior storage writes being durable.
	return r.raftLog.hasNextUnstableEnts() ||
		r.raftLog.hasNextUnstableSnapshot() ||
		len(r.msgsAfterAppend) > 0 ||
		!isHardStateEqual(r.hardState(), rn.prevHardSt)
}

// sendStorageAppend creates the write request that is sent to the local
// storage. The request also carries a set of messages that should be sent after
// the writes in the request are durable.
//
// The StorageAppend is immediately "accepted" by the caller, so the next
// StorageAppend will build on top of it.
func (rn *RawNode) sendStorageAppend() StorageAppend {
	r := rn.raft
	app := StorageAppend{Entries: r.raftLog.nextUnstableEnts()}
	if hs := r.hardState(); !isHardStateEqual(hs, rn.prevHardSt) {
		app.HardState = hs
		rn.prevHardSt = hs
	}
	// See StorageAppend.Mark() comment for why the accTerm() is attached.
	if snap := r.raftLog.nextUnstableSnapshot(); snap != nil {
		app.Snapshot = snap
		app.LeadTerm = r.raftLog.accTerm()
	}
	if ln := len(app.Entries); ln > 0 {
		app.LeadTerm = r.raftLog.accTerm()
	}
	r.raftLog.acceptUnstable()
	// Attach all messages in msgsAfterAppend as responses to be delivered after
	// the write is durable. The messages are stepped into RawNode right
	// before it handles StorageAppendAck.
	//
	// NB: it is important for performance that MsgStorageAppendAck is handled
	// after self-directed MsgAppResp messages on the leader (which will be
	// contained in msgsAfterAppend). This ordering allows the MsgAppResp handling
	// to use a fast-path in r.raftLog.term() before the newly appended entries
	// are removed from the unstable log.
	// TODO(pav-kv): this is no longer critical, after the termCache introduction.
	app.Responses, r.msgsAfterAppend = r.msgsAfterAppend, nil
	return app
}

// AckAppend notifies the RawNode that the storage write has been persisted.
func (rn *RawNode) AckAppend(ack StorageAppendAck) {
	r := rn.raft
	for msg := range ack.Step(r.id) {
		if err := rn.Step(msg); err != nil {
			r.logger.Fatalf("failed to step local raft message [%s]: %v",
				DescribeMessage(msg, emptyEntryFormatter), err)
		}
	}
	// The snapshot precedes the entries. We acknowledge the snapshot first, then
	// the entries, as required by the unstable structure.
	if ack.SnapIndex != 0 {
		r.appliedSnap(ack.SnapIndex)
	}
	if ack.Mark.Index > ack.SnapIndex {
		r.raftLog.stableTo(ack.Mark)
	}
}

// AckApplying accepts all committed entries <= index as being applied to the
// state machine. The caller gives a promise to eventually apply these entries
// and call AckApplied to confirm. They can do so asynchronously, while this
// RawNode keeps making progress.
//
// It is allowed to never call AckApplying, and call AckApplied straight away.
// Technically, AckApplying only prevents committed indices <= index from
// causing Ready signals.
//
// Requires: all AckApplying calls must have increasing indices.
// Requires: index <= Ready.Committed.Last. That is, the caller can only accept
// "apply-able" entries that it learns about from Ready().
func (rn *RawNode) AckApplying(index pb.Index) {
	rn.raft.raftLog.acceptApplying(uint64(index))
}

// AckApplied acknowledges that the given entries have been applied. Must be
// called for every span of applied entries, in order. If the caller chose to
// apply entries asynchronously, they should synchronize the order of these
// calls with applying snapshots.
func (rn *RawNode) AckApplied(entries []pb.Entry) {
	if len(entries) == 0 {
		return
	}
	rn.raft.appliedTo(entries[len(entries)-1].Index)
	rn.raft.reduceUncommittedSize(payloadsSize(entries))
}

// applyUnstableEntries returns whether entries are allowed to be applied once
// they are known to be committed but before they have been written locally to
// stable storage.
func (rn *RawNode) applyUnstableEntries() bool {
	return rn.raft.testingKnobs.ApplyUnstableEntries()
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// TODO(nvanbenschoten): order these cases in terms of cost and frequency.
	r := rn.raft
	if softSt := r.softState(); !softSt.equal(rn.prevSoftSt) {
		return true
	}
	if rn.needStorageAppend() {
		return true
	}
	if !r.raftLog.nextCommittedSpan(rn.applyUnstableEntries()).Empty() {
		return true
	}
	return len(r.msgs) > 0
}

// Term returns the current in-memory term of this RawNode. This term may not
// yet have been persisted in storage.
func (rn *RawNode) Term() uint64 {
	return rn.raft.Term
}

// State returns the current role of the RawNode.
func (rn *RawNode) State() pb.StateType {
	return rn.raft.state
}

// Lead returns the leader of Term(), or None if the leader is unknown.
//
// NB: it is possible that Lead() returns this node's ID, yet State() does not
// return StateLeader. It means this node was the leader, but it has stepped
// down. If the caller needs to know whether this node is acting as the leader,
// it should check the State() instead of Lead() == ID.
func (rn *RawNode) Lead() pb.PeerID {
	return rn.raft.lead
}

// LogMark returns the current log mark of the raft log. It is not guaranteed to
// be in stable storage, unless this method is called right after RawNode is
// initialized (in which case its state reflects the stable storage).
func (rn *RawNode) LogMark() LogMark {
	return rn.raft.raftLog.unstable.mark()
}

// NextUnstableIndex returns the index of the next entry that will be sent to
// local storage, if there are any. All entries < this index are either stored,
// or have been sent to storage.
//
// NB: NextUnstableIndex can regress when the node accepts appends or snapshots
// from a newer leader.
func (rn *RawNode) NextUnstableIndex() uint64 {
	return rn.raft.raftLog.unstable.entryInProgress + 1
}

// SendPing sends a MsgApp ping to the given peer, if it is in StateReplicate
// and there was no recent MsgApp to this peer.
//
// Returns true if the ping was added to the message queue.
func (rn *RawNode) SendPing(to pb.PeerID) bool {
	return rn.raft.sendPing(to)
}

// Status returns the current status of the given group. This allocates, see
// SparseStatus, BasicStatus and WithProgress for allocation-friendlier choices.
func (rn *RawNode) Status() Status {
	status := getStatus(rn.raft)
	return status
}

// BasicStatus returns a BasicStatus. Notably this does not contain the
// Progress map; see WithProgress for an allocation-free way to inspect it.
func (rn *RawNode) BasicStatus() BasicStatus {
	return getBasicStatus(rn.raft)
}

// SparseStatus returns a SparseStatus. Notably, it doesn't include Config and
// Progress.Inflights, which are expensive to copy.
func (rn *RawNode) SparseStatus() SparseStatus {
	return getSparseStatus(rn.raft)
}

// ReplicaProgress returns the progress for the replica with the given ID.
// It returns nil if the replica is not being tracked.
func (rn *RawNode) ReplicaProgress(id pb.PeerID) *tracker.Progress {
	return getReplicaProgress(rn.raft, id)
}

// SupportingFortifiedLeader indicates if this peer supports a fortified leader.
func (rn *RawNode) SupportingFortifiedLeader() bool {
	return rn.raft.supportingFortifiedLeader()
}

// WithProgress is a helper to introspect the Progress for this node and its
// peers.
func (rn *RawNode) WithProgress(visitor func(id pb.PeerID, pr tracker.Progress)) {
	withProgress(rn.raft, visitor)
}

// WithBasicProgress is a helper to introspect the BasicProgress for this node
// and its peers.
func (rn *RawNode) WithBasicProgress(visitor func(id pb.PeerID, pr tracker.BasicProgress)) {
	rn.raft.trk.WithBasicProgress(visitor)
}

// ReportUnreachable reports the given node is not reachable for the last send.
func (rn *RawNode) ReportUnreachable(id pb.PeerID) {
	_ = rn.raft.Step(pb.Message{Type: pb.MsgUnreachable, From: id})
}

// ReportSnapshot reports the status of the snapshot sent to the given peer.
//
// Any failure in sending a snapshot (e.g. while streaming it from leader to
// follower) must be reported to the leader with SnapshotFailure.
//
// When the leader sends a snapshot to a peer, it pauses log replication until
// this peer can apply the snapshot and advance its state. If the peer can't do
// that, e.g. due to a crash, it could end up in a limbo, never getting any
// updates from the leader. Therefore, it is crucial that the app catches any
// failure in sending a snapshot and reports it back to the leader, so that the
// log replication probing can resume. In case of uncertainty, the app must err
// on the side of reporting SnapshotFailure.
//
// A successful snapshot must be reported with SnapshotFinish or a MsgAppResp
// from the peer. It is advisory to report SnapshotFinish in success cases,
// regardless of the MsgAppResp.
func (rn *RawNode) ReportSnapshot(id pb.PeerID, status SnapshotStatus) {
	rej := status == SnapshotFailure

	_ = rn.raft.Step(pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej})
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee pb.PeerID) {
	_ = rn.raft.Step(pb.Message{Type: pb.MsgTransferLeader, From: transferee})
}

// ForgetLeader forgets a follower's current leader, changing it to None. It
// remains a leaderless follower in the current term, without campaigning.
//
// This is useful with PreVote+CheckQuorum, where followers will normally not
// grant pre-votes if they've heard from the leader in the past election timeout
// interval. Leaderless followers can grant pre-votes immediately, so if a
// quorum of followers have strong reason to believe the leader is dead (for
// example via a side-channel or external failure detector) and forget it then
// they can elect a new leader immediately, without waiting out the election
// timeout. They will also revert to normal followers if they hear from the
// leader again, or transition to candidates on an election timeout.
//
// For example, consider a three-node cluster where 1 is the leader and 2+3 have
// just received a heartbeat from it. If 2 and 3 believe the leader has now died
// (maybe they know that an orchestration system shut down 1's VM), we can
// instruct 2 to forget the leader and 3 to campaign. 2 will then be able to
// grant 3's pre-vote and elect 3 as leader immediately (normally 2 would reject
// the vote until an election timeout passes because it has heard from the
// leader recently). However, 3 can not campaign unilaterally, a quorum have to
// agree that the leader is dead, which avoids disrupting the leader if
// individual nodes are wrong about it being dead.
func (rn *RawNode) ForgetLeader() error {
	return rn.raft.Step(pb.Message{Type: pb.MsgForgetLeader})
}

func (rn *RawNode) TestingStepDown() error {
	return rn.raft.testingStepDown()
}

func (rn *RawNode) TestingFortificationStateString() string {
	return rn.raft.fortificationTracker.String()
}

func (rn *RawNode) TestingSendDeFortify(id pb.PeerID) error {
	return rn.raft.testingSendDeFortify(id)
}
