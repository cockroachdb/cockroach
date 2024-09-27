package rafttype

import "fmt"

// NOTE: This code is a derivative of the raftpb package. If anything is added
// to the protobuf definition it should be manually added here as well. The
// primary difference is that this package has a Context attached to messages.
type PeerID uint64

// Epoch is an epoch in the Store Liveness fabric, referencing an uninterrupted
// period of support from one store to another.
type Epoch int64

type EntryType int32

func (t EntryType) String() string {
	return fmt.Sprintf("EntryType(%d)", t)
}

const (
	EntryNormal       EntryType = 0
	EntryConfChange   EntryType = 1
	EntryConfChangeV2 EntryType = 2
)

type MessageType int32

func (t MessageType) String() string {
	return fmt.Sprintf("MessageType(%d)", t)
}

const (
	MsgHup               MessageType = 0
	MsgBeat              MessageType = 1
	MsgProp              MessageType = 2
	MsgApp               MessageType = 3
	MsgAppResp           MessageType = 4
	MsgVote              MessageType = 5
	MsgVoteResp          MessageType = 6
	MsgSnap              MessageType = 7
	MsgHeartbeat         MessageType = 8
	MsgHeartbeatResp     MessageType = 9
	MsgUnreachable       MessageType = 10
	MsgSnapStatus        MessageType = 11
	MsgCheckQuorum       MessageType = 12
	MsgTransferLeader    MessageType = 13
	MsgTimeoutNow        MessageType = 14
	MsgPreVote           MessageType = 17
	MsgPreVoteResp       MessageType = 18
	MsgStorageAppend     MessageType = 19
	MsgStorageAppendResp MessageType = 20
	MsgStorageApply      MessageType = 21
	MsgStorageApplyResp  MessageType = 22
	MsgForgetLeader      MessageType = 23
	MsgFortifyLeader     MessageType = 24
	MsgFortifyLeaderResp MessageType = 25
)

type ConfState struct {
	// The voters in the incoming config. (If the configuration is not joint,
	// then the outgoing config is empty).
	Voters []PeerID
	// The learners in the incoming config.
	Learners []PeerID
	// The voters in the outgoing config.
	VotersOutgoing []PeerID
	// The peers that will become learners when the outgoing config is removed.
	// These peers are necessarily currently in voters (or they would have been
	// added to the incoming config right away).
	LearnersNext []PeerID
	// If set, the config is joint and Raft will automatically transition into
	// the final config (i.e. remove the outgoing config) when this is safe.
	AutoLeave bool
}
type Snapshot struct {
	Data     []byte
	Metadata SnapshotMetadata
}
type SnapshotMetadata struct {
	ConfState ConfState
	Index     uint64
	Term      uint64
}

type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

func (e Entry) String() string {
	return fmt.Sprintf("Entry(Term:%d, Index:%d, Type:%s)", e.Term, e.Index, e.Type)
}

type Message struct {
	Type MessageType
	To   PeerID
	From PeerID
	Term uint64
	// logTerm is generally used for appending Raft logs to followers. For
	// example, (type=MsgApp,index=100,logTerm=5) means the leader appends entries
	// starting at index=101, and the term of the entry at index 100 is 5.
	// (type=MsgAppResp,reject=true,index=100,logTerm=5) means follower rejects
	// some entries from its leader as it already has an entry with term 5 at
	// index 100. (type=MsgStorageAppendResp,index=100,logTerm=5) means the local
	// node wrote entries up to index=100 in stable storage, and the term of the
	// entry at index 100 was 5. This doesn't always mean that the corresponding
	// MsgStorageAppend message was the one that carried these entries, just that
	// those entries were stable at the time of processing the corresponding
	// MsgStorageAppend.
	LogTerm   uint64
	Index     uint64
	Entries   []Entry
	Commit    uint64
	Lead      PeerID
	LeadEpoch Epoch
	// (type=MsgStorageAppend,vote=5,term=10) means the local node is voting for
	// peer 5 in term 10. For MsgStorageAppends, the term, vote, lead, leadEpoch,
	// and commit fields will either all be set (to facilitate the construction of
	// a HardState) if any of the fields have changed or will all be unset if none
	// of the fields have changed.
	Vote PeerID
	// snapshot is non-nil and non-empty for MsgSnap messages and nil for all
	// other message types. However, peer nodes running older binary versions may
	// send a non-nil, empty value for the snapshot field of non-MsgSnap messages.
	// Code should be prepared to handle such messages.
	Snapshot   *Snapshot
	Reject     bool
	RejectHint uint64
	Context    []byte
	// responses are populated by a raft node to instruct storage threads on how
	// to respond and who to respond to when the work associated with a message
	// is complete. Populated for MsgStorageAppend and MsgStorageApply messages.
	Responses []Message
	// match is the log index up to which the follower's log must persistently
	// match the leader's. If the follower's persistent log is shorter, it means
	// the follower has broken its promise and violated safety of Raft. Typically
	// this means the environment (Storage) hasn't provided the required
	// durability guarantees.
	//
	// If a follower sees a match index exceeding its log's last index, it must
	// cease its membership (stop voting and acking appends) in the raft group, in
	// order to limit the damage. Today it simply panics.
	//
	// match is only populated by the leader when sending messages to a voting
	// follower. This can be 0 if the leader hasn't yet established the follower's
	// match index, or for backward compatibility.
	Match uint64
}

type HardState struct {
	Term      uint64
	Vote      PeerID
	Commit    uint64
	Lead      PeerID
	LeadEpoch Epoch
}

//TODO: All this should be simplified to just use ConfChangeV2.

type ConfChangeType int32

const (
	ConfChangeAddNode        ConfChangeType = 0
	ConfChangeRemoveNode     ConfChangeType = 1
	ConfChangeUpdateNode     ConfChangeType = 2
	ConfChangeAddLearnerNode ConfChangeType = 3
)

type ConfChange struct {
	Type    ConfChangeType
	NodeID  PeerID
	Context []byte
	// NB: this is used only by etcd to thread through a unique identifier.
	// Ideally it should really use the Context instead. No counterpart to
	// this field exists in ConfChangeV2.
	ID uint64
}

func (c ConfChangeType) String() string {
	return fmt.Sprintf("ConfChangeType(%d)", c)
}

// ConfChangeSingle is an individual configuration change operation. Multiple
// such operations can be carried out atomically via a ConfChangeV2.
type ConfChangeSingle struct {
	Type   ConfChangeType
	NodeID PeerID
}
type ConfChangeV2 struct {
	Transition ConfChangeTransition
	Changes    []ConfChangeSingle
	Context    []byte
}

// ConfChangeTransition specifies the behavior of a configuration change with
// respect to joint consensus.
type ConfChangeTransition int32

const (
	// Automatically use the simple protocol if possible, otherwise fall back
	// to ConfChangeJointImplicit. Most applications will want to use this.
	ConfChangeTransitionAuto ConfChangeTransition = 0
	// Use joint consensus unconditionally, and transition out of them
	// automatically (by proposing a zero configuration change).
	//
	// This option is suitable for applications that want to minimize the time
	// spent in the joint configuration and do not store the joint configuration
	// in the state machine (outside of InitialState).
	ConfChangeTransitionJointImplicit ConfChangeTransition = 1
	// Use joint consensus and remain in the joint configuration until the
	// application proposes a no-op configuration change. This is suitable for
	// applications that want to explicitly control the transitions, for example
	// to use a custom payload (via the Context field).
	ConfChangeTransitionJointExplicit ConfChangeTransition = 2
)
