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

import pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"

type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)

var emptyState = pb.HardState{}

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	RaftState pb.StateType
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.RaftState == b.RaftState
}

// StorageAppend describes a storage append request. It contains the updates
// that must be written to storage atomically, such as new HardState, snapshot,
// and/or log entries. When the updates are durable on the log storage, the
// attached messages can be sent.
type StorageAppend struct {
	// The current HardState to be saved to stable storage. Empty if there is no
	// update to the HardState.
	pb.HardState
	// Entries contains the log entries to be appended to the log in storage.
	// Empty if there are no new entries.
	//
	// Invariant: Entries[0].Index <= LogStorage.LastIndex() + 1.
	//
	// All entries >= Entry[0].Index, if any, must be truncated from the log, and
	// Entries are written to replace them.
	Entries []pb.Entry
	// Snapshot is the snapshot to be saved to stable storage. Empty if there is
	// no snapshot.
	//
	// When installing a snapshot, the raft log must be cleared and initialized to
	// a state according to the Snapshot.Metadata.{Index,Term}.
	Snapshot *pb.Snapshot
	// Mark is the log mark that identifies this storage write. Populated if
	// Entries or Snapshot is not empty. A non-empty Mark never regresses.
	Mark LogMark
	// Responses contains messages that should be sent AFTER the updates above
	// have been *durably* persisted in log storage. Messages addressed to the
	// local RawNode can be stepped into it directly.
	Responses []pb.Message
}

// Empty returns true iff the storage append is empty.
func (m *StorageAppend) Empty() bool {
	return IsEmptyHardState(m.HardState) &&
		len(m.Entries) == 0 && m.Snapshot == nil && len(m.Responses) == 0
}

// NeedAck returns true if the RawNode wants to be notified after the writes are
// durable on the log storage.
func (m *StorageAppend) NeedAck() bool {
	return len(m.Entries) != 0 || m.Snapshot != nil
}

// ToMessage converts the StorageAppend to a legacy pb.MsgStorageAppend.
// TODO(pav-kv): remove this.
func (m *StorageAppend) ToMessage(self pb.PeerID) pb.Message {
	return pb.Message{
		Type:      pb.MsgStorageAppend,
		From:      self,
		To:        LocalAppendThread,
		Term:      m.Term,
		Vote:      m.Vote,
		Commit:    m.Commit,
		Lead:      m.Lead,
		LeadEpoch: m.LeadEpoch,
		Entries:   m.Entries,
		LogTerm:   m.Mark.Term,
		Index:     m.Mark.Index,
		Snapshot:  m.Snapshot,
		Responses: m.Responses,
	}
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// StorageAppend contains a write request that the application must eventually
	// apply to the storage, and acknowledge to RawNode once it is durable.
	StorageAppend

	// Committed is the log span that has been committed and can be applied to the
	// state machine. Two subsequently accepted committed spans are contiguous,
	// except after a snapshot which can create a "gap" in this sequence.
	//
	// The caller should use RawNode.LogSnapshot() to fetch the committed entries
	// from the log and apply them to the state machine. When a batch of entries
	// has been applied, the caller should call RawNode.AckApplied, to prevent
	// these entries from showing up in subsequent Ready signals.
	//
	// The caller can also only confirm obligation to apply entries, using the
	// RawNode.AckApplying(index) call, which stops committed indices <= index
	// from causing other Ready events, but the entries are still not considered
	// applied by raft.
	//
	// Invariants:
	//	- Committed.After <= Committed.Last
	//	- Committed.After == last index previously accepted for application
	//	- Committed.After == snapshot.Index after a snapshot
	//	- Committed.Last <= committed index known to the RawNode
	//
	// Committed.Last < committed index if the latest committed entries are not
	// yet durable in the log.
	// TODO(pav-kv): reconsider if we can relax this to always == committed index.
	Committed pb.LogSpan

	// Messages specifies outbound messages.
	//
	// If async storage writes are not enabled, these messages must be sent
	// AFTER Entries are appended to stable storage.
	//
	// If async storage writes are enabled, these messages can be sent
	// immediately as the messages that have the completion of the async writes
	// as a precondition are attached to the individual MsgStorage{Append,Apply}
	// messages instead.
	//
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	//
	// TODO(pav-kv): remove MsgStorageAppend from this slice, and allow all these
	// messages to be sent immediately.
	Messages []pb.Message
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a == b
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, emptyState)
}

type Peer struct {
	ID      pb.PeerID
	Context []byte
}

func confChangeToMsg(c pb.ConfChangeI) (pb.Message, error) {
	typ, data, err := pb.MarshalConfChange(c)
	if err != nil {
		return pb.Message{}, err
	}
	return pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: typ, Data: data}}}, nil
}
