package rafttype

import (
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/redact"
)

func (e *Entry) Size() int {
	return (&raftpb.Entry{Term: e.Term, Index: e.Index, Type: raftpb.EntryType(e.Type), Data: e.Data}).Size()
}

//TODO: This probably doesn't belong in Raft.

// Priority specifies per-entry priorities, that are local to the interaction
// between a leader-replica pair, i.e., they are not an invariant of a
// particular entry in the raft log (the replica could be the leader itself or
// a follower). There are four priorities, ranging from low to high. These
// form the foundation for a priority based admission control sub-system, for
// the replication layer, where the functionality is split between the Raft
// layer and higher layer that exercises Raft. We discuss Raft's
// responsibility in the functionality below.
//
// Raft is not concerned with how the higher layer at the leader assigns
// priorities, or communicates them to a replica.
//
// Raft provides (a) tracking of an Admitted vector (one element per priority,
// for each replica), (b) pinging for followers (using MsgApps) when Admitted
// vector elements are lagging behind Match, (c) (for followers) piggy-backing
// Admitted vectors on MsgApp and MsgAppResp to allow the leader to converge
// to the follower's state, (d) method for a replica to advance the value of
// the Admitted vector.
//
// (d) is the entry point for the higher layer to participate in the liveness
// of Admitted, but the nitty-gritty details of liveness are handled by Raft.
//
// Note that even though the priorities are per-entry on a leader-replica
// pair, we expect the higher layer to advance Admitted for all priorities.
// That is, if Admitted[LowPri]=10 and entries 11, 12 are assigned HighPri, it
// is the responsibility of the higher layer to set Admitted[LowPri]=12,
// without waiting for future LowPri entries to arrive.
type Priority uint8

const (
	LowPri Priority = iota
	NormalPri
	AboveNormalPri
	HighPri
	NumPriorities
)

func (p Priority) String() string {
	return redact.StringWithoutMarkers(p)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (p Priority) SafeFormat(w redact.SafePrinter, _ rune) {
	switch p {
	case LowPri:
		w.Printf("LowPri")
	case NormalPri:
		w.Printf("NormalPri")
	case AboveNormalPri:
		w.Printf("AboveNormalPri")
	case HighPri:
		w.Printf("HighPri")
	default:
		panic("invalid raft priority")
	}
}

func ConvertMessage(message raftpb.Message) Message {
	return Message{
		Type:       MessageType(message.Type),
		To:         PeerID(message.To),
		From:       PeerID(message.From),
		Term:       message.Term,
		LogTerm:    message.LogTerm,
		Index:      message.Index,
		Entries:    convertEntries(message.Entries),
		Commit:     message.Commit,
		Snapshot:   convertSnapshot(message.Snapshot),
		Reject:     message.Reject,
		RejectHint: message.RejectHint,
		Context:    message.Context,
	}
}

func convertEntries(entries []raftpb.Entry) []Entry {
	result := make([]Entry, len(entries))
	for i, entry := range entries {
		result[i] = Entry{
			Term:  entry.Term,
			Index: entry.Index,
			Type:  EntryType(entry.Type),
			Data:  entry.Data,
		}
	}
	return result
}

func convertSnapshot(snapshot *raftpb.Snapshot) *Snapshot {
	if snapshot == nil {
		return nil
	}
	return &Snapshot{
		Data:     snapshot.Data,
		Metadata: convertSnapshotMetadata(snapshot.Metadata),
	}
}

func convertSnapshotMetadata(metadata raftpb.SnapshotMetadata) SnapshotMetadata {
	return SnapshotMetadata{
		ConfState: convertConfState(metadata.ConfState),
		Index:     metadata.Index,
		Term:      metadata.Term,
	}
}

func convertConfState(confState raftpb.ConfState) ConfState {
	return ConfState{
		Voters:         convertPeers(confState.Voters),
		Learners:       convertPeers(confState.Learners),
		VotersOutgoing: convertPeers(confState.VotersOutgoing),
		LearnersNext:   convertPeers(confState.LearnersNext),
		AutoLeave:      confState.AutoLeave,
	}
}
func convertPeers(peers []raftpb.PeerID) []PeerID {
	result := make([]PeerID, len(peers))
	for i, peer := range peers {
		result[i] = PeerID(peer)
	}
	return result
}

func (m Message) Convert() raftpb.Message {
	return raftpb.Message{
		Type:       raftpb.MessageType(m.Type),
		To:         raftpb.PeerID(m.To),
		From:       raftpb.PeerID(m.From),
		Term:       m.Term,
		LogTerm:    m.LogTerm,
		Index:      m.Index,
		Entries:    ConvertEntries(m.Entries),
		Commit:     m.Commit,
		Snapshot:   m.Snapshot.Convert(),
		Reject:     m.Reject,
		RejectHint: m.RejectHint,
		Context:    m.Context,
	}
}

func ConvertEntries(e []Entry) []raftpb.Entry {
	result := make([]raftpb.Entry, len(e))
	for i, entry := range e {
		result[i] = entry.Convert()
	}
	return result
}

func (e Entry) Convert() raftpb.Entry {
	return raftpb.Entry{
		Term:  e.Term,
		Index: e.Index,
		Type:  raftpb.EntryType(e.Type),
		Data:  e.Data,
	}
}

func (s Snapshot) Convert() *raftpb.Snapshot {
	return &raftpb.Snapshot{
		Data:     s.Data,
		Metadata: s.Metadata.Convert(),
	}
}

func (s SnapshotMetadata) Convert() raftpb.SnapshotMetadata {
	return raftpb.SnapshotMetadata{
		ConfState: s.ConfState.Convert(),
		Index:     s.Index,
		Term:      s.Term,
	}
}

func (c ConfState) Convert() raftpb.ConfState {
	return raftpb.ConfState{
		Voters:         convertPeerIDs(c.Voters),
		Learners:       convertPeerIDs(c.Learners),
		VotersOutgoing: convertPeerIDs(c.VotersOutgoing),
		LearnersNext:   convertPeerIDs(c.LearnersNext),
		AutoLeave:      c.AutoLeave,
	}
}

func convertPeerIDs(peers []PeerID) []raftpb.PeerID {
	result := make([]raftpb.PeerID, len(peers))
	for i, peer := range peers {
		result[i] = raftpb.PeerID(peer)
	}
	return result
}

func (h HardState) Convert() raftpb.HardState {
	return raftpb.HardState{
		Term:      h.Term,
		Vote:      raftpb.PeerID(h.Vote),
		Commit:    h.Commit,
		Lead:      raftpb.PeerID(h.Lead),
		LeadEpoch: raftpb.Epoch(h.LeadEpoch),
	}
}
