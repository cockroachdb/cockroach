// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftpb

import (
	"fmt"

	"github.com/cockroachdb/redact"
)

// PeerID is a custom type for peer IDs in a raft group.
type PeerID uint64

// SafeValue implements the redact.SafeValue interface.
func (p PeerID) SafeValue() {}

// Epoch is an epoch in the Store Liveness fabric, referencing an uninterrupted
// period of support from one store to another.
type Epoch int64

// SafeValue implements the redact.SafeValue interface.
func (e Epoch) SafeValue() {}

// The enums in raft are all safe for redaction.
func (MessageType) SafeValue()          {}
func (EntryType) SafeValue()            {}
func (ConfChangeType) SafeValue()       {}
func (ConfChangeTransition) SafeValue() {}

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

// StateType represents the role of a node in a cluster.
type StateType uint64

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
	NumStates
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func (st StateType) String() string {
	return stmap[st]
}

func (st StateType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", st.String())), nil
}
