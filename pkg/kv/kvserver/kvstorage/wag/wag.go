// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package wag

import (
	"cmp"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type NodeType uint8

const (
	NodeEmpty NodeType = iota
	NodeInit
	NodeApply
	NodeDestroy
)

type LogID uint64

type LogAddr struct {
	LogID LogID
	Index kvpb.RaftIndex
}

func (l LogAddr) Compare(o LogAddr) int {
	return cmp.Or(cmp.Compare(l.LogID, o.LogID), cmp.Compare(l.Index, o.Index))
}

// FullAddr identifies a committed entry in the raft log, or the corresponding
// applied state of a Range.
type FullAddr struct {
	RangeID roachpb.RangeID
	LogAddr
}

// ========= Stuff in storage ==========

// WAG stores the topologically sorted DAG of critical replica lifecycle events
// ahead of applying them to the state machine.
type WAG struct {
	Nodes []Node
}

// Init constructs a WAG node which initializes (or moves forward) a replica
// with the snapshot, and subsumes 0 or more other replicas. Should be stored in
// the same batch with the newly created LogID (addr.LogID) initialized with the
// new raft state.
func (w *WAG) Init(addr FullAddr, snap Snapshot, subsume []FullAddr) {
	for _, s := range subsume {
		w.Nodes = append(w.Nodes, Node{FullAddr: s, Type: NodeApply})
	}
	ids := make([]roachpb.RangeID, len(subsume))
	for i := range subsume {
		ids[i] = subsume[i].RangeID
	}
	w.Nodes = append(w.Nodes, Node{
		FullAddr: addr, Type: NodeInit,
		Init: &Init{Snap: snap, Subsume: ids},
	})
}

// Split constructs a WAG node which should be bundled with the newly created
// raft state of the RHS replica. The node simply instructs the pre-split range
// to apply all the commands up to the split trigger (which initializes the RHS
// state machine).
func (w *WAG) Split(addr FullAddr) {
	w.Nodes = append(w.Nodes, Node{FullAddr: addr, Type: NodeApply})
}

// Merge constructs a WAG node which checkpoints the merge of two ranges. No
// need to bundle it with other writes - the RHS raft state will be removed
// asynchronously, after the merge is durable.
//
// This command simply instructs the RHS range to apply all the remaining
// commands, and the LHS to apply everything up to and including the merge
// trigger. The two things will happen in order.
func (w *WAG) Merge(to, from FullAddr) {
	w.Nodes = append(w.Nodes, Node{FullAddr: from, Type: NodeApply})
	w.Nodes = append(w.Nodes, Node{FullAddr: to, Type: NodeApply})
}

// Destroy checkpoints the replica before its unconditional destruction.
func (w *WAG) Destroy(addr FullAddr) {
	w.Nodes = append(w.Nodes, Node{FullAddr: addr, Type: NodeDestroy})
}

type Node struct {
	FullAddr
	Type NodeType
	Init *Init
}

type Snapshot struct {
	ID   logstore.EntryID
	SSTs []string
}

type Init struct {
	Snap    Snapshot
	Subsume []roachpb.RangeID
}

// ======= Store/replicas state and applying/replaying logic ========

type Replica struct {
	RangeID roachpb.RangeID
	LogID   LogID
	Applied logstore.EntryID
}

func (r *Replica) Addr() LogAddr {
	return LogAddr{LogID: r.LogID, Index: r.Applied.Index}
}

type Tombstone struct {
	RangeID   roachpb.RangeID
	NextLogID LogID
}

func (t *Tombstone) Addr() LogAddr {
	return LogAddr{LogID: t.NextLogID}
}

type Store struct {
	Replicas   map[roachpb.RangeID]Replica
	Tombstones map[roachpb.RangeID]Tombstone
}

func (s *Store) Replay(w WAG) {
	for _, n := range w.Nodes {
		s.ApplyNode(n)
	}
}

func (s *Store) Applied(addr FullAddr) bool {
	if r, ok := s.Replicas[addr.RangeID]; ok {
		return r.Addr().Compare(addr.LogAddr) >= 0
	} else if t, ok := s.Tombstones[addr.RangeID]; ok {
		return t.Addr().Compare(addr.LogAddr) >= 0
	}
	return false
}

func (s *Store) ApplyNode(n Node) {
	if s.Applied(n.FullAddr) {
		return
	}
	switch n.Type {
	case NodeEmpty:
	case NodeInit:
		s.Init(n.RangeID, *n.Init)
	case NodeApply:
		s.Apply(n.RangeID, n.Index)
	case NodeDestroy:
		s.Destroy(n.RangeID)
	}
}

func (s *Store) Init(rangeID roachpb.RangeID, node Init) {
	for _, ss := range node.Subsume {
		s.Destroy(ss)
	}
	r, ok := s.Replicas[rangeID]
	if !ok {
		r = Replica{RangeID: rangeID, LogID: s.Tombstones[rangeID].NextLogID}
	} else {
		r.LogID++
	}
	_ = node.Snap.SSTs // ingest
	r.Applied = node.Snap.ID
	s.Replicas[rangeID] = r
}

func (s *Store) Apply(rangeID roachpb.RangeID, index kvpb.RaftIndex) {
	r := s.Replicas[rangeID]
	_, _ = r.Applied, index  // apply (r.Applied, index] commands
	term := kvpb.RaftTerm(1) // from the last command
	r.Applied = logstore.EntryID{Index: index, Term: term}
	s.Replicas[rangeID] = r
}

func (s *Store) Destroy(rangeID roachpb.RangeID) {
	logID := s.Replicas[rangeID].LogID
	delete(s.Replicas, rangeID)
	s.Tombstones[rangeID] = Tombstone{NextLogID: logID + 1}
}
