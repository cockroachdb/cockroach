// This code has been modified from its original form by Cockroach Labs, Inc.
// All modifications are Copyright 2024 Cockroach Labs, Inc.
//
// Copyright 2024 The etcd Authors
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
	"fmt"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
)

// entryID uniquely identifies a raft log entry.
//
// Every entry is associated with a leadership term which issued this entry and
// initially appended it to the log. There can only be one leader at any term,
// and a leader never issues two entries with the same index.
type entryID struct {
	term  uint64
	index uint64
}

// pbEntryID returns the ID of the given pb.Entry.
func pbEntryID(entry *pb.Entry) entryID {
	return entryID{term: entry.Term, index: entry.Index}
}

// logMark is a position in a log consistent with the leader at a specific term.
//
// This is different from entryID. The entryID ties an entry to the term of the
// leader who proposed it, while the logMark identifies an entry in a particular
// leader's coordinate system. Different leaders can have different entries at a
// particular index.
//
// Generally, all entries in raft form a tree (branching when a new leader
// starts proposing entries at its term). A logMark identifies a position in a
// particular branch of this tree.
type logMark struct {
	// term is the term of the leader whose log is considered.
	term uint64
	// index is the position in this leader's log.
	index uint64
}

// logSlice describes a correct slice of a raft log.
//
// Every log slice is considered in a context of a specific leader term. This
// term does not necessarily match entryID.term of the entries, since a leader
// log contains both entries from its own term, and some earlier terms.
//
// Two slices with a matching logSlice.term are guaranteed to be consistent,
// i.e. they never contain two different entries at the same index. The reverse
// is not true: two slices with different logSlice.term may contain both
// matching and mismatching entries. Specifically, logs at two different leader
// terms share a common prefix, after which they *permanently* diverge.
//
// A well-formed logSlice conforms to raft safety properties. It provides the
// following guarantees:
//
//  1. entries[i].Index == prev.index + 1 + i,
//  2. prev.term <= entries[0].Term,
//  3. entries[i-1].Term <= entries[i].Term,
//  4. entries[len-1].Term <= term.
//
// Property (1) means the slice is contiguous. Properties (2) and (3) mean that
// the terms of the entries in a log never regress. Property (4) means that a
// leader log at a specific term never has entries from higher terms.
//
// Users of this struct can assume the invariants hold true. Exception is the
// "gateway" code that initially constructs logSlice, such as when its content
// is sourced from a message that was received via transport, or from Storage,
// or in a test code that manually hard-codes this struct. In these cases, the
// invariants should be validated using the valid() method.
type logSlice struct {
	// term is the leader term containing the given entries in its log.
	term uint64
	// prev is the ID of the entry immediately preceding the entries.
	prev entryID
	// entries contains the consecutive entries representing this slice.
	entries []pb.Entry
}

// lastIndex returns the index of the last entry in this log slice. Returns
// prev.index if there are no entries.
func (s logSlice) lastIndex() uint64 {
	return s.prev.index + uint64(len(s.entries))
}

// lastEntryID returns the ID of the last entry in this log slice, or prev if
// there are no entries.
func (s logSlice) lastEntryID() entryID {
	if ln := len(s.entries); ln != 0 {
		return pbEntryID(&s.entries[ln-1])
	}
	return s.prev
}

// mark returns the logMark identifying the end of this logSlice.
func (s logSlice) mark() logMark {
	return logMark{term: s.term, index: s.lastIndex()}
}

// termAt returns the term of the entry at the given index.
// Requires: prev.index <= index <= lastIndex().
func (s logSlice) termAt(index uint64) uint64 {
	if index == s.prev.index {
		return s.prev.term
	}
	return s.entries[index-s.prev.index-1].Term
}

// forward returns a logSlice with prev forwarded to the given index.
// Requires: prev.index <= index <= lastIndex().
func (s logSlice) forward(index uint64) logSlice {
	return logSlice{
		term:    s.term,
		prev:    entryID{term: s.termAt(index), index: index},
		entries: s.entries[index-s.prev.index:],
	}
}

// valid returns nil iff the logSlice is a well-formed log slice. See logSlice
// comment for details on what constitutes a valid raft log slice.
func (s logSlice) valid() error {
	prev := s.prev
	for i := range s.entries {
		id := pbEntryID(&s.entries[i])
		if id.term < prev.term || id.index != prev.index+1 {
			return fmt.Errorf("leader term %d: entries %+v and %+v not consistent", s.term, prev, id)
		}
		prev = id
	}
	if s.term < prev.term {
		return fmt.Errorf("leader term %d: entry %+v has a newer term", s.term, prev)
	}
	return nil
}

// snapshot is a state machine snapshot tied to the term of the leader who
// observed this committed state.
//
// Semantically, from the log perspective, this type is equivalent to a logSlice
// from 0 to lastEntryID(), plus a commit logMark. All leader logs at terms >=
// snapshot.term contain all entries up to the lastEntryID(). At earlier terms,
// logs may or may not be consistent with this snapshot, depending on whether
// they contain the lastEntryID().
type snapshot struct {
	// term is the term of the leader log with whom the snapshot is consistent.
	//
	// Invariant: term >= term[generated] >= term[committed], where:
	//	- term[committed] is the term under which the lastEntryID was committed.
	//	  By raft invariants, all leaders at terms >= term[committed] observe
	//	  entries up through lastEntryID() in their logs.
	//	- term[generated] is the term under which the snapshot was generated.
	//
	// Note that we don't simply say term == term[generated] == term[committed],
	// for generality. It's possible that an entry is committed, the leader goes
	// offline, and another (later) leader generates and disseminates this
	// snapshot. The snapshot can also change hands / be delegated / generated by
	// a Learner observing a later term.
	term uint64
	// snap is the content of the snapshot.
	snap pb.Snapshot
}

// lastIndex returns the index of the last entry in this snapshot.
func (s snapshot) lastIndex() uint64 {
	return s.snap.Metadata.Index
}

// lastEntryID returns the ID of the last entry covered by this snapshot.
func (s snapshot) lastEntryID() entryID {
	return entryID{term: s.snap.Metadata.Term, index: s.snap.Metadata.Index}
}

// mark returns committed logMark of this snapshot, in the coordinate system of
// the leader who observes this committed state.
func (s snapshot) mark() logMark {
	return logMark{term: s.term, index: s.snap.Metadata.Index}
}

// valid returns nil iff the snapshot is well-formed.
func (s snapshot) valid() error {
	if entryTerm := s.snap.Metadata.Term; entryTerm > s.term {
		return fmt.Errorf("leader term %d: snap %+v has a newer term", s.term, s.lastEntryID())
	}
	return nil
}
