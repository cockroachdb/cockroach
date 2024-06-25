// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raft

// LogMark identifies a position in the log tied to a specific leader term.
type LogMark struct {
	// Term is the leader term whose log is being considered.
	// NB: this term does not necessarily match the "entry term" at Index.
	Term uint64
	// Index is the entry index in this log.
	Index uint64
}

// Less returns true if the log mark "happens before" the other mark. All writes
// and acknowledgements happen in this order.
func (l LogMark) Less(other LogMark) bool {
	return l.Term < other.Term || l.Term == other.Term && l.Index < other.Index
}

// ForkTracker tracks the in-flight state of raft log writes.
//
//	term
//	  ^    [f0]        [f1]      [write]
//	  │      (-----+-----+-----------]
//	  │      |           |           |
//	9 ┤      | . . . . . | . . (-----]
//	7 ┤      | . . . . . (-----]
//	5 ┤      | . . . . .     (----------]
//	3 ┤      | . . (-----------------------]
//	2 ┤      (-----]
//	1 ┤                  (-----]
//		│             (ack)┘
//	  └──────┬─────┬─────┬─────┬─────┬─────┬─────> index
//	         20    30    40    50    60    70
//
// A log write occurs on behalf of a leader at a specific term. Writes are
// ordered by (term, index). The index can regress only when the term goes up.
// Write acknowledgements are ordered by (term, index), correspondingly.
//
// For a log index, there can be multiple writes in flight, under different
// terms. A later-term write at an index obsoletes writes at lower terms whose
// index >= this index. The purpose of this tracker is to no-op acknowledgements
// of the obsolete writes: as long as there is at least one higher-term write at
// an index, this index is not acknowledged.
//
// For each log index, we track the latest term under which it is written. We do
// this efficiently, by maintaining an ordered list of "fork" points. An entry
// at index between two consecutive forks (including the "write" pseudo-fork)
// can only be released by an ack at term >= fork.term.
type ForkTracker struct {
	// write is the current state of the log:
	//	- write.Term is the leader term on whose behalf the last append was made.
	//	- write.Index is the last index in the current log.
	write LogMark
	// ack is the current acknowledged log mark.
	//
	// Invariant: ack <= write.
	ack LogMark

	// forks lists all forks in (ack, write]. Can be empty.
	//
	// Invariants (if forks is not empty):
	//	- ack < forks[0] < forks[1] < ... < forks[len-1] <= write
	//	- forks have unique terms
	//	- forks have increasing indices
	//
	// Normally, there is 0 or 1 fork. A higher number of forks is possible only
	// if leader changes spin rapidly, or acknowledgements are slow. The slice of
	// forks is optimized for being short, to avoid allocations in most cases.
	forks shortSlice[LogMark]
}

// NewForkTracker returns a tracker initialized to the given log state.
func NewForkTracker(mark LogMark) ForkTracker {
	return ForkTracker{write: mark, ack: mark}
}

// Append adds the (from.Index, to] range of indices written on behalf of the
// leader at term from.Term.
func (f *ForkTracker) Append(from LogMark, to uint64) bool {
	if from.Index > to { // incorrect interval
		return false
	} else if from.Less(f.write) { // incorrect order of writes
		return false
	} else if from.Index > f.write.Index { // writes have gaps
		// TODO(pav-kv): support this case for AC.
		return false
	}

	// Append-only case when there is no fork.
	write := LogMark{Term: from.Term, Index: to}
	if from.Term == f.write.Term || from.Index >= f.write.Index {
		f.write = write
		return true
	}

	// Otherwise, we are introducing a fork:
	//	from.Term > f.write.Term && from.Index < f.write.Index.
	//
	// Truncate all the obsolete forks, and append a new one.
	pop := len(f.forks.slice)
	for ; pop > 0; pop-- {
		if from.Index >= f.forks.slice[pop-1].Index {
			break
		}
	}
	f.forks.slice = f.forks.slice[:pop]

	f.write = write
	if from.Index <= f.ack.Index {
		f.ack = from
	} else {
		f.forks.slice = append(f.forks.slice[:pop], from)
	}
	return true
}

// Ack acknowledges all writes up to the given log mark.
func (f *ForkTracker) Ack(to LogMark) bool {
	if to.Less(f.ack) { // incorrect order of acknowledgements
		return false
	}
	f.ack = to

	// Remove all forks that are now in the past.
	skip := 0
	for ln := len(f.forks.slice); skip < ln; skip++ {
		if f.forks.slice[skip].Term > to.Term {
			break
		}
	}
	f.forks.skip(skip)

	return true
}

// Released returns the current released log index. All in-flight writes are in
// the (Released, write.Index] interval. All entries with lower indices have
// been acknowledged.
func (f *ForkTracker) Released() uint64 {
	if len(f.forks.slice) == 0 {
		return f.ack.Index
	}
	return min(f.ack.Index, f.forks.slice[0].Index)
}

const shortSliceLen = 2

type shortSlice[T any] struct {
	short [shortSliceLen]T
	slice []T
}

func (s *shortSlice[T]) skip(count int) {
	if count == 0 {
		return
	}
	s.slice = s.slice[count:]
	if ln := len(s.slice); ln <= shortSliceLen {
		s.slice = s.short[:copy(s.short[:ln], s.slice)]
	}
}
