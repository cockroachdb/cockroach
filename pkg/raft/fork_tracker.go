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

// logMark identifies a position in the log tied to a specific leader term.
//
// NB: the term does not necessarily match the "entry term" t1 at this index. An
// entry proposed at term t1 and committed at t2, is present in the term = t1
// leader log, some (possibly none or all) logs at t1 < term < t2, and all logs
// at term >= t2. Between t1 and t2, leaders can overwrite this t1 entry.
//
// If t2 proposes/commits a different entry at this index (not the t1 entry),
// the t1 entry never reappears in logs at term >= t2.
type logMark entryID

// ForkTracker tracks the in-flight state of raft log writes.
//
//	term
//	  ^    (ack)  (f0)  (f1)      (write)
//	  │      (-----+-----+-----------]
//	  │      |     |     |           |
//	9 ┤      . . . . . . . . . (-----]
//	7 ┤      . . . . . . (-----]
//	5 ┤      . . . . . .     (----------]
//	3 ┤      . . . (-----------------------]
//	2 ┤      (--------]
//	1 ┤                  (-----]
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
// at index > fork.index can only be acked by term >= fork.term.
type ForkTracker struct {
	// write is the current state of the log:
	//	- write.term is the leader term on whose behalf the last append was made.
	//	- write.index is the last index in the current log.
	write logMark
	// ack is the current acknowledged log mark.
	//
	// Invariant: ack <= write, both by term and by index.
	ack logMark

	// forks lists all forks in (ack, write]. Can be empty.
	//
	// Invariant (if forks is not empty):
	//	ack < forks[0] < forks[1] < ... < forks[len-1] <= write
	// The inequality is both by term and by index.
	forks shortSlice[logMark]
}

func NewForkTracker(term uint64, index uint64) ForkTracker {
	mark := logMark{term: term, index: index}
	return ForkTracker{write: mark, ack: mark}
}

// Append adds the (from, to] range of indices written under the given term.
func (f *ForkTracker) Append(term uint64, from, to uint64) bool {
	if term < f.write.term || from > f.write.index || to < from {
		return false
	}
	if from == f.write.index {
		f.write = logMark{term: term, index: to}
		return true
	}
	if term == f.write.term {
		return false
	}
	// term > write.term && from < write.index

	pop := len(f.forks.slice)
	for ; pop > 0; pop-- {
		if from >= f.forks.slice[pop-1].index {
			break
		}
	}

	if pop != 0 {
		f.forks.slice = f.forks.slice[:pop]
		f.forks.compress()
	}
	f.forks.slice = append(f.forks.slice, logMark{term: term, index: from})
	f.write = logMark{term: term, index: to}
	if f.ack.index > from {
		f.ack = logMark{term: term, index: from}
	}
	return true
}

// Ack acknowledges all writes up to the given log index at the given term.
func (f *ForkTracker) Ack(term uint64, index uint64) {
	if term < f.ack.term {
		return
	}
	skip := 0
	for ln := len(f.forks.slice); skip < ln; skip++ {
		if f.forks.slice[skip].term > term {
			break
		}
	}
	if skip != 0 {
		f.forks.slice = f.forks.slice[skip:]
		f.forks.compress()
	}

	if len(f.forks.slice) == 0 {
		f.ack = logMark{term: term, index: index}
	} else {
		f.ack = logMark{term: term, index: min(index, f.forks.slice[0].index)}
	}
}

const shortSliceLen = 2

type shortSlice[T any] struct {
	short [shortSliceLen]T
	slice []T
}

func (s *shortSlice[T]) compress() {
	if ln := len(s.slice); ln <= shortSliceLen {
		s.slice = s.short[:copy(s.short[:ln], s.slice)]
	}
}
