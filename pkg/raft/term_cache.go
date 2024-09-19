// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raft

// termCache is a fixed-size cache for terms in the raft log.
//
// Entries in a raft log have monotonic terms. Typically, the log contains long
// runs of same-term entries, because the leader tends to be stable. This
// observation allows "compressing" the terms of a log range into a few key
// points at which the term changes.
//
// This type tracks all log operations, such as appends and truncations, and
// maintains the last few term change key points for a suffix of the log. In
// practice, two latest change points are enough to cover most cases.
//
// Entry term queries in raft are used in a few cases:
//   - the commit criterion on the leader
//   - constructing a log append for an acceptor
//   - log matching when accepting a log append
//   - snapshot commit optimization
//
// NB: the cache starts cold, only knowing about the term of the last log entry.
type termCache struct {
	cut  entryID
	last entryID
}

// init resets the cache to the given cut point and no entries following it.
func (t *termCache) init(at entryID) {
	t.cut, t.last = at, at
}

// term returns the term of the entry at the given index. Returns false if the
// index is below the cut point.
func (t *termCache) term(index uint64) (uint64, bool) {
	if index >= t.last.index {
		return t.last.term, true
	}
	if index >= t.cut.index {
		return t.cut.term, true
	}
	return 0, false
}

// append informs the term cache that the given logSlice has been appended to
// the log.
func (t *termCache) append(ls logSlice) {
	t.truncate(ls.prev)
	for i := range ls.entries {
		t.add(pbEntryID(&ls.entries[i]))
	}
}

func (t *termCache) truncate(after entryID) {
	if after.index < t.last.index {
		t.last = after
		if after.index < t.cut.index {
			t.cut = after
		}
	}
}

func (t *termCache) add(id entryID) {
	if id.term != t.last.term {
		t.cut, t.last = t.last, id
	}
}
