// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// CommitIndex is an in-memory mapping from MVCC commit timestamps to the
// transaction IDs that committed at those timestamps. It is populated during
// Raft command application from MVCCCommitIntentOp entries in the LogicalOpLog,
// which fire for every resolved intent — covering both 1PC (inline
// resolution) and 2PC (explicit ResolveIntent) commit paths.
//
// The index enables read dependency tracking: given the MVCC timestamp of a
// value that a transaction read, the CommitIndex can identify which
// transaction wrote that value.
//
// Record is called under raftMu (single writer). Lookup may be called
// concurrently from any goroutine.
type CommitIndex struct {
	mu struct {
		syncutil.RWMutex
		entries []commitEntry
	}
}

type commitEntry struct {
	ts     hlc.Timestamp
	txnIDs []uuid.UUID
}

// NewCommitIndex creates a new, empty CommitIndex.
func NewCommitIndex() *CommitIndex {
	return &CommitIndex{}
}

// Record adds a mapping from ts to txnID. If the most recent entry has the
// same timestamp, the txnID is appended to that entry's slice; otherwise a
// new entry is appended. Entries must be recorded in non-decreasing timestamp
// order (guaranteed by Raft application ordering).
func (c *CommitIndex) Record(ts hlc.Timestamp, txnID uuid.UUID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := len(c.mu.entries); n > 0 && c.mu.entries[n-1].ts.Equal(ts) {
		c.mu.entries[n-1].txnIDs = append(c.mu.entries[n-1].txnIDs, txnID)
	} else {
		c.mu.entries = append(c.mu.entries, commitEntry{
			ts:     ts,
			txnIDs: []uuid.UUID{txnID},
		})
	}
}

// Lookup returns the transaction IDs that committed at the given timestamp.
// Returns (nil, false) if no entry exists for that timestamp.
func (c *CommitIndex) Lookup(ts hlc.Timestamp) ([]uuid.UUID, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	i := sort.Search(len(c.mu.entries), func(i int) bool {
		return !c.mu.entries[i].ts.Less(ts)
	})
	if i < len(c.mu.entries) && c.mu.entries[i].ts.Equal(ts) {
		return c.mu.entries[i].txnIDs, true
	}
	return nil, false
}

// Len returns the number of distinct timestamps in the index.
func (c *CommitIndex) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.mu.entries)
}
