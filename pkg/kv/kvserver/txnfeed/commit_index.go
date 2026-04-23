// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/maypok86/otter/v2"
)

// commitIndexMaxSize is the maximum number of distinct timestamps stored
// in the CommitIndex. When exceeded, otter evicts entries according to
// its internal admission/eviction policy (S3-FIFO).
const commitIndexMaxSize = 100000

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
// Record and Lookup may be called concurrently from any goroutine. The
// underlying otter.Cache is thread-safe, so no external synchronization
// is needed.
type CommitIndex struct {
	cache *otter.Cache[hlc.Timestamp, []uuid.UUID]
}

// NewCommitIndex creates a new, empty CommitIndex.
func NewCommitIndex() (*CommitIndex, error) {
	c, err := otter.New[hlc.Timestamp, []uuid.UUID](&otter.Options[hlc.Timestamp, []uuid.UUID]{
		MaximumSize:     commitIndexMaxSize,
		InitialCapacity: 1024,
	})
	if err != nil {
		return nil, err
	}
	return &CommitIndex{cache: c}, nil
}

// Record atomically adds a mapping from ts to txnID. Duplicate (ts, txnID)
// pairs are ignored — this is expected since 2PC intent resolution fires
// MVCCCommitIntentOp for every resolved intent, producing many Record calls
// with the same txnID. When a new slice is needed, it is allocated fresh to
// avoid mutating a slice that concurrent readers may hold.
func (c *CommitIndex) Record(ts hlc.Timestamp, txnID uuid.UUID) {
	c.cache.Compute(ts, func(existing []uuid.UUID, found bool) ([]uuid.UUID, otter.ComputeOp) {
		if found {
			for _, id := range existing {
				if id == txnID {
					return existing, otter.CancelOp
				}
			}
			newSlice := make([]uuid.UUID, len(existing)+1)
			copy(newSlice, existing)
			newSlice[len(existing)] = txnID
			return newSlice, otter.WriteOp
		}
		return []uuid.UUID{txnID}, otter.WriteOp
	})
}

// Lookup returns the transaction IDs that committed at the given timestamp.
// Returns (nil, false) if no entry exists for that timestamp.
func (c *CommitIndex) Lookup(ts hlc.Timestamp) ([]uuid.UUID, bool) {
	return c.cache.GetIfPresent(ts)
}

// EstimatedLen returns the approximate number of distinct timestamps in
// the index.
func (c *CommitIndex) EstimatedLen() int {
	return c.cache.EstimatedSize()
}
