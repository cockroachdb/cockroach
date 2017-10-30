// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tscache

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// MinRetentionWindow specifies the minimum duration to hold entries in the
// cache before allowing eviction. After this window expires, transactions
// writing to this node with timestamps lagging by more than MinRetentionWindow
// will necessarily have to advance their commit timestamp.
const MinRetentionWindow = 10 * time.Second

// Cache is a bounded in-memory cache that records key ranges and the
// timestamps at which they were most recently read and written. The structure
// serves to protect against violations of Snapshot Isolation, which requires
// that the outcome of reads must be preserved even in the presence of
// read-write conflicts (i.e. a write to a key at a lower timestamp than a
// previous read must not succeed). Cache corresponds to the "status oracle"
// discussed in Yabandeh's A Critique of Snapshot Isolation.
//
// The cache is updated after the completion of each read operation with the
// range of all keys that the request was predicated upon. It is then consulted
// for each write operation, allowing them to detect read-write violations that
// would allow them to write "under" a read that has already been performed.
//
// The cache is size-limited, so to prevent read-write conflicts for arbitrarily
// old requests, it pessimistically maintains a “low water mark”. This value
// always ratchets with monotonic increases and is equivalent to the earliest
// timestamp of any key range that is present in the cache. If a write operation
// writes to a key not present in the cache, the “low water mark” is consulted
// instead to determine read-write conflicts. The low water mark is initialized
// to the current system time plus the maximum clock offset.
type Cache interface {
	// AddRequest adds the specified request to the cache in an unexpanded state.
	AddRequest(req *Request)
	// ExpandRequests expands any request that overlaps the specified span and
	// which is newer than the specified timestamp.
	ExpandRequests(span roachpb.RSpan, timestamp hlc.Timestamp)

	// SetLowWater sets the low water mark of the cache for the specified span
	// to the provided timestamp.
	SetLowWater(start, end roachpb.Key, timestamp hlc.Timestamp)
	// GlobalLowWater returns the low water mark for the entire cache.
	GlobalLowWater() hlc.Timestamp

	// GetMaxRead returns the maximum read timestamp which overlaps the interval
	// spanning from start to end. If that timestamp belongs to a single
	// transaction, that transaction's ID is returned. If no part of the
	// specified range is overlapped by timestamps from different transactions
	// in the cache, the low water timestamp is returned for the read
	// timestamps. Also returns an "ok" bool, indicating whether an explicit
	// match of the interval was found in the cache (as opposed to using the
	// low-water mark).
	GetMaxRead(start, end roachpb.Key) (hlc.Timestamp, uuid.UUID, bool)
	// GetMaxWrite behaves like GetMaxRead, but returns the maximum write
	// timestamp which overlaps the interval spanning from start to end.
	GetMaxWrite(start, end roachpb.Key) (hlc.Timestamp, uuid.UUID, bool)

	// The following methods are used for testing within this package:
	//
	// add the specified timestamp to the cache covering the range of keys from
	// start to end.
	add(start, end roachpb.Key, timestamp hlc.Timestamp, txnID uuid.UUID, readTSCache bool)
	// clear clears the cache and resets the low-water mark.
	clear(lowWater hlc.Timestamp)
}

// New returns a new timestamp cache with the supplied hybrid clock.
func New(clock *hlc.Clock) Cache {
	return newTreeImpl(clock)
}

// cacheValue combines a timestamp with an optional txnID. It is shared between
// multiple Cache implementations.
type cacheValue struct {
	ts    hlc.Timestamp
	txnID uuid.UUID
}

// noTxnID is used when a cacheValue has no corresponding TxnID.
var noTxnID uuid.UUID

// lowWaterTxnIDMarker is a special txn ID that identifies a cache entry as a
// low water mark. It is specified when a lease is acquired to clear the
// timestamp cache for a range. Also see Cache.getMax where this txn ID is
// checked in order to return whether the max read/write timestamp came from a
// regular entry or one of these low water mark entries.
var lowWaterTxnIDMarker = func() uuid.UUID {
	// The specific txn ID used here isn't important. We use something that is:
	// a) non-zero
	// b) obvious
	u, err := uuid.FromString("11111111-1111-1111-1111-111111111111")
	if err != nil {
		panic(err)
	}
	return u
}()
