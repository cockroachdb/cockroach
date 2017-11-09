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
	"fmt"
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

	// GetMaxRead returns the maximum read timestamp which overlaps the interval
	// spanning from start to end. If that maximum timestamp belongs to a single
	// transaction, that transaction's ID is returned. Otherwise, if that
	// maximum is shared between multiple transactions, no transaction ID is
	// returned. Finally, if no part of the specified range is overlapped by
	// timestamp intervals from any transactions in the cache, the low water
	// timestamp is returned for the read timestamps.
	GetMaxRead(start, end roachpb.Key) (hlc.Timestamp, uuid.UUID)
	// GetMaxWrite behaves like GetMaxRead, but returns the maximum write
	// timestamp which overlaps the interval spanning from start to end.
	GetMaxWrite(start, end roachpb.Key) (hlc.Timestamp, uuid.UUID)

	// The following methods are used for testing within this package:
	//
	// add the specified timestamp to the cache covering the range of keys from
	// start to end.
	add(start, end roachpb.Key, ts hlc.Timestamp, txnID uuid.UUID, readCache bool)
	// clear clears the cache and resets the low-water mark.
	clear(lowWater hlc.Timestamp)
	// getLowWater return the low water mark for the specified cache.
	getLowWater(readCache bool) hlc.Timestamp
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

func (v cacheValue) String() string {
	var txnIDStr string
	switch v.txnID {
	case noTxnID:
		txnIDStr = "none"
	default:
		txnIDStr = v.txnID.String()
	}
	return fmt.Sprintf("{ts: %s, txnID: %s}", v.ts, txnIDStr)
}

// ratchetValue returns the cacheValue that results from ratcheting the provided
// old and new cacheValues. It also returns flags reflecting whether the value
// was updated.
//
// This ratcheting policy is shared across all Cache implementations, even if
// they do not use this function directly.
func ratchetValue(old, new cacheValue) (cacheValue, bool) {
	if old.ts.Less(new.ts) {
		// Ratchet to new value.
		return new, true
	} else if new.ts.Less(old.ts) {
		// Nothing to update.
		return old, false
	} else if new.txnID != old.txnID {
		// old.ts == new.ts but the values have different txnIDs. Remove the
		// transaction ID from the value so that it is no longer owned by any
		// transaction.
		new.txnID = noTxnID
		return new, old.txnID != noTxnID
	}
	// old == new.
	return old, false
}
