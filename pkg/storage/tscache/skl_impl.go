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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// sklPageSize is the size of each page in the sklImpl's read and write
// intervalSkl.
const sklPageSize = 32 << 20 // 32 MB

// sklImpl implements the Cache interface. It maintains a pair of skiplists
// containing keys or key ranges and the timestamps at which they were most
// recently read or written. If a timestamp was read or written by a
// transaction, the txn ID is stored with the timestamp to avoid advancing
// timestamps on successive requests from the same transaction.
//
// TODO(nvanbenschoten): We should export some metrics from here, including:
// - page rotations/min (rCache & wCache)
// - page count         (rCache & wCache)
type sklImpl struct {
	rCache, wCache *intervalSkl
	clock          *hlc.Clock
}

var _ Cache = &sklImpl{}

// newSklImpl returns a new treeImpl with the supplied hybrid clock.
func newSklImpl(clock *hlc.Clock) *sklImpl {
	tc := sklImpl{clock: clock}
	tc.clear(clock.Now())
	return &tc
}

// clear clears the cache and resets the low-water mark.
func (tc *sklImpl) clear(lowWater hlc.Timestamp) {
	pageSize := uint32(sklPageSize)
	if util.RaceEnabled {
		// Race testing consumes significantly more memory that normal testing.
		// In addition, while running a group of tests in parallel, each will
		// create a timestamp cache for every Store needed. Reduce the page size
		// during race testing to accommodate these two factors.
		pageSize /= 4
	}
	tc.rCache = newIntervalSkl(tc.clock, MinRetentionWindow, pageSize)
	tc.wCache = newIntervalSkl(tc.clock, MinRetentionWindow, pageSize)
	tc.rCache.floorTS = lowWater
	tc.wCache.floorTS = lowWater
}

// getSkl returns either the read or write intervalSkl.
func (tc *sklImpl) getSkl(readCache bool) *intervalSkl {
	if readCache {
		return tc.rCache
	}
	return tc.wCache
}

// add the specified timestamp to the cache covering the range of keys from
// start to end. If end is nil, the range covers the start key only. txnID is
// nil for no transaction. readCache specifies whether the command adding this
// timestamp should update the read timestamp; false to update the write
// timestamp cache.
func (tc *sklImpl) add(start, end roachpb.Key, ts hlc.Timestamp, txnID uuid.UUID, readCache bool) {
	skl := tc.getSkl(readCache)

	val := cacheValue{ts: ts, txnID: txnID}
	if len(end) == 0 {
		skl.Add(nonNil(start), val)
	} else {
		skl.AddRange(nonNil(start), end, excludeTo, val)
	}
}

// AddRequest implements the Cache interface.
func (tc *sklImpl) AddRequest(req *Request) {
	for _, sp := range req.Reads {
		tc.add(sp.Key, sp.EndKey, req.Timestamp, req.TxnID, true /* readCache */)
	}
	for _, sp := range req.Writes {
		tc.add(sp.Key, sp.EndKey, req.Timestamp, req.TxnID, false /* readCache */)
	}
	if req.Txn.Key != nil {
		// Make the transaction key from the request key. We're guaranteed
		// req.TxnID != nil because we only hit this code path for
		// EndTransactionRequests.
		key := keys.TransactionKey(req.Txn.Key, req.TxnID)
		tc.add(key, nil, req.Timestamp, req.TxnID, false /* readCache */)
	}
	req.release()
}

// ExpandRequests implements the Cache interface.
func (tc *sklImpl) ExpandRequests(span roachpb.RSpan, ts hlc.Timestamp) { /* no-op */ }

// SetLowWater implements the Cache interface.
func (tc *sklImpl) SetLowWater(start, end roachpb.Key, ts hlc.Timestamp) {
	tc.add(start, end, ts, noTxnID, false /* readCache */)
	tc.add(start, end, ts, noTxnID, true /* readCache */)
}

// getLowWater implements the Cache interface.
func (tc *sklImpl) getLowWater(readCache bool) hlc.Timestamp {
	return tc.getSkl(readCache).FloorTS()
}

// GetMaxRead implements the Cache interface.
func (tc *sklImpl) GetMaxRead(start, end roachpb.Key) (hlc.Timestamp, uuid.UUID) {
	return tc.getMax(start, end, true /* readCache */)
}

// GetMaxWrite implements the Cache interface.
func (tc *sklImpl) GetMaxWrite(start, end roachpb.Key) (hlc.Timestamp, uuid.UUID) {
	return tc.getMax(start, end, false /* readCache */)
}

func (tc *sklImpl) getMax(start, end roachpb.Key, readCache bool) (hlc.Timestamp, uuid.UUID) {
	skl := tc.getSkl(readCache)

	var val cacheValue
	if len(end) == 0 {
		val = skl.LookupTimestamp(nonNil(start))
	} else {
		val = skl.LookupTimestampRange(nonNil(start), end, excludeTo)
	}
	return val.ts, val.txnID
}

// intervalSkl doesn't handle nil keys the same way as empty keys. Cockroach's
// KV API layer doesn't make a distinction.
var emptyStartKey = []byte("")

func nonNil(b []byte) []byte {
	if b == nil {
		return emptyStartKey
	}
	return b
}
