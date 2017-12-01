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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// sklPageSize is the size of each page in the sklImpl's read and write
// intervalSkl.
// TODO(nvanbenschoten): Bump back up to 32 MB when not testing.
const sklPageSize = 8 << 20 // 8 MB

// sklImpl implements the Cache interface. It maintains a pair of skiplists
// containing keys or key ranges and the timestamps at which they were most
// recently read or written. If a timestamp was read or written by a
// transaction, the txn ID is stored with the timestamp to avoid advancing
// timestamps on successive requests from the same transaction.
type sklImpl struct {
	rCache, wCache *intervalSkl
	clock          *hlc.Clock
	metrics        Metrics
}

var _ Cache = &sklImpl{}

// newSklImpl returns a new treeImpl with the supplied hybrid clock.
func newSklImpl(clock *hlc.Clock, metrics Metrics) *sklImpl {
	tc := sklImpl{clock: clock, metrics: metrics}
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
	tc.rCache = newIntervalSkl(tc.clock, MinRetentionWindow, pageSize, tc.metrics.Skl.Read)
	tc.wCache = newIntervalSkl(tc.clock, MinRetentionWindow, pageSize, tc.metrics.Skl.Write)
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

// Add implements the Cache interface.
func (tc *sklImpl) Add(start, end roachpb.Key, ts hlc.Timestamp, txnID uuid.UUID, readCache bool) {
	skl := tc.getSkl(readCache)

	val := cacheValue{ts: ts, txnID: txnID}
	if len(end) == 0 {
		skl.Add(nonNil(start), val)
	} else {
		skl.AddRange(nonNil(start), end, excludeTo, val)
	}
}

// SetLowWater implements the Cache interface.
func (tc *sklImpl) SetLowWater(start, end roachpb.Key, ts hlc.Timestamp) {
	tc.Add(start, end, ts, noTxnID, false /* readCache */)
	tc.Add(start, end, ts, noTxnID, true /* readCache */)
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
