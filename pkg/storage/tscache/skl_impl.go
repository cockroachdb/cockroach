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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	// defaultSklPageSize is the default size of each page in the sklImpl's
	// read and write intervalSkl.
	defaultSklPageSize = 32 << 20 // 32 MB
	// TestSklPageSize is passed to tests as the size of each page in the
	// sklImpl to limit its memory footprint. Reducing this size can hurt
	// performance but it decreases the risk of OOM failures when many tests
	// are running concurrently.
	TestSklPageSize = 32 << 10 // 32 KB
)

// sklImpl implements the Cache interface. It maintains a pair of skiplists
// containing keys or key ranges and the timestamps at which they were most
// recently read or written. If a timestamp was read or written by a
// transaction, the txn ID is stored with the timestamp to avoid advancing
// timestamps on successive requests from the same transaction.
type sklImpl struct {
	rCache, wCache *intervalSkl
	clock          *hlc.Clock
	pageSize       uint32
	metrics        Metrics
}

var _ Cache = &sklImpl{}

// newSklImpl returns a new treeImpl with the supplied hybrid clock.
func newSklImpl(clock *hlc.Clock, pageSize uint32, metrics Metrics) *sklImpl {
	if pageSize == 0 {
		pageSize = defaultSklPageSize
	}
	tc := sklImpl{clock: clock, pageSize: pageSize, metrics: metrics}
	tc.clear(clock.Now())
	return &tc
}

// clear clears the cache and resets the low-water mark.
func (tc *sklImpl) clear(lowWater hlc.Timestamp) {
	tc.rCache = newIntervalSkl(tc.clock, MinRetentionWindow, tc.pageSize, tc.metrics.Skl.Read)
	tc.wCache = newIntervalSkl(tc.clock, MinRetentionWindow, tc.pageSize, tc.metrics.Skl.Write)
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
	start, end = boundKeyLens(start, end)

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

// boundKeyLens makes sure that the key lengths provided are below the maximum
// key length allowed by intervalSkl. This is very large (4 GB), so we don't
// expect to ever see keys of this size in practice. Still, we'd rather not
// panic.
func boundKeyLens(start, end roachpb.Key) (roachpb.Key, roachpb.Key) {
	// If either key is too long, truncate its length, making sure to
	// always grow the [start,end) range instead of shrinking it.
	if l := len(start); l > maxSklKeyLen {
		start = start[:maxSklKeyLen]
		log.Errorf(context.TODO(), "start key with length %d exceeds maximum key length of %d..."+
			"losing precision in timestamp cache", l, maxSklKeyLen)
	}
	if l := len(end); l > maxSklKeyLen {
		end = end[:maxSklKeyLen].Next() // next to grow range
		log.Errorf(context.TODO(), "end key with length %d exceeds maximum key length of %d..."+
			"losing precision in timestamp cache", l, maxSklKeyLen)
	}
	return start, end
}
