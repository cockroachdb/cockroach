// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tscache

import (
	"context"

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
	TestSklPageSize = 128 << 10 // 128 KB
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
func newSklImpl(clock *hlc.Clock, pageSize uint32) *sklImpl {
	if pageSize == 0 {
		pageSize = defaultSklPageSize
	}
	tc := sklImpl{clock: clock, pageSize: pageSize, metrics: makeMetrics()}
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
	start, end = tc.boundKeyLengths(start, end)

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

// boundKeyLengths makes sure that the key lengths provided are well below the
// size of each sklPage, otherwise we'll never be successful in adding it to
// an intervalSkl.
func (tc *sklImpl) boundKeyLengths(start, end roachpb.Key) (roachpb.Key, roachpb.Key) {
	// We bound keys to 1/32 of the page size. These could be slightly larger
	// and still not trigger the "key range too large" panic in intervalSkl,
	// but anything larger could require multiple page rotations before it's
	// able to fit in if other ranges are being added concurrently.
	maxKeySize := int(tc.pageSize / 32)

	// If either key is too long, truncate its length, making sure to always
	// grow the [start,end) range instead of shrinking it. This will reduce the
	// precision of the entry in the cache, which could allow independent
	// requests to interfere, but will never permit consistency anomalies.
	if l := len(start); l > maxKeySize {
		start = start[:maxKeySize]
		log.Warningf(context.TODO(), "start key with length %d exceeds maximum key length of %d; "+
			"losing precision in timestamp cache", l, maxKeySize)
	}
	if l := len(end); l > maxKeySize {
		end = end[:maxKeySize].PrefixEnd() // PrefixEnd to grow range
		log.Warningf(context.TODO(), "end key with length %d exceeds maximum key length of %d; "+
			"losing precision in timestamp cache", l, maxKeySize)
	}
	return start, end
}

// Metrics implements the Cache interface.
func (tc *sklImpl) Metrics() Metrics {
	return tc.metrics
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
