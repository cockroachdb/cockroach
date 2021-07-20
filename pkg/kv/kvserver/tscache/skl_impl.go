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

// sklImpl implements the Cache interface. It maintains a collection of
// skiplists containing keys or key ranges and the timestamps at which
// they were most recently read or written. If a timestamp was read or
// written by a transaction, the txn ID is stored with the timestamp to
// avoid advancing timestamps on successive requests from the same
// transaction.
type sklImpl struct {
	cache   *intervalSkl
	clock   *hlc.Clock
	metrics Metrics
}

var _ Cache = &sklImpl{}

// newSklImpl returns a new treeImpl with the supplied hybrid clock.
func newSklImpl(clock *hlc.Clock) *sklImpl {
	tc := sklImpl{clock: clock, metrics: makeMetrics()}
	tc.clear(clock.Now())
	return &tc
}

// clear clears the cache and resets the low-water mark.
func (tc *sklImpl) clear(lowWater hlc.Timestamp) {
	tc.cache = newIntervalSkl(tc.clock, MinRetentionWindow, tc.metrics.Skl)
	tc.cache.floorTS = lowWater
}

// Add implements the Cache interface.
func (tc *sklImpl) Add(start, end roachpb.Key, ts hlc.Timestamp, txnID uuid.UUID) {
	start, end = tc.boundKeyLengths(start, end)

	val := cacheValue{ts: ts, txnID: txnID}
	if len(end) == 0 {
		tc.cache.Add(nonNil(start), val)
	} else {
		tc.cache.AddRange(nonNil(start), end, excludeTo, val)
	}
}

// getLowWater implements the Cache interface.
func (tc *sklImpl) getLowWater() hlc.Timestamp {
	return tc.cache.FloorTS()
}

// GetMax implements the Cache interface.
func (tc *sklImpl) GetMax(start, end roachpb.Key) (hlc.Timestamp, uuid.UUID) {
	var val cacheValue
	if len(end) == 0 {
		val = tc.cache.LookupTimestamp(nonNil(start))
	} else {
		val = tc.cache.LookupTimestampRange(nonNil(start), end, excludeTo)
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
	maxKeySize := int(maximumSklPageSize / 32)

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
