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
	skltscache "github.com/nvanbenschoten/tscache"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type skiplistImpl struct {
	rCache, wCache *skltscache.Cache
}

var _ Cache = &skiplistImpl{}

func newSkiplistImpl(clock *hlc.Clock) *skiplistImpl {
	now := clock.Now()
	return &skiplistImpl{
		rCache: skltscache.NewWithFloor(defaultCacheSize, now),
		wCache: skltscache.NewWithFloor(defaultCacheSize, now),
	}
}

func spanToSklSpan(sp roachpb.Span) roachpb.Span {
	if len(sp.EndKey) == 0 {
		sp.EndKey = sp.Key.Next()
		sp.Key = sp.EndKey[:len(sp.Key)]
	}
	return sp
}

func (tc *skiplistImpl) AddRequest(req Request) {
	for i := range req.Reads {
		sp := &req.Reads[i]
		tc.add(sp.Key, sp.EndKey, req.Timestamp, req.TxnID, true)
	}
	for i := range req.Writes {
		sp := &req.Writes[i]
		tc.add(sp.Key, sp.EndKey, req.Timestamp, req.TxnID, false)
	}
	if req.Txn.Key != nil {
		// Make the transaction key from the request key. We're guaranteed
		// req.TxnID != nil because we only hit this code path for
		// EndTransactionRequests.
		key := keys.TransactionKey(req.Txn.Key, req.TxnID)
		// We set txnID=nil because we want hits for same txn ID.
		tv := skltscache.NewTimestampValue(req.Timestamp, uuid.UUID{})
		tc.wCache.Add(key, tv)
	}
}

func (tc *skiplistImpl) add(start, end roachpb.Key, timestamp hlc.Timestamp, txnID uuid.UUID, readTSCache bool) {
	tv := skltscache.NewTimestampValue(timestamp, txnID)
	tcache := tc.wCache
	if readTSCache {
		tcache = tc.rCache
	}
	if len(end) == 0 {
		end = start.Next()
		start = end[:len(start)]
	}
	tcache.AddRange(start, end, skltscache.ExcludeTo, tv)
}

func (tc *skiplistImpl) ExpandRequests(span roachpb.RSpan, timestamp hlc.Timestamp) {
	// no-op.
}

func (tc *skiplistImpl) SetLowWater(start, end roachpb.Key, timestamp hlc.Timestamp) {
	tc.rCache.AddRange(start, end, skltscache.ExcludeTo, skltscache.NewTimestampValue(timestamp, lowWaterTxnIDMarker))
	tc.wCache.AddRange(start, end, skltscache.ExcludeTo, skltscache.NewTimestampValue(timestamp, lowWaterTxnIDMarker))
}

func (tc *skiplistImpl) GlobalLowWater() hlc.Timestamp {
	rFloorTs := tc.rCache.FloorTS()
	wFloorTs := tc.wCache.FloorTS()
	if rFloorTs.Less(wFloorTs) {
		return rFloorTs
	}
	return wFloorTs
}

func (tc *skiplistImpl) GetMaxRead(start, end roachpb.Key) (hlc.Timestamp, uuid.UUID, bool) {
	return tc.getMax(start, end, true)
}

func (tc *skiplistImpl) GetMaxWrite(start, end roachpb.Key) (hlc.Timestamp, uuid.UUID, bool) {
	return tc.getMax(start, end, false)
}

func (tc *skiplistImpl) getMax(
	start, end roachpb.Key, readTSCache bool,
) (hlc.Timestamp, uuid.UUID, bool) {
	if end == nil {
		end = start
	}
	cache := tc.wCache
	if readTSCache {
		cache = tc.rCache
	}
	tv, floor := cache.LookupTimestampRangeWithFlag(start, end, skltscache.ExcludeTo)
	ts, txnID := tv.Ts(), tv.TxnID()
	if txnID == lowWaterTxnIDMarker {
		floor = true
	}
	return ts, txnID, !floor
}

func (tc *skiplistImpl) clear(lowWater hlc.Timestamp) {
	tc.rCache.Clear(lowWater)
	tc.wCache.Clear(lowWater)
}
