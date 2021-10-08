// Copyright 2017 Andy Kimball
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
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/andy-kimball/arenaskl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

var (
	emptyVal = cacheValue{}
	floorTS  = makeTS(100, 0)
	floorVal = makeValWithoutID(floorTS)
)

func makeTS(walltime int64, logical int32) hlc.Timestamp {
	return hlc.Timestamp{WallTime: walltime, Logical: logical}
}

func makeValWithoutID(ts hlc.Timestamp) cacheValue {
	return cacheValue{ts: ts, txnID: noTxnID}
}

func makeVal(ts hlc.Timestamp, txnIDStr string) cacheValue {
	txnIDBytes := []byte(txnIDStr)
	if len(txnIDBytes) < 16 {
		// If too short, pad front with zeros.
		oldTxnIDBytes := txnIDBytes
		txnIDBytes = make([]byte, 16)
		copy(txnIDBytes[16-len(oldTxnIDBytes):], oldTxnIDBytes)
	}
	txnID, err := uuid.FromBytes(txnIDBytes)
	if err != nil {
		panic(err)
	}
	return cacheValue{ts: ts, txnID: txnID}
}

func makeSklMetrics() sklMetrics {
	return makeMetrics().Skl
}

// setFixedPageSize sets the pageSize of the intervalSkl to a fixed value.
func (s *intervalSkl) setFixedPageSize(pageSize uint32) {
	s.pageSize = pageSize
	s.pageSizeFixed = true
	s.pages.Init() // clear
	s.pushNewPage(0 /* maxTime */, nil /* arena */)
}

// setMinPages sets the minimum number of pages intervalSkl will evict down to.
// This is only exposed as a testing method because there's no reason to use
// this outside of testing.
func (s *intervalSkl) setMinPages(minPages int) {
	s.minPages = minPages
}

func TestIntervalSklAdd(t *testing.T) {
	testutils.RunTrueAndFalse(t, "synthetic", func(t *testing.T, synthetic bool) {
		ts1 := makeTS(200, 0).WithSynthetic(synthetic)
		ts2 := makeTS(200, 201).WithSynthetic(synthetic)
		ts2Ceil := makeTS(202, 0).WithSynthetic(synthetic)

		val1 := makeVal(ts1, "1")
		val2 := makeVal(ts2, "2")

		s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())

		s.Add([]byte("apricot"), val1)
		require.Equal(t, ts1, s.frontPage().maxTime.get())
		require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
		require.Equal(t, val1, s.LookupTimestamp([]byte("apricot")))
		require.Equal(t, emptyVal, s.LookupTimestamp([]byte("banana")))

		s.Add([]byte("banana"), val2)
		require.Equal(t, ts2Ceil, s.frontPage().maxTime.get())
		require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
		require.Equal(t, val1, s.LookupTimestamp([]byte("apricot")))
		require.Equal(t, val2, s.LookupTimestamp([]byte("banana")))
		require.Equal(t, emptyVal, s.LookupTimestamp([]byte("cherry")))
	})
}

func TestIntervalSklSingleRange(t *testing.T) {
	val1 := makeVal(makeTS(100, 10), "1")
	val2 := makeVal(makeTS(200, 50), "2")
	val3 := makeVal(makeTS(300, 50), "3")
	val4 := makeVal(makeTS(400, 50), "4")

	s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())

	// val1:  [a--------------o]
	s.AddRange([]byte("apricot"), []byte("orange"), 0, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("raspberry")))

	// Try again and make sure it's a no-op.
	// val1:  [a--------------o]
	s.AddRange([]byte("apricot"), []byte("orange"), 0, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("raspberry")))

	// Ratchet up the timestamps.
	// val1:  [a--------------o]
	// val2:  [a--------------o]
	s.AddRange([]byte("apricot"), []byte("orange"), 0, val2)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("raspberry")))

	// Add disjoint open range.
	// val1:  [a--------------o]  (p--------------t)
	// val2:  [a--------------o]
	s.AddRange([]byte("pear"), []byte("tomato"), excludeFrom|excludeTo, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("peach")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("watermelon")))

	// Try again and make sure it's a no-op.
	// val1:  [a--------------o]  (p--------------t)
	// val2:  [a--------------o]
	s.AddRange([]byte("pear"), []byte("tomato"), excludeFrom|excludeTo, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("peach")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("watermelon")))

	// Ratchet up the timestamps.
	// val1:  [a--------------o]  (p--------------t)
	// val2:  [a--------------o]  (p--------------t)
	s.AddRange([]byte("pear"), []byte("tomato"), excludeFrom|excludeTo, val2)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("peach")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("watermelon")))

	// Add disjoint left-open range.
	// val1:  [a--------------o]  (p--------------t)
	// val2:  [a--------------o]  (p--------------t)
	// val3:                                     (t--------------w]
	s.AddRange([]byte("tomato"), []byte("watermelon"), excludeFrom, val3)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("peach")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("tomato")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("watermelon")))

	// Add disjoint right-open range.
	// val1:  [a--------------o]      (p--------------t)
	// val2:  [a--------------o]      (p--------------t)
	// val3:                                         (t--------------w]
	// val4:                      [p--p)
	s.AddRange([]byte("peach"), []byte("pear"), excludeTo, val4)
	require.Equal(t, val4, s.LookupTimestamp([]byte("peach")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("tomato")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("watermelon")))
}

func TestIntervalSklKeyBoundaries(t *testing.T) {
	val1 := makeVal(makeTS(200, 200), "1")
	val2 := makeVal(makeTS(200, 201), "2")
	val3 := makeVal(makeTS(300, 0), "3")
	val4 := makeVal(makeTS(400, 0), "4")
	val5 := makeVal(makeTS(500, 0), "5")

	s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())
	s.floorTS = floorTS

	// Can't insert a key at infinity.
	require.Panics(t, func() { s.Add([]byte(nil), val1) })
	require.Panics(t, func() { s.AddRange([]byte(nil), []byte(nil), 0, val1) })
	require.Panics(t, func() { s.AddRange([]byte(nil), []byte(nil), excludeFrom, val1) })
	require.Panics(t, func() { s.AddRange([]byte(nil), []byte(nil), excludeTo, val1) })
	require.Panics(t, func() { s.AddRange([]byte(nil), []byte(nil), excludeFrom|excludeTo, val1) })

	// Can't lookup a key at infinity.
	require.Panics(t, func() { s.LookupTimestamp([]byte(nil)) })
	require.Panics(t, func() { s.LookupTimestampRange([]byte(nil), []byte(nil), 0) })
	require.Panics(t, func() { s.LookupTimestampRange([]byte(nil), []byte(nil), excludeFrom) })
	require.Panics(t, func() { s.LookupTimestampRange([]byte(nil), []byte(nil), excludeTo) })
	require.Panics(t, func() { s.LookupTimestampRange([]byte(nil), []byte(nil), excludeFrom|excludeTo) })

	// Single timestamp at minimum key.
	// val1:  [""]
	s.Add([]byte(""), val1)
	require.Equal(t, val1, s.LookupTimestamp([]byte("")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))

	// Range extending to infinity. excludeTo doesn't make a difference because
	// the boundary is open.
	// val1:  [""]
	// val2:       [b----------->
	s.AddRange([]byte("banana"), nil, excludeTo, val2)
	require.Equal(t, val1, s.LookupTimestamp([]byte("")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("orange")))

	// val1:  [""]
	// val2:       [b----------->
	// val3:       [b----------->
	s.AddRange([]byte("banana"), nil, 0, val3)
	require.Equal(t, val1, s.LookupTimestamp([]byte("")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("orange")))

	// Range starting at the minimum key. excludeFrom makes a difference because
	// the boundary is closed.
	// val1:  [""]
	// val2:       [b----------->
	// val3:       [b----------->
	// val4:  (""----------k]
	s.AddRange([]byte(""), []byte("kiwi"), excludeFrom, val4)
	require.Equal(t, val1, s.LookupTimestamp([]byte("")))
	require.Equal(t, val4, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val4, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("orange")))

	// val1:  [""]
	// val2:       [b----------->
	// val3:       [b----------->
	// val4:  (""----------k]
	// val5:  [""----------k]
	s.AddRange([]byte(""), []byte("kiwi"), 0, val5)
	require.Equal(t, val5, s.LookupTimestamp([]byte("")))
	require.Equal(t, val5, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val5, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("orange")))
}

func TestIntervalSklSupersetRange(t *testing.T) {
	val1 := makeVal(makeTS(200, 1), "1")
	val2 := makeVal(makeTS(201, 0), "2")
	val3 := makeVal(makeTS(300, 0), "3")
	val4 := makeVal(makeTS(400, 0), "4")
	val5 := makeVal(makeTS(500, 0), "5")
	val6 := makeVal(makeTS(600, 0), "6")

	s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())
	s.floorTS = floorTS

	// Same range.
	// val1:  [k---------o]
	// val2:  [k---------o]
	s.AddRange([]byte("kiwi"), []byte("orange"), 0, val1)
	s.AddRange([]byte("kiwi"), []byte("orange"), 0, val2)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("mango")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("raspberry")))

	// Superset range, but with lower timestamp.
	// val1:  [g--------------p]
	// val2:    [k---------o]
	s.AddRange([]byte("grape"), []byte("pear"), 0, val1)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("watermelon")))

	// Superset range, but with higher timestamp.
	// val1:    [g--------------p]
	// val2:      [k---------o]
	// val3: [b-------------------r]
	s.AddRange([]byte("banana"), []byte("raspberry"), 0, val3)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("watermelon")))

	// Equal range but left-open.
	// val1:    [g--------------p]
	// val2:      [k---------o]
	// val3: [b-------------------r]
	// val4: (b-------------------r]
	s.AddRange([]byte("banana"), []byte("raspberry"), excludeFrom, val4)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val4, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, val4, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val4, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val4, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, val4, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("watermelon")))

	// Equal range but right-open.
	// val1:    [g--------------p]
	// val2:      [k---------o]
	// val3: [b-------------------r]
	// val4: (b-------------------r]
	// val5: [b-------------------r)
	s.AddRange([]byte("banana"), []byte("raspberry"), excludeTo, val5)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val5, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val5, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, val5, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val5, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val5, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, val4, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("watermelon")))

	// Equal range but fully-open.
	// val1:    [g--------------p]
	// val2:      [k---------o]
	// val3: [b-------------------r]
	// val4: (b-------------------r]
	// val5: [b-------------------r)
	// val6: (b-------------------r)
	s.AddRange([]byte("banana"), []byte("raspberry"), (excludeFrom | excludeTo), val6)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val5, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val6, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, val6, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val6, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val6, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, val4, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("watermelon")))
}

func TestIntervalSklContiguousRanges(t *testing.T) {
	ts1 := makeTS(201, 0)

	val1 := makeVal(ts1, "1")
	val2 := makeVal(ts1, "2")
	val2WithoutID := makeValWithoutID(ts1)

	s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())
	s.floorTS = floorTS

	// val1:  [b---------k)
	// val2:            [k---------o)
	s.AddRange([]byte("banana"), []byte("kiwi"), excludeTo, val1)
	s.AddRange([]byte("kiwi"), []byte("orange"), excludeTo, val2)

	// Test single-key lookups over the contiguous range.
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("mango")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("orange")))

	// Test range lookups over the contiguous range.
	require.Equal(t, floorVal, s.LookupTimestampRange([]byte(""), []byte("banana"), excludeTo))
	require.Equal(t, val1, s.LookupTimestampRange([]byte(""), []byte("kiwi"), excludeTo))
	require.Equal(t, val2WithoutID, s.LookupTimestampRange([]byte(""), []byte("orange"), excludeTo))
	require.Equal(t, val2WithoutID, s.LookupTimestampRange([]byte(""), []byte(nil), excludeTo))
	require.Equal(t, val1, s.LookupTimestampRange([]byte("banana"), []byte("kiwi"), excludeTo))
	require.Equal(t, val2, s.LookupTimestampRange([]byte("kiwi"), []byte("orange"), excludeTo))
	require.Equal(t, val2, s.LookupTimestampRange([]byte("kiwi"), []byte(nil), excludeTo))
	require.Equal(t, val2WithoutID, s.LookupTimestampRange([]byte("banana"), []byte("orange"), excludeTo))
	require.Equal(t, val2WithoutID, s.LookupTimestampRange([]byte("banana"), []byte(nil), excludeTo))
	require.Equal(t, floorVal, s.LookupTimestampRange([]byte("orange"), []byte(nil), excludeTo))
}

func TestIntervalSklOverlappingRanges(t *testing.T) {
	val1 := makeVal(makeTS(200, 1), "1")
	val2 := makeVal(makeTS(201, 0), "2")
	val3 := makeVal(makeTS(300, 0), "3")
	val4 := makeVal(makeTS(400, 0), "4")

	s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())
	s.floorTS = floorTS

	// val1:  [b---------k]
	// val2:        [g------------r)
	s.AddRange([]byte("banana"), []byte("kiwi"), 0, val1)
	s.AddRange([]byte("grape"), []byte("raspberry"), excludeTo, val2)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("raspberry")))

	// val1:    [b---------k]
	// val2:         [g------------r)
	// val3:  [a---------------o]
	s.AddRange([]byte("apricot"), []byte("orange"), 0, val3)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("raspberry")))

	// val1:    [b---------k]
	// val2:          [g------------r)
	// val3:  [a---------------o]
	// val4:              (k------------->
	s.AddRange([]byte("kiwi"), []byte(nil), excludeFrom, val4)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val4, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val4, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, val4, s.LookupTimestamp([]byte("raspberry")))
}

func TestIntervalSklSingleKeyRanges(t *testing.T) {
	val1 := makeVal(makeTS(100, 100), "1")

	s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())

	// Don't allow inverted ranges.
	require.Panics(t, func() { s.AddRange([]byte("kiwi"), []byte("apple"), 0, val1) })
	require.Equal(t, ratchetingTime(0), s.frontPage().maxTime)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("raspberry")))

	// If from key is same as to key and both are excluded, then range is
	// zero-length.
	s.AddRange([]byte("banana"), []byte("banana"), excludeFrom|excludeTo, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("kiwi")))

	// If from key is same as to key and at least one endpoint is included, then
	// range has length one.
	s.AddRange([]byte("mango"), []byte("mango"), 0, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("mango")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("cherry")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("orange")))

	s.AddRange([]byte("banana"), []byte("banana"), excludeFrom, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("mango")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("cherry")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("orange")))

	s.AddRange([]byte("cherry"), []byte("cherry"), excludeTo, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("mango")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("cherry")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("orange")))
}

func TestIntervalSklRatchetTxnIDs(t *testing.T) {
	ts1 := makeTS(100, 100)
	ts2 := makeTS(250, 50)
	ts3 := makeTS(350, 50)

	val1 := makeVal(ts1, "1")
	val2 := makeVal(ts1, "2")
	val2WithoutID := makeValWithoutID(ts1)
	val3 := makeVal(ts2, "2") // same txn ID as tsVal2
	val4 := makeVal(ts2, "3")
	val4WithoutID := makeValWithoutID(ts2)
	val5 := makeVal(ts3, "4")
	val6 := makeVal(ts3, "5")
	val6WithoutID := makeValWithoutID(ts3)

	s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())

	s.AddRange([]byte("apricot"), []byte("raspberry"), 0, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("tomato")))

	// Ratchet up the txnID with the same timestamp; txnID should be removed.
	s.AddRange([]byte("apricot"), []byte("tomato"), 0, val2)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val2WithoutID, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val2WithoutID, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val2WithoutID, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("watermelon")))

	// Ratchet up the timestamp with the same txnID.
	s.AddRange([]byte("apricot"), []byte("orange"), 0, val3)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val2WithoutID, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("watermelon")))

	// Ratchet up the txnID with the same timestamp; txnID should be removed.
	s.AddRange([]byte("apricot"), []byte("banana"), 0, val4)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val4WithoutID, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val4WithoutID, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val2WithoutID, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("watermelon")))

	// Ratchet up the timestamp with a new txnID using excludeTo.
	s.AddRange([]byte("apricot"), []byte("orange"), excludeTo, val5)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val5, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val5, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val2WithoutID, s.LookupTimestamp([]byte("raspberry")))

	// Ratchet up the txnID with the same timestamp using excludeTo; txnID should be removed.
	s.AddRange([]byte("apricot"), []byte("banana"), excludeTo, val6)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val6WithoutID, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val5, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val2WithoutID, s.LookupTimestamp([]byte("raspberry")))

	// Ratchet up the txnID with the same timestamp using excludeFrom; txnID should be removed.
	s.AddRange([]byte("banana"), []byte(nil), excludeFrom, val6)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val6WithoutID, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val5, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val6, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val6, s.LookupTimestamp([]byte("raspberry")))
}

func TestIntervalSklLookupRange(t *testing.T) {
	ts1 := makeTS(100, 100)
	ts2 := makeTS(200, 201)
	ts3 := makeTS(300, 201)
	ts4 := makeTS(400, 201)

	val1 := makeVal(ts1, "1")
	val2 := makeVal(ts2, "2")
	val3 := makeVal(ts2, "3")
	val3WithoutID := makeValWithoutID(ts2)
	val4 := makeVal(ts3, "4")
	val5 := makeVal(ts4, "5")

	s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())

	// Perform range lookups over a single key.
	s.Add([]byte("apricot"), val1)
	// from = "" and to = nil means that we're scanning over all keys.
	require.Equal(t, val1, s.LookupTimestampRange([]byte(""), []byte(nil), 0))
	require.Equal(t, val1, s.LookupTimestampRange([]byte(""), []byte(nil), excludeFrom))
	require.Equal(t, val1, s.LookupTimestampRange([]byte(""), []byte(nil), excludeTo))
	require.Equal(t, val1, s.LookupTimestampRange([]byte(""), []byte(nil), (excludeFrom|excludeTo)))

	require.Equal(t, emptyVal, s.LookupTimestampRange([]byte("apple"), []byte("apple"), 0))
	require.Equal(t, val1, s.LookupTimestampRange([]byte("apple"), []byte("apricot"), 0))
	require.Equal(t, val1, s.LookupTimestampRange([]byte("apple"), []byte("apricot"), excludeFrom))
	require.Equal(t, emptyVal, s.LookupTimestampRange([]byte("apple"), []byte("apricot"), excludeTo))

	require.Equal(t, val1, s.LookupTimestampRange([]byte("apricot"), []byte("apricot"), 0))
	require.Equal(t, val1, s.LookupTimestampRange([]byte("apricot"), []byte("apricot"), excludeFrom))
	require.Equal(t, val1, s.LookupTimestampRange([]byte("apricot"), []byte("apricot"), excludeTo))
	require.Equal(t, emptyVal, s.LookupTimestampRange([]byte("apricot"), []byte("apricot"), (excludeFrom|excludeTo)))

	// Perform range lookups over a series of keys.
	s.Add([]byte("banana"), val2)
	s.Add([]byte("cherry"), val3)
	require.Equal(t, val2, s.LookupTimestampRange([]byte("apricot"), []byte("banana"), 0))
	require.Equal(t, val2, s.LookupTimestampRange([]byte("apricot"), []byte("banana"), excludeFrom))
	require.Equal(t, val1, s.LookupTimestampRange([]byte("apricot"), []byte("banana"), excludeTo))
	require.Equal(t, emptyVal, s.LookupTimestampRange([]byte("apricot"), []byte("banana"), (excludeFrom|excludeTo)))

	require.Equal(t, val3WithoutID, s.LookupTimestampRange([]byte("apricot"), []byte("cherry"), 0))
	require.Equal(t, val3WithoutID, s.LookupTimestampRange([]byte("apricot"), []byte("cherry"), excludeFrom))
	require.Equal(t, val2, s.LookupTimestampRange([]byte("apricot"), []byte("cherry"), excludeTo))
	require.Equal(t, val2, s.LookupTimestampRange([]byte("apricot"), []byte("cherry"), (excludeFrom|excludeTo)))

	require.Equal(t, val3WithoutID, s.LookupTimestampRange([]byte("banana"), []byte("cherry"), 0))
	require.Equal(t, val3, s.LookupTimestampRange([]byte("banana"), []byte("cherry"), excludeFrom))
	require.Equal(t, val2, s.LookupTimestampRange([]byte("banana"), []byte("cherry"), excludeTo))
	require.Equal(t, emptyVal, s.LookupTimestampRange([]byte("banana"), []byte("cherry"), (excludeFrom|excludeTo)))

	// Open ranges should scan until the last key.
	require.Equal(t, val3WithoutID, s.LookupTimestampRange([]byte("apricot"), []byte(nil), 0))
	require.Equal(t, val3WithoutID, s.LookupTimestampRange([]byte("banana"), []byte(nil), 0))
	require.Equal(t, val3, s.LookupTimestampRange([]byte("cherry"), []byte(nil), 0))
	require.Equal(t, emptyVal, s.LookupTimestampRange([]byte("tomato"), []byte(nil), 0))

	// Subset lookup range.
	s.AddRange([]byte("apple"), []byte("cherry"), excludeTo, val4)
	require.Equal(t, val4, s.LookupTimestampRange([]byte("apple"), []byte("berry"), 0))
	require.Equal(t, val4, s.LookupTimestampRange([]byte("apple"), []byte("berry"), excludeFrom))
	require.Equal(t, val4, s.LookupTimestampRange([]byte("berry"), []byte("blueberry"), 0))
	require.Equal(t, val4, s.LookupTimestampRange([]byte("berry"), []byte("cherry"), 0))
	require.Equal(t, val4, s.LookupTimestampRange([]byte("berry"), []byte("cherry"), excludeTo))

	// Overlapping range without endpoints.
	s.AddRange([]byte("banana"), []byte("cherry"), (excludeFrom | excludeTo), val5)
	require.Equal(t, val4, s.LookupTimestampRange([]byte("apple"), []byte("banana"), (excludeFrom|excludeTo)))
	require.Equal(t, val5, s.LookupTimestampRange([]byte("banana"), []byte("cherry"), (excludeFrom|excludeTo)))
	require.Equal(t, val5, s.LookupTimestampRange([]byte("apple"), []byte("cherry"), (excludeFrom|excludeTo)))
}

func TestIntervalSklLookupRangeSingleKeyRanges(t *testing.T) {
	ts1 := makeTS(100, 100)
	ts2 := makeTS(200, 201)

	val1 := makeVal(ts1, "1")
	val2 := makeVal(ts2, "2")
	val3 := makeVal(ts2, "3")
	val3WithoutID := makeValWithoutID(ts2)

	key1 := []byte("a")
	key2 := append(key1, 0x0)
	key3 := append(key2, 0x0)
	key4 := append(key3, 0x0)

	// Perform range lookups over [key, key.Next()) ranges.
	t.Run("[key, key.Next())", func(t *testing.T) {
		s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())

		s.AddRange(key1, key2, excludeTo, val1)
		s.AddRange(key2, key3, excludeTo, val2)
		s.AddRange(key3, key4, excludeTo, val3)

		require.Equal(t, val1, s.LookupTimestampRange([]byte(""), key1, 0))
		require.Equal(t, emptyVal, s.LookupTimestampRange([]byte(""), key1, excludeTo))
		require.Equal(t, val2, s.LookupTimestampRange([]byte(""), key2, 0))
		require.Equal(t, val1, s.LookupTimestampRange([]byte(""), key2, excludeTo))

		require.Equal(t, val2, s.LookupTimestampRange(key1, key2, 0))
		require.Equal(t, val2, s.LookupTimestampRange(key1, key2, excludeFrom))
		require.Equal(t, val1, s.LookupTimestampRange(key1, key2, excludeTo))
		// This may be surprising. We actually return the gapVal of the first range
		// even though there isn't a discrete byte value between key1 and key2 (this
		// is a feature, not a bug!). It demonstrates the difference between the
		// first two options (which behave exactly the same) and the third:
		// a) Add(key, val)
		// b) AddRange(key, key, 0, val)
		// c) AddRange(key, key.Next(), excludeTo, val)
		//
		// NB: If the behavior is not needed, it's better to use one of the
		// first two options because they allow us to avoid storing a gap value.
		require.Equal(t, val1, s.LookupTimestampRange(key1, key2, (excludeFrom|excludeTo)))

		// val1 and val2 both have the same timestamp but have different IDs, so
		// the cacheValue ratcheting policy enforces that the result of scanning
		// over both should be a value with their timestamp but with no txnID.
		require.Equal(t, val3WithoutID, s.LookupTimestampRange(key1, key3, 0))
		require.Equal(t, val3WithoutID, s.LookupTimestampRange(key1, key3, excludeFrom))
		require.Equal(t, val2, s.LookupTimestampRange(key1, key3, excludeTo))
		require.Equal(t, val2, s.LookupTimestampRange(key1, key3, (excludeFrom|excludeTo)))

		require.Equal(t, val3WithoutID, s.LookupTimestampRange(key2, key3, 0))
		// Again, this may be surprising. The logic is the same as above.
		require.Equal(t, val3WithoutID, s.LookupTimestampRange(key2, key3, excludeFrom))
		require.Equal(t, val2, s.LookupTimestampRange(key2, key3, excludeTo))
		require.Equal(t, val2, s.LookupTimestampRange(key2, key3, (excludeFrom|excludeTo)))

		require.Equal(t, val3, s.LookupTimestampRange(key3, []byte(nil), 0))
		require.Equal(t, val3, s.LookupTimestampRange(key3, []byte(nil), excludeFrom))
	})

	// Perform the same lookups, but this time use single key ranges.
	t.Run("[key, key]", func(t *testing.T) {
		s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())

		s.AddRange(key1, key1, 0, val1) // same as Add(key1, val1)
		s.AddRange(key2, key2, 0, val2) //   ...   Add(key2, val2)
		s.AddRange(key3, key3, 0, val3) //   ...   Add(key3, val3)

		require.Equal(t, val1, s.LookupTimestampRange([]byte(""), key1, 0))
		require.Equal(t, emptyVal, s.LookupTimestampRange([]byte(""), key1, excludeTo))
		require.Equal(t, val2, s.LookupTimestampRange([]byte(""), key2, 0))
		require.Equal(t, val1, s.LookupTimestampRange([]byte(""), key2, excludeTo))

		require.Equal(t, val2, s.LookupTimestampRange(key1, key2, 0))
		require.Equal(t, val2, s.LookupTimestampRange(key1, key2, excludeFrom))
		require.Equal(t, val1, s.LookupTimestampRange(key1, key2, excludeTo))
		// DIFFERENT!
		require.Equal(t, emptyVal, s.LookupTimestampRange(key1, key2, (excludeFrom|excludeTo)))

		require.Equal(t, val3WithoutID, s.LookupTimestampRange(key1, key3, 0))
		require.Equal(t, val3WithoutID, s.LookupTimestampRange(key1, key3, excludeFrom))
		require.Equal(t, val2, s.LookupTimestampRange(key1, key3, excludeTo))
		require.Equal(t, val2, s.LookupTimestampRange(key1, key3, (excludeFrom|excludeTo)))

		require.Equal(t, val3WithoutID, s.LookupTimestampRange(key2, key3, 0))
		// DIFFERENT!
		require.Equal(t, val3, s.LookupTimestampRange(key2, key3, excludeFrom))
		require.Equal(t, val2, s.LookupTimestampRange(key2, key3, excludeTo))
		// DIFFERENT!
		require.Equal(t, emptyVal, s.LookupTimestampRange(key2, key3, (excludeFrom|excludeTo)))

		require.Equal(t, val3, s.LookupTimestampRange(key3, []byte(nil), 0))
		// DIFFERENT!
		require.Equal(t, emptyVal, s.LookupTimestampRange(key3, []byte(nil), excludeFrom))
	})
}

// TestIntervalSklLookupEqualsEarlierMaxTime tests that we properly handle
// the lookup when the timestamp for a range found in the later page is equal to
// the maxTime of the earlier page.
func TestIntervalSklLookupEqualsEarlierMaxTime(t *testing.T) {
	ts1 := makeTS(200, 0) // without Logical part
	ts2 := makeTS(200, 1) // with Logical part
	ts2Ceil := makeTS(202, 0)

	txnID1 := "1"
	txnID2 := "2"

	testutils.RunTrueAndFalse(t, "logical", func(t *testing.T, logical bool) {
		testutils.RunTrueAndFalse(t, "synthetic", func(t *testing.T, synthetic bool) {

			s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())
			s.floorTS = floorTS

			// Insert an initial value into intervalSkl.
			initTS := ts1
			if logical {
				initTS = ts2
			}
			initTS = initTS.WithSynthetic(synthetic)
			origVal := makeVal(initTS, txnID1)
			s.AddRange([]byte("banana"), []byte("orange"), 0, origVal)

			// Verify the later page's maxTime is what we expect.
			expMaxTS := ts1
			if logical {
				expMaxTS = ts2Ceil
			}
			expMaxTS = expMaxTS.WithSynthetic(synthetic)
			require.Equal(t, expMaxTS, s.frontPage().maxTime.get())

			// Rotate the page so that new writes will go to a different page.
			s.rotatePages(s.frontPage())

			// Write to overlapping and non-overlapping parts of the new page
			// with the values that have the same timestamp as the maxTime of
			// the earlier page. One value has the same txnID as the previous
			// write in the earlier page and one has a different txnID.
			valSameID := makeVal(expMaxTS, txnID1)
			valDiffID := makeVal(expMaxTS, txnID2)
			valNoID := makeValWithoutID(expMaxTS)
			s.Add([]byte("apricot"), valSameID)
			s.Add([]byte("banana"), valSameID)
			s.Add([]byte("orange"), valDiffID)
			s.Add([]byte("raspberry"), valDiffID)

			require.Equal(t, valSameID, s.LookupTimestamp([]byte("apricot")))
			require.Equal(t, valSameID, s.LookupTimestamp([]byte("banana")))
			if logical {
				// If the initial timestamp had a logical part then
				// s.earlier.maxTime is inexact (see ratchetMaxTimestamp). When
				// we search in the earlier page, we'll find the exact timestamp
				// of the overlapping range and realize that its not the same as
				// the timestamp of the range in the later page. Because of
				// this, ratchetValue WON'T remove the txnID.
				require.Equal(t, valDiffID, s.LookupTimestamp([]byte("orange")))
			} else {
				// If the initial timestamp did not have a logical part then
				// s.earlier.maxTime is exact. When we search in the earlier
				// page, we'll find the overlapping range and realize that it is
				// the same as the timestamp of the range in the later page.
				// Because of this, ratchetValue WILL remove the txnID.
				require.Equal(t, valNoID, s.LookupTimestamp([]byte("orange")))
			}
			require.Equal(t, valDiffID, s.LookupTimestamp([]byte("raspberry")))
			require.Equal(t, floorVal, s.LookupTimestamp([]byte("tomato")))
		})
	})
}

func TestIntervalSklFill(t *testing.T) {
	const n = 200
	const txnID = "123"

	s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())
	s.setFixedPageSize(1500)

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		s.AddRange(key, key, 0, makeVal(makeTS(int64(100+i), int32(i)), txnID))
	}

	require.True(t, makeTS(100, 0).Less(s.floorTS))

	// Verify that the last key inserted is still in the intervalSkl and has not
	// been rotated out.
	lastKey := []byte(fmt.Sprintf("%05d", n-1))
	expVal := makeVal(makeTS(int64(100+n-1), int32(n-1)), txnID)
	require.Equal(t, expVal, s.LookupTimestamp(lastKey))

	// Verify that all keys inserted are either still in the intervalSkl or have
	// been rotated out and used to ratchet the floorTS.
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		require.False(t, s.LookupTimestamp(key).ts.Less(s.floorTS))
	}
}

// Repeatedly fill the structure and make sure timestamp lookups always increase.
func TestIntervalSklFill2(t *testing.T) {
	const n = 10000
	const txnID = "123"

	// n >> 1000 so the intervalSkl's pages will be filled.
	s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())
	s.setFixedPageSize(1000)

	key := []byte("some key")
	for i := 0; i < n; i++ {
		val := makeVal(makeTS(int64(i), int32(i)), txnID)
		s.Add(key, val)
		require.True(t, val.ts.LessEq(s.LookupTimestamp(key).ts))
	}
}

// TestIntervalSklMinRetentionWindow tests that if a value is within the minimum
// retention window, its page will never be evicted and it will never be subsumed
// by the floor timestamp.
func TestIntervalSklMinRetentionWindow(t *testing.T) {
	manual := hlc.NewManualClock(200)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)

	const minRet = 500
	s := newIntervalSkl(clock, minRet, makeSklMetrics())
	s.setFixedPageSize(1500)
	s.floorTS = floorTS

	// Add an initial value. Rotate the page so it's alone.
	origKey := []byte("banana")
	origVal := makeVal(clock.Now(), "1")
	s.Add(origKey, origVal)
	s.rotatePages(s.frontPage())

	// Add a large number of other values, forcing rotations. Continue until
	// there are more pages than s.minPages.
	manual.Increment(300)
	for i := 0; s.pages.Len() <= s.minPages; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		s.Add(key, makeVal(clock.Now(), "2"))
	}

	// We should still be able to look up the initial value.
	require.Equal(t, origVal, s.LookupTimestamp(origKey))

	// Even if we rotate the pages, we should still be able to look up the
	// value. No pages should be evicted.
	pagesBefore := s.pages.Len()
	s.rotatePages(s.frontPage())
	require.Equal(t, pagesBefore+1, s.pages.Len(), "page should not be evicted")
	require.Equal(t, origVal, s.LookupTimestamp(origKey))

	// Increment the clock so that the original value is not in the minimum
	// retention window. Rotate the pages and the back page should be evicted.
	manual.Increment(300)
	s.rotatePages(s.frontPage())

	newVal := s.LookupTimestamp(origKey)
	require.NotEqual(t, origVal, newVal, "the original value should be evicted")

	_, update := ratchetValue(origVal, newVal)
	require.True(t, update, "the original value should have been ratcheted to the new value")

	// Increment the clock again so that all the other values can be evicted.
	// The pages should collapse back down to s.minPages.
	manual.Increment(300)
	s.rotatePages(s.frontPage())
	require.Equal(t, s.pages.Len(), s.minPages)
}

// TestIntervalSklRotateWithSyntheticTimestamps tests that if a page is evicted
// and subsumed by the floor timestamp, then the floor timestamp will continue
// to carry the synthtic flag, if necessary.
func TestIntervalSklRotateWithSyntheticTimestamps(t *testing.T) {
	manual := hlc.NewManualClock(200)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)

	const minRet = 500
	s := newIntervalSkl(clock, minRet, makeSklMetrics())
	s.setFixedPageSize(1500)
	s.floorTS = floorTS

	// Add an initial value with a synthetic timestamp.
	// Rotate the page so it's alone.
	origKey := []byte("banana")
	origTS := clock.Now().WithSynthetic(true)
	origVal := makeVal(origTS, "1")
	s.Add(origKey, origVal)
	s.rotatePages(s.frontPage())

	// We should still be able to look up the initial value.
	require.Equal(t, origVal, s.LookupTimestamp(origKey))

	// Increment the clock so that the original value is not in the minimum
	// retention window. Rotate the pages and the back page should be evicted.
	manual.Increment(600)
	s.rotatePages(s.frontPage())

	// The initial value's page was evicted, so it should no longer exist.
	// However, since it had the highest timestamp of all values added, its
	// timestamp should still exist. Critically, this timestamp should still
	// be marked as synthetic.
	newVal := s.LookupTimestamp(origKey)
	require.NotEqual(t, origVal, newVal, "the original value should be evicted")
	require.Equal(t, uuid.Nil, newVal.txnID, "the original value's txn ID should be lost")
	require.Equal(t, origVal.ts, newVal.ts, "the original value's timestamp should persist")
	require.True(t, newVal.ts.Synthetic, "the synthetic flag should persist")
}

func TestIntervalSklConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer util.EnableRacePreemptionPoints()()

	testCases := []struct {
		name     string
		pageSize uint32
		minPages int
	}{
		// Test concurrency with a small page size in order to force lots of
		// page rotations.
		{name: "Rotates", pageSize: 4096},
		// Test concurrency with a small page size and a large number of pages
		// in order to force lots of page growth.
		{name: "Pages", pageSize: 4096, minPages: 16},
		// Test concurrency with a larger page size in order to test slot
		// concurrency without the added complication of page rotations.
		{name: "Slots", pageSize: initialSklPageSize},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Run one subtest using a real clock to generate timestamps and one
			// subtest using a fake clock to generate timestamps. The former is
			// good for simulating real conditions while the latter is good for
			// testing timestamp collisions.
			testutils.RunTrueAndFalse(t, "useClock", func(t *testing.T, useClock bool) {
				clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
				s := newIntervalSkl(clock, 0 /* minRet */, makeSklMetrics())
				s.setFixedPageSize(tc.pageSize)
				if tc.minPages != 0 {
					s.setMinPages(tc.minPages)
				}

				// We run a goroutine for each slot. Goroutines insert new value
				// over random intervals, but verify that the value in their
				// slot always ratchets.
				slots := 4 * runtime.GOMAXPROCS(0)
				if util.RaceEnabled {
					// We add in a lot of preemption points when race detection
					// is enabled, so things will already be very slow. Reduce
					// the concurrency to that we don't time out.
					slots /= 2
				}

				var wg sync.WaitGroup
				for i := 0; i < slots; i++ {
					wg.Add(1)

					// Each goroutine gets its own key to watch as it performs
					// random operations.
					go func(i int) {
						defer wg.Done()

						rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
						key := []byte(fmt.Sprintf("%05d", i))
						txnID := uuid.MakeV4()
						maxVal := cacheValue{}

						rounds := 1000
						if util.RaceEnabled {
							// Reduce the number of rounds for race builds.
							rounds /= 2
						}
						for j := 0; j < rounds; j++ {
							// Choose a random range.
							from, middle, to := randRange(rng, slots)
							opt := randRangeOpt(rng)

							// Add a new value to the range.
							ts := hlc.Timestamp{WallTime: int64(j)}
							if useClock {
								ts = clock.Now()
							}
							if rng.Intn(2) == 0 {
								ts = ts.WithSynthetic(true)
							}
							nowVal := cacheValue{ts: ts, txnID: txnID}
							s.AddRange(from, to, opt, nowVal)

							// Test single-key lookup at from, if possible.
							if (opt & excludeFrom) == 0 {
								val := s.LookupTimestamp(from)
								assertRatchet(t, nowVal, val)
							}

							// Test single-key lookup between from and to, if possible.
							if middle != nil {
								val := s.LookupTimestamp(middle)
								assertRatchet(t, nowVal, val)
							}

							// Test single-key lookup at to, if possible.
							if (opt & excludeTo) == 0 {
								val := s.LookupTimestamp(to)
								assertRatchet(t, nowVal, val)
							}

							// Test range lookup between from and to, if possible.
							if !(bytes.Equal(from, to) && opt == (excludeFrom|excludeTo)) {
								val := s.LookupTimestampRange(from, to, opt)
								assertRatchet(t, nowVal, val)
							}

							// Make sure the value at our key did not decrease.
							val := s.LookupTimestamp(key)
							assertRatchet(t, maxVal, val)
							maxVal = val
						}
					}(i)
				}

				wg.Wait()
			})
		})
	}
}

func TestIntervalSklConcurrentVsSequential(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer util.EnableRacePreemptionPoints()()

	// Run one subtest using a real clock to generate timestamps and one subtest
	// using a fake clock to generate timestamps. The former is good for
	// simulating real conditions while the latter is good for testing timestamp
	// collisions.
	testutils.RunTrueAndFalse(t, "useClock", func(t *testing.T, useClock bool) {
		rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
		clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

		const smallPageSize = 32 * 1024 // 32 KB
		const retainForever = math.MaxInt64
		sequentialS := newIntervalSkl(clock, retainForever, makeSklMetrics())
		sequentialS.setFixedPageSize(smallPageSize)
		concurrentS := newIntervalSkl(clock, retainForever, makeSklMetrics())
		concurrentS.setFixedPageSize(smallPageSize)

		// We run a goroutine for each slot. Goroutines insert new value
		// over random intervals, but verify that the value in their
		// slot always ratchets.
		slots := 4 * runtime.GOMAXPROCS(0)
		if util.RaceEnabled {
			// We add in a lot of preemption points when race detection
			// is enabled, so things will already be very slow. Reduce
			// the concurrency to that we don't time out.
			slots /= 2
		}

		txnIDs := make([]uuid.UUID, slots)
		for i := range txnIDs {
			txnIDs[i] = uuid.MakeV4()
		}

		rounds := 1000
		if util.RaceEnabled {
			// Reduce the number of rounds for race builds.
			rounds /= 2
		}
		for j := 0; j < rounds; j++ {
			// This is a lot of log output so only un-comment to debug.
			// t.Logf("round %d", j)

			// Create a set of actions to perform.
			type action struct {
				from, middle, to []byte
				opt              rangeOptions
				val              cacheValue
			}
			actions := make([]action, slots)
			for i := range actions {
				var a action
				a.from, a.middle, a.to = randRange(rng, slots)
				a.opt = randRangeOpt(rng)

				ts := hlc.Timestamp{WallTime: int64(j)}
				if useClock {
					ts = clock.Now()
				}
				if rng.Intn(2) == 0 {
					ts = ts.WithSynthetic(true)
				}
				a.val = cacheValue{ts: ts, txnID: txnIDs[i]}

				// This is a lot of log output so only un-comment to debug.
				// t.Logf("action (%s,%s)[%d] = %s", string(a.from), string(a.to), a.opt, a.val)
				actions[i] = a
			}

			// Perform each action, first in order on the "sequential"
			// intervalSkl, then in parallel on the "concurrent" intervalSkl.
			// t.Log("sequential actions")
			for _, a := range actions {
				sequentialS.AddRange(a.from, a.to, a.opt, a.val)
			}

			// t.Log("concurrent actions")
			var wg sync.WaitGroup
			for _, a := range actions {
				wg.Add(1)
				go func(a action) {
					concurrentS.AddRange(a.from, a.to, a.opt, a.val)
					wg.Done()
				}(a)
			}
			wg.Wait()

			// Ask each intervalSkl the same questions. We should get the same
			// answers.
			for _, a := range actions {
				// Test single-key lookup at from, if possible.
				if (a.opt & excludeFrom) == 0 {
					valS := sequentialS.LookupTimestamp(a.from)
					valC := concurrentS.LookupTimestamp(a.from)
					require.Equal(t, valS, valC, "key=%s", string(a.from))
					assertRatchet(t, a.val, valS)
				}

				// Test single-key lookup between from and to, if possible.
				if a.middle != nil {
					valS := sequentialS.LookupTimestamp(a.middle)
					valC := concurrentS.LookupTimestamp(a.middle)
					require.Equal(t, valS, valC, "key=%s", string(a.middle))
					assertRatchet(t, a.val, valS)
				}

				// Test single-key lookup at to, if possible.
				if (a.opt & excludeTo) == 0 {
					valS := sequentialS.LookupTimestamp(a.to)
					valC := concurrentS.LookupTimestamp(a.to)
					require.Equal(t, valS, valC, "key=%s", string(a.to))
					assertRatchet(t, a.val, valS)
				}

				// Test range lookup between from and to, if possible.
				if !(bytes.Equal(a.from, a.to) && a.opt == (excludeFrom|excludeTo)) {
					valS := sequentialS.LookupTimestampRange(a.from, a.to, a.opt)
					valC := concurrentS.LookupTimestampRange(a.from, a.to, a.opt)
					require.Equal(t, valS, valC, "range=(%s,%s)[%d]",
						string(a.from), string(a.to), a.opt)
					assertRatchet(t, a.val, valS)
				}
			}
		}
	})
}

// assertRatchet asserts that it would be possible for the first cacheValue
// (before) to be ratcheted to the second cacheValue (after).
func assertRatchet(t *testing.T, before, after cacheValue) {
	// Value ratcheting is an anti-symmetric relation R, so if R(before, after)
	// holds with before != after, then R(after, before) must not hold. Another
	// way to look at this is that ratcheting is a monotonically increasing
	// function, so if after comes later than before and the two are not equal,
	// then before could not also come later than after.
	//
	// If before == after, ratcheting will be a no-op, so the assertion will
	// still hold.
	_, upgrade := ratchetValue(after, before)
	require.False(t, upgrade, "ratchet inversion from %s to %s", before, after)
}

// TestIntervalSklMaxEncodedSize ensures that we do not enter an infinite page
// rotation loop for ranges that are too large to fit in a single page. Instead,
// we detect this scenario early and panic.
func TestIntervalSklMaxEncodedSize(t *testing.T) {
	manual := hlc.NewManualClock(200)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)

	ts := clock.Now()
	val := makeVal(ts, "1")

	testutils.RunTrueAndFalse(t, "fit", func(t *testing.T, fit bool) {
		testutils.RunTrueAndFalse(t, "fixed", func(t *testing.T, fixed bool) {
			var key []byte
			var encSize int
			if fixed {
				// Create an arbitrarily sized key. We'll set the pageSize to
				// either exactly accommodate this or to be one byte too small.
				key = make([]byte, 65)
				encSize = encodedRangeSize(key, nil, 0)
			} else {
				// Create either the largest possible key that will fit in the
				// maximumSklPageSize or a key one byte larger than this. This
				// test forces the intervalSkl to quickly grow its page size
				// until it is large enough to accommodate the key.
				encSize = maximumSklPageSize - initialSklAllocSize
				encOverhead := encodedRangeSize(nil, nil, 0)
				keySize := encSize - encOverhead
				if !fit {
					keySize++
				}
				key = make([]byte, keySize)
				if fit {
					require.Equal(t, encSize, encodedRangeSize(key, nil, 0))
				} else {
					require.Equal(t, encSize+1, encodedRangeSize(key, nil, 0))
				}
			}

			s := newIntervalSkl(clock, 1, makeSklMetrics())
			if fixed {
				fixedSize := uint32(initialSklAllocSize + encSize)
				if !fit {
					fixedSize--
				}
				s.setFixedPageSize(fixedSize)
			}
			initPageSize := s.pageSize

			if fit {
				require.NotPanics(t, func() { s.Add(key, val) })
			} else {
				require.Panics(t, func() { s.Add(key, val) })
			}

			if fit && !fixed {
				// Page size should have grown to maximum.
				require.Equal(t, uint32(maximumSklPageSize), s.pageSize)
			} else {
				// Page size should not have grown.
				require.Equal(t, initPageSize, s.pageSize)
			}
		})
	})
}

// TestArenaReuse tests that arenas are re-used when possible during page
// rotations. Skiplist memory arenas are only re-used when they have the same
// capacity as the new page.
func TestArenaReuse(t *testing.T) {
	s := newIntervalSkl(nil /* clock */, 0 /* minRet */, makeSklMetrics())

	// Track the unique arenas that we observe in use.
	arenas := make(map[*arenaskl.Arena]struct{})
	const iters = 256
	for i := 0; i < iters; i++ {
		for e := s.pages.Front(); e != nil; e = e.Next() {
			p := e.Value.(*sklPage)
			arenas[p.list.Arena()] = struct{}{}
		}
		s.rotatePages(s.frontPage())
	}

	// We expect to see a single arena with each of the allocation sizes between
	// initialSklPageSize and maximumSklPageSize. We then expect to see repeated
	// pages with the same size once we hit maximumSklPageSize. Only then do we
	// expect to see arena re-use.
	//
	// Example:
	//  initSize = 4
	//  maxSize  = 32
	//  minPages = 2
	//
	//  arena sizes:
	//   4  (A1)
	//   8  (A2)
	//   16 (A3)
	//   32 (A4)
	//   32 (A5)
	//   32 (A4)
	//   32 (A5)
	//   ...
	//
	intermediatePages := int(math.Log2(maximumSklPageSize) - math.Log2(initialSklPageSize))
	expArenas := defaultMinSklPages + intermediatePages
	require.Less(t, expArenas, iters)
	require.Equal(t, expArenas, len(arenas))
}

// TestEncodedValSize tests that the encodedValSize does not change unexpectedly
// due to changes in the cacheValue struct size. If the struct size does change,
// if should be done so deliberately.
func TestEncodedValSize(t *testing.T) {
	require.Equal(t, encodedValSize, int(unsafe.Sizeof(cacheValue{})), "encodedValSize should equal sizeof(cacheValue{})")
	require.Equal(t, 32, encodedValSize, "encodedValSize should not change unexpectedly")
}

func BenchmarkIntervalSklAdd(b *testing.B) {
	const max = 500000000 // max size of range
	const txnID = "123"

	clock := hlc.NewClock(hlc.UnixNano, time.Millisecond)
	s := newIntervalSkl(clock, MinRetentionWindow, makeSklMetrics())
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	size := 1
	for i := 0; i < 9; i++ {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			type op struct {
				from, to []byte
				val      cacheValue
			}
			ops := make([]op, b.N)
			for i := range ops {
				rnd := int64(rng.Int31n(max))
				ops[i] = op{
					from: []byte(fmt.Sprintf("%020d", rnd)),
					to:   []byte(fmt.Sprintf("%020d", rnd+int64(size-1))),
					val:  makeVal(clock.Now(), txnID),
				}
			}

			b.ResetTimer()
			for _, op := range ops {
				s.AddRange(op.from, op.to, 0, op.val)
			}
		})

		size *= 10
	}
}

func BenchmarkIntervalSklAddAndLookup(b *testing.B) {
	const max = 1000000000 // max size of range
	const data = 500000    // number of ranges
	const txnID = "123"

	clock := hlc.NewClock(hlc.UnixNano, time.Millisecond)
	s := newIntervalSkl(clock, MinRetentionWindow, makeSklMetrics())
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	for i := 0; i < data; i++ {
		from, to := makeRange(rng.Int31n(max))
		nowVal := makeVal(clock.Now(), txnID)
		s.AddRange(from, to, excludeFrom|excludeTo, nowVal)
	}

	for i := 0; i <= 10; i++ {
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			type op struct {
				read     bool
				from, to []byte
				val      cacheValue
			}
			ops := make([]op, b.N)
			for i := range ops {
				readFrac := rng.Int31n(10)
				keyNum := rng.Int31n(max)

				if readFrac < int32(i) {
					ops[i] = op{
						read: true,
						from: []byte(fmt.Sprintf("%020d", keyNum)),
					}
				} else {
					from, to := makeRange(keyNum)
					ops[i] = op{
						read: false,
						from: from,
						to:   to,
						val:  makeVal(clock.Now(), txnID),
					}
				}
			}

			b.ResetTimer()
			for _, op := range ops {
				if op.read {
					s.LookupTimestamp(op.from)
				} else {
					s.AddRange(op.from, op.to, excludeFrom|excludeTo, op.val)
				}
			}
		})
	}
}

// makeRange creates a key range from the provided input. The range will start
// at the provided key and will have an end key that is a deterministic function
// of the provided key. This means that for a given input, the function will
// always produce the same range.
func makeRange(start int32) (from, to []byte) {
	var end int32

	// Most ranges are small. Larger ranges are less and less likely.
	rem := start % 100
	if rem < 80 {
		end = start + 0
	} else if rem < 90 {
		end = start + 100
	} else if rem < 95 {
		end = start + 10000
	} else if rem < 99 {
		end = start + 1000000
	} else {
		end = start + 100000000
	}

	from = []byte(fmt.Sprintf("%020d", start))
	to = []byte(fmt.Sprintf("%020d", end))
	return
}

// randRange creates a random range with keys within the specified number of
// slots. The function also returns a middle key, if one exists.
func randRange(rng *rand.Rand, slots int) (from, middle, to []byte) {
	fromNum := rng.Intn(slots)
	toNum := rng.Intn(slots)
	if fromNum > toNum {
		fromNum, toNum = toNum, fromNum
	}

	from = []byte(fmt.Sprintf("%05d", fromNum))
	to = []byte(fmt.Sprintf("%05d", toNum))
	if middleNum := fromNum + 1; middleNum < toNum {
		middle = []byte(fmt.Sprintf("%05d", middleNum))
	}
	return
}

func randRangeOpt(rng *rand.Rand) rangeOptions {
	return rangeOptions(rng.Intn(int(excludeFrom|excludeTo) + 1))
}

func BenchmarkIntervalSklDecodeValue(b *testing.B) {
	runBenchWithVals(b, func(b *testing.B, val cacheValue) {
		var arr [encodedValSize]byte
		enc := encodeValue(arr[:0], val)
		for i := 0; i < b.N; i++ {
			_, _ = decodeValue(enc)
		}
	})
}

func BenchmarkIntervalSklEncodeValue(b *testing.B) {
	runBenchWithVals(b, func(b *testing.B, val cacheValue) {
		var arr [encodedValSize]byte
		for i := 0; i < b.N; i++ {
			_ = encodeValue(arr[:0], val)
		}
	})
}

func runBenchWithVals(b *testing.B, fn func(*testing.B, cacheValue)) {
	for _, withLogical := range []bool{false, true} {
		for _, withSynthetic := range []bool{false, true} {
			for _, withTxnID := range []bool{false, true} {
				var val cacheValue
				val.ts.WallTime = 15
				if withLogical {
					val.ts.Logical = 10
				}
				if withSynthetic {
					val.ts.Synthetic = true
				}
				if withTxnID {
					val.txnID = uuid.MakeV4()
				}
				name := fmt.Sprintf("logical=%t,synthetic=%t,txnID=%t", withLogical, withSynthetic, withTxnID)
				b.Run(name, func(b *testing.B) { fn(b, val) })
			}
		}
	}
}
