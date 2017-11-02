// Copyright (C) 2017 Andy Kimball
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tscache

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const arenaSize = 64 * 1024 * 1024 // 64 MB

var (
	emptyVal = cacheValue{}
	floorTs  = makeTS(100, 0)
	floorVal = makeValWithoutID(floorTs)
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

func TestIntervalSklAdd(t *testing.T) {
	val1 := makeVal(makeTS(100, 100), "1")
	val2 := makeVal(makeTS(200, 201), "2")

	s := newIntervalSkl(arenaSize)

	s.Add([]byte("apricot"), val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("banana")))

	s.Add([]byte("banana"), val2)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("cherry")))
}

func TestIntervalSklSingleRange(t *testing.T) {
	val1 := makeVal(makeTS(100, 100), "1")
	val2 := makeVal(makeTS(200, 50), "2")

	s := newIntervalSkl(arenaSize)

	s.AddRange([]byte("apricot"), []byte("orange"), 0, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("raspberry")))

	// Try again and make sure it's a no-op.
	s.AddRange([]byte("apricot"), []byte("orange"), 0, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("raspberry")))

	// Ratchet up the timestamps.
	s.AddRange([]byte("apricot"), []byte("orange"), 0, val2)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("raspberry")))

	// Add disjoint range.
	s.AddRange([]byte("pear"), []byte("tomato"), excludeFrom|excludeTo, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("peach")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("watermelon")))

	// Try again and make sure it's a no-op.
	s.AddRange([]byte("pear"), []byte("tomato"), excludeFrom|excludeTo, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("peach")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("watermelon")))

	// Ratchet up the timestamps.
	s.AddRange([]byte("pear"), []byte("tomato"), excludeFrom|excludeTo, val2)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("peach")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("watermelon")))
}

func TestIntervalSklOpenRanges(t *testing.T) {
	val1 := makeVal(makeTS(200, 200), "1")
	val2 := makeVal(makeTS(200, 201), "2")

	s := newIntervalSkl(arenaSize)
	s.floorTs = floorTs

	s.AddRange([]byte("banana"), nil, excludeFrom, val1)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("orange")))

	s.AddRange([]byte(""), []byte("kiwi"), 0, val2)
	require.Equal(t, val2, s.LookupTimestamp(nil))
	require.Equal(t, val2, s.LookupTimestamp([]byte("")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("orange")))
}

func TestIntervalSklSupersetRange(t *testing.T) {
	val1 := makeVal(makeTS(200, 1), "1")
	val2 := makeVal(makeTS(201, 0), "2")
	val3 := makeVal(makeTS(300, 0), "3")

	s := newIntervalSkl(arenaSize)
	s.floorTs = floorTs

	// Same range.
	s.AddRange([]byte("kiwi"), []byte("orange"), 0, val1)
	s.AddRange([]byte("kiwi"), []byte("orange"), 0, val2)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("mango")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("raspberry")))

	// Superset range, but with lower timestamp.
	s.AddRange([]byte("grape"), []byte("pear"), 0, val1)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("watermelon")))

	// Superset range, but with higher timestamp.
	s.AddRange([]byte("banana"), []byte("raspberry"), 0, val3)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("watermelon")))
}

func TestIntervalSklContiguousRanges(t *testing.T) {
	ts1 := makeTS(201, 0)

	val1 := makeVal(ts1, "1")
	val2 := makeVal(ts1, "2")
	val2WithoutID := makeValWithoutID(ts1)

	s := newIntervalSkl(arenaSize)
	s.floorTs = floorTs

	s.AddRange([]byte("banana"), []byte("kiwi"), excludeTo, val1)
	s.AddRange([]byte("kiwi"), []byte("orange"), excludeTo, val2)

	// Test single-key lookups over the contiguous range.
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("kiwi")))
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

	s := newIntervalSkl(arenaSize)
	s.floorTs = floorTs

	s.AddRange([]byte("banana"), []byte("kiwi"), 0, val1)
	s.AddRange([]byte("grape"), []byte("raspberry"), excludeTo, val2)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("raspberry")))

	s.AddRange([]byte("apricot"), []byte("orange"), 0, val3)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val3, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("raspberry")))

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

func TestIntervalSklBoundaryRange(t *testing.T) {
	val1 := makeVal(makeTS(100, 100), "1")

	s := newIntervalSkl(arenaSize)

	// Don't allow nil from and to keys.
	require.Panics(t, func() { s.AddRange([]byte(nil), []byte(nil), excludeFrom, val1) })

	// Don't allow inverted ranges.
	require.Panics(t, func() { s.AddRange([]byte("kiwi"), []byte("apple"), 0, val1) })
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("raspberry")))

	// If from key is same as to key, and both are excluded, then range is
	// zero-length.
	s.AddRange([]byte("banana"), []byte("banana"), excludeFrom|excludeTo, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("kiwi")))

	// If from key is same as to key, then range has length one.
	s.AddRange([]byte("mango"), []byte("mango"), 0, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("mango")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("orange")))

	// If from key is same as to key, then range has length one.
	s.AddRange([]byte("banana"), []byte("banana"), excludeTo, val1)
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, emptyVal, s.LookupTimestamp([]byte("cherry")))
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

	s := newIntervalSkl(arenaSize)

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

	s := newIntervalSkl(arenaSize)

	// Perform range lookups over a single key.
	s.Add([]byte("apricot"), val1)
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
		s := newIntervalSkl(arenaSize)

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
		s := newIntervalSkl(arenaSize)

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

// TestIntervalSklLookupEqualsEarlierMaxWallTime tests that we properly handle
// the lookup when the timestamp for a range found in the later page is equal to
// the maxWallTime of the earlier page.
func TestIntervalSklLookupEqualsEarlierMaxWallTime(t *testing.T) {
	ts1 := makeTS(200, 0) // without Logical part
	ts2 := makeTS(200, 1) // with Logical part
	ts2Ceil := makeTS(201, 0)

	txnID1 := "1"
	txnID2 := "2"

	forTrueAndFalse(t, "tsWithLogicalPart", func(t *testing.T, logicalPart bool) {
		s := newIntervalSkl(arenaSize)
		s.floorTs = floorTs

		// Insert an initial value into intervalSkl.
		initTS := ts1
		if logicalPart {
			initTS = ts2
		}
		origVal := makeVal(initTS, txnID1)
		s.AddRange([]byte("banana"), []byte("orange"), 0, origVal)

		// Verify the later page's maxWallTime is what we expect.
		expMaxTS := ts1
		if logicalPart {
			expMaxTS = ts2Ceil
		}
		require.Equal(t, expMaxTS.WallTime, s.later.maxWallTime)

		// Rotate the page so that new writes will go to a different page.
		s.rotatePages(s.later)

		// Write to overlapping and non-overlapping parts of the new page with
		// the values that have the same timestamp as the maxWallTime of the
		// earlier page. One value has the same txnID as the previous write in
		// the earlier page and one has a different txnID..
		valSameID := makeVal(expMaxTS, txnID1)
		valDiffID := makeVal(expMaxTS, txnID2)
		valNoID := makeValWithoutID(expMaxTS)
		s.Add([]byte("apricot"), valSameID)
		s.Add([]byte("banana"), valSameID)
		s.Add([]byte("orange"), valDiffID)
		s.Add([]byte("raspberry"), valDiffID)

		require.Equal(t, valSameID, s.LookupTimestamp([]byte("apricot")))
		require.Equal(t, valSameID, s.LookupTimestamp([]byte("banana")))
		if logicalPart {
			// If the initial timestamp had a logical part then
			// s.earlier.maxWallTime is inexact (see ratchetMaxTimestamp). When
			// we search in the earlier page, we'll find the exact timestamp of
			// the overlapping range and realize that its not the same as the
			// timestamp of the range in the later page. Because of this,
			// ratchetValue WON'T remove the txnID.
			require.Equal(t, valDiffID, s.LookupTimestamp([]byte("orange")))
		} else {
			// If the initial timestamp did not have a logical part then
			// s.earlier.maxWallTime is exact. When we search in the earlier
			// page, we'll find the overlapping range and realize that it is the
			// same as the timestamp of the range in the later page. Because of
			// this, ratchetValue WILL remove the txnID.
			require.Equal(t, valNoID, s.LookupTimestamp([]byte("orange")))
		}
		require.Equal(t, valDiffID, s.LookupTimestamp([]byte("raspberry")))
		require.Equal(t, floorVal, s.LookupTimestamp([]byte("tomato")))
	})
}

func TestIntervalSklFill(t *testing.T) {
	const n = 200
	const txnID = "123"

	// Use constant seed so that skiplist towers will be of predictable size.
	rand.Seed(0)

	s := newIntervalSkl(3000)

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		s.AddRange(key, key, 0, makeVal(makeTS(int64(100+i), int32(i)), txnID))
	}

	floorTs := s.floorTs
	require.True(t, makeTS(100, 0).Less(floorTs))

	lastKey := []byte(fmt.Sprintf("%05d", n-1))
	expVal := makeVal(makeTS(int64(100+n-1), int32(n-1)), txnID)
	require.Equal(t, expVal, s.LookupTimestamp(lastKey))

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		require.False(t, s.LookupTimestamp(key).ts.Less(floorTs))
	}
}

// Repeatedly fill the structure and make sure timestamp lookups always increase.
func TestIntervalSklFill2(t *testing.T) {
	const n = 10000
	const txnID = "123"

	s := newIntervalSkl(997)
	key := []byte("some key")

	for i := 0; i < n; i++ {
		val := makeVal(makeTS(int64(i), int32(i)), txnID)
		s.Add(key, val)
		require.True(t, !s.LookupTimestamp(key).ts.Less(val.ts))
	}
}

func TestIntervalSklConcurrency(t *testing.T) {
	testCases := []struct {
		name string
		size uint32
	}{
		// Test concurrency with a small page size in order to force lots of
		// page rotations.
		{name: "Rotates", size: 2048},
		// Test concurrency with a larger page size in order to test slot
		// concurrency without the added complication of page rotations.
		{name: "Slots", size: arenaSize},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Run one subtest using a real clock to generate timestampts and
			// one subtest using a fake clock to generate timestamps. The former
			// is good for simulating real conditions while the latter is good
			// for testing timestamp collisions.
			forTrueAndFalse(t, "useClock", func(t *testing.T, useClock bool) {
				const n = 10000
				const slots = 20

				var wg sync.WaitGroup
				clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
				s := newIntervalSkl(tc.size)

				for i := 0; i < slots; i++ {
					wg.Add(1)

					// Each goroutine gets its own key to watch as it performs
					// random operations.
					go func(i int) {
						defer wg.Done()

						rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
						key := []byte(fmt.Sprintf("%05d", i))
						txnID := strconv.Itoa(i)
						maxVal := cacheValue{}

						for j := 0; j < n; j++ {
							// Choose a random range.
							fromNum := rng.Intn(slots)
							toNum := rng.Intn(slots)
							if fromNum > toNum {
								fromNum, toNum = toNum, fromNum
							}

							from := []byte(fmt.Sprintf("%05d", fromNum))
							to := []byte(fmt.Sprintf("%05d", toNum))

							// Choose random range options.
							opt := rangeOptions(rng.Intn(int(excludeFrom|excludeTo) + 1))

							// Add a new value to the range.
							ts := hlc.Timestamp{WallTime: int64(j)}
							if useClock {
								ts = clock.Now()
							}
							nowVal := makeVal(ts, txnID)
							s.AddRange(from, to, opt, nowVal)

							// Test single-key lookup at from, if possible.
							if (opt & excludeFrom) == 0 {
								val := s.LookupTimestamp(from)
								assertRatchet(t, nowVal, val)
							}

							// Test single-key lookup at to, if possible.
							if (opt & excludeTo) == 0 {
								val := s.LookupTimestamp(to)
								assertRatchet(t, nowVal, val)
							}

							// Test range lookup between from and to, if possible.
							if !(fromNum == toNum && opt == (excludeFrom|excludeTo)) {
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

func BenchmarkAdd(b *testing.B) {
	const max = 500000000
	const txnID = "123"

	clock := hlc.NewClock(hlc.UnixNano, time.Millisecond)
	s := newIntervalSkl(64 * 1024 * 1024)
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	size := 1
	for i := 0; i < 9; i++ {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				rnd := int64(rng.Int31n(max))
				from := []byte(fmt.Sprintf("%020d", rnd))
				to := []byte(fmt.Sprintf("%020d", rnd+int64(size-1)))
				s.AddRange(from, to, 0, makeVal(clock.Now(), txnID))
			}
		})

		size *= 10
	}
}

func BenchmarkAddAndLookup(b *testing.B) {
	const parallel = 1
	const max = 1000000000
	const data = 500000
	const txnID = "123"

	s := newIntervalSkl(arenaSize)
	clock := hlc.NewClock(hlc.UnixNano, time.Millisecond)
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	for i := 0; i < data; i++ {
		from, to := makeRange(rng.Int31n(max))
		nowVal := makeVal(clock.Now(), txnID)
		s.AddRange(from, to, excludeFrom|excludeTo, nowVal)
	}

	for i := 0; i <= 10; i++ {
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			var wg sync.WaitGroup

			for p := 0; p < parallel; p++ {
				wg.Add(1)

				go func(i int) {
					defer wg.Done()

					rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

					for n := 0; n < b.N/parallel; n++ {
						readFrac := rng.Int31()
						keyNum := rng.Int31n(max)

						if (readFrac % 10) < int32(i) {
							key := []byte(fmt.Sprintf("%020d", keyNum))
							s.LookupTimestamp(key)
						} else {
							from, to := makeRange(keyNum)
							nowVal := makeVal(clock.Now(), txnID)
							s.AddRange(from, to, excludeFrom|excludeTo, nowVal)
						}
					}
				}(i)
			}

			wg.Wait()
		})
	}
}

func makeRange(start int32) (from, to []byte) {
	var end int32

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
