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
	val1 := makeVal(makeTS(200, 1), "1")
	val2 := makeVal(makeTS(201, 0), "2")

	s := newIntervalSkl(arenaSize)
	s.floorTs = floorTs

	s.AddRange([]byte("banana"), []byte("kiwi"), excludeTo, val1)
	s.AddRange([]byte("kiwi"), []byte("orange"), excludeTo, val2)
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, val1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, val2, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, floorVal, s.LookupTimestamp([]byte("orange")))
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

	// Don't allow nil from key.
	require.Panics(t, func() { s.AddRange([]byte(nil), []byte(nil), excludeFrom, val1) })

	// If from key is greater than to key, then range is zero-length.
	s.AddRange([]byte("kiwi"), []byte("apple"), 0, val1)
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
			const n = 10000
			const slots = 20

			var wg sync.WaitGroup
			clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
			s := newIntervalSkl(tc.size)

			for i := 0; i < slots; i++ {
				wg.Add(1)

				go func(i int) {
					defer wg.Done()

					rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
					key := []byte(fmt.Sprintf("%05d", i))
					maxVal := cacheValue{}

					for j := 0; j < n; j++ {
						fromNum := rng.Intn(slots)
						toNum := rng.Intn(slots)

						if fromNum > toNum {
							fromNum, toNum = toNum, fromNum
						} else if fromNum == toNum {
							toNum++
						}

						from := []byte(fmt.Sprintf("%05d", fromNum))
						to := []byte(fmt.Sprintf("%05d", toNum))

						now := clock.Now()
						nowVal := makeValWithoutID(now)
						s.AddRange(from, to, 0, nowVal)

						val := s.LookupTimestamp(from)
						require.False(t, val.ts.Less(now))

						val = s.LookupTimestamp(to)
						require.False(t, val.ts.Less(now))

						val = s.LookupTimestamp(key)
						require.False(t, val.ts.Less(maxVal.ts))
						maxVal = val
					}
				}(i)
			}

			wg.Wait()
		})
	}
}

func BenchmarkIntervalSklAdd(b *testing.B) {
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

func BenchmarkIntervalSklAddAndLookup(b *testing.B) {
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
