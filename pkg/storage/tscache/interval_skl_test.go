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
)

const arenaSize = 64 * 1024 * 1024

func makeTS(walltime int64, logical int32) hlc.Timestamp {
	return hlc.Timestamp{
		WallTime: walltime,
		Logical:  logical,
	}
}

func TestIntervalSklAdd(t *testing.T) {
	ts1 := makeTS(100, 100)
	ts2 := makeTS(200, 201)

	s := newIntervalSkl(arenaSize)

	s.Add([]byte("apricot"), ts1)
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("banana")))

	s.Add([]byte("banana"), ts2)
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("cherry")))
}

func TestIntervalSklSingleRange(t *testing.T) {
	ts1 := makeTS(100, 100)
	ts2 := makeTS(200, 50)

	s := newIntervalSkl(arenaSize)

	s.AddRange([]byte("apricot"), []byte("orange"), 0, ts1)
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("raspberry")))

	// Try again and make sure it's a no-op.
	s.AddRange([]byte("apricot"), []byte("orange"), 0, ts1)
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("raspberry")))

	// Ratchet up the timestamps.
	s.AddRange([]byte("apricot"), []byte("orange"), 0, ts2)
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("raspberry")))

	// Add disjoint range.
	s.AddRange([]byte("pear"), []byte("tomato"), excludeFrom|excludeTo, ts1)
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("peach")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("tomato")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("watermelon")))

	// Try again and make sure it's a no-op.
	s.AddRange([]byte("pear"), []byte("tomato"), excludeFrom|excludeTo, ts1)
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("peach")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("tomato")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("watermelon")))

	// Ratchet up the timestamps.
	s.AddRange([]byte("pear"), []byte("tomato"), excludeFrom|excludeTo, ts2)
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("peach")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("tomato")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("watermelon")))
}

func TestIntervalSklOpenRanges(t *testing.T) {
	floorTs := makeTS(100, 0)
	ts1 := makeTS(200, 200)
	ts2 := makeTS(200, 201)

	s := newIntervalSkl(arenaSize)
	s.floorTs = floorTs

	s.AddRange([]byte("banana"), nil, excludeFrom, ts1)
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("orange")))

	s.AddRange([]byte(""), []byte("kiwi"), 0, ts2)
	require.Equal(t, ts2, s.LookupTimestamp(nil))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("orange")))
}

func TestIntervalSklSupersetRange(t *testing.T) {
	floorTs := makeTS(100, 0)
	ts1 := makeTS(200, 1)
	ts2 := makeTS(201, 0)
	ts3 := makeTS(300, 0)

	s := newIntervalSkl(arenaSize)
	s.floorTs = floorTs

	// Same range.
	s.AddRange([]byte("kiwi"), []byte("orange"), 0, ts1)
	s.AddRange([]byte("kiwi"), []byte("orange"), 0, ts2)
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("mango")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("raspberry")))

	// Superset range, but with lower timestamp.
	s.AddRange([]byte("grape"), []byte("pear"), 0, ts1)
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("watermelon")))

	// Superset range, but with higher timestamp.
	s.AddRange([]byte("banana"), []byte("raspberry"), 0, ts3)
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("watermelon")))
}

func TestIntervalSklContiguousRanges(t *testing.T) {
	floorTs := makeTS(100, 0)
	ts1 := makeTS(200, 1)
	ts2 := makeTS(201, 0)

	s := newIntervalSkl(arenaSize)
	s.floorTs = floorTs

	s.AddRange([]byte("banana"), []byte("kiwi"), excludeTo, ts1)
	s.AddRange([]byte("kiwi"), []byte("orange"), excludeTo, ts2)
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("orange")))
}

func TestIntervalSklOverlappingRanges(t *testing.T) {
	floorTs := makeTS(100, 0)
	ts1 := makeTS(200, 1)
	ts2 := makeTS(201, 0)
	ts3 := makeTS(300, 0)
	ts4 := makeTS(400, 0)

	s := newIntervalSkl(arenaSize)
	s.floorTs = floorTs

	s.AddRange([]byte("banana"), []byte("kiwi"), 0, ts1)
	s.AddRange([]byte("grape"), []byte("raspberry"), excludeTo, ts2)
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("raspberry")))

	s.AddRange([]byte("apricot"), []byte("orange"), 0, ts3)
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, ts2, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("raspberry")))

	s.AddRange([]byte("kiwi"), []byte(nil), excludeFrom, ts4)
	require.Equal(t, floorTs, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("apricot")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("grape")))
	require.Equal(t, ts3, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, ts4, s.LookupTimestamp([]byte("orange")))
	require.Equal(t, ts4, s.LookupTimestamp([]byte("pear")))
	require.Equal(t, ts4, s.LookupTimestamp([]byte("raspberry")))
}

func TestIntervalSklBoundaryRange(t *testing.T) {
	ts1 := makeTS(100, 100)

	s := newIntervalSkl(arenaSize)

	// Don't allow nil from key.
	require.Panics(t, func() { s.AddRange([]byte(nil), []byte(nil), excludeFrom, ts1) })

	// If from key is greater than to key, then range is zero-length.
	s.AddRange([]byte("kiwi"), []byte("apple"), 0, ts1)
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("raspberry")))

	// If from key is same as to key, and both are excluded, then range is
	// zero-length.
	s.AddRange([]byte("banana"), []byte("banana"), excludeFrom|excludeTo, ts1)
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("kiwi")))

	// If from key is same as to key, then range has length one.
	s.AddRange([]byte("mango"), []byte("mango"), 0, ts1)
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("mango")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("orange")))

	// If from key is same as to key, then range has length one.
	s.AddRange([]byte("banana"), []byte("banana"), excludeTo, ts1)
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts1, s.LookupTimestamp([]byte("banana")))
	require.Equal(t, hlc.Timestamp{}, s.LookupTimestamp([]byte("cherry")))
}

func TestIntervalSklFill(t *testing.T) {
	const n = 200

	// Use constant seed so that skiplist towers will be of predictable size.
	rand.Seed(0)

	s := newIntervalSkl(3000)

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		s.AddRange(key, key, 0, makeTS(int64(100+i), int32(i)))
	}

	floorTs := s.floorTs
	require.True(t, makeTS(100, 0).Less(floorTs))

	lastKey := []byte(fmt.Sprintf("%05d", n-1))
	require.Equal(t, makeTS(int64(100+n-1), int32(n-1)), s.LookupTimestamp(lastKey))

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		require.False(t, s.LookupTimestamp(key).Less(floorTs))
	}
}

// Repeatedly fill the structure and make sure timestamp lookups always increase.
func TestIntervalSklFill2(t *testing.T) {
	const n = 10000

	s := newIntervalSkl(997)
	key := []byte("some key")

	for i := 0; i < n; i++ {
		ts := makeTS(int64(i), int32(i))
		s.Add(key, ts)
		require.True(t, !s.LookupTimestamp(key).Less(ts))
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
					maxTs := hlc.Timestamp{}

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
						s.AddRange(from, to, 0, now)

						ts := s.LookupTimestamp(from)
						require.False(t, ts.Less(now))

						ts = s.LookupTimestamp(to)
						require.False(t, ts.Less(now))

						ts = s.LookupTimestamp(key)
						require.False(t, ts.Less(maxTs))
						maxTs = ts
					}
				}(i)
			}

			wg.Wait()
		})
	}
}

func BenchmarkAdd(b *testing.B) {
	const max = 500000000

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
				s.AddRange(from, to, 0, clock.Now())
			}
		})

		size *= 10
	}
}

func BenchmarkAddAndLookup(b *testing.B) {
	const parallel = 1
	const max = 1000000000
	const data = 500000

	s := newIntervalSkl(arenaSize)
	clock := hlc.NewClock(hlc.UnixNano, time.Millisecond)
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	for i := 0; i < data; i++ {
		from, to := makeRange(rng.Int31n(max))
		s.AddRange(from, to, excludeFrom|excludeTo, clock.Now())
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
							s.AddRange(from, to, excludeFrom|excludeTo, clock.Now())
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
