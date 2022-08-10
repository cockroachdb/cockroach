// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"bytes"
	"context"
	"math"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// kvPair is a bytes -> bytes kv pair.
type kvPair struct {
	key       roachpb.Key
	value     []byte
	timestamp []byte
}

func makeTestData(t *testing.T, num int, withTimestamp bool) (kvs []kvPair, totalSize sz) {
	kvs = make([]kvPair, num)
	r, _ := randutil.NewTestRand()
	alloc := make([]byte, num*500)
	randutil.ReadTestdataBytes(r, alloc)

	previousTS := hlc.Timestamp{}
	for i := range kvs {
		if len(alloc) < 1500 {
			const refill = 15000
			alloc = make([]byte, refill)
			randutil.ReadTestdataBytes(r, alloc)
		}

		makeDupKeyDiffTS := withTimestamp && r.Intn(3) == 0 && i > 0
		if makeDupKeyDiffTS {
			prevKey := kvs[i-1].key
			kvs[i].key = alloc[:len(prevKey)]
			copy(kvs[i].key, prevKey)
		} else {
			kvs[i].key = alloc[:randutil.RandIntInRange(r, 2, 100)]
		}
		alloc = alloc[len(kvs[i].key):]

		kvs[i].value = alloc[:randutil.RandIntInRange(r, 0, 1000)]
		alloc = alloc[len(kvs[i].value):]

		if withTimestamp {
			if makeDupKeyDiffTS {
				previousTS = hlc.Timestamp{WallTime: previousTS.WallTime + 1}
			} else {
				previousTS = hlc.Timestamp{WallTime: randutil.FastInt63()}
			}
			tsLen, err := previousTS.MarshalTo(alloc)
			require.NoError(t, err)
			kvs[i].timestamp = alloc[:tsLen]
			alloc = alloc[tsLen:]
		}
		totalSize += sz(len(kvs[i].key) + len(kvs[i].value) + len(kvs[i].timestamp))
	}
	return kvs, totalSize
}

func TestKvBuf(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testkvBufWithTimestamp := func(t *testing.T, withTimestamp bool) {
		src, totalSize := makeTestData(t, 50000, true)

		ctx := context.Background()
		noneMonitor := mon.NewMonitorWithLimit("none", mon.MemoryResource,
			0, nil, nil, 0, 0, nil)
		noneMonitor.StartNoReserved(ctx, nil /* pool */)
		none := noneMonitor.MakeBoundAccount()
		lots := mon.NewUnlimitedMonitor(ctx, "lots", mon.MemoryResource,
			nil, nil, 0, nil).MakeBoundAccount()

		// Write everything to our buf.
		b := kvBuf{}
		for i := range src {
			size := sz(len(src[i].key) + len(src[i].value) + len(src[i].timestamp))
			fits := len(b.entries) <= cap(b.entries) && len(b.slab)+int(size) <= cap(b.slab)

			require.Equal(t, fits, b.fits(ctx, size, 0, &none))
			if !fits {
				// Ensure trying to grow with either, but not both, allowing fails.
				require.False(t, b.fits(ctx, size, 0, &lots))
				require.False(t, b.fits(ctx, size, math.MaxInt64, &none))
				// Allow it to grow and then re-check a no-alloc fits call.
				require.True(t, b.fits(ctx, size, math.MaxInt64, &lots))
				require.True(t, b.fits(ctx, size, 0, &none))
			}
			before := b.MemSize()
			if err := b.append(src[i].key, src[i].value, src[i].timestamp); err != nil {
				t.Fatal(err)
			}
			require.Equal(t, before, b.MemSize())
		}

		// Sanity check our buf has right size.
		if expected, actual := len(src), b.Len(); expected != actual {
			t.Fatalf("expected len %d got %d", expected, actual)
		}
		if expected, actual := totalSize, b.KVSize(); expected != actual {
			t.Fatalf("expected len %d got %d", expected, actual)
		}
		if expected, actual := cap(b.entries)*16+cap(b.slab), int(b.MemSize()); expected != actual {
			t.Fatalf("expected len %d got %d", expected, actual)
		}

		// Read back what we wrote.
		for i := range src {
			if expected, actual := src[i].key, b.Key(i); !bytes.Equal(expected, actual) {
				t.Fatalf("expected %s\ngot %s", expected, actual)
			}
			if expected, actual := src[i].value, b.Value(i); !bytes.Equal(expected, actual) {
				t.Fatalf("expected %s\ngot %s", expected, actual)
			}
			if expected, actual := src[i].timestamp, b.Timestamp(i); !bytes.Equal(expected, actual) {
				t.Fatalf("expected %s\ngot %s", expected, actual)
			}
		}
		sort.Slice(src, func(i, j int) bool {
			return compareKeyTimestamp(src[i].key, src[i].timestamp, src[j].key, src[j].timestamp) < 0
		})
		sort.Sort(&b)
		for i := range src {
			if expected, actual := src[i].key, b.Key(i); !bytes.Equal(expected, actual) {
				t.Fatalf("expected %s\ngot %s", expected, actual)
			}
			if expected, actual := src[i].value, b.Value(i); !bytes.Equal(expected, actual) {
				t.Fatalf("expected %s\ngot %s", expected, actual)
			}
		}
	}

	t.Run("with-timestamp", func(t *testing.T) {
		testkvBufWithTimestamp(t, true /* withTimestamp */)
	})

	t.Run("without-timestamp", func(t *testing.T) {
		testkvBufWithTimestamp(t, false /* withTimestamp */)
	})

}
