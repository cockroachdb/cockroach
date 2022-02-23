// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestEngineComparer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	keyAMetadata := MVCCKey{
		Key: []byte("a"),
	}
	keyA2 := MVCCKey{
		Key:       []byte("a"),
		Timestamp: hlc.Timestamp{WallTime: 2},
	}
	keyA1 := MVCCKey{
		Key:       []byte("a"),
		Timestamp: hlc.Timestamp{WallTime: 1},
	}
	keyB2 := MVCCKey{
		Key:       []byte("b"),
		Timestamp: hlc.Timestamp{WallTime: 2},
	}

	require.Equal(t, -1, EngineComparer.Compare(EncodeMVCCKey(keyAMetadata), EncodeMVCCKey(keyA1)),
		"expected key metadata to sort first")
	require.Equal(t, -1, EngineComparer.Compare(EncodeMVCCKey(keyA2), EncodeMVCCKey(keyA1)),
		"expected higher timestamp to sort first")
	require.Equal(t, -1, EngineComparer.Compare(EncodeMVCCKey(keyA2), EncodeMVCCKey(keyB2)),
		"expected lower key to sort first")

	suffix := func(key []byte) []byte {
		return key[EngineComparer.Split(key):]
	}
	require.Equal(t, -1, EngineComparer.Compare(suffix(EncodeMVCCKey(keyA2)), suffix(EncodeMVCCKey(keyA1))),
		"expected bare suffix with higher timestamp to sort first")
}

func TestPebbleTimeBoundPropCollector(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.RunTest(t, testutils.TestDataPath(t, "time_bound_props"), func(t *testing.T, d *datadriven.TestData) string {
		c := &pebbleTimeBoundPropCollector{}
		switch d.Cmd {
		case "build":
			for _, line := range strings.Split(d.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) != 2 {
					return fmt.Sprintf("malformed line: %s, expected: <key>/<timestamp> <value>", line)
				}
				keyParts := strings.Split(parts[0], "/")
				if len(keyParts) != 2 {
					return fmt.Sprintf("malformed key: %s, expected: <key>/<timestamp>", parts[0])
				}

				key := []byte(keyParts[0])
				timestamp, err := strconv.Atoi(keyParts[1])
				if err != nil {
					return err.Error()
				}
				ikey := pebble.InternalKey{
					UserKey: EncodeMVCCKey(MVCCKey{
						Key:       key,
						Timestamp: hlc.Timestamp{WallTime: int64(timestamp)},
					}),
				}

				value := []byte(parts[1])
				if timestamp == 0 {
					if n, err := fmt.Sscanf(string(value), "timestamp=%d", &timestamp); err != nil {
						return err.Error()
					} else if n != 1 {
						return fmt.Sprintf("malformed txn timestamp: %s, expected timestamp=<value>", value)
					}
					meta := &enginepb.MVCCMetadata{}
					meta.Timestamp.WallTime = int64(timestamp)
					meta.Txn = &enginepb.TxnMeta{}
					var err error
					value, err = protoutil.Marshal(meta)
					if err != nil {
						return err.Error()
					}
				}

				if err := c.Add(ikey, value); err != nil {
					return err.Error()
				}
			}

			// Retrieve the properties and sort them for test determinism.
			m := make(map[string]string)
			if err := c.Finish(m); err != nil {
				return err.Error()
			}
			var keys []string
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			var buf bytes.Buffer
			for _, k := range keys {
				fmt.Fprintf(&buf, "%s: %x\n", k, m[k])
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestPebbleIterReuse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Regression test for https://github.com/cockroachdb/cockroach/issues/42354
	// and similar issues arising from improper re-initialization of cached
	// iterators.

	eng := createTestPebbleEngine()
	defer eng.Close()

	batch := eng.NewBatch()
	defer batch.Close()
	for i := 0; i < 100; i++ {
		key := MVCCKey{[]byte{byte(i)}, hlc.Timestamp{WallTime: 100}}
		if err := batch.PutMVCC(key, []byte("foo")); err != nil {
			t.Fatal(err)
		}
	}

	iter1 := batch.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{LowerBound: []byte{40}, UpperBound: []byte{50}})
	valuesCount := 0
	// Seek to a value before the lower bound. Identical to seeking to the lower bound.
	iter1.SeekGE(MVCCKey{Key: []byte{30}})
	for ; ; iter1.Next() {
		ok, err := iter1.Valid()
		if err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
		i := iter1.UnsafeKey().Key[0]
		if i < 40 || i >= 50 {
			t.Fatalf("iterator returned key out of bounds: %d", i)
		}

		valuesCount++
	}

	if valuesCount != 10 {
		t.Fatalf("expected 10 values, got %d", valuesCount)
	}
	iter1.Close()
	iter1 = nil

	// Create another iterator, with no lower bound but an upper bound that
	// is lower than the previous iterator's lower bound. This should still result
	// in the right amount of keys being returned; the lower bound from the
	// previous iterator should get zeroed.
	iter2 := batch.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: []byte{10}})
	valuesCount = 0
	// This is a peculiar test that is disregarding how local and global keys
	// affect the behavior of MVCCIterators. This test is writing []byte{0}
	// which precedes the localPrefix. Ignore the local and preceding keys in
	// this seek.
	iter2.SeekGE(MVCCKey{Key: []byte{2}})
	for ; ; iter2.Next() {
		ok, err := iter2.Valid()
		if err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}

		i := iter2.UnsafeKey().Key[0]
		if i >= 10 {
			t.Fatalf("iterator returned key out of bounds: %d", i)
		}
		valuesCount++
	}

	if valuesCount != 8 {
		t.Fatalf("expected 8 values, got %d", valuesCount)
	}
	iter2.Close()
}

type iterBoundsChecker struct {
	t                  *testing.T
	expectSetBounds    bool
	boundsSlices       [2][]byte
	boundsSlicesCopied [2][]byte
}

func (ibc *iterBoundsChecker) postSetBounds(lower, upper []byte) {
	require.True(ibc.t, ibc.expectSetBounds)
	ibc.expectSetBounds = false
	// The slices passed in the second from last SetBounds call
	// must still be the same.
	for i := range ibc.boundsSlices {
		if ibc.boundsSlices[i] != nil {
			if !bytes.Equal(ibc.boundsSlices[i], ibc.boundsSlicesCopied[i]) {
				ibc.t.Fatalf("bound slice changed: expected: %x, actual: %x",
					ibc.boundsSlicesCopied[i], ibc.boundsSlices[i])
			}
		}
	}
	// Stash the bounds for later checking.
	for i, bound := range [][]byte{lower, upper} {
		ibc.boundsSlices[i] = bound
		if bound != nil {
			ibc.boundsSlicesCopied[i] = append(ibc.boundsSlicesCopied[i][:0], bound...)
		} else {
			ibc.boundsSlicesCopied[i] = nil
		}
	}
}

func TestPebbleIterBoundSliceStabilityAndNoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	eng := createTestPebbleEngine().(*Pebble)
	defer eng.Close()
	iter := newPebbleIterator(
		eng.db, nil, IterOptions{UpperBound: roachpb.Key("foo")}, StandardDurability)
	defer iter.Close()
	checker := &iterBoundsChecker{t: t}
	iter.testingSetBoundsListener = checker

	tc := []struct {
		expectSetBounds bool
		setUpperOnly    bool
		lb              roachpb.Key
		ub              roachpb.Key
	}{
		{
			// [nil, www)
			expectSetBounds: true,
			ub:              roachpb.Key("www"),
		},
		{
			// [nil, www)
			expectSetBounds: false,
			ub:              roachpb.Key("www"),
		},
		{
			// [nil, www)
			expectSetBounds: false,
			setUpperOnly:    true,
			ub:              roachpb.Key("www"),
		},
		{
			// [ddd, www)
			expectSetBounds: true,
			lb:              roachpb.Key("ddd"),
			ub:              roachpb.Key("www"),
		},
		{
			// [ddd, www)
			expectSetBounds: false,
			setUpperOnly:    true,
			ub:              roachpb.Key("www"),
		},
		{
			// [ddd, xxx)
			expectSetBounds: true,
			setUpperOnly:    true,
			ub:              roachpb.Key("xxx"),
		},
		{
			// [aaa, bbb)
			expectSetBounds: true,
			lb:              roachpb.Key("aaa"),
			ub:              roachpb.Key("bbb"),
		},
		{
			// [ccc, ddd)
			expectSetBounds: true,
			lb:              roachpb.Key("ccc"),
			ub:              roachpb.Key("ddd"),
		},
		{
			// [ccc, nil)
			expectSetBounds: true,
			lb:              roachpb.Key("ccc"),
		},
		{
			// [ccc, nil)
			expectSetBounds: false,
			lb:              roachpb.Key("ccc"),
		},
	}
	var lb, ub roachpb.Key
	for _, c := range tc {
		t.Run(fmt.Sprintf("%v", c), func(t *testing.T) {
			checker.expectSetBounds = c.expectSetBounds
			checker.t = t
			if c.setUpperOnly {
				iter.SetUpperBound(c.ub)
				ub = c.ub
			} else {
				iter.setBounds(c.lb, c.ub)
				lb, ub = c.lb, c.ub
			}
			require.False(t, checker.expectSetBounds)
			for i, bound := range [][]byte{lb, ub} {
				if (bound == nil) != (checker.boundsSlicesCopied[i] == nil) {
					t.Fatalf("inconsistent nil %d", i)
				}
				if bound != nil {
					expected := append([]byte(nil), bound...)
					expected = append(expected, 0x00)
					if !bytes.Equal(expected, checker.boundsSlicesCopied[i]) {
						t.Fatalf("expected: %x, actual: %x", expected, checker.boundsSlicesCopied[i])
					}
				}
			}
		})
	}
}

func makeMVCCKey(a string) MVCCKey {
	return MVCCKey{Key: []byte(a)}
}

func TestPebbleSeparatorSuccessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sepCases := []struct {
		a, b, want MVCCKey
	}{
		// Many cases here are adapted from a Pebble unit test.

		// Non-empty b values.
		{makeMVCCKey("black"), makeMVCCKey("blue"), makeMVCCKey("blb")},
		{makeMVCCKey(""), makeMVCCKey("2"), makeMVCCKey("")},
		{makeMVCCKey("1"), makeMVCCKey("2"), makeMVCCKey("1")},
		{makeMVCCKey("1"), makeMVCCKey("29"), makeMVCCKey("2")},
		{makeMVCCKey("13"), makeMVCCKey("19"), makeMVCCKey("14")},
		{makeMVCCKey("13"), makeMVCCKey("99"), makeMVCCKey("2")},
		{makeMVCCKey("135"), makeMVCCKey("19"), makeMVCCKey("14")},
		{makeMVCCKey("1357"), makeMVCCKey("19"), makeMVCCKey("14")},
		{makeMVCCKey("1357"), makeMVCCKey("2"), makeMVCCKey("14")},
		{makeMVCCKey("13\xff"), makeMVCCKey("14"), makeMVCCKey("13\xff")},
		{makeMVCCKey("13\xff"), makeMVCCKey("19"), makeMVCCKey("14")},
		{makeMVCCKey("1\xff\xff"), makeMVCCKey("19"), makeMVCCKey("1\xff\xff")},
		{makeMVCCKey("1\xff\xff"), makeMVCCKey("2"), makeMVCCKey("1\xff\xff")},
		{makeMVCCKey("1\xff\xff"), makeMVCCKey("9"), makeMVCCKey("2")},
		{makeMVCCKey("1\xfd\xff"), makeMVCCKey("1\xff"), makeMVCCKey("1\xfe")},
		{MVCCKey{
			Key:       []byte("1\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}, makeMVCCKey("9"), makeMVCCKey("2")},
		{MVCCKey{
			Key:       []byte("1\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}, makeMVCCKey("19"), MVCCKey{
			Key:       []byte("1\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		},
		},
		// Empty b values.
		{makeMVCCKey(""), makeMVCCKey(""), makeMVCCKey("")},
		{makeMVCCKey("green"), makeMVCCKey(""), makeMVCCKey("green")},
		{makeMVCCKey("1"), makeMVCCKey(""), makeMVCCKey("1")},
		{makeMVCCKey("11\xff"), makeMVCCKey(""), makeMVCCKey("11\xff")},
		{makeMVCCKey("1\xff"), makeMVCCKey(""), makeMVCCKey("1\xff")},
		{makeMVCCKey("1\xff\xff"), makeMVCCKey(""), makeMVCCKey("1\xff\xff")},
		{makeMVCCKey("\xff"), makeMVCCKey(""), makeMVCCKey("\xff")},
		{makeMVCCKey("\xff\xff"), makeMVCCKey(""), makeMVCCKey("\xff\xff")},
	}
	for _, tc := range sepCases {
		t.Run("", func(t *testing.T) {
			got := string(EngineComparer.Separator(nil, EncodeMVCCKey(tc.a), EncodeMVCCKey(tc.b)))
			if got != string(EncodeMVCCKey(tc.want)) {
				t.Errorf("a, b = %q, %q: got %q, want %q", tc.a, tc.b, got, tc.want)
			}
		})
	}

	succCases := []struct {
		a, want MVCCKey
	}{
		// Many cases adapted from Pebble test.
		{makeMVCCKey("black"), makeMVCCKey("c")},
		{makeMVCCKey("green"), makeMVCCKey("h")},
		{makeMVCCKey(""), makeMVCCKey("")},
		{makeMVCCKey("13"), makeMVCCKey("2")},
		{makeMVCCKey("135"), makeMVCCKey("2")},
		{makeMVCCKey("13\xff"), makeMVCCKey("2")},
		{MVCCKey{
			Key:       []byte("1\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}, makeMVCCKey("2")},
		{makeMVCCKey("\xff"), makeMVCCKey("\xff")},
		{makeMVCCKey("\xff\xff"), makeMVCCKey("\xff\xff")},
		{makeMVCCKey("\xff\xff\xff"), makeMVCCKey("\xff\xff\xff")},
		{makeMVCCKey("\xfe\xff\xff"), makeMVCCKey("\xff")},
		{MVCCKey{
			Key:       []byte("\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}, MVCCKey{
			Key:       []byte("\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}},
	}
	for _, tc := range succCases {
		t.Run("", func(t *testing.T) {
			got := string(EngineComparer.Successor(nil, EncodeMVCCKey(tc.a)))
			if got != string(EncodeMVCCKey(tc.want)) {
				t.Errorf("a = %q: got %q, want %q", tc.a, got, tc.want)
			}
		})
	}

}

func TestPebbleMetricEventListener(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettings()
	MaxSyncDurationFatalOnExceeded.Override(ctx, &settings.SV, false)
	p, err := Open(ctx, InMemory(), CacheSize(1<<20 /* 1 MiB */), Settings(settings))
	require.NoError(t, err)
	defer p.Close()

	require.Equal(t, int64(0), p.writeStallCount)
	require.Equal(t, int64(0), p.diskSlowCount)
	require.Equal(t, int64(0), p.diskStallCount)
	p.eventListener.WriteStallBegin(pebble.WriteStallBeginInfo{})
	require.Equal(t, int64(1), p.writeStallCount)
	require.Equal(t, int64(0), p.diskSlowCount)
	require.Equal(t, int64(0), p.diskStallCount)
	p.eventListener.DiskSlow(pebble.DiskSlowInfo{Duration: 1 * time.Second})
	require.Equal(t, int64(1), p.writeStallCount)
	require.Equal(t, int64(1), p.diskSlowCount)
	require.Equal(t, int64(0), p.diskStallCount)
	p.eventListener.DiskSlow(pebble.DiskSlowInfo{Duration: 70 * time.Second})
	require.Equal(t, int64(1), p.writeStallCount)
	require.Equal(t, int64(1), p.diskSlowCount)
	require.Equal(t, int64(1), p.diskStallCount)
}

func TestPebbleIterConsistency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	eng := createTestPebbleEngine()
	defer eng.Close()
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	k1 := MVCCKey{[]byte("a"), ts1}
	require.NoError(t, eng.PutMVCC(k1, []byte("a1")))

	var (
		roEngine  = eng.NewReadOnly(StandardDurability)
		batch     = eng.NewBatch()
		roEngine2 = eng.NewReadOnly(StandardDurability)
		batch2    = eng.NewBatch()
	)
	defer roEngine.Close()
	defer batch.Close()
	defer roEngine2.Close()
	defer batch2.Close()

	require.False(t, eng.ConsistentIterators())
	require.True(t, roEngine.ConsistentIterators())
	require.True(t, batch.ConsistentIterators())
	require.True(t, roEngine2.ConsistentIterators())
	require.True(t, batch2.ConsistentIterators())

	// Since an iterator is created on pebbleReadOnly, pebbleBatch before
	// writing a newer version of "a", the newer version will not be visible to
	// iterators that are created later.
	roEngine.NewMVCCIterator(MVCCKeyIterKind, IterOptions{UpperBound: []byte("a")}).Close()
	batch.NewMVCCIterator(MVCCKeyIterKind, IterOptions{UpperBound: []byte("a")}).Close()
	eng.NewMVCCIterator(MVCCKeyIterKind, IterOptions{UpperBound: []byte("a")}).Close()
	// Pin the state for iterators.
	require.Nil(t, roEngine2.PinEngineStateForIterators())
	require.Nil(t, batch2.PinEngineStateForIterators())

	// Write a newer version of "a"
	require.NoError(t, eng.PutMVCC(MVCCKey{[]byte("a"), ts2}, []byte("a2")))

	checkMVCCIter := func(iter MVCCIterator) {
		defer iter.Close()
		iter.SeekGE(MVCCKey{Key: []byte("a")})
		valid, err := iter.Valid()
		require.Equal(t, true, valid)
		require.NoError(t, err)
		k := iter.UnsafeKey()
		require.True(t, k1.Equal(k), "expected %s != actual %s", k1.String(), k.String())
		iter.Next()
		valid, err = iter.Valid()
		require.False(t, valid)
		require.NoError(t, err)
	}
	checkEngineIter := func(iter EngineIterator) {
		defer iter.Close()
		valid, err := iter.SeekEngineKeyGE(EngineKey{Key: []byte("a")})
		require.Equal(t, true, valid)
		require.NoError(t, err)
		k, err := iter.UnsafeEngineKey()
		require.NoError(t, err)
		require.True(t, k.IsMVCCKey())
		mvccKey, err := k.ToMVCCKey()
		require.NoError(t, err)
		require.True(
			t, k1.Equal(mvccKey), "expected %s != actual %s", k1.String(), mvccKey.String())
		valid, err = iter.NextEngineKey()
		require.False(t, valid)
		require.NoError(t, err)
	}

	checkMVCCIter(roEngine.NewMVCCIterator(MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
	checkMVCCIter(roEngine.NewMVCCIterator(MVCCKeyIterKind, IterOptions{Prefix: true}))
	checkMVCCIter(batch.NewMVCCIterator(MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
	checkMVCCIter(batch.NewMVCCIterator(MVCCKeyIterKind, IterOptions{Prefix: true}))
	checkMVCCIter(roEngine2.NewMVCCIterator(MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
	checkMVCCIter(roEngine2.NewMVCCIterator(MVCCKeyIterKind, IterOptions{Prefix: true}))
	checkMVCCIter(batch2.NewMVCCIterator(MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
	checkMVCCIter(batch2.NewMVCCIterator(MVCCKeyIterKind, IterOptions{Prefix: true}))

	checkEngineIter(roEngine.NewEngineIterator(IterOptions{UpperBound: []byte("b")}))
	checkEngineIter(roEngine.NewEngineIterator(IterOptions{Prefix: true}))
	checkEngineIter(batch.NewEngineIterator(IterOptions{UpperBound: []byte("b")}))
	checkEngineIter(batch.NewEngineIterator(IterOptions{Prefix: true}))
	checkEngineIter(roEngine2.NewEngineIterator(IterOptions{UpperBound: []byte("b")}))
	checkEngineIter(roEngine2.NewEngineIterator(IterOptions{Prefix: true}))
	checkEngineIter(batch2.NewEngineIterator(IterOptions{UpperBound: []byte("b")}))
	checkEngineIter(batch2.NewEngineIterator(IterOptions{Prefix: true}))

	checkIterSeesBothValues := func(iter MVCCIterator) {
		defer iter.Close()
		iter.SeekGE(MVCCKey{Key: []byte("a")})
		count := 0
		for ; ; iter.Next() {
			valid, err := iter.Valid()
			require.NoError(t, err)
			if !valid {
				break
			}
			count++
		}
		require.Equal(t, 2, count)
	}
	// The eng iterator will see both values.
	checkIterSeesBothValues(eng.NewMVCCIterator(MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
	// The indexed batches will see 2 values since the second one is written to the batch.
	require.NoError(t, batch.PutMVCC(MVCCKey{[]byte("a"), ts2}, []byte("a2")))
	require.NoError(t, batch2.PutMVCC(MVCCKey{[]byte("a"), ts2}, []byte("a2")))
	checkIterSeesBothValues(batch.NewMVCCIterator(MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
	checkIterSeesBothValues(batch2.NewMVCCIterator(MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
}

func BenchmarkMVCCKeyCompare(b *testing.B) {
	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	keys := make([][]byte, 1000)
	for i := range keys {
		k := MVCCKey{
			Key: randutil.RandBytes(rng, 8),
			Timestamp: hlc.Timestamp{
				WallTime: int64(rng.Intn(5)),
			},
		}
		keys[i] = EncodeMVCCKey(k)
	}

	b.ResetTimer()
	var c int
	for i, j := 0, 0; i < b.N; i, j = i+1, j+3 {
		c = EngineKeyCompare(keys[i%len(keys)], keys[j%len(keys)])
	}
	if testing.Verbose() {
		fmt.Fprint(ioutil.Discard, c)
	}
}

type testValue struct {
	key       roachpb.Key
	value     roachpb.Value
	timestamp hlc.Timestamp
	txn       *roachpb.Transaction
}

func intent(key roachpb.Key, val string, ts hlc.Timestamp) testValue {
	var value = roachpb.MakeValueFromString(val)
	value.InitChecksum(key)
	tx := roachpb.MakeTransaction(fmt.Sprintf("txn-%v", key), key, roachpb.NormalUserPriority, ts, 1000, 99)
	var txn = &tx
	return testValue{key, value, ts, txn}
}

func value(key roachpb.Key, val string, ts hlc.Timestamp) testValue {
	var value = roachpb.MakeValueFromString(val)
	value.InitChecksum(key)
	return testValue{key, value, ts, nil}
}

func fillInData(ctx context.Context, engine Engine, data []testValue) error {
	batch := engine.NewBatch()
	defer batch.Close()
	for _, val := range data {
		if err := MVCCPut(ctx, batch, nil, val.key, val.timestamp, val.value, val.txn); err != nil {
			return err
		}
	}
	return batch.Commit(true)
}

func ts(ts int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: ts}
}

func key(k int) roachpb.Key {
	return []byte(fmt.Sprintf("%05d", k))
}

func requireTxnForValue(t *testing.T, val testValue, intent roachpb.Intent) {
	require.Equal(t, val.txn.Key, intent.Txn.Key)
}

func TestSstExportFailureIntentBatching(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test function uses a fixed time and key range to produce SST.
	// Use varying inserted keys for values and intents to putting them in and out of ranges.
	checkReportedErrors := func(data []testValue, expectedIntentIndices []int) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

			engine := createTestPebbleEngine()
			defer engine.Close()

			require.NoError(t, fillInData(ctx, engine, data))

			destination := &MemFile{}
			_, _, _, err := engine.ExportMVCCToSst(ctx, ExportOptions{
				StartKey:           MVCCKey{Key: key(10)},
				EndKey:             key(20000),
				StartTS:            ts(999),
				EndTS:              ts(2000),
				ExportAllRevisions: true,
				TargetSize:         0,
				MaxSize:            0,
				MaxIntents:         uint64(MaxIntentsPerWriteIntentError.Default()),
				StopMidKey:         false,
				UseTBI:             true,
			}, destination)
			if len(expectedIntentIndices) == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				e := (*roachpb.WriteIntentError)(nil)
				if !errors.As(err, &e) {
					require.Fail(t, "Expected WriteIntentFailure, got %T", err)
				}
				require.Equal(t, len(expectedIntentIndices), len(e.Intents))
				for i, dataIdx := range expectedIntentIndices {
					requireTxnForValue(t, data[dataIdx], e.Intents[i])
				}
			}
		}
	}

	// Export range is fixed to k:["00010", "10000"), ts:(999, 2000] for all tests.
	testDataCount := int(MaxIntentsPerWriteIntentError.Default() + 1)
	testData := make([]testValue, testDataCount*2)
	expectedErrors := make([]int, testDataCount)
	for i := 0; i < testDataCount; i++ {
		testData[i*2] = value(key(i*2+11), "value", ts(1000))
		testData[i*2+1] = intent(key(i*2+12), "intent", ts(1001))
		expectedErrors[i] = i*2 + 1
	}
	t.Run("Receive no more than limit intents", checkReportedErrors(testData, expectedErrors[:MaxIntentsPerWriteIntentError.Default()]))
}

// TestExportSplitMidKey verifies that split mid key in exports will omit
// resume timestamps where they are unnecessary e.g. when we split at the
// new key. In this case we can safely use the SST as is without the need
// to merge with the remaining versions of the key.
func TestExportSplitMidKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	engine := createTestPebbleEngine()
	defer engine.Close()

	const keyValueSize = 11

	var testData = []testValue{
		value(key(1), "value1", ts(1000)),
		value(key(2), "value2", ts(1000)),
		value(key(2), "value3", ts(2000)),
		value(key(3), "value4", ts(2000)),
	}
	require.NoError(t, fillInData(ctx, engine, testData))

	for _, test := range []struct {
		exportAll    bool
		useTBI       bool
		stopMidKey   bool
		useMaxSize   bool
		resumeCount  int
		resumeWithTs int
	}{
		{false, true, false, false, 3, 0},
		{true, true, false, false, 3, 0},
		{false, true, true, false, 3, 0},
		// No resume timestamps since we fall under max size criteria
		{true, true, true, false, 3, 0},
		{true, true, true, true, 4, 1},
		{false, false, false, false, 3, 0},
		{true, false, false, false, 3, 0},
		{false, false, true, false, 3, 0},
		// No resume timestamps since we fall under max size criteria
		{true, false, true, false, 3, 0},
		{true, false, true, true, 4, 1},
	} {
		t.Run(
			fmt.Sprintf(
				"exportAll=%t,useTBI=%t,stopMidKey=%t,useMaxSize=%t",
				test.exportAll, test.useTBI, test.stopMidKey, test.useMaxSize),
			func(t *testing.T) {
				firstKeyTS := hlc.Timestamp{}
				resumeKey := key(1)
				resumeWithTs := 0
				resumeCount := 0
				var maxSize uint64 = 0
				if test.useMaxSize {
					maxSize = keyValueSize * 2
				}
				for !resumeKey.Equal(roachpb.Key{}) {
					dest := &MemFile{}
					_, resumeKey, firstKeyTS, _ = engine.ExportMVCCToSst(
						ctx, ExportOptions{
							StartKey:           MVCCKey{Key: resumeKey, Timestamp: firstKeyTS},
							EndKey:             key(3).Next(),
							StartTS:            hlc.Timestamp{},
							EndTS:              hlc.Timestamp{WallTime: 9999},
							ExportAllRevisions: test.exportAll,
							TargetSize:         1,
							MaxSize:            maxSize,
							StopMidKey:         test.stopMidKey,
							UseTBI:             test.useTBI,
						}, dest)
					if !firstKeyTS.IsEmpty() {
						resumeWithTs++
					}
					resumeCount++
				}
				require.Equal(t, test.resumeCount, resumeCount)
				require.Equal(t, test.resumeWithTs, resumeWithTs)
			})
	}
}

// nonFatalLogger implements pebble.Logger by recording that a fatal log event
// was encountered at least once. Fatal log events are downgraded to Info level.
type nonFatalLogger struct {
	pebble.Logger
	t      *testing.T
	caught atomic.Value
}

func (l *nonFatalLogger) Fatalf(format string, args ...interface{}) {
	l.caught.Store(true)
	l.t.Logf(format, args...)
}

func TestPebbleKeyValidationFunc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Capture fatal errors by swapping out the logger.
	l := &nonFatalLogger{t: t}
	opt := func(cfg *engineConfig) error {
		cfg.Opts.Logger = l
		return nil
	}
	engine := createTestPebbleEngine(opt).(*Pebble)
	defer engine.Close()

	// Write a key with an invalid version length (=1) to simulate
	// programmer-error.
	ek := EngineKey{
		Key:     roachpb.Key("foo"),
		Version: make([]byte, 1),
	}

	// Sanity check: the key should fail validation.
	err := ek.Validate()
	require.Error(t, err)

	err = engine.PutEngineKey(ek, []byte("bar"))
	require.NoError(t, err)

	// Force a flush to trigger the compaction error.
	err = engine.Flush()
	require.NoError(t, err)

	// A fatal error was captured by the logger.
	require.True(t, l.caught.Load().(bool))
}

func TestPebbleBackgroundError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	loc := Location{
		dir: "",
		fs: &errorFS{
			FS:         vfs.NewMem(),
			errorCount: 3,
		},
	}
	eng, err := Open(context.Background(), loc)
	require.NoError(t, err)
	defer eng.Close()

	require.NoError(t, eng.PutMVCC(MVCCKey{[]byte("a"), hlc.Timestamp{WallTime: 1}}, []byte("a")))
	require.NoError(t, eng.db.Flush())
}

type errorFS struct {
	vfs.FS
	errorCount int32
}

func (fs *errorFS) Create(name string) (vfs.File, error) {
	if filepath.Ext(name) == ".sst" && atomic.AddInt32(&fs.errorCount, -1) >= 0 {
		return nil, errors.New("background error")
	}
	return fs.FS.Create(name)
}

type countingResourceLimiter struct {
	softCount int64
	hardCount int64
	count     int64
}

func (l *countingResourceLimiter) IsExhausted() ResourceLimitReached {
	l.count++
	if l.count > l.hardCount {
		return ResourceLimitReachedHard
	}
	if l.count > l.softCount {
		return ResourceLimitReachedSoft
	}
	return ResourceLimitNotReached
}

var _ ResourceLimiter = &countingResourceLimiter{}

type queryLimits struct {
	minKey       int64
	maxKey       int64
	minTimestamp hlc.Timestamp
	maxTimestamp hlc.Timestamp
	latest       bool
}

func testKey(id int64) roachpb.Key {
	return []byte(fmt.Sprintf("key-%08d", id))
}

type dataLimits struct {
	minKey          int64
	maxKey          int64
	minTimestamp    hlc.Timestamp
	maxTimestamp    hlc.Timestamp
	tombstoneChance float64
}

type resourceLimits struct {
	softThreshold int64
	hardThreshold int64
}

func generateData(t *testing.T, engine Engine, limits dataLimits, totalEntries int64) {
	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	for i := int64(0); i < totalEntries; i++ {
		key := testKey(limits.minKey + rand.Int63n(limits.maxKey-limits.minKey))
		timestamp := limits.minTimestamp.Add(rand.Int63n(limits.maxTimestamp.WallTime-limits.minTimestamp.WallTime), 0)
		size := 256
		if rng.Float64() < limits.tombstoneChance {
			size = 0
		}
		require.NoError(t, engine.PutMVCC(MVCCKey{Key: key, Timestamp: timestamp}, randutil.RandBytes(rng, size)), "Write data to test storage")
	}
	require.NoError(t, engine.Flush(), "Flush engine data")
}

func TestExportResourceLimits(t *testing.T) {
	defer leaktest.AfterTest(t)()

	engine := createTestPebbleEngine()
	defer engine.Close()

	limits := dataLimits{
		minKey:          0,
		maxKey:          1000,
		minTimestamp:    hlc.Timestamp{WallTime: 100000},
		maxTimestamp:    hlc.Timestamp{WallTime: 200000},
		tombstoneChance: 0.01,
	}
	generateData(t, engine, limits, (limits.maxKey-limits.minKey)*10)

	// Outer loop runs tests on subsets of mvcc dataset.
	for _, query := range []queryLimits{
		{
			minKey:       0,
			maxKey:       1000,
			minTimestamp: hlc.Timestamp{WallTime: 100000},
			maxTimestamp: hlc.Timestamp{WallTime: 200000},
			latest:       false,
		},
		{
			minKey:       200,
			maxKey:       800,
			minTimestamp: hlc.Timestamp{WallTime: 100000},
			maxTimestamp: hlc.Timestamp{WallTime: 200000},
			latest:       false,
		},
		{
			minKey:       0,
			maxKey:       1000,
			minTimestamp: hlc.Timestamp{WallTime: 150000},
			maxTimestamp: hlc.Timestamp{WallTime: 175000},
			latest:       false,
		},
		{
			minKey:       0,
			maxKey:       1000,
			minTimestamp: hlc.Timestamp{WallTime: 100000},
			maxTimestamp: hlc.Timestamp{WallTime: 200000},
			latest:       true,
		},
	} {
		t.Run(fmt.Sprintf("minKey=%d,maxKey=%d,minTs=%v,maxTs=%v,latest=%t", query.minKey, query.maxKey, query.minTimestamp, query.maxTimestamp, query.latest),
			func(t *testing.T) {
				matchingData := exportAllData(t, engine, query)
				// Inner loop exercises various thresholds to see that we always progress and respect soft
				// and hard limits.
				for _, resources := range []resourceLimits{
					// soft threshold under version count, high threshold above
					{softThreshold: 5, hardThreshold: 20},
					// soft threshold above version count
					{softThreshold: 15, hardThreshold: 30},
					// low threshold to check we could always progress
					{softThreshold: 0, hardThreshold: 0},
					// equal thresholds to check we force breaks mid keys
					{softThreshold: 15, hardThreshold: 15},
					// very high hard thresholds to eliminate mid key breaking completely
					{softThreshold: 5, hardThreshold: math.MaxInt64},
					// very high thresholds to eliminate breaking completely
					{softThreshold: math.MaxInt64, hardThreshold: math.MaxInt64},
				} {
					t.Run(fmt.Sprintf("softThreshold=%d,hardThreshold=%d", resources.softThreshold, resources.hardThreshold),
						func(t *testing.T) {
							assertDataEqual(t, engine, matchingData, query, resources)
						})
				}
			})
	}
}

func exportAllData(t *testing.T, engine Engine, limits queryLimits) []MVCCKey {
	sstFile := &MemFile{}
	_, _, _, err := engine.ExportMVCCToSst(context.Background(), ExportOptions{
		StartKey:           MVCCKey{Key: testKey(limits.minKey), Timestamp: limits.minTimestamp},
		EndKey:             testKey(limits.maxKey),
		StartTS:            limits.minTimestamp,
		EndTS:              limits.maxTimestamp,
		ExportAllRevisions: !limits.latest,
		UseTBI:             true,
	}, sstFile)
	require.NoError(t, err, "Failed to export expected data")
	return sstToKeys(t, sstFile.Data())
}

func sstToKeys(t *testing.T, data []byte) []MVCCKey {
	var results []MVCCKey
	it, err := NewMemSSTIterator(data, false)
	require.NoError(t, err, "Failed to read exported data")
	defer it.Close()
	for it.SeekGE(MVCCKey{Key: []byte{}}); ; {
		ok, err := it.Valid()
		require.NoError(t, err, "Failed to advance iterator while preparing data")
		if !ok {
			break
		}
		results = append(results, MVCCKey{
			Key:       append(roachpb.Key(nil), it.UnsafeKey().Key...),
			Timestamp: it.UnsafeKey().Timestamp,
		})
		it.Next()
	}
	return results
}

func assertDataEqual(
	t *testing.T, engine Engine, data []MVCCKey, query queryLimits, resources resourceLimits,
) {
	var (
		err       error
		key       = testKey(query.minKey)
		ts        = query.minTimestamp
		dataIndex = 0
	)
	for {
		// Export chunk
		limiter := countingResourceLimiter{softCount: resources.softThreshold, hardCount: resources.hardThreshold}
		sstFile := &MemFile{}
		_, key, ts, err = engine.ExportMVCCToSst(context.Background(), ExportOptions{
			StartKey:           MVCCKey{Key: key, Timestamp: ts},
			EndKey:             testKey(query.maxKey),
			StartTS:            query.minTimestamp,
			EndTS:              query.maxTimestamp,
			ExportAllRevisions: !query.latest,
			UseTBI:             true,
			StopMidKey:         true,
			ResourceLimiter:    &limiter,
		}, sstFile)
		require.NoError(t, err, "Failed to export to Sst")

		chunk := sstToKeys(t, sstFile.Data())
		require.LessOrEqual(t, len(chunk), len(data)-dataIndex, "Remaining test data")
		for _, key := range chunk {
			require.True(t, key.Equal(data[dataIndex]), "Returned key is not equal")
			dataIndex++
		}
		require.LessOrEqual(t, limiter.count-1, resources.hardThreshold, "Fragment size")

		// Last chunk check.
		if len(key) == 0 {
			break
		}
		require.GreaterOrEqual(t, limiter.count-1, resources.softThreshold, "Fragment size")
		if resources.hardThreshold == math.MaxInt64 {
			require.True(t, ts.IsEmpty(), "Should never break mid key on high hard thresholds")
		}
	}
	require.Equal(t, dataIndex, len(data), "Not all expected data was consumed")
}

func TestPebbleMVCCTimeIntervalCollector(t *testing.T) {
	defer leaktest.AfterTest(t)()

	aKey := roachpb.Key("a")
	collector := &pebbleDataBlockMVCCTimeIntervalCollector{}
	finishAndCheck := func(lower, upper uint64) {
		l, u, err := collector.FinishDataBlock()
		require.NoError(t, err)
		require.Equal(t, lower, l)
		require.Equal(t, upper, u)
	}
	// Nothing added.
	finishAndCheck(0, 0)
	uuid := uuid.Must(uuid.FromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
	ek, _ := LockTableKey{aKey, lock.Exclusive, uuid[:]}.ToEngineKey(nil)
	require.NoError(t, collector.Add(pebble.InternalKey{UserKey: ek.Encode()}, []byte("foo")))
	// The added key was not an MVCCKey.
	finishAndCheck(0, 0)
	require.NoError(t, collector.Add(pebble.InternalKey{
		UserKey: EncodeMVCCKey(MVCCKey{aKey, hlc.Timestamp{WallTime: 2, Logical: 1}})},
		[]byte("foo")))
	// Added 1 MVCCKey which sets both the upper and lower bound.
	finishAndCheck(2, 3)
	require.NoError(t, collector.Add(pebble.InternalKey{
		UserKey: EncodeMVCCKey(MVCCKey{aKey, hlc.Timestamp{WallTime: 22, Logical: 1}})},
		[]byte("foo")))
	require.NoError(t, collector.Add(pebble.InternalKey{
		UserKey: EncodeMVCCKey(MVCCKey{aKey, hlc.Timestamp{WallTime: 25, Logical: 1}})},
		[]byte("foo")))
	// Added 2 MVCCKeys.
	finishAndCheck(22, 26)
	// Using the same suffix for all keys in a block results in an interval of
	// width one (inclusive lower bound to exclusive upper bound).
	suffix := EncodeMVCCTimestampSuffix(hlc.Timestamp{WallTime: 42, Logical: 1})
	require.NoError(t, collector.UpdateKeySuffixes(
		nil /* old prop */, nil /* old suffix */, suffix,
	))
	finishAndCheck(42, 43)
	// An invalid key results in an error.
	// Case 1: malformed sentinel.
	key := EncodeMVCCKey(MVCCKey{aKey, hlc.Timestamp{WallTime: 2, Logical: 1}})
	sentinelPos := len(key) - 1 - int(key[len(key)-1])
	key[sentinelPos] = '\xff'
	require.Error(t, collector.UpdateKeySuffixes(nil, nil, key))
	// Case 2: malformed bare suffix (too short).
	suffix = EncodeMVCCTimestampSuffix(hlc.Timestamp{WallTime: 42, Logical: 1})[1:]
	require.Error(t, collector.UpdateKeySuffixes(nil, nil, suffix))
}

func TestPebbleMVCCTimeIntervalCollectorAndFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	overrideOptions := func(cfg *engineConfig) error {
		cfg.Opts.FormatMajorVersion = pebble.FormatNewest
		for i := range cfg.Opts.Levels {
			cfg.Opts.Levels[i].BlockSize = 1
			cfg.Opts.Levels[i].IndexBlockSize = 1
		}
		return nil
	}
	eng := NewDefaultInMemForTesting(overrideOptions)
	defer eng.Close()
	// We are simply testing that the integration is working.
	aKey := roachpb.Key("a")
	for i := 0; i < 10; i++ {
		require.NoError(t, eng.PutMVCC(
			MVCCKey{aKey, hlc.Timestamp{WallTime: int64(i), Logical: 1}},
			[]byte(fmt.Sprintf("val%d", i))))
	}
	require.NoError(t, eng.Flush())
	iter := eng.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
		LowerBound:       aKey,
		MinTimestampHint: hlc.Timestamp{WallTime: 5},
		MaxTimestampHint: hlc.Timestamp{WallTime: 7},
	})
	defer iter.Close()
	iter.SeekGE(MVCCKey{Key: aKey})
	var err error
	var valid bool
	var found []int64
	for valid, err = iter.Valid(); valid; {
		found = append(found, iter.Key().Timestamp.WallTime)
		iter.Next()
		valid, err = iter.Valid()
	}
	require.NoError(t, err)
	expected := []int64{7, 6, 5}
	require.Equal(t, expected, found)
}

func TestPebbleFlushCallbackAndDurabilityRequirement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := createTestPebbleEngine()
	defer eng.Close()

	ts := hlc.Timestamp{WallTime: 1}
	k := MVCCKey{[]byte("a"), ts}
	// Write.
	require.NoError(t, eng.PutMVCC(k, []byte("a1")))
	cbCount := int32(0)
	eng.RegisterFlushCompletedCallback(func() {
		atomic.AddInt32(&cbCount, 1)
	})
	roStandard := eng.NewReadOnly(StandardDurability)
	defer roStandard.Close()
	roGuaranteed := eng.NewReadOnly(GuaranteedDurability)
	defer roGuaranteed.Close()
	roGuaranteedPinned := eng.NewReadOnly(GuaranteedDurability)
	defer roGuaranteedPinned.Close()
	require.NoError(t, roGuaranteedPinned.PinEngineStateForIterators())
	// Returns the value found or nil.
	checkGetAndIter := func(reader Reader) []byte {
		v, err := reader.MVCCGet(k)
		require.NoError(t, err)
		iter := reader.NewMVCCIterator(MVCCKeyIterKind, IterOptions{UpperBound: k.Key.Next()})
		defer iter.Close()
		iter.SeekGE(k)
		valid, err := iter.Valid()
		require.NoError(t, err)
		require.Equal(t, v != nil, valid)
		if valid {
			require.Equal(t, v, iter.Value())
		}
		return v
	}
	require.Equal(t, "a1", string(checkGetAndIter(roStandard)))
	// Write is not visible yet.
	require.Nil(t, checkGetAndIter(roGuaranteed))
	require.Nil(t, checkGetAndIter(roGuaranteedPinned))

	// Flush the engine and wait for it to complete.
	require.NoError(t, eng.Flush())
	testutils.SucceedsSoon(t, func() error {
		if atomic.LoadInt32(&cbCount) < 1 {
			return errors.Errorf("not flushed")
		}
		return nil
	})
	// Write is visible to new guaranteed reader. We need to use a new reader
	// due to iterator caching.
	roGuaranteed2 := eng.NewReadOnly(GuaranteedDurability)
	defer roGuaranteed2.Close()
	require.Equal(t, "a1", string(checkGetAndIter(roGuaranteed2)))
}
