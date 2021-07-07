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
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestPebbleTimeBoundPropCollector(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.RunTest(t, "testdata/time_bound_props", func(t *testing.T, d *datadriven.TestData) string {
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
					UserKey: EncodeKey(MVCCKey{
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
	iter := newPebbleIterator(eng.db, nil, IterOptions{UpperBound: roachpb.Key("foo")})
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
			got := string(EngineComparer.Separator(nil, EncodeKey(tc.a), EncodeKey(tc.b)))
			if got != string(EncodeKey(tc.want)) {
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
			got := string(EngineComparer.Successor(nil, EncodeKey(tc.a)))
			if got != string(EncodeKey(tc.want)) {
				t.Errorf("a = %q: got %q, want %q", tc.a, got, tc.want)
			}
		})
	}

}

func TestPebbleDiskSlowEmit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettings()
	MaxSyncDurationFatalOnExceeded.Override(ctx, &settings.SV, false)
	p := newPebbleInMem(
		context.Background(),
		roachpb.Attributes{},
		1<<20,   /* cacheSize */
		512<<20, /* storeSize */
		vfs.NewMem(),
		"",
		settings,
	)
	defer p.Close()

	require.Equal(t, uint64(0), p.diskSlowCount)
	require.Equal(t, uint64(0), p.diskStallCount)
	p.eventListener.DiskSlow(pebble.DiskSlowInfo{Duration: 1 * time.Second})
	require.Equal(t, uint64(1), p.diskSlowCount)
	require.Equal(t, uint64(0), p.diskStallCount)
	p.eventListener.DiskSlow(pebble.DiskSlowInfo{Duration: 70 * time.Second})
	require.Equal(t, uint64(1), p.diskSlowCount)
	require.Equal(t, uint64(1), p.diskStallCount)
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

	roEngine := eng.NewReadOnly()
	batch := eng.NewBatch()
	roEngine2 := eng.NewReadOnly()
	batch2 := eng.NewBatch()

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
		iter.Close()
	}
	checkEngineIter := func(iter EngineIterator) {
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
		iter.Close()
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
		iter.Close()
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
		keys[i] = EncodeKey(k)
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
	tx := roachpb.MakeTransaction(fmt.Sprintf("txn-%v", key), key, roachpb.NormalUserPriority, ts, 1000)
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
			_, _, err := engine.ExportMVCCToSst(key(10), key(20000), ts(999), ts(2000),
				true, 0, 0, true, destination)
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
