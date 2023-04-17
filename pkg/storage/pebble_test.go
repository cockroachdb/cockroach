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
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	for _, k := range []MVCCKey{keyAMetadata, keyA2, keyA1, keyB2} {
		b := EncodeMVCCKey(k)
		require.Equal(t, 2, EngineComparer.Split(b))
	}
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
		key := MVCCKey{Key: []byte{byte(i)}, Timestamp: hlc.Timestamp{WallTime: 100}}
		value := MVCCValue{Value: roachpb.MakeValueFromString("foo")}
		if err := batch.PutMVCC(key, value); err != nil {
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
	p, err := Open(ctx, InMemory(), settings, CacheSize(1<<20 /* 1 MiB */))
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
	k1 := MVCCKey{Key: []byte("a"), Timestamp: ts1}
	v1 := MVCCValue{Value: roachpb.MakeValueFromString("a1")}
	require.NoError(t, eng.PutMVCC(k1, v1))

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
	k2 := MVCCKey{Key: []byte("a"), Timestamp: ts2}
	v2 := MVCCValue{Value: roachpb.MakeValueFromString("a2")}
	require.NoError(t, eng.PutMVCC(k2, v2))

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
	require.NoError(t, batch.PutMVCC(
		MVCCKey{Key: []byte("a"), Timestamp: ts2},
		MVCCValue{Value: roachpb.MakeValueFromString("a2")},
	))
	require.NoError(t, batch2.PutMVCC(
		MVCCKey{Key: []byte("a"), Timestamp: ts2},
		MVCCValue{Value: roachpb.MakeValueFromString("a2")},
	))
	checkIterSeesBothValues(batch.NewMVCCIterator(MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
	checkIterSeesBothValues(batch2.NewMVCCIterator(MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
}

func BenchmarkMVCCKeyCompare(b *testing.B) {
	keys := makeRandEncodedKeys()
	b.ResetTimer()
	for i, j := 0, 0; i < b.N; i, j = i+1, j+3 {
		_ = EngineKeyCompare(keys[i%len(keys)], keys[j%len(keys)])
	}
}

func BenchmarkMVCCKeyEqual(b *testing.B) {
	keys := makeRandEncodedKeys()
	b.ResetTimer()
	for i, j := 0, 0; i < b.N; i, j = i+1, j+3 {
		_ = EngineKeyEqual(keys[i%len(keys)], keys[j%len(keys)])
	}
}

func BenchmarkMVCCKeySplit(b *testing.B) {
	keys := makeRandEncodedKeys()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EngineComparer.Split(keys[i%len(keys)])
	}
}

func makeRandEncodedKeys() [][]byte {
	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	keys := make([][]byte, 1000)
	for i := range keys {
		k := MVCCKey{
			Key: []byte("shared" + [...]string{"a", "b", "c"}[rng.Intn(3)]),
			Timestamp: hlc.Timestamp{
				WallTime: rng.Int63n(5),
			},
		}
		if rng.Int31n(5) == 0 {
			// 20% of keys have a logical component.
			k.Timestamp.Logical = rng.Int31n(4) + 1
		}
		if rng.Int31n(1000) == 0 && !k.Timestamp.IsEmpty() {
			// 0.1% of keys have a synthetic component.
			k.Timestamp.Synthetic = true
		}
		keys[i] = EncodeMVCCKey(k)
	}
	return keys
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
		if err := MVCCPut(ctx, batch, nil, val.key, val.timestamp, hlc.ClockTimestamp{}, val.value, val.txn); err != nil {
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

// nonFatalLogger implements pebble.Logger by recording that a fatal log event
// was encountered at least once. Fatal log events are downgraded to Info level.
type nonFatalLogger struct {
	pebble.LoggerAndTracer
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
	// nonFatalLogger.LoggerAndTracer is nil, since the test exercises only
	// Fatalf.
	l := &nonFatalLogger{t: t}
	opt := func(cfg *engineConfig) error {
		cfg.Opts.LoggerAndTracer = l
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
	eng, err := Open(context.Background(), loc, cluster.MakeClusterSettings())
	require.NoError(t, err)
	defer eng.Close()

	require.NoError(t, eng.PutMVCC(
		MVCCKey{Key: []byte("a"), Timestamp: hlc.Timestamp{WallTime: 1}},
		MVCCValue{Value: roachpb.MakeValueFromString("a")},
	))
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

func TestPebbleMVCCTimeIntervalCollector(t *testing.T) {
	defer leaktest.AfterTest(t)()

	aKey := roachpb.Key("a")
	collector := &pebbleDataBlockMVCCTimeIntervalPointCollector{}
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

// TestPebbleMVCCTimeIntervalCollectorAndFilter tests that point and range key
// time interval collection and filtering works. It only tests basic
// integration.
func TestPebbleMVCCTimeIntervalCollectorAndFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up an engine with tiny blocks, so each point key gets its own block,
	// and disable compactions to keep SSTs separate.
	overrideOptions := func(cfg *engineConfig) error {
		cfg.Opts.DisableAutomaticCompactions = true
		for i := range cfg.Opts.Levels {
			cfg.Opts.Levels[i].BlockSize = 1
			cfg.Opts.Levels[i].IndexBlockSize = 1
		}
		return nil
	}
	eng := NewDefaultInMemForTesting(overrideOptions)
	defer eng.Close()

	// Point keys a@3, a@5, a@7 in separate blocks in a single SST.
	require.NoError(t, eng.PutMVCC(pointKey("a", 3), stringValue("a3")))
	require.NoError(t, eng.PutMVCC(pointKey("a", 5), stringValue("a5")))
	require.NoError(t, eng.PutMVCC(pointKey("a", 7), stringValue("a7")))
	require.NoError(t, eng.Flush())

	// Separate range keys [b-c)@5, [c-d)@7, [d-e)@9 share a block in a single SST.
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("b", "c", 5), MVCCValue{}))
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("c", "d", 7), MVCCValue{}))
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("d", "e", 9), MVCCValue{}))
	require.NoError(t, eng.Flush())

	// Overlapping range keys [x-z)@5, [x-z)@7 share a block in a single SST.
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("x", "z", 5), MVCCValue{}))
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("x", "z", 7), MVCCValue{}))
	require.NoError(t, eng.Flush())

	// NB: Range key filtering is currently disabled, see comment in
	// pebbleIterator.setOptions().
	testcases := map[string]struct {
		minTimestamp hlc.Timestamp
		maxTimestamp hlc.Timestamp
		expect       []interface{}
	}{
		"no bounds": {wallTS(0), wallTS(0), []interface{}{
			pointKV("a", 7, "a7"),
			pointKV("a", 5, "a5"),
			pointKV("a", 3, "a3"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
		"all": {wallTS(1), wallTS(10), []interface{}{
			pointKV("a", 7, "a7"),
			pointKV("a", 5, "a5"),
			pointKV("a", 3, "a3"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
		"above all": {wallTS(10), wallTS(11), []interface{}{
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
		"below all": {wallTS(0), wallTS(1), []interface{}{
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
		"intersect": {wallTS(5), wallTS(5), []interface{}{
			pointKV("a", 5, "a5"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
		"between": {wallTS(6), wallTS(6), []interface{}{
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
		"touches lower": {wallTS(1), wallTS(3), []interface{}{
			pointKV("a", 3, "a3"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
		"touches upper": {wallTS(9), wallTS(10), []interface{}{
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
	}

	keyTypes := []IterKeyType{IterKeyTypePointsAndRanges, IterKeyTypePointsOnly, IterKeyTypeRangesOnly}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			for _, keyType := range keyTypes {
				t.Run(keyType.String(), func(t *testing.T) {
					// Filter out expected values based on key type.
					var expect []interface{}
					for _, kv := range tc.expect {
						if _, isPoint := kv.(MVCCKeyValue); !isPoint && keyType == IterKeyTypePointsOnly {
							continue
						} else if _, isRange := kv.(MVCCRangeKeyValue); !isRange && keyType == IterKeyTypeRangesOnly {
							continue
						}
						expect = append(expect, kv)
					}

					iter := eng.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
						KeyTypes:         keyType,
						UpperBound:       keys.MaxKey,
						MinTimestampHint: tc.minTimestamp,
						MaxTimestampHint: tc.maxTimestamp,
					})
					defer iter.Close()
					require.Equal(t, expect, scanIter(t, iter))
				})
			}
		})
	}
}

// TestPebbleMVCCTimeIntervalWithClears tests that point and range key
// time interval collection and filtering works in the presence of
// point/range clears.
func TestPebbleMVCCTimeIntervalWithClears(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up an engine. In this case, we use large blocks to force all point and
	// range keys into the same blocks, to demonstrate the effect of clears in
	// separate SSTs when the clearing SST does not satisfy the filter. We
	// disable compactions to keep SSTs separate.
	overrideOptions := func(cfg *engineConfig) error {
		cfg.Opts.DisableAutomaticCompactions = true
		for i := range cfg.Opts.Levels {
			cfg.Opts.Levels[i].BlockSize = 65536
			cfg.Opts.Levels[i].IndexBlockSize = 65536
		}
		return nil
	}
	eng := NewDefaultInMemForTesting(overrideOptions)
	defer eng.Close()

	// Point keys a@3, a@5, a@7 in a single block in a single SST.
	require.NoError(t, eng.PutMVCC(pointKey("a", 3), stringValue("a3")))
	require.NoError(t, eng.PutMVCC(pointKey("a", 5), stringValue("a5")))
	require.NoError(t, eng.PutMVCC(pointKey("a", 7), stringValue("a7")))
	require.NoError(t, eng.Flush())

	// Separate range keys [b-c)@5, [c-d)@7, [d-e)@9 in a single block in a single SST.
	//
	// NB: Range key filtering is currently disabled, see comment in
	// pebbleIterator.setOptions().
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("b", "c", 5), MVCCValue{}))
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("c", "d", 7), MVCCValue{}))
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("d", "e", 9), MVCCValue{}))
	require.NoError(t, eng.Flush())

	// Clear a@5 and [c-d)@7 in a separate SST.
	require.NoError(t, eng.ClearMVCC(pointKey("a", 5)))
	require.NoError(t, eng.ClearMVCCRangeKey(rangeKey("c", "d", 7)))
	require.NoError(t, eng.Flush())

	testcases := map[string]struct {
		minTimestamp hlc.Timestamp
		maxTimestamp hlc.Timestamp
		expect       []interface{}
	}{
		"no bounds": {wallTS(0), wallTS(0), []interface{}{
			pointKV("a", 7, "a7"),
			pointKV("a", 3, "a3"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
		}},
		"all": {wallTS(1), wallTS(10), []interface{}{
			pointKV("a", 7, "a7"),
			pointKV("a", 3, "a3"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
		}},
		"at cleared point": {wallTS(5), wallTS(5), []interface{}{
			pointKV("a", 7, "a7"),
			pointKV("a", 3, "a3"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
		}},
		// NB: This reveals a@5 which has been deleted, because the SST block
		// containing the point clear does not satisfy the [7-7] filter.
		"at cleared range": {wallTS(7), wallTS(7), []interface{}{
			pointKV("a", 7, "a7"),
			pointKV("a", 5, "a5"),
			pointKV("a", 3, "a3"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
		}},
		// NB: This reveals a@5 which has been deleted, because the SST block
		// containing the point clear does not satisfy the [1-3] filter. Range keys
		// are not filtered.
		"touches lower": {wallTS(1), wallTS(3), []interface{}{
			pointKV("a", 7, "a7"),
			pointKV("a", 5, "a5"),
			pointKV("a", 3, "a3"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
		}},
		// Range keys are not filtered, so we see the [c-d)@7 clear even though it
		// wouldn't satisfy the [9-10] filter.
		"touches upper": {wallTS(9), wallTS(10), []interface{}{
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
		}},
	}

	keyTypes := []IterKeyType{IterKeyTypePointsAndRanges, IterKeyTypePointsOnly, IterKeyTypeRangesOnly}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			for _, keyType := range keyTypes {
				t.Run(keyType.String(), func(t *testing.T) {
					// Filter out expected values based on key type.
					var expect []interface{}
					for _, kv := range tc.expect {
						if _, isPoint := kv.(MVCCKeyValue); !isPoint && keyType == IterKeyTypePointsOnly {
							continue
						} else if _, isRange := kv.(MVCCRangeKeyValue); !isRange && keyType == IterKeyTypeRangesOnly {
							continue
						}
						expect = append(expect, kv)
					}

					iter := eng.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
						KeyTypes:         keyType,
						UpperBound:       keys.MaxKey,
						MinTimestampHint: tc.minTimestamp,
						MaxTimestampHint: tc.maxTimestamp,
					})
					defer iter.Close()
					require.Equal(t, expect, scanIter(t, iter))
				})
			}
		})
	}
}

// TestPebbleMVCCTimeIntervalWithRangeClears tests how point and range key
// time interval collection and filtering works in the presence of
// a ranged clear (i.e. Pebble range tombstone).
func TestPebbleMVCCTimeIntervalWithRangeClears(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up an engine with tiny blocks, so each point key gets its own block,
	// and disable compactions to keep SSTs separate.
	overrideOptions := func(cfg *engineConfig) error {
		cfg.Opts.DisableAutomaticCompactions = true
		for i := range cfg.Opts.Levels {
			cfg.Opts.Levels[i].BlockSize = 1
			cfg.Opts.Levels[i].IndexBlockSize = 1
		}
		return nil
	}
	eng := NewDefaultInMemForTesting(overrideOptions)
	defer eng.Close()

	// Point keys a@3, a@5, a@7 in separate blocks in a single SST.
	require.NoError(t, eng.PutMVCC(pointKey("a", 3), stringValue("a3")))
	require.NoError(t, eng.PutMVCC(pointKey("a", 5), stringValue("a5")))
	require.NoError(t, eng.PutMVCC(pointKey("a", 7), stringValue("a7")))
	require.NoError(t, eng.Flush())

	// Separate range keys [b-c)@5, [c-d)@7, [d-e)@9 in a single block in a single SST.
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("b", "c", 5), MVCCValue{}))
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("c", "d", 7), MVCCValue{}))
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("d", "e", 9), MVCCValue{}))
	require.NoError(t, eng.Flush())

	// Clear [a-z) in a separate SST.
	require.NoError(t, eng.ClearMVCCRange(roachpb.Key("a"), roachpb.Key("z"), true, true))
	require.NoError(t, eng.Flush())

	testcases := map[string]struct {
		minTimestamp hlc.Timestamp
		maxTimestamp hlc.Timestamp
		expect       []interface{}
	}{
		"no bounds":     {wallTS(0), wallTS(0), nil},
		"all":           {wallTS(1), wallTS(10), nil},
		"above all":     {wallTS(10), wallTS(11), nil},
		"below all":     {wallTS(0), wallTS(1), nil},
		"intersect":     {wallTS(5), wallTS(5), nil},
		"between":       {wallTS(6), wallTS(6), nil},
		"touches lower": {wallTS(1), wallTS(3), nil},
		"touches upper": {wallTS(9), wallTS(10), nil},
	}

	keyTypes := []IterKeyType{IterKeyTypePointsAndRanges, IterKeyTypePointsOnly, IterKeyTypeRangesOnly}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			for _, keyType := range keyTypes {
				t.Run(keyType.String(), func(t *testing.T) {
					// Filter out expected values based on key type.
					var expect []interface{}
					for _, kv := range tc.expect {
						if _, isPoint := kv.(MVCCKeyValue); !isPoint && keyType == IterKeyTypePointsOnly {
							continue
						} else if _, isRange := kv.(MVCCRangeKeyValue); !isRange && keyType == IterKeyTypeRangesOnly {
							continue
						}
						expect = append(expect, kv)
					}

					iter := eng.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
						KeyTypes:         keyType,
						UpperBound:       keys.MaxKey,
						MinTimestampHint: tc.minTimestamp,
						MaxTimestampHint: tc.maxTimestamp,
					})
					defer iter.Close()
					require.Equal(t, expect, scanIter(t, iter))
				})
			}
		})
	}
}

// TestPebbleTablePropertyFilter tests that pebbleIterator still respects
// crdb.ts.min and crdb.ts.max table properties in SSTs written by 22.1 and
// older nodes.
func TestPebbleTablePropertyFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up a static property collector which always writes the same table
	// properties [5-7] regardless of the SSTable contents. We keep the default
	// block property collects too, which will use the actual SSTable timestamps.
	overrideOptions := func(cfg *engineConfig) error {
		cfg.Opts.TablePropertyCollectors = []func() pebble.TablePropertyCollector{
			func() pebble.TablePropertyCollector {
				return &staticTablePropertyCollector{
					props: map[string]string{
						"crdb.ts.min": "\x00\x00\x00\x00\x00\x00\x00\x05", // WallTime: 5
						"crdb.ts.max": "\x00\x00\x00\x00\x00\x00\x00\x07", // WallTime: 7
					},
				}
			},
		}
		return nil
	}

	eng := NewDefaultInMemForTesting(overrideOptions)
	defer eng.Close()

	// Write keys with timestamps 1 and 7.
	require.NoError(t, eng.PutMVCC(pointKey("a", 1), stringValue("a1")))
	require.NoError(t, eng.PutMVCC(pointKey("b", 7), stringValue("b7")))
	require.NoError(t, eng.Flush())

	// Table and block properties now think the SST covers these spans:
	//
	// Block properties: [1-7]
	// Table properties: [5-7]
	//
	// Both must be satisfied in order for the (only) SST to be included.
	testcases := map[string]struct {
		minTimestamp int64
		maxTimestamp int64
		expectResult bool
	}{
		"tableprop lower inclusive": {4, 5, true},
		"tableprop upper inclusive": {7, 8, true},
		"tableprop exact":           {5, 7, true},
		"tableprop within":          {6, 6, true},
		"tableprop covering":        {4, 8, true},
		"tableprop below":           {3, 4, false},
		"both above":                {8, 9, false},
		"blockprop only":            {1, 3, false}, // needs both block and table props
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			iter := eng.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
				UpperBound:       keys.MaxKey,
				MinTimestampHint: hlc.Timestamp{WallTime: tc.minTimestamp},
				MaxTimestampHint: hlc.Timestamp{WallTime: tc.maxTimestamp},
			})
			defer iter.Close()

			kvs := scanIter(t, iter)
			if tc.expectResult {
				require.Equal(t, []interface{}{
					pointKV("a", 1, "a1"),
					pointKV("b", 7, "b7"),
				}, kvs)
			} else {
				require.Empty(t, kvs)
			}
		})
	}
}

type staticTablePropertyCollector struct {
	props map[string]string
}

func (c *staticTablePropertyCollector) Add(pebble.InternalKey, []byte) error {
	return nil
}

func (c *staticTablePropertyCollector) Finish(userProps map[string]string) error {
	for k, v := range c.props {
		userProps[k] = v
	}
	return nil
}

func (c *staticTablePropertyCollector) Name() string {
	return "staticTablePropertyCollector"
}

func TestPebbleFlushCallbackAndDurabilityRequirement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	DisableMetamorphicSimpleValueEncoding(t)

	eng := createTestPebbleEngine()
	defer eng.Close()

	ts := hlc.Timestamp{WallTime: 1}
	k := MVCCKey{Key: []byte("a"), Timestamp: ts}
	v := MVCCValue{Value: roachpb.MakeValueFromString("a1")}
	// Write.
	require.NoError(t, eng.PutMVCC(k, v))
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
		v := mvccGetRaw(t, reader, k)
		iter := reader.NewMVCCIterator(MVCCKeyIterKind, IterOptions{UpperBound: k.Key.Next()})
		defer iter.Close()
		iter.SeekGE(k)
		valid, err := iter.Valid()
		require.NoError(t, err)
		require.Equal(t, v != nil, valid)
		if valid {
			value, err := iter.Value()
			require.NoError(t, err)
			require.Equal(t, v, value)
		}
		return v
	}
	require.Equal(t, v.Value.RawBytes, checkGetAndIter(roStandard))
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
	require.Equal(t, v.Value.RawBytes, checkGetAndIter(roGuaranteed2))
}

// TestPebbleReaderMultipleIterators tests that all Pebble readers support
// multiple concurrent iterators of the same type.
func TestPebbleReaderMultipleIterators(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := NewDefaultInMemForTesting()
	defer eng.Close()

	a1 := MVCCKey{Key: roachpb.Key("a"), Timestamp: hlc.Timestamp{WallTime: 1}}
	b1 := MVCCKey{Key: roachpb.Key("b"), Timestamp: hlc.Timestamp{WallTime: 1}}
	c1 := MVCCKey{Key: roachpb.Key("c"), Timestamp: hlc.Timestamp{WallTime: 1}}

	v1 := MVCCValue{Value: roachpb.MakeValueFromString("1")}
	v2 := MVCCValue{Value: roachpb.MakeValueFromString("2")}
	v3 := MVCCValue{Value: roachpb.MakeValueFromString("3")}
	vx := MVCCValue{Value: roachpb.MakeValueFromString("x")}

	decodeValue := func(encoded []byte, err error) MVCCValue {
		require.NoError(t, err)
		value, err := DecodeMVCCValue(encoded)
		require.NoError(t, err)
		return value
	}

	require.NoError(t, eng.PutMVCC(a1, v1))
	require.NoError(t, eng.PutMVCC(b1, v2))
	require.NoError(t, eng.PutMVCC(c1, v3))

	readOnly := eng.NewReadOnly(StandardDurability)
	defer readOnly.Close()
	require.NoError(t, readOnly.PinEngineStateForIterators())

	snapshot := eng.NewSnapshot()
	defer snapshot.Close()
	require.NoError(t, snapshot.PinEngineStateForIterators())

	batch := eng.NewBatch()
	defer batch.Close()
	require.NoError(t, batch.PinEngineStateForIterators())

	// These writes should not be visible to any of the pinned iterators.
	require.NoError(t, eng.PutMVCC(a1, vx))
	require.NoError(t, eng.PutMVCC(b1, vx))
	require.NoError(t, eng.PutMVCC(c1, vx))

	testcases := map[string]Reader{
		"Engine":   eng,
		"ReadOnly": readOnly,
		"Snapshot": snapshot,
		"Batch":    batch,
	}
	for name, r := range testcases {
		t.Run(name, func(t *testing.T) {
			// Make sure we can create two iterators of the same type.
			i1 := r.NewMVCCIterator(MVCCKeyIterKind, IterOptions{LowerBound: a1.Key, UpperBound: keys.MaxKey})
			i2 := r.NewMVCCIterator(MVCCKeyIterKind, IterOptions{LowerBound: b1.Key, UpperBound: keys.MaxKey})

			// Make sure the iterators are independent.
			i1.SeekGE(a1)
			i2.SeekGE(a1)
			require.Equal(t, a1, i1.UnsafeKey())
			require.Equal(t, b1, i2.UnsafeKey()) // b1 because of LowerBound

			// Check iterator consistency.
			if r.ConsistentIterators() {
				require.Equal(t, v1, decodeValue(i1.UnsafeValue()))
				require.Equal(t, v2, decodeValue(i2.UnsafeValue()))
			} else {
				require.Equal(t, vx, decodeValue(i1.UnsafeValue()))
				require.Equal(t, vx, decodeValue(i2.UnsafeValue()))
			}

			// Closing one iterator shouldn't affect the other.
			i1.Close()
			i2.Next()
			require.Equal(t, c1, i2.UnsafeKey())
			i2.Close()

			// Quick check for engine iterators too.
			e1 := r.NewEngineIterator(IterOptions{UpperBound: keys.MaxKey})
			defer e1.Close()
			e2 := r.NewEngineIterator(IterOptions{UpperBound: keys.MaxKey})
			defer e2.Close()
		})
	}
}

func TestShortAttributeExtractor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var txnUUID [uuid.Size]byte
	lockKey, _ := LockTableKey{
		Key: roachpb.Key("a"), Strength: lock.Exclusive, TxnUUID: txnUUID[:]}.ToEngineKey(nil)
	v := MVCCValue{}
	tombstoneVal, err := EncodeMVCCValue(v)
	require.NoError(t, err)
	var sv roachpb.Value
	sv.SetString("foo")
	v = MVCCValue{Value: sv}
	strVal, err := EncodeMVCCValue(v)
	require.NoError(t, err)
	valHeader := enginepb.MVCCValueHeader{LocalTimestamp: hlc.ClockTimestamp{WallTime: 5}}
	v = MVCCValue{MVCCValueHeader: valHeader}
	tombstoneWithHeaderVal, err := EncodeMVCCValue(v)
	require.NoError(t, err)
	v = MVCCValue{MVCCValueHeader: valHeader, Value: sv}
	strWithHeaderVal, err := EncodeMVCCValue(v)
	require.NoError(t, err)
	mvccKey := EncodeMVCCKey(MVCCKey{Key: roachpb.Key("a"), Timestamp: hlc.Timestamp{WallTime: 20}})
	testCases := []struct {
		name   string
		key    []byte
		value  []byte
		attr   pebble.ShortAttribute
		errStr string
	}{
		{
			name:  "no-version",
			key:   EncodeMVCCKey(MVCCKey{Key: roachpb.Key("a")}),
			value: []byte(nil),
		},
		{
			name:  "lock-key",
			key:   lockKey.Encode(),
			value: []byte(nil),
		},
		{
			name:  "tombstone-val",
			key:   mvccKey,
			value: tombstoneVal,
			attr:  1,
		},
		{
			name:  "str-val",
			key:   mvccKey,
			value: strVal,
		},
		{
			name:  "tombstone-with-header-val",
			key:   mvccKey,
			value: tombstoneWithHeaderVal,
			attr:  1,
		},
		{
			name:  "str-with-header-val",
			key:   mvccKey,
			value: strWithHeaderVal,
		},
		{
			name:   "invalid-val",
			key:    mvccKey,
			value:  []byte("v"),
			errStr: "invalid encoded mvcc value",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prefixLen := EngineComparer.Split(tc.key)
			attr, err := shortAttributeExtractorForValues(tc.key, prefixLen, tc.value)
			if len(tc.errStr) != 0 {
				require.ErrorContains(t, err, tc.errStr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.attr, attr)
			}
		})
	}
}

func TestIncompatibleVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	loc := Location{
		dir: "",
		fs:  vfs.NewMem(),
	}

	p, err := Open(ctx, loc, cluster.MakeTestingClusterSettings())
	require.NoError(t, err)
	p.Close()

	// Overwrite the min version file with an unsupported version.
	version := roachpb.Version{Major: 21, Minor: 1}
	b, err := protoutil.Marshal(&version)
	require.NoError(t, err)
	require.NoError(t, fs.SafeWriteToFile(loc.fs, loc.dir, MinVersionFilename, b))

	_, err = Open(ctx, loc, cluster.MakeTestingClusterSettings())
	require.ErrorContains(t, err, "is too old for running version")
}

func TestNoMinVerFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	loc := Location{
		dir: "",
		fs:  vfs.NewMem(),
	}

	st := cluster.MakeTestingClusterSettings()
	p, err := Open(ctx, loc, st)
	require.NoError(t, err)
	p.Close()

	// Remove the min version filename.
	require.NoError(t, loc.fs.Remove(loc.fs.PathJoin(loc.dir, MinVersionFilename)))

	// We are still allowed the open the store if we haven't written anything to it.
	// This is useful in case the initial Open crashes right before writinng the
	// min version file.
	p, err = Open(ctx, loc, st)
	require.NoError(t, err)

	// Now write something to the store.
	k := MVCCKey{Key: []byte("a"), Timestamp: hlc.Timestamp{WallTime: 1}}
	v := MVCCValue{Value: roachpb.MakeValueFromString("a1")}
	require.NoError(t, p.PutMVCC(k, v))
	p.Close()

	// Remove the min version filename.
	require.NoError(t, loc.fs.Remove(loc.fs.PathJoin(loc.dir, MinVersionFilename)))

	_, err = Open(ctx, loc, st)
	require.ErrorContains(t, err, "store has no min-version file")
}

func TestApproximateDiskBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rng, _ := randutil.NewTestRand()

	p, err := Open(ctx, InMemory(), cluster.MakeTestingClusterSettings())
	require.NoError(t, err)
	defer p.Close()

	key := func(i int) roachpb.Key {
		return keys.SystemSQLCodec.TablePrefix(uint32(i))
	}

	// Write keys 0000...0999.
	b := p.NewWriteBatch()
	for i := 0; i < 1000; i++ {
		require.NoError(t, b.PutMVCC(
			MVCCKey{Key: key(i), Timestamp: hlc.Timestamp{WallTime: int64(i + 1)}},
			MVCCValue{Value: roachpb.Value{RawBytes: randutil.RandBytes(rng, 100)}},
		))
	}
	require.NoError(t, b.Commit(true /* sync */))
	require.NoError(t, p.Flush())

	approxBytes := func(span roachpb.Span) uint64 {
		v, err := p.ApproximateDiskBytes(span.Key, span.EndKey)
		require.NoError(t, err)
		t.Logf("%s (%x-%x): %d bytes", span, span.Key, span.EndKey, v)
		return v
	}

	all := approxBytes(roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax})
	for i := 0; i < 1000; i++ {
		s := roachpb.Span{Key: key(i), EndKey: key(i + 1)}
		if v := approxBytes(s); v >= all {
			t.Errorf("ApproximateDiskBytes(%q) = %d >= entire DB size %d", s, v, all)
		}
	}
}
