// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
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
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func ensureRangeEqual(
	t *testing.T, sortedKeys []string, keyMap map[string][]byte, keyvals []MVCCKeyValue,
) {
	t.Helper()
	if len(keyvals) != len(sortedKeys) {
		t.Errorf("length mismatch. expected %s, got %s", sortedKeys, keyvals)
	}
	for i, kv := range keyvals {
		if sortedKeys[i] != string(kv.Key.Key) {
			t.Errorf("key mismatch at index %d: expected %q, got %q", i, sortedKeys[i], kv.Key)
		}
		if !bytes.Equal(keyMap[sortedKeys[i]], kv.Value) {
			t.Errorf("value mismatch at index %d: expected %q, got %q", i, keyMap[sortedKeys[i]], kv.Value)
		}
	}
}

// TestEngineBatchCommit writes a batch containing 10K rows (all the
// same key) and concurrently attempts to read the value in a tight
// loop. The test verifies that either there is no value for the key
// or it contains the final value, but never a value in between.
func TestEngineBatchCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	numWrites := 10000
	key := mvccKey("a")
	finalVal := []byte(strconv.Itoa(numWrites - 1))

	e := NewDefaultInMemForTesting()
	defer e.Close()

	// Start a concurrent read operation in a busy loop.
	readsBegun := make(chan struct{})
	readsDone := make(chan error)
	writesDone := make(chan struct{})
	go func() {
		readsDone <- func() error {
			readsBegunAlias := readsBegun
			for {
				select {
				case <-writesDone:
					return nil
				default:
					val, err := mvccGetRawWithError(t, e, key)
					if err != nil {
						return err
					}
					if val != nil && !bytes.Equal(val, finalVal) {
						return errors.Errorf("key value should be empty or %q; got %q", string(finalVal), string(val))
					}
					if readsBegunAlias != nil {
						close(readsBegunAlias)
						readsBegunAlias = nil
					}
				}
			}
		}()
	}()
	// Wait until we've succeeded with first read.
	<-readsBegun

	// Create key/values and put them in a batch to engine.
	batch := e.NewBatch()
	defer batch.Close()
	for i := 0; i < numWrites; i++ {
		if err := batch.PutUnversioned(key.Key, []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(false /* sync */); err != nil {
		t.Fatal(err)
	}
	close(writesDone)
	if err := <-readsDone; err != nil {
		t.Fatal(err)
	}
}

func TestEngineBatchStaleCachedIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Prevent regression of a bug which caused spurious MVCC errors due to an
	// invalid optimization which let an iterator return key-value pairs which
	// had since been deleted from the underlying engine.
	// Discovered in #6878.

	eng := NewDefaultInMemForTesting()
	defer eng.Close()

	// Focused failure mode: highlights the actual bug.
	{
		batch := eng.NewBatch()
		defer batch.Close()
		iter, err := batch.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
		if err != nil {
			t.Fatal(err)
		}
		key := MVCCKey{Key: roachpb.Key("b")}

		if err := batch.PutUnversioned(key.Key, []byte("foo")); err != nil {
			t.Fatal(err)
		}

		iter.SeekGE(key)

		if err := batch.ClearUnversioned(key.Key, ClearOptions{}); err != nil {
			t.Fatal(err)
		}

		// MVCCIterator should not reuse its cached result.
		iter.SeekGE(key)

		if ok, err := iter.Valid(); err != nil {
			t.Fatal(err)
		} else if ok {
			v, _ := iter.UnsafeValue()
			t.Fatalf("iterator unexpectedly valid: %v -> %v", iter.UnsafeKey(), v)
		}

		iter.Close()
	}

	// Higher-level failure mode. Mostly for documentation.
	{
		batch := eng.NewBatch()
		defer batch.Close()

		key := roachpb.Key("z")

		// Put a value so that the deletion below finds a value to seek
		// to.
		if _, err := MVCCPut(
			context.Background(),
			batch,
			key,
			hlc.Timestamp{},
			roachpb.MakeValueFromString("x"),
			MVCCWriteOptions{},
		); err != nil {
			t.Fatal(err)
		}

		// Seek the iterator to `key` and clear the value (but without
		// telling the iterator about that).
		if _, _, err := MVCCDelete(context.Background(), batch, key, hlc.Timestamp{}, MVCCWriteOptions{}); err != nil {
			t.Fatal(err)
		}

		// Trigger a seek on the cached iterator by seeking to the (now
		// absent) key.
		// The underlying iterator will already be in the right position
		// due to a seek in MVCCDelete (followed by a Clear, which does not
		// invalidate the iterator's cache), and if it reports its cached
		// result back, we'll see the (newly deleted) value (due to the
		// failure mode above).
		if valRes, err := MVCCGet(context.Background(), batch, key,
			hlc.Timestamp{}, MVCCGetOptions{}); err != nil {
			t.Fatal(err)
		} else if valRes.Value != nil {
			t.Fatalf("expected no value, got %+v", valRes.Value)
		}
	}
}

func TestEngineBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	numShuffles := 100
	key := mvccKey("a")
	// Those are randomized below.
	type data struct {
		key   MVCCKey
		value []byte
		merge bool
	}
	batch := []data{
		{key, appender("~ockroachDB"), false},
		{key, appender("C~ckroachDB"), false},
		{key, appender("Co~kroachDB"), false},
		{key, appender("Coc~roachDB"), false},
		{key, appender("Cock~oachDB"), false},
		{key, appender("Cockr~achDB"), false},
		{key, appender("Cockro~chDB"), false},
		{key, appender("Cockroa~hDB"), false},
		{key, appender("Cockroac~DB"), false},
		{key, appender("Cockroach~B"), false},
		{key, appender("CockroachD~"), false},
		{key, nil, false},
		{key, appender("C"), true},
		{key, appender(" o"), true},
		{key, appender("  c"), true},
		{key, appender(" k"), true},
		{key, appender("r"), true},
		{key, appender(" o"), true},
		{key, appender("  a"), true},
		{key, appender(" c"), true},
		{key, appender("h"), true},
		{key, appender(" D"), true},
		{key, appender("  B"), true},
	}

	apply := func(rw ReadWriter, d data) error {
		if d.value == nil {
			return rw.ClearUnversioned(d.key.Key, ClearOptions{})
		} else if d.merge {
			return rw.Merge(d.key, d.value)
		}
		return rw.PutUnversioned(d.key.Key, d.value)
	}

	get := func(rw ReadWriter, key MVCCKey) []byte {
		b := mvccGetRaw(t, rw, key)
		var m enginepb.MVCCMetadata
		if err := protoutil.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		if !m.IsInline() {
			return nil
		}
		valueBytes, err := MakeValue(m).GetBytes()
		if err != nil {
			t.Fatal(err)
		}
		return valueBytes
	}

	for i := 0; i < numShuffles; i++ {
		// In each run, create an array of shuffled operations.
		shuffledIndices := rand.Perm(len(batch))
		currentBatch := make([]data, len(batch))
		for k := range currentBatch {
			currentBatch[k] = batch[shuffledIndices[k]]
		}
		// Reset the key
		if err := engine.ClearUnversioned(key.Key, ClearOptions{}); err != nil {
			t.Fatal(err)
		}
		// Run it once with individual operations and remember the result.
		for i, op := range currentBatch {
			if err := apply(engine, op); err != nil {
				t.Errorf("%d: op %v: %+v", i, op, err)
				continue
			}
		}
		expectedValue := get(engine, key)
		// Run the whole thing as a batch and compare.
		b := engine.NewBatch()
		defer b.Close()
		if err := b.ClearUnversioned(key.Key, ClearOptions{}); err != nil {
			t.Fatal(err)
		}
		for _, op := range currentBatch {
			if err := apply(b, op); err != nil {
				t.Fatal(err)
			}
		}
		// Try getting the value from the batch.
		actualValue := get(b, key)
		if !bytes.Equal(actualValue, expectedValue) {
			t.Errorf("%d: expected %s, but got %s", i, expectedValue, actualValue)
		}
		// Try using an iterator to get the value from the batch.
		iter, err := b.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
		if err != nil {
			t.Fatal(err)
		}
		iter.SeekGE(key)
		if ok, err := iter.Valid(); !ok {
			if currentBatch[len(currentBatch)-1].value != nil {
				t.Errorf("%d: batch seek invalid, err=%v", i, err)
			}
		} else if !iter.UnsafeKey().Equal(key) {
			t.Errorf("%d: batch seek expected key %s, but got %s", i, key, iter.UnsafeKey())
		} else {
			var m enginepb.MVCCMetadata
			if err := iter.ValueProto(&m); err != nil {
				t.Fatal(err)
			}
			valueBytes, err := MakeValue(m).GetBytes()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(valueBytes, expectedValue) {
				t.Errorf("%d: expected %s, but got %s", i, expectedValue, valueBytes)
			}
		}
		iter.Close()
		// Commit the batch and try getting the value from the engine.
		if err := b.Commit(false /* sync */); err != nil {
			t.Errorf("%d: %+v", i, err)
			continue
		}
		actualValue = get(engine, key)
		if !bytes.Equal(actualValue, expectedValue) {
			t.Errorf("%d: expected %s, but got %s", i, expectedValue, actualValue)
		}
	}
}

func TestEnginePutGetDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Test for correct handling of empty keys, which should produce errors.
	for i, err := range []error{
		engine.PutUnversioned(mvccKey("").Key, []byte("")),
		engine.PutUnversioned(NilKey.Key, []byte("")),
		engine.ClearUnversioned(NilKey.Key, ClearOptions{}),
		engine.ClearUnversioned(NilKey.Key, ClearOptions{}),
		engine.ClearUnversioned(mvccKey("").Key, ClearOptions{}),
	} {
		if err == nil {
			t.Fatalf("%d: illegal handling of empty key", i)
		}
	}

	// Test for allowed keys, which should go through.
	testCases := []struct {
		key   MVCCKey
		value []byte
	}{
		{mvccKey("dog"), []byte("woof")},
		{mvccKey("cat"), []byte("meow")},
		{mvccKey("emptyval"), nil},
		{mvccKey("emptyval2"), []byte("")},
		{mvccKey("server"), []byte("42")},
	}
	for _, c := range testCases {
		val := mvccGetRaw(t, engine, c.key)
		if len(val) != 0 {
			t.Errorf("expected key %q value.Bytes to be nil: got %+v", c.key, val)
		}
		if err := engine.PutUnversioned(c.key.Key, c.value); err != nil {
			t.Errorf("put: expected no error, but got %s", err)
		}
		val = mvccGetRaw(t, engine, c.key)
		if !bytes.Equal(val, c.value) {
			t.Errorf("expected key value %s to be %+v: got %+v", c.key, c.value, val)
		}
		if err := engine.ClearUnversioned(c.key.Key, ClearOptions{
			ValueSizeKnown: true,
			ValueSize:      uint32(len(val)),
		}); err != nil {
			t.Errorf("delete: expected no error, but got %s", err)
		}
		val = mvccGetRaw(t, engine, c.key)
		if len(val) != 0 {
			t.Errorf("expected key %s value.Bytes to be nil: got %+v", c.key, val)
		}
	}
}

func addMergeTimestamp(t *testing.T, data []byte, ts int64) []byte {
	var v enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	v.MergeTimestamp = &hlc.LegacyTimestamp{WallTime: ts}
	return mustMarshal(&v)
}

var testtime = int64(-446061360000000000)

type tsSample struct {
	offset int32
	count  uint32
	sum    float64
	max    float64
	min    float64
}

// timeSeriesRow generates a simple InternalTimeSeriesData object which starts
// at the given timestamp and has samples of the given duration. The time series
// is written using the older sample-row data format. The object is stored in an
// MVCCMetadata object and marshaled to bytes.
func timeSeriesRow(start int64, duration int64, samples ...tsSample) []byte {
	tsv := timeSeriesRowAsValue(start, duration, samples...)
	return mustMarshal(&enginepb.MVCCMetadataSubsetForMergeSerialization{RawBytes: tsv.RawBytes})
}

func timeSeriesRowAsValue(start int64, duration int64, samples ...tsSample) roachpb.Value {
	ts := &roachpb.InternalTimeSeriesData{
		StartTimestampNanos: start,
		SampleDurationNanos: duration,
	}
	for _, sample := range samples {
		newSample := roachpb.InternalTimeSeriesSample{
			Offset: sample.offset,
			Count:  sample.count,
			Sum:    sample.sum,
		}
		if sample.count > 1 {
			newSample.Max = proto.Float64(sample.max)
			newSample.Min = proto.Float64(sample.min)
		}
		ts.Samples = append(ts.Samples, newSample)
	}
	var v roachpb.Value
	if err := v.SetProto(ts); err != nil {
		panic(err)
	}
	return v
}

// TestEngineMerge tests that the passing through of engine merge operations
// to the goMerge function works as expected. The semantics are tested more
// exhaustively in the merge tests themselves.
func TestEngineMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	testcases := []struct {
		testKey  MVCCKey
		merges   [][]byte
		expected []byte
	}{
		{
			// Test case with RawBytes only.
			mvccKey("haste not in life"),
			[][]byte{
				appender("x"),
				appender("y"),
				appender("z"),
			},
			appender("xyz"),
		},
		{
			// Test case with RawBytes and MergeTimestamp.
			mvccKey("timeseriesmerged"),
			[][]byte{
				addMergeTimestamp(t, timeSeriesRow(testtime, 1000, []tsSample{
					{1, 1, 5, 5, 5},
				}...), 27),
				timeSeriesRow(testtime, 1000, []tsSample{
					{2, 1, 5, 5, 5},
					{1, 2, 10, 7, 3},
				}...),
				addMergeTimestamp(t, timeSeriesRow(testtime, 1000, []tsSample{
					{10, 1, 5, 5, 5},
				}...), 53),
				timeSeriesRow(testtime, 1000, []tsSample{
					{5, 1, 5, 5, 5},
					{3, 1, 5, 5, 5},
				}...),
			},
			addMergeTimestamp(t, timeSeriesRow(testtime, 1000, []tsSample{
				{1, 2, 10, 7, 3},
				{2, 1, 5, 5, 5},
				{3, 1, 5, 5, 5},
				{5, 1, 5, 5, 5},
				{10, 1, 5, 5, 5},
			}...), 27),
		},
	}
	engineBytes := make([][]byte, len(testcases))
	for tcIndex, tc := range testcases {
		for i, update := range tc.merges {
			if err := engine.Merge(tc.testKey, update); err != nil {
				t.Fatalf("%d: %+v", i, err)
			}
		}
		result := mvccGetRaw(t, engine, tc.testKey)
		engineBytes[tcIndex] = result
		var resultV, expectedV enginepb.MVCCMetadata
		if err := protoutil.Unmarshal(result, &resultV); err != nil {
			t.Fatal(err)
		}
		if err := protoutil.Unmarshal(tc.expected, &expectedV); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(resultV, expectedV) {
			t.Errorf("unexpected append-merge result: %v != %v", resultV, expectedV)
		}
	}
}

func TestEngineMustExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tempDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	env := fs.MustInitPhysicalTestingEnv(tempDir)
	_, err := Open(context.Background(), env, cluster.MakeClusterSettings(), MustExist)
	if err == nil {
		t.Fatal("expected error related to missing directory")
	}
	if !strings.Contains(fmt.Sprint(err), "does not exist") {
		t.Fatal(err)
	}
	env.Close()
}

func TestEngineTimeBound(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	minTimestamp := hlc.Timestamp{WallTime: 3, Logical: 0}
	maxTimestamp := hlc.Timestamp{WallTime: 7, Logical: 0}

	times := []hlc.Timestamp{
		{WallTime: 5, Logical: 0},
		minTimestamp,
		maxTimestamp,
		{WallTime: 5, Logical: 0},
	}

	for i, time := range times {
		s := fmt.Sprintf("%02d", i)
		key := MVCCKey{Key: roachpb.Key(s), Timestamp: time}
		value := MVCCValue{Value: roachpb.MakeValueFromString(s)}
		require.NoError(t, engine.PutMVCC(key, value))
	}
	require.NoError(t, engine.Flush())

	batch := engine.NewBatch()
	defer batch.Close()

	testCases := map[string]struct {
		iter func() (MVCCIterator, error)
		keys int
	}{
		"right not touching": {
			iter: func() (MVCCIterator, error) {
				return batch.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
					MinTimestamp: maxTimestamp.WallNext(),
					MaxTimestamp: maxTimestamp.WallNext().WallNext(),
					UpperBound:   roachpb.KeyMax,
				})
			},
			keys: 0,
		},
		"left not touching": {
			iter: func() (MVCCIterator, error) {
				return batch.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
					MinTimestamp: minTimestamp.WallPrev().WallPrev(),
					MaxTimestamp: minTimestamp.WallPrev(),
					UpperBound:   roachpb.KeyMax,
				})
			},
			keys: 0,
		},
		"right touching": {
			iter: func() (MVCCIterator, error) {
				return batch.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
					MinTimestamp: maxTimestamp,
					MaxTimestamp: maxTimestamp,
					UpperBound:   roachpb.KeyMax,
				})
			},
			keys: 1, // only one key exists at the max timestamp @7
		},
		// Although the block-property and table filters have historically
		// ignored logical timestamps (and synthetic bits), the
		// MVCCIncrementalIterator does not. It performs a strict hlc.Timestamp
		// comparison. Both @7,1 and @7,2 are greater than @7, so no keys are
		// visible.
		"right touching enfoces logical": {
			iter: func() (MVCCIterator, error) {
				return batch.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
					MinTimestamp: maxTimestamp.Next(),        // @7,1
					MaxTimestamp: maxTimestamp.Next().Next(), // @7,2
					UpperBound:   roachpb.KeyMax,
				})
			},
			keys: 0,
		},
		"left touching": {
			iter: func() (MVCCIterator, error) {
				return batch.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
					MinTimestamp: minTimestamp,
					MaxTimestamp: minTimestamp,
					UpperBound:   roachpb.KeyMax,
				})
			},
			keys: 1,
		},
		"left touching upperbound": {
			iter: func() (MVCCIterator, error) {
				return batch.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
					MinTimestamp: minTimestamp,
					MaxTimestamp: minTimestamp,
					UpperBound:   []byte("02"),
				})
			},
			keys: 1,
		},
		"between": {
			iter: func() (MVCCIterator, error) {
				return batch.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
					MinTimestamp: minTimestamp.Next(),
					MaxTimestamp: minTimestamp.Next(),
					UpperBound:   roachpb.KeyMax,
				})
			},
			keys: 0,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tbi, err := tc.iter()
			require.NoError(t, err)
			defer tbi.Close()

			var count int
			for tbi.SeekGE(NilKey); ; tbi.Next() {
				ok, err := tbi.Valid()
				require.NoError(t, err)
				if !ok {
					break
				}
				count++
			}

			require.Equal(t, tc.keys, count)
		})
	}

	// Make a regular iterator. Before #21721, this would accidentally pick up the
	// time bounded iterator instead.
	iter, err := batch.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	var count int
	for iter.SeekGE(MVCCKey{Key: keys.LocalMax}); ; iter.Next() {
		ok, err := iter.Valid()
		require.NoError(t, err)
		if !ok {
			break
		}
		count++
	}

	// Make sure the iterator sees the writes (i.e. it's not the time bounded iterator).
	require.Equal(t, len(times), count)
}

func TestFlushNumSSTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engine := NewDefaultInMemForTesting(DiskWriteStatsCollector(vfs.NewDiskWriteStatsCollector()))
	defer engine.Close()

	batch := engine.NewBatch()
	for i := 0; i < 10000; i++ {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(i))
		err := batch.PutUnversioned(key, []byte("foobar"))
		if err != nil {
			t.Fatal(err)
		}
	}

	err := batch.Commit(true)
	if err != nil {
		t.Fatal(err)
	}
	batch.Close()

	err = engine.Flush()
	if err != nil {
		t.Fatal(err)
	}

	m := engine.GetMetrics()
	if m.NumSSTables() == 0 {
		t.Fatal("expected non-zero sstables, got 0")
	}
}

func TestEngineScan1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engine := NewDefaultInMemForTesting()

	testCases := []struct {
		key   MVCCKey
		value []byte
	}{
		{mvccKey("dog"), []byte("woof")},
		{mvccKey("cat"), []byte("meow")},
		{mvccKey("server"), []byte("42")},
		{mvccKey("french"), []byte("Allô?")},
		{mvccKey("german"), []byte("hallo")},
		{mvccKey("chinese"), []byte("你好")},
	}
	keyMap := map[string][]byte{}
	for _, c := range testCases {
		if err := engine.PutUnversioned(c.key.Key, c.value); err != nil {
			t.Errorf("could not put key %q: %+v", c.key, err)
		}
		keyMap[string(c.key.Key)] = c.value
	}
	sortedKeys := make([]string, len(testCases))
	for i, t := range testCases {
		sortedKeys[i] = string(t.key.Key)
	}
	sort.Strings(sortedKeys)

	keyvals, err := Scan(context.Background(), engine, roachpb.Key("chinese"), roachpb.Key("german"), 0)
	if err != nil {
		t.Fatalf("could not run scan: %+v", err)
	}
	ensureRangeEqual(t, sortedKeys[1:4], keyMap, keyvals)

	// Check an end of range which does not equal an existing key.
	keyvals, err = Scan(context.Background(), engine, roachpb.Key("chinese"), roachpb.Key("german1"), 0)
	if err != nil {
		t.Fatalf("could not run scan: %+v", err)
	}
	ensureRangeEqual(t, sortedKeys[1:5], keyMap, keyvals)

	keyvals, err = Scan(context.Background(), engine, roachpb.Key("chinese"), roachpb.Key("german"), 2)
	if err != nil {
		t.Fatalf("could not run scan: %+v", err)
	}
	ensureRangeEqual(t, sortedKeys[1:3], keyMap, keyvals)

	// Should return all key/value pairs in lexicographic order. Note that
	// LocalMax is the lowest possible global key.
	startKeys := []roachpb.Key{roachpb.Key("cat"), keys.LocalMax}
	for _, startKey := range startKeys {
		keyvals, err = Scan(context.Background(), engine, startKey, roachpb.KeyMax, 0)
		if err != nil {
			t.Fatalf("could not run scan: %+v", err)
		}
		ensureRangeEqual(t, sortedKeys, keyMap, keyvals)
	}

	// Test iterator stats.
	ro := engine.NewReader(StandardDurability)
	iter, err := ro.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{LowerBound: roachpb.Key("cat"), UpperBound: roachpb.Key("server")})
	require.NoError(t, err)
	iter.SeekGE(MVCCKey{Key: roachpb.Key("cat")})
	for {
		valid, err := iter.Valid()
		require.NoError(t, err)
		if !valid {
			break
		}
		iter.Next()
	}
	stats := iter.Stats().Stats
	// Setting non-deterministic InternalStats to empty.
	stats.InternalStats = pebble.InternalIteratorStats{}
	require.Equal(t, "seeked 1 times (1 internal); stepped 5 times (5 internal)", stats.String())
	iter.Close()
	iter, err = ro.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{LowerBound: roachpb.Key("cat"), UpperBound: roachpb.Key("server")})
	require.NoError(t, err)
	// pebble.Iterator is reused, but stats are reset.
	stats = iter.Stats().Stats
	// Setting non-deterministic InternalStats to empty.
	stats.InternalStats = pebble.InternalIteratorStats{}
	require.Equal(t, "seeked 0 times (0 internal); stepped 0 times (0 internal)", stats.String())
	iter.SeekGE(MVCCKey{Key: roachpb.Key("french")})
	iter.SeekLT(MVCCKey{Key: roachpb.Key("server")})
	stats = iter.Stats().Stats
	// Setting non-deterministic InternalStats to empty.
	stats.InternalStats = pebble.InternalIteratorStats{}
	require.Equal(t, "seeked 2 times (1 fwd/1 rev, internal: 1 fwd/1 rev); stepped 0 times (0 fwd/0 rev, internal: 0 fwd/1 rev)", stats.String())
	iter.Close()
	ro.Close()
	engine.Close()
}

func verifyScan(start, end roachpb.Key, max int64, expKeys []MVCCKey, engine Engine, t *testing.T) {
	kvs, err := Scan(context.Background(), engine, start, end, max)
	if err != nil {
		t.Errorf("scan %q-%q: expected no error, but got %s", start, end, err)
	}
	if len(kvs) != len(expKeys) {
		t.Errorf("scan %q-%q: expected scanned keys mismatch %d != %d: %v",
			start, end, len(kvs), len(expKeys), kvs)
	}
	for i, kv := range kvs {
		if !kv.Key.Equal(expKeys[i]) {
			t.Errorf("scan %q-%q: expected keys equal %q != %q", start, end, kv.Key, expKeys[i])
		}
	}
}

func TestEngineScan2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// TODO(Tobias): Merge this with TestEngineScan1 and remove
	// either verifyScan or the other helper function.

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	keys := []MVCCKey{
		mvccKey("a"),
		mvccKey("aa"),
		mvccKey("aaa"),
		mvccKey("ab"),
		mvccKey("abc"),
		mvccKey(roachpb.RKeyMax),
	}

	insertKeys(keys, engine, t)

	// Scan all keys (non-inclusive of final key).
	verifyScan(localMax, roachpb.KeyMax, 10, keys[:5], engine, t)
	verifyScan(roachpb.Key("a"), roachpb.KeyMax, 10, keys[:5], engine, t)

	// Scan sub range.
	verifyScan(roachpb.Key("aab"), roachpb.Key("abcc"), 10, keys[3:5], engine, t)
	verifyScan(roachpb.Key("aa0"), roachpb.Key("abcc"), 10, keys[2:5], engine, t)

	// Scan with max values.
	verifyScan(localMax, roachpb.KeyMax, 3, keys[:3], engine, t)
	verifyScan(roachpb.Key("a0"), roachpb.KeyMax, 3, keys[1:4], engine, t)

	// Scan with max value 0 gets all values.
	verifyScan(localMax, roachpb.KeyMax, 0, keys[:5], engine, t)
}

func TestSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	key := mvccKey("a")
	val1 := []byte("1")
	if err := engine.PutUnversioned(key.Key, val1); err != nil {
		t.Fatal(err)
	}
	val := mvccGetRaw(t, engine, key)
	if !bytes.Equal(val, val1) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			val, val1)
	}

	snap := engine.NewSnapshot()
	defer snap.Close()

	val2 := []byte("2")
	if err := engine.PutUnversioned(key.Key, val2); err != nil {
		t.Fatal(err)
	}
	val = mvccGetRaw(t, engine, key)
	valSnapshot := mvccGetRaw(t, snap, key)
	if !bytes.Equal(val, val2) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			val, val2)
	}
	if !bytes.Equal(valSnapshot, val1) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			valSnapshot, val1)
	}

	keyvals, _ := Scan(context.Background(), engine, key.Key, roachpb.KeyMax, 0)
	keyvalsSnapshot, error := Scan(context.Background(), snap, key.Key, roachpb.KeyMax, 0)
	if error != nil {
		t.Fatalf("error : %s", error)
	}
	if len(keyvals) != 1 || !bytes.Equal(keyvals[0].Value, val2) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			keyvals[0].Value, val2)
	}
	if len(keyvalsSnapshot) != 1 || !bytes.Equal(keyvalsSnapshot[0].Value, val1) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			keyvalsSnapshot[0].Value, val1)
	}
}

// TestSnapshotMethods verifies that snapshots allow only read-only
// engine operations.
func TestSnapshotMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	keys := []MVCCKey{mvccKey("a"), mvccKey("b")}
	vals := [][]byte{[]byte("1"), []byte("2")}
	for i := range keys {
		if err := engine.PutUnversioned(keys[i].Key, vals[i]); err != nil {
			t.Fatal(err)
		}
		val := mvccGetRaw(t, engine, keys[i])
		if !bytes.Equal(vals[i], val) {
			t.Fatalf("expected %s, but found %s", vals[i], val)
		}
	}
	snap := engine.NewSnapshot()
	defer snap.Close()

	// Verify Get.
	for i := range keys {
		valSnapshot := mvccGetRaw(t, snap, keys[i])
		if !bytes.Equal(vals[i], valSnapshot) {
			t.Fatalf("the value %s in get result does not match the value %s in snapshot",
				vals[i], valSnapshot)
		}
	}

	// Verify Scan.
	keyvals, _ := Scan(context.Background(), engine, localMax, roachpb.KeyMax, 0)
	keyvalsSnapshot, err := Scan(context.Background(), snap, localMax, roachpb.KeyMax, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(keyvals, keyvalsSnapshot) {
		t.Fatalf("the key/values %v in scan result does not match the value %s in snapshot",
			keyvals, keyvalsSnapshot)
	}

	// Verify MVCCIterate.
	index := 0
	if err := snap.MVCCIterate(context.Background(), localMax, roachpb.KeyMax,
		MVCCKeyAndIntentsIterKind, IterKeyTypePointsOnly,
		fs.UnknownReadCategory, func(kv MVCCKeyValue, _ MVCCRangeKeyStack) error {
			if !kv.Key.Equal(keys[index]) || !bytes.Equal(kv.Value, vals[index]) {
				t.Errorf("%d: key/value not equal between expected and snapshot: %s/%s, %s/%s",
					index, keys[index], vals[index], kv.Key, kv.Value)
			}
			index++
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	// Write a new key to engine.
	newKey := mvccKey("c")
	newVal := []byte("3")
	if err := engine.PutUnversioned(newKey.Key, newVal); err != nil {
		t.Fatal(err)
	}

	// Verify NewMVCCIterator still iterates over original snapshot.
	iter, err := snap.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
	if err != nil {
		t.Fatal(err)
	}
	iter.SeekGE(newKey)
	if ok, err := iter.Valid(); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Error("expected invalid iterator when seeking to element which shouldn't be visible to snapshot")
	}
	iter.Close()
}

func insertKeys(keys []MVCCKey, engine Engine, t *testing.T) {
	insertKeysAndValues(keys, nil, engine, t)
}

func insertKeysAndValues(keys []MVCCKey, values [][]byte, engine Engine, t *testing.T) {
	// Add keys to store in random order (make sure they sort!).
	order := rand.Perm(len(keys))
	for _, idx := range order {
		var val []byte
		if idx < len(values) {
			val = values[idx]
		} else {
			val = []byte("value")
		}
		if err := engine.PutUnversioned(keys[idx].Key, val); err != nil {
			t.Errorf("put: expected no error, but got %s", err)
		}
	}
}

func TestCreateCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	db, err := Open(
		context.Background(),
		fs.MustInitPhysicalTestingEnv(dir),
		cluster.MakeTestingClusterSettings())
	assert.NoError(t, err)
	defer db.Close()

	checkpointDir := filepath.Join(dir, "checkpoint")

	assert.NoError(t, err)
	assert.NoError(t, db.CreateCheckpoint(checkpointDir, nil))
	assert.DirExists(t, checkpointDir)
	m, err := filepath.Glob(checkpointDir + "/*")
	assert.NoError(t, err)
	assert.True(t, len(m) > 0)

	// Verify that we can open the checkpoint.
	db2, err := Open(
		context.Background(),
		fs.MustInitPhysicalTestingEnv(checkpointDir),
		cluster.MakeTestingClusterSettings(),
		MustExist)
	require.NoError(t, err)
	db2.Close()

	// Verify that creating another checkpoint in the same directory fails.
	if err := db.CreateCheckpoint(checkpointDir, nil); !testutils.IsError(err, "exists") {
		t.Fatal(err)
	}
}

func mustInitTestEnv(t testing.TB, baseFS vfs.FS, dir string) *fs.Env {
	e, err := fs.InitEnv(context.Background(), baseFS, dir, fs.EnvConfig{}, nil /* statsCollector */)
	require.NoError(t, err)
	return e
}

func TestCreateCheckpoint_SpanConstrained(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rng, _ := randutil.NewTestRand()
	key := func(i int) roachpb.Key {
		return keys.SystemSQLCodec.TablePrefix(uint32(i))
	}

	mem := vfs.NewMem()
	dir := "foo"
	db, err := Open(
		ctx,
		mustInitTestEnv(t, mem, dir),
		cluster.MakeTestingClusterSettings(),
		TargetFileSize(2<<10 /* 2 KB */),
	)
	assert.NoError(t, err)
	defer db.Close()

	// Write keys /Table/1/../Table/10000.
	// 10,000 * 100 byte KVs = ~1MB.
	b := db.NewWriteBatch()
	const maxTableID = 10000
	for i := 1; i <= maxTableID; i++ {
		require.NoError(t, b.PutMVCC(
			MVCCKey{Key: key(i), Timestamp: hlc.Timestamp{WallTime: int64(i)}},
			MVCCValue{Value: roachpb.Value{RawBytes: randutil.RandBytes(rng, 100)}},
		))
	}
	require.NoError(t, b.Commit(true /* sync */))
	require.NoError(t, db.Flush())

	sstables, err := db.db.SSTables()
	require.NoError(t, err)
	for _, tbls := range sstables {
		for _, tbl := range tbls {
			t.Logf("%s: %s-%s", tbl.FileNum, tbl.Smallest, tbl.Largest)
		}
	}

	checkpointRootDir := mem.PathJoin(dir, "checkpoint")
	require.NoError(t, db.Env().MkdirAll(checkpointRootDir, os.ModePerm))

	var checkpointNum int
	checkpointSpan := func(s roachpb.Span) string {
		checkpointNum++
		dir := mem.PathJoin(checkpointRootDir, fmt.Sprintf("%06d", checkpointNum))
		t.Logf("Writing checkpoint for span %s to %q", s, dir)
		assert.NoError(t, db.CreateCheckpoint(dir, []roachpb.Span{s}))
		// Check that the dir exists.
		_, err := mem.Stat(dir)
		assert.NoError(t, err)
		m, err := mem.List(dir)
		assert.NoError(t, err)
		assert.True(t, len(m) > 0)
		t.Logf("Checkpoint wrote files: %s", strings.Join(m, ", "))
		return dir
	}
	verifyCheckpoint := func(dir string, low, high int) {
		t.Logf("Verifying checkpoint for span [%d,%d) in %q", low, high, dir)
		// Verify that we can open the checkpoint.
		cDB, err := Open(
			ctx,
			mustInitTestEnv(t, mem, dir),
			cluster.MakeTestingClusterSettings(),
			MustExist)
		require.NoError(t, err)
		defer cDB.Close()

		iter, err := cDB.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
			LowerBound: key(low),
			UpperBound: key(high),
		})
		require.NoError(t, err)
		defer iter.Close()
		iter.SeekGE(MVCCKey{Key: key(low)})
		count := 0
		for {
			if valid, err := iter.Valid(); !valid {
				require.NoError(t, err)
				break
			}
			count++
			iter.Next()
		}
		require.Equal(t, count, high-low)
	}

	for i := 0; i < 10; i++ {
		start := randutil.RandIntInRange(rng, 1, maxTableID)
		end := randutil.RandIntInRange(rng, 1, maxTableID)
		for start == end {
			end = randutil.RandIntInRange(rng, 1, maxTableID)
		}
		if start > end {
			start, end = end, start
		}

		span := roachpb.Span{Key: key(start), EndKey: key(end)}
		dir := checkpointSpan(span)
		verifyCheckpoint(dir, start, end)
	}
}

func TestIngestDelayLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := cluster.MakeTestingClusterSettings()

	max, ramp := time.Second*5, time.Second*5/10

	for _, tc := range []struct {
		exp           time.Duration
		fileCount     int64
		sublevelCount int32
	}{
		{0, 0, 0},
		{0, 19, -1},
		{0, 20, -1},
		{ramp, 21, -1},
		{ramp * 2, 22, -1},
		{ramp * 2, 22, 22},
		{ramp * 2, 55, 22},
		{max, 55, -1},
	} {
		var m pebble.Metrics
		m.Levels[0].NumFiles = tc.fileCount
		m.Levels[0].Sublevels = tc.sublevelCount
		require.Equal(t, tc.exp, calculatePreIngestDelay(s, &m))
	}
}

func TestEngineFS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engine := NewDefaultInMemForTesting()
	defer engine.Close()
	e := engine.Env()

	testCases := []string{
		"1a: f = create /bar",
		"1b: f.write abcdefghijklmnopqrstuvwxyz",
		"1c: f.close",
		"2a: f = open /bar",
		"2b: f.read 5 == abcde",
		"2c: f.readat 2 1 == bc",
		"2d: f.readat 5 20 == uvwxy",
		"2e: f.close",
		"3a: link /bar /baz",
		"3b: f = open /baz",
		"3c: f.read 5 == abcde",
		"3d: f.close",
		"4a: delete /bar",
		"4b: f = open /baz",
		"4c: f.read 5 == abcde",
		"4d: f.close",
		"4e: open /bar [does-not-exist]",
		"5a: rename /baz /foo",
		"5b: f = open /foo",
		"5c: f.readat 5 20 == uvwxy",
		"5d: f.close",
		"5e: open /baz [does-not-exist]",
		"6a: f = create /red",
		"6b: f.write blue",
		"6c: f.sync",
		"6d: f.close",
		"7a: f = opendir /",
		"7b: f.sync",
		"7c: f.close",
		"8a: f = create-with-sync /bar",
		"8b: f.write ghe",
		"8c: f.close",
		"8d: f = open /bar",
		"8e: f.read 3 == ghe",
		"8f: f.close",
		"9a: create-dir /dir1",
		"9b: create /dir1/bar",
		"9c: list-dir /dir1 == bar",
		"9d: create /dir1/baz",
		"9e: list-dir /dir1 == bar,baz",
		"9f: delete /dir1/bar",
		"9g: delete /dir1/baz",
		"9h: delete /dir1",
	}

	var f vfs.File
	for _, tc := range testCases {
		s := strings.Split(tc, " ")[1:]

		saveF := s[0] == "f" && s[1] == "="
		if saveF {
			s = s[2:]
		}

		fails := s[len(s)-1][0] == '['
		var errorStr string
		if fails {
			errorStr = s[len(s)-1][1:]
			errorStr = errorStr[:len(errorStr)-1]
			s = s[:len(s)-1]
		}

		var (
			g   vfs.File
			err error
		)
		switch s[0] {
		case "create":
			g, err = e.Create(s[1], fs.UnspecifiedWriteCategory)
		case "create-with-sync":
			g, err = fs.CreateWithSync(e, s[1], 1, fs.UnspecifiedWriteCategory)
		case "link":
			err = e.Link(s[1], s[2])
		case "open":
			g, err = e.Open(s[1])
		case "opendir":
			g, err = e.OpenDir(s[1])
		case "delete":
			err = e.Remove(s[1])
		case "rename":
			err = e.Rename(s[1], s[2])
		case "create-dir":
			err = e.MkdirAll(s[1], os.ModePerm)
		case "list-dir":
			result, err := e.List(s[1])
			if err != nil {
				break
			}
			sort.Strings(result)
			got := strings.Join(result, ",")
			want := s[3]
			if got != want {
				t.Fatalf("%q: got %s, want %s", tc, got, want)
			}
		case "f.write":
			_, err = f.Write([]byte(s[1]))
		case "f.read":
			n, _ := strconv.Atoi(s[1])
			buf := make([]byte, n)
			_, err = io.ReadFull(f, buf)
			if err != nil {
				break
			}
			if got, want := string(buf), s[3]; got != want {
				t.Fatalf("%q: got %q, want %q", tc, got, want)
			}
		case "f.readat":
			n, _ := strconv.Atoi(s[1])
			off, _ := strconv.Atoi(s[2])
			buf := make([]byte, n)
			_, err = f.ReadAt(buf, int64(off))
			if err != nil {
				break
			}
			if got, want := string(buf), s[4]; got != want {
				t.Fatalf("%q: got %q, want %q", tc, got, want)
			}
		case "f.close":
			f, err = nil, f.Close()
		case "f.sync":
			err = f.Sync()
		default:
			t.Fatalf("bad test case: %q", tc)
		}

		if saveF {
			f, g = g, nil
		} else if g != nil {
			g.Close()
		}

		if fails {
			if err == nil {
				t.Fatalf("%q: got nil error, want non-nil %s", tc, errorStr)
			}
			var actualErrStr string
			if oserror.IsExist(err) {
				actualErrStr = "exists"
			} else if oserror.IsNotExist(err) {
				actualErrStr = "does-not-exist"
			} else {
				actualErrStr = "error"
			}
			if errorStr != actualErrStr {
				t.Fatalf("%q: got %s, want %s", tc, actualErrStr, errorStr)
			}
		} else {
			if err != nil {
				t.Fatalf("%q: %v", tc, err)
			}
		}
	}
}

func TestEngineFSFileNotFoundError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()
	db, err := Open(context.Background(), fs.MustInitPhysicalTestingEnv(dir), cluster.MakeClusterSettings(), CacheSize(testCacheSize))
	require.NoError(t, err)
	defer db.Close()
	env := db.Env()

	// Verify Remove returns os.ErrNotExist if file does not exist.
	if err := env.Remove("/non/existent/file"); !oserror.IsNotExist(err) {
		t.Fatalf("expected IsNotExist, but got %v (%T)", err, err)
	}
	// Verify RemoveAll returns nil if path does not exist.
	if err := env.RemoveAll("/non/existent/file"); err != nil {
		t.Fatalf("expected nil, but got %v (%T)", err, err)
	}

	fname := filepath.Join(dir, "random.file")
	data := "random data"
	if f, err := env.Create(fname, fs.UnspecifiedWriteCategory); err != nil {
		t.Fatalf("unable to open file with filename %s, got err %v", fname, err)
	} else {
		// Write data to file so we can read it later.
		if _, err := f.Write([]byte(data)); err != nil {
			t.Fatalf("error writing data: '%s' to file %s, got err %v", data, fname, err)
		}
		if err := f.Sync(); err != nil {
			t.Fatalf("error syncing data, got err %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("error closing file %s, got err %v", fname, err)
		}
	}

	if b, err := fs.ReadFile(env, fname); err != nil {
		t.Errorf("unable to read file with filename %s, got err %v", fname, err)
	} else if string(b) != data {
		t.Errorf("expected content in %s is '%s', got '%s'", fname, data, string(b))
	}

	if err := env.Remove(fname); err != nil {
		t.Errorf("unable to delete file with filename %s, got err %v", fname, err)
	}

	// Verify ReadFile returns os.ErrNotExist if reading an already deleted file.
	if _, err := fs.ReadFile(env, fname); !oserror.IsNotExist(err) {
		t.Fatalf("expected IsNotExist, but got %v (%T)", err, err)
	}

	// Verify Remove returns os.ErrNotExist if deleting an already deleted file.
	if err := env.Remove(fname); !oserror.IsNotExist(err) {
		t.Fatalf("expected IsNotExist, but got %v (%T)", err, err)
	}
}

func TestFS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	engineDest := map[string]*fs.Env{
		"in_memory":  InMemory(),
		"filesystem": fs.MustInitPhysicalTestingEnv(dir),
	}
	for name, loc := range engineDest {
		t.Run(name, func(t *testing.T) {
			engine, err := Open(context.Background(), loc, cluster.MakeClusterSettings(), CacheSize(testCacheSize), ForTesting)
			require.NoError(t, err)
			defer engine.Close()
			e := engine.Env()

			path := func(rel string) string {
				return filepath.Join(dir, rel)
			}
			expectLS := func(dir string, want []string) {
				t.Helper()

				got, err := e.List(dir)
				sort.Strings(got)
				require.NoError(t, err)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("e.List(%q) = %#v, want %#v", dir, got, want)
				}
			}

			// Create a/ and assert that it's empty.
			require.NoError(t, e.MkdirAll(path("a"), os.ModePerm))
			expectLS(path("a"), []string{})
			if _, err := e.Stat(path("a/b/c")); !oserror.IsNotExist(err) {
				t.Fatal(`e.Stat("a/b/c") should not exist`)
			}

			// Create a/b/ and a/b/c/ in a single MkdirAll call.
			// Then ensure that a duplicate call returns a nil error.
			require.NoError(t, e.MkdirAll(path("a/b/c"), os.ModePerm))
			require.NoError(t, e.MkdirAll(path("a/b/c"), os.ModePerm))
			expectLS(path("a"), []string{"b"})
			expectLS(path("a/b"), []string{"c"})
			expectLS(path("a/b/c"), []string{})
			_, err = e.Stat(path("a/b/c"))
			require.NoError(t, err)

			// Create a file at a/b/c/foo.
			f, err := e.Create(path("a/b/c/foo"), fs.UnspecifiedWriteCategory)
			require.NoError(t, err)
			require.NoError(t, f.Close())
			expectLS(path("a/b/c"), []string{"foo"})

			// Create a file at a/b/c/bar.
			f, err = e.Create(path("a/b/c/bar"), fs.UnspecifiedWriteCategory)
			require.NoError(t, err)
			require.NoError(t, f.Close())
			expectLS(path("a/b/c"), []string{"bar", "foo"})
			_, err = e.Stat(path("a/b/c/bar"))
			require.NoError(t, err)

			// RemoveAll a file.
			require.NoError(t, e.RemoveAll(path("a/b/c/bar")))
			expectLS(path("a/b/c"), []string{"foo"})

			// RemoveAll a directory that contains subdirectories and
			// descendant files.
			require.NoError(t, e.RemoveAll(path("a/b")))
			expectLS(path("a"), []string{})
		})
	}
}

func TestGetIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	eng, err := Open(ctx, InMemory(), cluster.MakeClusterSettings(), CacheSize(1<<20 /* 1 MiB */))
	require.NoError(t, err)
	defer eng.Close()

	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	keyD, keyE, keyF := roachpb.Key("d"), roachpb.Key("e"), roachpb.Key("f")
	val := stringValue("val").Value

	txn1ID := uuid.MakeV4()
	txn1TS := hlc.Timestamp{Logical: 1}
	txn1 := &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: keyA, ID: txn1ID, Epoch: 1, WriteTimestamp: txn1TS, MinTimestamp: txn1TS, CoordinatorNodeID: 1}, ReadTimestamp: txn1TS}
	txn2ID := uuid.MakeV4()
	txn2TS := hlc.Timestamp{Logical: 2}
	txn2 := &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: keyA, ID: txn2ID, Epoch: 2, WriteTimestamp: txn2TS, MinTimestamp: txn2TS, CoordinatorNodeID: 2}, ReadTimestamp: txn2TS}

	// Key "a" only has an intent from txn1.
	_, err = MVCCPut(ctx, eng, keyA, txn1.ReadTimestamp, val, MVCCWriteOptions{Txn: txn1})
	require.NoError(t, err)

	// Key "b" has an intent, an exclusive lock, and a shared lock from txn1.
	// NOTE: acquire in increasing strength order so that acquisition is never
	// skipped.
	err = MVCCAcquireLock(ctx, eng, &txn1.TxnMeta, txn1.IgnoredSeqNums, lock.Shared, keyB, nil, 0, 0)
	require.NoError(t, err)
	err = MVCCAcquireLock(ctx, eng, &txn1.TxnMeta, txn1.IgnoredSeqNums, lock.Exclusive, keyB, nil, 0, 0)
	require.NoError(t, err)
	_, err = MVCCPut(ctx, eng, keyB, txn1.ReadTimestamp, val, MVCCWriteOptions{Txn: txn1})
	require.NoError(t, err)

	// Key "c" only has an intent from txn2.
	_, err = MVCCPut(ctx, eng, keyC, txn2.ReadTimestamp, val, MVCCWriteOptions{Txn: txn2})
	require.NoError(t, err)

	// Key "d" has an exclusive lock and a shared lock from txn2.
	err = MVCCAcquireLock(ctx, eng, &txn2.TxnMeta, txn2.IgnoredSeqNums, lock.Shared, keyD, nil, 0, 0)
	require.NoError(t, err)
	err = MVCCAcquireLock(ctx, eng, &txn2.TxnMeta, txn2.IgnoredSeqNums, lock.Exclusive, keyD, nil, 0, 0)
	require.NoError(t, err)

	// Key "e" has a shared lock from each txn.
	err = MVCCAcquireLock(ctx, eng, &txn1.TxnMeta, txn1.IgnoredSeqNums, lock.Shared, keyE, nil, 0, 0)
	require.NoError(t, err)
	err = MVCCAcquireLock(ctx, eng, &txn2.TxnMeta, txn2.IgnoredSeqNums, lock.Shared, keyE, nil, 0, 0)
	require.NoError(t, err)

	// Key "f" has no intent/locks.

	tests := []struct {
		key         roachpb.Key
		expErr      bool
		expFound    bool
		expFoundTxn *roachpb.Transaction
	}{
		{key: keyA, expErr: false, expFound: true, expFoundTxn: txn1},
		{key: keyB, expErr: false, expFound: true, expFoundTxn: txn1},
		{key: keyC, expErr: false, expFound: true, expFoundTxn: txn2},
		{key: keyD, expErr: false, expFound: false, expFoundTxn: nil},
		{key: keyE, expErr: false, expFound: false, expFoundTxn: nil},
		{key: keyF, expErr: false, expFound: false, expFoundTxn: nil},
	}
	for _, test := range tests {
		t.Run(string(test.key), func(t *testing.T) {
			intent, err := GetIntent(ctx, eng, test.key)
			if test.expErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if test.expFound {
					require.NotNil(t, intent)
					require.Equal(t, test.key, intent.Key)
					require.Equal(t, test.expFoundTxn.TxnMeta, intent.Txn)
				} else {
					require.Nil(t, intent)
				}
			}
		})
	}
}

func TestScanLocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	maxKey := keys.MaxKey

	locks := map[string]lock.Strength{
		"a": lock.Shared,
		"b": lock.Exclusive,
		"c": lock.Intent,
	}
	var keys []roachpb.Key
	for k := range locks {
		keys = append(keys, roachpb.Key(k))
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })

	testcases := map[string]struct {
		from        roachpb.Key
		to          roachpb.Key
		max         int64
		targetBytes int64
		expectLocks []roachpb.Key
	}{
		"no keys":         {keys[0], keys[0], 0, 0, keys[:0]},
		"one key":         {keys[0], keys[1], 0, 0, keys[0:1]},
		"two keys":        {keys[0], keys[2], 0, 0, keys[0:2]},
		"all keys":        {keys[0], maxKey, 0, 0, keys},
		"offset mid":      {keys[1], keys[2], 0, 0, keys[1:2]},
		"offset last":     {keys[2], maxKey, 0, 0, keys[2:]},
		"offset post":     {roachpb.Key("x"), maxKey, 0, 0, []roachpb.Key{}},
		"nil end":         {keys[0], nil, 0, 0, []roachpb.Key{}},
		"-1 max":          {keys[0], maxKey, -1, 0, keys[:0]},
		"2 max":           {keys[0], maxKey, 2, 0, keys[0:2]},
		"-1 byte":         {keys[0], maxKey, 0, -1, keys[:0]},
		"1 byte":          {keys[0], maxKey, 0, 1, keys[0:1]},
		"80 bytes":        {keys[0], maxKey, 0, 80, keys[0:2]},
		"80 bytes or one": {keys[0], maxKey, 1, 80, keys[0:1]},
		"1000 bytes":      {keys[0], maxKey, 0, 1000, keys},
	}

	eng, err := Open(ctx, InMemory(), cluster.MakeClusterSettings(), CacheSize(1<<20 /* 1 MiB */))
	require.NoError(t, err)
	defer eng.Close()

	for k, str := range locks {
		var err error
		if str == lock.Intent {
			_, err = MVCCPut(ctx, eng, roachpb.Key(k), txn1.ReadTimestamp, roachpb.Value{RawBytes: roachpb.Key(k)}, MVCCWriteOptions{Txn: txn1})
		} else {
			err = MVCCAcquireLock(ctx, eng, &txn1.TxnMeta, txn1.IgnoredSeqNums, str, roachpb.Key(k), nil, 0, 0)
		}
		require.NoError(t, err)
	}

	for name, tc := range testcases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			scannedLocks, err := ScanLocks(ctx, eng, tc.from, tc.to, tc.max, tc.targetBytes)
			require.NoError(t, err)
			require.Len(t, scannedLocks, len(tc.expectLocks), "unexpected number of locks")
			for i, l := range scannedLocks {
				require.Equal(t, tc.expectLocks[i], l.Key)
				require.Equal(t, txn1.TxnMeta, l.Txn)
				require.Equal(t, locks[string(l.Key)], l.Strength)
			}
		})
	}
}

// TestEngineClearRange tests Clear*Range methods and related helpers.
func TestEngineClearRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Set up initial dataset, where [b-g) will be cleared.
	// [] is intent, o---o is MVCC range tombstone.
	//
	// 6  [a6][b6]       [e6]     [g6]
	// 5   a5  b5  c5
	// 4   o-------------------o   o---o
	// 3                  e3
	// 2   o-------------------o   g2
	// 1           c1  o---------------o
	//     a   b   c   d   e   f   g   h
	//
	// After a complete clear, the remaining state will be:
	//
	// 6  [a6]                    [g6]
	// 5   a5
	// 4   o---o                   o---o
	// 3
	// 2   o---o                   g2
	// 1           								 o---o
	//     a   b   c   d   e   f   g   h
	//
	// However, certain clearers cannot clear intents, range keys, or point keys.
	writeInitialData := func(t *testing.T, rw ReadWriter) {
		txn := roachpb.MakeTransaction("test", nil, isolation.Serializable, roachpb.NormalUserPriority, wallTS(6), 1, 1, 0, false /* omitInRangefeeds */)
		_, err := MVCCPut(ctx, rw, roachpb.Key("c"), wallTS(1), stringValue("c1").Value, MVCCWriteOptions{})
		require.NoError(t, err)
		require.NoError(t, rw.PutMVCCRangeKey(rangeKey("d", "h", 1), MVCCValue{}))
		require.NoError(t, rw.PutMVCCRangeKey(rangeKey("a", "f", 2), MVCCValue{}))
		_, err = MVCCPut(ctx, rw, roachpb.Key("g"), wallTS(2), stringValue("g2").Value, MVCCWriteOptions{})
		require.NoError(t, err)
		_, err = MVCCPut(ctx, rw, roachpb.Key("e"), wallTS(3), stringValue("e3").Value, MVCCWriteOptions{})
		require.NoError(t, err)
		require.NoError(t, rw.PutMVCCRangeKey(rangeKey("a", "f", 4), MVCCValue{}))
		require.NoError(t, rw.PutMVCCRangeKey(rangeKey("g", "h", 4), MVCCValue{}))
		_, err = MVCCPut(ctx, rw, roachpb.Key("a"), wallTS(5), stringValue("a2").Value, MVCCWriteOptions{})
		require.NoError(t, err)
		_, err = MVCCPut(ctx, rw, roachpb.Key("b"), wallTS(5), stringValue("b2").Value, MVCCWriteOptions{})
		require.NoError(t, err)
		_, err = MVCCPut(ctx, rw, roachpb.Key("c"), wallTS(5), stringValue("c2").Value, MVCCWriteOptions{})
		require.NoError(t, err)
		_, err = MVCCPut(ctx, rw, roachpb.Key("a"), wallTS(6), stringValue("a6").Value, MVCCWriteOptions{Txn: &txn})
		require.NoError(t, err)
		_, err = MVCCPut(ctx, rw, roachpb.Key("b"), wallTS(6), stringValue("b6").Value, MVCCWriteOptions{Txn: &txn})
		require.NoError(t, err)
		_, err = MVCCPut(ctx, rw, roachpb.Key("e"), wallTS(6), stringValue("e6").Value, MVCCWriteOptions{Txn: &txn})
		require.NoError(t, err)
		_, err = MVCCPut(ctx, rw, roachpb.Key("g"), wallTS(6), stringValue("g6").Value, MVCCWriteOptions{Txn: &txn})
		require.NoError(t, err)
	}
	start, end := roachpb.Key("b"), roachpb.Key("g")

	testcases := map[string]struct {
		clearRange      func(ReadWriter, roachpb.Key, roachpb.Key) error
		clearsIntents   bool
		clearsPointKeys bool
		clearsRangeKeys bool
	}{
		"ClearRawRange": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return rw.ClearRawRange(start, end, true /* pointKeys */, true /* rangeKeys */)
			},
			clearsPointKeys: true,
			clearsRangeKeys: true,
			clearsIntents:   false,
		},
		"ClearRawRange point keys": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return rw.ClearRawRange(start, end, true /* pointKeys */, false /* rangeKeys */)
			},
			clearsPointKeys: true,
			clearsRangeKeys: false,
			clearsIntents:   false,
		},
		"ClearRawRange range keys": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return rw.ClearRawRange(start, end, false /* pointKeys */, true /* rangeKeys */)
			},
			clearsPointKeys: false,
			clearsRangeKeys: true,
			clearsIntents:   false,
		},

		"ClearMVCCRange": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return rw.ClearMVCCRange(start, end, true /* pointKeys */, true /* rangeKeys */)
			},
			clearsPointKeys: true,
			clearsRangeKeys: true,
			clearsIntents:   true,
		},
		"ClearMVCCRange point keys": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return rw.ClearMVCCRange(start, end, true /* pointKeys */, false /* rangeKeys */)
			},
			clearsPointKeys: true,
			clearsRangeKeys: false,
			clearsIntents:   true,
		},
		"ClearMVCCRange range keys": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return rw.ClearMVCCRange(start, end, false /* pointKeys */, true /* rangeKeys */)
			},
			clearsPointKeys: false,
			clearsRangeKeys: true,
			clearsIntents:   false,
		},

		"ClearMVCCIteratorRange": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return rw.ClearMVCCIteratorRange(start, end, true /* pointKeys */, true /* rangeKeys */)
			},
			clearsPointKeys: true,
			clearsRangeKeys: true,
			clearsIntents:   true,
		},
		"ClearMVCCIteratorRange point keys": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return rw.ClearMVCCIteratorRange(start, end, true /* pointKeys */, false /* rangeKeys */)
			},
			clearsPointKeys: true,
			clearsRangeKeys: false,
			clearsIntents:   true,
		},

		"ClearMVCCIteratorRange range keys": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return rw.ClearMVCCIteratorRange(start, end, false /* pointKeys */, true /* rangeKeys */)
			},
			clearsPointKeys: false,
			clearsRangeKeys: true,
			clearsIntents:   false,
		},

		"ClearMVCCVersions": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return rw.ClearMVCCVersions(MVCCKey{Key: start}, MVCCKey{Key: end})
			},
			clearsPointKeys: true,
			clearsRangeKeys: false,
			clearsIntents:   false,
		},

		"ClearRangeWithHeuristic individual": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return ClearRangeWithHeuristic(ctx, rw, rw, start, end, math.MaxInt, math.MaxInt)
			},
			clearsPointKeys: true,
			clearsRangeKeys: true,
			clearsIntents:   false,
		},
		"ClearRangeWithHeuristic ranged": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return ClearRangeWithHeuristic(ctx, rw, rw, start, end, 1, 1)
			},
			clearsPointKeys: true,
			clearsRangeKeys: true,
			clearsIntents:   false,
		},
		"ClearRangeWithHeuristic point keys individual": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return ClearRangeWithHeuristic(ctx, rw, rw, start, end, math.MaxInt, 0)
			},
			clearsPointKeys: true,
			clearsRangeKeys: false,
			clearsIntents:   false,
		},
		"ClearRangeWithHeuristic point keys ranged": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return ClearRangeWithHeuristic(ctx, rw, rw, start, end, 1, 0)
			},
			clearsPointKeys: true,
			clearsRangeKeys: false,
			clearsIntents:   false,
		},
		"ClearRangeWithHeuristic range keys individual": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return ClearRangeWithHeuristic(ctx, rw, rw, start, end, 0, math.MaxInt)
			},
			clearsPointKeys: false,
			clearsRangeKeys: true,
			clearsIntents:   false,
		},
		"ClearRangeWithHeuristic range keys ranged": {
			clearRange: func(rw ReadWriter, start, end roachpb.Key) error {
				return ClearRangeWithHeuristic(ctx, rw, rw, start, end, 0, 1)
			},
			clearsPointKeys: false,
			clearsRangeKeys: true,
			clearsIntents:   false,
		},
	}
	testutils.RunTrueAndFalse(t, "batch", func(t *testing.T, useBatch bool) {
		for name, tc := range testcases {
			t.Run(name, func(t *testing.T) {
				eng := NewDefaultInMemForTesting()
				defer eng.Close()
				writeInitialData(t, eng)

				rw := ReadWriter(eng)
				if useBatch {
					batch := eng.NewBatch()
					defer batch.Close()
					rw = batch
				}

				require.NoError(t, tc.clearRange(rw, start, end))

				// Check point key clears. We'll find provisional values for the intents.
				if tc.clearsPointKeys {
					require.Equal(t, []MVCCKey{
						pointKey("a", 6), pointKey("a", 5), pointKey("g", 6), pointKey("g", 2),
					}, scanPointKeys(t, rw))
				} else {
					require.Equal(t, []MVCCKey{
						pointKey("a", 6), pointKey("a", 5),
						pointKey("b", 6), pointKey("b", 5),
						pointKey("c", 5), pointKey("c", 1),
						pointKey("e", 6), pointKey("e", 3),
						pointKey("g", 6), pointKey("g", 2),
					}, scanPointKeys(t, rw))
				}

				// Check intent clears.
				if tc.clearsIntents {
					require.Equal(t, []roachpb.Key{roachpb.Key("a"), roachpb.Key("g")}, scanLockKeys(t, rw))
				} else {
					require.Equal(t, []roachpb.Key{
						roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("e"), roachpb.Key("g"),
					}, scanLockKeys(t, rw))
				}

				// Which range keys we find will depend on the clearer.
				if tc.clearsRangeKeys {
					require.Equal(t, []MVCCRangeKeyValue{
						rangeKV("a", "b", 4, MVCCValue{}),
						rangeKV("a", "b", 2, MVCCValue{}),
						rangeKV("g", "h", 4, MVCCValue{}),
						rangeKV("g", "h", 1, MVCCValue{}),
					}, scanRangeKeys(t, rw))
				} else {
					require.Equal(t, []MVCCRangeKeyValue{
						rangeKV("a", "d", 4, MVCCValue{}),
						rangeKV("a", "d", 2, MVCCValue{}),
						rangeKV("d", "f", 4, MVCCValue{}),
						rangeKV("d", "f", 2, MVCCValue{}),
						rangeKV("d", "f", 1, MVCCValue{}),
						rangeKV("f", "g", 1, MVCCValue{}),
						rangeKV("g", "h", 4, MVCCValue{}),
						rangeKV("g", "h", 1, MVCCValue{}),
					}, scanRangeKeys(t, rw))
				}
			})
		}
	})
}

// TestEngineIteratorVisibility checks iterator visibility for various readers.
// See comment on Engine.NewMVCCIterator for detailed visibility semantics.
func TestEngineIteratorVisibility(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testcases := map[string]struct {
		makeReader       func(Engine) Reader
		expectConsistent bool
		canWrite         bool
		readOwnWrites    bool
	}{
		"Engine": {
			makeReader:       func(e Engine) Reader { return e },
			expectConsistent: false,
			canWrite:         true,
			readOwnWrites:    true,
		},
		"Batch": {
			makeReader:       func(e Engine) Reader { return e.NewBatch() },
			expectConsistent: true,
			canWrite:         true,
			readOwnWrites:    true,
		},
		"UnindexedBatch": {
			makeReader:       func(e Engine) Reader { return e.NewUnindexedBatch() },
			expectConsistent: true,
			canWrite:         true,
			readOwnWrites:    false,
		},
		"Reader": {
			makeReader:       func(e Engine) Reader { return e.NewReader(StandardDurability) },
			expectConsistent: true,
			canWrite:         false,
		},
		"ReadOnly": {
			makeReader:       func(e Engine) Reader { return e.NewReadOnly(StandardDurability) },
			expectConsistent: true,
			canWrite:         false,
		},
		"Snapshot": {
			makeReader:       func(e Engine) Reader { return e.NewSnapshot() },
			expectConsistent: true,
			canWrite:         false,
		},
	}
	keyKinds := []MVCCIterKind{MVCCKeyAndIntentsIterKind, MVCCKeyIterKind}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			testutils.RunValues(t, "IterKind", keyKinds, func(t *testing.T, iterKind MVCCIterKind) {
				eng := NewDefaultInMemForTesting()
				defer eng.Close()

				// Write initial point and range keys.
				require.NoError(t, eng.PutMVCC(pointKey("a", 1), stringValue("a1")))
				require.NoError(t, eng.PutMVCCRangeKey(rangeKey("b", "c", 1), MVCCValue{}))

				// Set up two readers: one regular and one which will be pinned.
				r := tc.makeReader(eng)
				defer r.Close()
				rPinned := tc.makeReader(eng)
				defer rPinned.Close()

				require.Equal(t, tc.expectConsistent, r.ConsistentIterators())

				// Create an iterator. This will see the old engine state regardless
				// of the type of reader.
				opts := IterOptions{
					KeyTypes:   IterKeyTypePointsAndRanges,
					LowerBound: keys.LocalMax,
					UpperBound: keys.MaxKey,
				}
				iterOld, err := r.NewMVCCIterator(context.Background(), iterKind, opts)
				if err != nil {
					t.Fatal(err)
				}
				defer iterOld.Close()

				// Pin the pinned reader, if it supports it. This should ensure later
				// iterators see the current state.
				if rPinned.ConsistentIterators() {
					require.NoError(t, rPinned.PinEngineStateForIterators(fs.UnknownReadCategory))
				} else {
					require.Error(t, rPinned.PinEngineStateForIterators(fs.UnknownReadCategory))
				}

				// Write a new key to the engine, and set up the expected results.
				require.NoError(t, eng.PutMVCC(pointKey("a", 2), stringValue("a2")))
				require.NoError(t, eng.PutMVCCRangeKey(rangeKey("b", "c", 2), MVCCValue{}))

				expectOld := []interface{}{
					pointKV("a", 1, "a1"),
					rangeKV("b", "c", 1, MVCCValue{}),
				}
				expectNew := []interface{}{
					pointKV("a", 2, "a2"),
					pointKV("a", 1, "a1"),
					rangeKV("b", "c", 2, MVCCValue{}),
					rangeKV("b", "c", 1, MVCCValue{}),
				}

				// The existing (old) iterator should all see the old engine state,
				// regardless of reader type.
				require.Equal(t, expectOld, scanIter(t, iterOld))

				// Create another iterator from the regular reader. Consistent iterators
				// should see the old state (because iterOld was already created for
				// it), others should see the new state.
				iterNew, err := r.NewMVCCIterator(context.Background(), iterKind, opts)
				if err != nil {
					t.Fatal(err)
				}
				defer iterNew.Close()
				if r.ConsistentIterators() {
					require.Equal(t, expectOld, scanIter(t, iterNew))
				} else {
					require.Equal(t, expectNew, scanIter(t, iterNew))
				}

				// Create a new iterator from the pinned reader. Readers with consistent
				// iterators should see the old (pinned) state, others should see the
				// new state.
				iterPinned, err := rPinned.NewMVCCIterator(context.Background(), iterKind, opts)
				if err != nil {
					t.Fatal(err)
				}
				defer iterPinned.Close()
				if rPinned.ConsistentIterators() {
					require.Equal(t, expectOld, scanIter(t, iterPinned))
				} else {
					require.Equal(t, expectNew, scanIter(t, iterPinned))
				}

				// If the reader is also a writer, check interactions with writes.
				// In particular, a Batch should read its own writes for any new
				// iterators, but not for any existing iterators.
				if tc.canWrite {
					w, ok := r.(Writer)
					require.Equal(t, tc.canWrite, ok)

					// Write a new point and range key to the writer (not engine), and set
					// up expected results.
					require.NoError(t, w.PutMVCC(pointKey("a", 3), stringValue("a3")))
					require.NoError(t, w.PutMVCCRangeKey(rangeKey("b", "c", 3), MVCCValue{}))
					expectNewAndOwn := []interface{}{
						pointKV("a", 3, "a3"),
						pointKV("a", 2, "a2"),
						pointKV("a", 1, "a1"),
						rangeKV("b", "c", 3, MVCCValue{}),
						rangeKV("b", "c", 2, MVCCValue{}),
						rangeKV("b", "c", 1, MVCCValue{}),
					}
					expectOldAndOwn := []interface{}{
						pointKV("a", 3, "a3"),
						pointKV("a", 1, "a1"),
						rangeKV("b", "c", 3, MVCCValue{}),
						rangeKV("b", "c", 1, MVCCValue{}),
					}

					// The existing iterators should see the same state as before these
					// writes, because they always have a consistent view from when they
					// were created.
					require.Equal(t, expectOld, scanIter(t, iterOld))
					if r.ConsistentIterators() {
						require.Equal(t, expectOld, scanIter(t, iterNew))
						require.Equal(t, expectOld, scanIter(t, iterPinned))
					} else {
						require.Equal(t, expectNew, scanIter(t, iterNew))
						require.Equal(t, expectNew, scanIter(t, iterPinned))
					}

					// A new iterator should read our own writes if the reader supports it,
					// but consistent iterators should not see the changes to the underlying
					// engine either way.
					iterOwn, err := r.NewMVCCIterator(context.Background(), iterKind, opts)
					if err != nil {
						t.Fatal(err)
					}
					defer iterOwn.Close()
					if tc.readOwnWrites {
						if r.ConsistentIterators() {
							require.Equal(t, expectOldAndOwn, scanIter(t, iterOwn))
						} else {
							require.Equal(t, expectNewAndOwn, scanIter(t, iterOwn))
						}
					} else {
						if r.ConsistentIterators() {
							require.Equal(t, expectOld, scanIter(t, iterOwn))
						} else {
							require.Equal(t, expectNew, scanIter(t, iterOwn))
						}
					}
				}
			})
		})
	}
}

// TestScanConflictingIntentsForDroppingLatchesEarly tests
// ScanConflictingIntentsForDroppingLatchesEarly for all non read-your-own-write
// cases. Read-your-own-write cases are tested separately in
// TestScanConflictingIntentsForDroppingLatchesEarlyReadYourOwnWrites.
func TestScanConflictingIntentsForDroppingLatchesEarly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	ts := func(nanos int) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: int64(nanos),
		}
	}
	newTxn := func(ts hlc.Timestamp) *roachpb.Transaction {
		txnID := uuid.MakeV4()
		return &roachpb.Transaction{
			TxnMeta: enginepb.TxnMeta{
				ID:             txnID,
				Epoch:          1,
				WriteTimestamp: ts,
				MinTimestamp:   ts,
			},
			ReadTimestamp: ts,
		}
	}

	belowTxnTS := ts(1)
	txnTS := ts(2)
	aboveTxnTS := ts(3)

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	keyC := roachpb.Key("c")
	keyD := roachpb.Key("d")
	val := roachpb.Value{RawBytes: []byte{'v'}}

	testCases := []struct {
		name                  string
		setup                 func(t *testing.T, rw ReadWriter, txn *roachpb.Transaction)
		start                 roachpb.Key
		end                   roachpb.Key
		expNeedsIntentHistory bool
		expErr                string
		expNumFoundIntents    int
	}{
		{
			name:               "invalid end key",
			start:              keyC,
			end:                keyA,
			expErr:             "start key must be less than end key",
			expNumFoundIntents: 0,
		},
		{
			name:                  "no end key",
			start:                 keyA,
			end:                   nil,
			expNeedsIntentHistory: false,
			expNumFoundIntents:    0,
		},
		{
			name:                  "no intents",
			start:                 keyB,
			end:                   keyC,
			expNeedsIntentHistory: false,
			expNumFoundIntents:    0,
		},
		{
			name: "conflicting txn intent at lower timestamp",
			setup: func(t *testing.T, rw ReadWriter, _ *roachpb.Transaction) {
				conflictingTxn := newTxn(belowTxnTS) // test txn should see this intent
				_, err := MVCCPut(
					ctx, rw, keyA, conflictingTxn.WriteTimestamp, val, MVCCWriteOptions{Txn: conflictingTxn},
				)
				require.NoError(t, err)
			},
			start:                 keyA,
			end:                   keyC,
			expNeedsIntentHistory: false,
			expNumFoundIntents:    1,
		},
		{
			name: "conflicting txn intent at higher timestamp",
			setup: func(t *testing.T, rw ReadWriter, _ *roachpb.Transaction) {
				conflictingTxn := newTxn(aboveTxnTS) // test txn shouldn't see this intent
				_, err := MVCCPut(
					ctx, rw, keyA, conflictingTxn.WriteTimestamp, val, MVCCWriteOptions{Txn: conflictingTxn},
				)
				require.NoError(t, err)
			},
			start:                 keyA,
			end:                   keyC,
			expNeedsIntentHistory: false,
			expNumFoundIntents:    0,
		},
		{
			name: "bounds do not include (latest) own write",
			setup: func(t *testing.T, rw ReadWriter, txn *roachpb.Transaction) {
				_, err := MVCCPut(ctx, rw, keyA, txn.WriteTimestamp, val, MVCCWriteOptions{Txn: txn})
				require.NoError(t, err)
			},
			start:                 keyB,
			end:                   keyC,
			expNeedsIntentHistory: false,
			expNumFoundIntents:    0,
		},
		{
			name: "shared and exclusive locks should be ignored",
			setup: func(t *testing.T, rw ReadWriter, _ *roachpb.Transaction) {
				txnA := newTxn(belowTxnTS)
				txnB := newTxn(belowTxnTS)
				err := MVCCAcquireLock(ctx, rw, &txnA.TxnMeta, txnA.IgnoredSeqNums, lock.Shared, keyA, nil /*ms*/, 0 /*maxConflicts*/, 0 /*targetLockConflictBytes*/)
				require.NoError(t, err)
				err = MVCCAcquireLock(ctx, rw, &txnB.TxnMeta, txnB.IgnoredSeqNums, lock.Shared, keyA, nil /*ms*/, 0 /*maxConflicts*/, 0 /*targetLockConflictBytes*/)
				require.NoError(t, err)
				err = MVCCAcquireLock(ctx, rw, &txnA.TxnMeta, txnA.IgnoredSeqNums, lock.Exclusive, keyB, nil /*ms*/, 0 /*maxConflicts*/, 0 /*targetLockConflictBytes*/)
				require.NoError(t, err)
			},
			start:                 keyA,
			end:                   keyC,
			expNeedsIntentHistory: false,
			expNumFoundIntents:    0,
		},
		{
			// Same thing as above, but no end key this time. This ends up using a
			// prefix iterator.
			name: "shared and exclusive locks should be ignored no end key",
			setup: func(t *testing.T, rw ReadWriter, _ *roachpb.Transaction) {
				txnA := newTxn(belowTxnTS)
				err := MVCCAcquireLock(ctx, rw, &txnA.TxnMeta, txnA.IgnoredSeqNums, lock.Shared, keyA, nil /*ms*/, 0 /*maxConflicts*/, 0 /*targetLockConflictBytes*/)
				require.NoError(t, err)
				err = MVCCAcquireLock(ctx, rw, &txnA.TxnMeta, txnA.IgnoredSeqNums, lock.Exclusive, keyA, nil /*ms*/, 0 /*maxConflicts*/, 0 /*targetLockConflictBytes*/)
				require.NoError(t, err)
			},
			start:                 keyA,
			end:                   nil,
			expNeedsIntentHistory: false,
			expNumFoundIntents:    0,
		},
		{
			name: "{exclusive, shared} locks and intents",
			setup: func(t *testing.T, rw ReadWriter, _ *roachpb.Transaction) {
				txnA := newTxn(belowTxnTS)
				txnB := newTxn(belowTxnTS)
				err := MVCCAcquireLock(ctx, rw, &txnA.TxnMeta, txnA.IgnoredSeqNums, lock.Shared, keyA, nil /*ms*/, 0 /*maxConflicts*/, 0 /*targetLockConflictBytes*/)
				require.NoError(t, err)
				err = MVCCAcquireLock(ctx, rw, &txnB.TxnMeta, txnB.IgnoredSeqNums, lock.Shared, keyA, nil /*ms*/, 0 /*maxConflicts*/, 0 /*targetLockConflictBytes*/)
				require.NoError(t, err)
				err = MVCCAcquireLock(ctx, rw, &txnA.TxnMeta, txnA.IgnoredSeqNums, lock.Exclusive, keyB, nil /*ms*/, 0 /*maxConflicts*/, 0 /*targetLockConflictBytes*/)
				require.NoError(t, err)
				require.NoError(t, err)
				_, err = MVCCPut(ctx, rw, keyC, txnA.WriteTimestamp, val, MVCCWriteOptions{Txn: txnA})
				require.NoError(t, err)
			},
			start:                 keyA,
			end:                   keyD,
			expNeedsIntentHistory: false,
			expNumFoundIntents:    1,
		},
		{
			name: "{exclusive, shared} locks and own intents",
			setup: func(t *testing.T, rw ReadWriter, txn *roachpb.Transaction) {
				txnA := newTxn(belowTxnTS)
				txnB := newTxn(belowTxnTS)
				err := MVCCAcquireLock(ctx, rw, &txnA.TxnMeta, txnA.IgnoredSeqNums, lock.Shared, keyA, nil /*ms*/, 0 /*maxConflicts*/, 0 /*targetLockConflictBytes*/)
				require.NoError(t, err)
				err = MVCCAcquireLock(ctx, rw, &txnB.TxnMeta, txnB.IgnoredSeqNums, lock.Shared, keyA, nil /*ms*/, 0 /*maxConflicts*/, 0 /*targetLockConflictBytes*/)
				require.NoError(t, err)
				err = MVCCAcquireLock(ctx, rw, &txnA.TxnMeta, txnA.IgnoredSeqNums, lock.Exclusive, keyB, nil /*ms*/, 0 /*maxConflicts*/, 0 /*targetLockConflictBytes*/)
				require.NoError(t, err)
				_, err = MVCCPut(ctx, rw, keyC, txn.WriteTimestamp, val, MVCCWriteOptions{Txn: txn})
				require.NoError(t, err)
			},
			start:                 keyA,
			end:                   keyD,
			expNeedsIntentHistory: true,
			expNumFoundIntents:    0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			eng := NewDefaultInMemForTesting()
			defer eng.Close()

			var intents []roachpb.Intent
			txn := newTxn(txnTS)
			if tc.setup != nil {
				tc.setup(t, eng, txn)
			}
			needsIntentHistory, err := ScanConflictingIntentsForDroppingLatchesEarly(
				ctx,
				eng,
				txn.ID,
				txn.ReadTimestamp,
				tc.start,
				tc.end,
				&intents,
				0, /* maxLockConflicts */
				0, /* targetLockConflictBytes */
			)
			if tc.expErr != "" {
				require.Error(t, err)
				testutils.IsError(err, tc.expErr)
				return // none of the other fields need to be tested.
			}

			require.NoError(t, err)
			require.Equal(t, tc.expNeedsIntentHistory, needsIntentHistory)
			require.Equal(t, tc.expNumFoundIntents, len(intents))
		})
	}
}

// TestScanConflictingIntentsForDroppingLatchesEarlyReadYourOwnWrites constructs
// various read-your-own-write cases and ensures we correctly determine whether
// callers of ScanConflictingIntentsForDroppingLatchesEarly need to consult
// intent history or not when performing a scan over the MVCC keyspace. Factors
// that go into this determination are:
// 1. Sequence numbers of the intent/read op.
// 2. Timestamps of the intent/read op.
// 3. Epochs of the intent/read op.
// 4. Whether any savepoints have been rolled back.
//
// NB: When scanning for conflicting intents to determine if latches can be
// dropped early, we fallback to using the intent interleaving iterator in all
// read-your-own-write cases. However, doing so is more restrictive than it
// needs to be -- the in-line test expectations correspond to what an optimized
// determination for `needsIntentHistory` would look like. However, for the
// purposes of this test, we assert that we always fall back to using the intent
// interleaving iterator in ALL read-your-own-write cases.
func TestScanConflictingIntentsForDroppingLatchesEarlyReadYourOwnWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	alwaysFallbackToIntentInterleavingIteratorForReadYourOwnWrites := true

	ctx := context.Background()
	ts := func(nanos int) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: int64(nanos),
		}
	}
	belowReadTS := ts(1)
	readTS := ts(2)
	aboveReadTS := ts(3)

	belowReadTxnEpoch := enginepb.TxnEpoch(1)
	readTxnEpoch := enginepb.TxnEpoch(3)
	aboveReadTxnEpoch := enginepb.TxnEpoch(2)

	belowReadSeqNumber := enginepb.TxnSeq(1)
	readSeqNumber := enginepb.TxnSeq(2)
	aboveReadSeqNumber := enginepb.TxnSeq(3)

	keyA := roachpb.Key("a")
	val := roachpb.Value{RawBytes: []byte{'v'}}

	testCases := []struct {
		name                  string
		intentTS              hlc.Timestamp
		intentSequenceNumber  enginepb.TxnSeq
		intentEpoch           enginepb.TxnEpoch
		expNeedsIntentHistory bool // currently unused when testing
		ignoredSeqNumbers     enginepb.IgnoredSeqNumRange
	}{
		{
			name:                  "equal {timestamp, seq number, epoch}",
			intentTS:              readTS,
			intentSequenceNumber:  readSeqNumber,
			intentEpoch:           readTxnEpoch,
			expNeedsIntentHistory: false,
		},
		{
			name:                  "higher {intent seq number} equal {timestamp, epoch}",
			intentTS:              readTS,
			intentSequenceNumber:  aboveReadSeqNumber,
			intentEpoch:           readTxnEpoch,
			expNeedsIntentHistory: true,
		},
		{
			name:                  "higher {intent seq number} equal {epoch} lower {intent timestamp}",
			intentTS:              belowReadTS,
			intentSequenceNumber:  aboveReadSeqNumber,
			intentEpoch:           readTxnEpoch,
			expNeedsIntentHistory: true,
		},
		{
			name:                  "higher {intent seq number, intent timestamp} equal {epoch}",
			intentTS:              aboveReadTS,
			intentSequenceNumber:  aboveReadSeqNumber,
			intentEpoch:           readTxnEpoch,
			expNeedsIntentHistory: true,
		},
		{
			// Naively scanning at the read's timestamp, without accounting for the
			// intent, would result in us missing our own write as the writeTS is
			// higher than the readTS.
			name:                  "higher {intent timestamp} equal {epoch} lower {intent seq number}",
			intentTS:              aboveReadTS,
			intentSequenceNumber:  belowReadSeqNumber,
			intentEpoch:           readTxnEpoch,
			expNeedsIntentHistory: true,
		},
		{
			// Naively scanning at the read's timestamp, without accounting for the
			// intent, would result in us missing our own write as the writeTS is
			// higher than the readTS.
			name:                  "higher {intent timestamp} equal {seq number, epoch}",
			intentTS:              aboveReadTS,
			intentSequenceNumber:  readSeqNumber,
			intentEpoch:           readTxnEpoch,
			expNeedsIntentHistory: true,
		},
		{
			name:                  "equal {timestamp, epoch} lower {intent seq number}",
			intentTS:              readTS,
			intentSequenceNumber:  belowReadSeqNumber,
			intentEpoch:           readTxnEpoch,
			expNeedsIntentHistory: false,
		},
		{
			name:                  "equal {epoch} lower {intent timestamp, intent seq number}",
			intentTS:              belowReadTS,
			intentSequenceNumber:  belowReadSeqNumber,
			intentEpoch:           readTxnEpoch,
			expNeedsIntentHistory: false,
		},
		{
			name:                  "equal {epoch, seq number} lower {intent timestamp}",
			intentTS:              belowReadTS,
			intentSequenceNumber:  readSeqNumber,
			intentEpoch:           readTxnEpoch,
			expNeedsIntentHistory: false,
		},

		// lower/higher epoch test cases aren't exhaustive.
		{
			name:                  "equal {timestamp, seq number} lower {epoch}",
			intentTS:              readTS,
			intentSequenceNumber:  readSeqNumber,
			intentEpoch:           belowReadTxnEpoch,
			expNeedsIntentHistory: true,
		},
		{
			name:                  "higher{epoch} equal {timestamp, seq number}",
			intentTS:              readTS,
			intentSequenceNumber:  readSeqNumber,
			intentEpoch:           aboveReadTxnEpoch,
			expNeedsIntentHistory: true,
		},
		{
			name:                  "lower {epoch, intent timestamp, intent seq number}",
			intentTS:              belowReadTS,
			intentSequenceNumber:  belowReadSeqNumber,
			intentEpoch:           belowReadTxnEpoch,
			expNeedsIntentHistory: true,
		},
		{
			name:                  "higher {epoch} lower {intent timestamp, intent seq number}",
			intentTS:              belowReadTS,
			intentSequenceNumber:  belowReadSeqNumber,
			intentEpoch:           aboveReadTxnEpoch,
			expNeedsIntentHistory: true,
		},
		// Savepoint related tests.
		{
			// Scenario from https://github.com/cockroachdb/cockroach/issues/94337.
			name:                  "intent part of rolled back savepoint",
			intentTS:              belowReadTS,
			intentSequenceNumber:  belowReadSeqNumber,
			intentEpoch:           readTxnEpoch,
			ignoredSeqNumbers:     enginepb.IgnoredSeqNumRange{Start: belowReadSeqNumber, End: belowReadSeqNumber},
			expNeedsIntentHistory: true,
		},
		{
			name:                  "intent not part of rolled back savepoint",
			intentTS:              readTS,
			intentSequenceNumber:  readSeqNumber,
			intentEpoch:           readTxnEpoch,
			ignoredSeqNumbers:     enginepb.IgnoredSeqNumRange{Start: belowReadSeqNumber, End: belowReadSeqNumber},
			expNeedsIntentHistory: true, // should be false
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			eng := NewDefaultInMemForTesting()
			defer eng.Close()

			txnID := uuid.MakeV4()
			txn := &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					ID: txnID,
				},
			}

			// Write the intent as dictated by the test case.
			txn.Epoch = tc.intentEpoch
			txn.Sequence = tc.intentSequenceNumber
			txn.ReadTimestamp = tc.intentTS
			txn.WriteTimestamp = tc.intentTS
			_, err := MVCCPut(ctx, eng, keyA, txn.WriteTimestamp, val, MVCCWriteOptions{Txn: txn})
			require.NoError(t, err)

			// Set up the read.
			txn.Epoch = readTxnEpoch
			txn.Sequence = readSeqNumber
			txn.ReadTimestamp = readTS
			txn.WriteTimestamp = readTS
			txn.IgnoredSeqNums = []enginepb.IgnoredSeqNumRange{tc.ignoredSeqNumbers}

			var intents []roachpb.Intent
			needsIntentHistory, err := ScanConflictingIntentsForDroppingLatchesEarly(
				ctx,
				eng,
				txn.ID,
				txn.ReadTimestamp,
				keyA,
				nil,
				&intents,
				0, /* maxLockConflicts */
				0, /* targetLockConflictBytes */
			)
			require.NoError(t, err)
			if alwaysFallbackToIntentInterleavingIteratorForReadYourOwnWrites {
				require.Equal(t, true, needsIntentHistory)
			} else {
				require.Equal(t, tc.expNeedsIntentHistory, needsIntentHistory)
			}
		})
	}
}

// TestEngineRangeKeyMutations tests that range key mutations work as expected,
// both for the engine directly and for batches.
func TestEngineRangeKeyMutations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "batch", func(t *testing.T, useBatch bool) {
		eng := NewDefaultInMemForTesting()
		defer eng.Close()

		rw := ReadWriter(eng)
		if useBatch {
			rw = eng.NewBatch()
			defer rw.Close()
		}

		// Check errors for invalid, empty, and zero-length range keys. Not
		// exhaustive, since we assume validation dispatches to
		// MVCCRangeKey.Validate() which is tested separately.
		for name, rk := range map[string]MVCCRangeKey{
			"empty":      {},
			"invalid":    rangeKey("b", "a", 1),
			"zeroLength": rangeKey("a", "a", 1),
		} {
			t.Run(name, func(t *testing.T) {
				require.Error(t, rw.PutMVCCRangeKey(rk, MVCCValue{}))
				require.Error(t, rw.PutRawMVCCRangeKey(rk, []byte{}))
				require.Error(t, rw.ClearMVCCRangeKey(rk))

				// Engine methods don't do validation, but just pass through to Pebble.
				require.NoError(t, rw.PutEngineRangeKey(
					rk.StartKey, rk.EndKey, EncodeMVCCTimestampSuffix(rk.Timestamp), nil))
				require.NoError(t, rw.ClearEngineRangeKey(
					rk.StartKey, rk.EndKey, EncodeMVCCTimestampSuffix(rk.Timestamp)))
				require.NoError(t, rw.ClearRawRange(
					rk.StartKey, rk.EndKey, false /* pointKeys */, true /* rangeKeys */))
			})
		}

		// Check that non-tombstone values error.
		require.Error(t, rw.PutMVCCRangeKey(rangeKey("a", "b", 1), stringValue("foo")))

		// Check that nothing got written during the errors above.
		require.Empty(t, scanRangeKeys(t, rw))

		// Write some range keys and read the fragmented keys back.
		for _, rangeKey := range []MVCCRangeKey{
			rangeKey("a", "d", 1),
			rangeKey("f", "h", 1),
			rangeKey("c", "g", 2),
		} {
			require.NoError(t, rw.PutMVCCRangeKey(rangeKey, MVCCValue{}))
		}
		require.Equal(t, []MVCCRangeKeyValue{
			rangeKV("a", "c", 1, MVCCValue{}),
			rangeKV("c", "d", 2, MVCCValue{}),
			rangeKV("c", "d", 1, MVCCValue{}),
			rangeKV("d", "f", 2, MVCCValue{}),
			rangeKV("f", "g", 2, MVCCValue{}),
			rangeKV("f", "g", 1, MVCCValue{}),
			rangeKV("g", "h", 1, MVCCValue{}),
		}, scanRangeKeys(t, rw))

		// Clear the f-g portion of [f-h)@1, twice for idempotency. This should not
		// affect any other range keys, apart from removing the fragment boundary
		// at f for [d-g)@2.
		require.NoError(t, rw.ClearMVCCRangeKey(rangeKey("f", "g", 1)))
		require.NoError(t, rw.ClearMVCCRangeKey(rangeKey("f", "g", 1)))
		require.Equal(t, []MVCCRangeKeyValue{
			rangeKV("a", "c", 1, MVCCValue{}),
			rangeKV("c", "d", 2, MVCCValue{}),
			rangeKV("c", "d", 1, MVCCValue{}),
			rangeKV("d", "g", 2, MVCCValue{}),
			rangeKV("g", "h", 1, MVCCValue{}),
		}, scanRangeKeys(t, rw))

		// Write [e-f)@2 on top of existing [d-g)@2. This should be a noop.
		require.NoError(t, rw.PutMVCCRangeKey(rangeKey("e", "f", 2), MVCCValue{}))
		require.Equal(t, []MVCCRangeKeyValue{
			rangeKV("a", "c", 1, MVCCValue{}),
			rangeKV("c", "d", 2, MVCCValue{}),
			rangeKV("c", "d", 1, MVCCValue{}),
			rangeKV("d", "g", 2, MVCCValue{}),
			rangeKV("g", "h", 1, MVCCValue{}),
		}, scanRangeKeys(t, rw))

		// Clear all range keys in the [c-f) span. Twice for idempotency.
		require.NoError(t, rw.ClearRawRange(roachpb.Key("c"), roachpb.Key("f"), false, true))
		require.NoError(t, rw.ClearRawRange(roachpb.Key("c"), roachpb.Key("f"), false, true))
		require.Equal(t, []MVCCRangeKeyValue{
			rangeKV("a", "c", 1, MVCCValue{}),
			rangeKV("f", "g", 2, MVCCValue{}),
			rangeKV("g", "h", 1, MVCCValue{}),
		}, scanRangeKeys(t, rw))

		// Write another couple of range keys to bridge the [c-g)@1 gap. We write a
		// raw engine key and a raw MVCC key rather than a regular MVCC key, to test
		// those methods too.
		valueRaw, err := EncodeMVCCValue(MVCCValue{})
		require.NoError(t, err)
		require.NoError(t, rw.PutEngineRangeKey(
			roachpb.Key("c"), roachpb.Key("e"), EncodeMVCCTimestampSuffix(wallTS(1)), valueRaw))
		require.NoError(t, rw.PutRawMVCCRangeKey(rangeKey("e", "g", 1), valueRaw))
		require.Equal(t, []MVCCRangeKeyValue{
			rangeKV("a", "f", 1, MVCCValue{}),
			rangeKV("f", "g", 2, MVCCValue{}),
			rangeKV("f", "g", 1, MVCCValue{}),
			rangeKV("g", "h", 1, MVCCValue{}),
		}, scanRangeKeys(t, rw))

		// Writing a range key [a-f)@2 which abuts [f-g)@2 should not merge if it
		// has a different value (local timestamp).
		require.NoError(t, rw.PutMVCCRangeKey(rangeKey("a", "f", 2), tombstoneLocalTS(7)))
		require.Equal(t, []MVCCRangeKeyValue{
			rangeKV("a", "f", 2, tombstoneLocalTS(7)),
			rangeKV("a", "f", 1, MVCCValue{}),
			rangeKV("f", "g", 2, MVCCValue{}),
			rangeKV("f", "g", 1, MVCCValue{}),
			rangeKV("g", "h", 1, MVCCValue{}),
		}, scanRangeKeys(t, rw))

		// If using a batch, make sure nothing has been written to the engine, then
		// commit the batch and make sure it gets written to the engine.
		if useBatch {
			require.Empty(t, scanRangeKeys(t, eng))
			require.NoError(t, rw.(Batch).Commit(true))
			require.Equal(t, []MVCCRangeKeyValue{
				rangeKV("a", "f", 2, tombstoneLocalTS(7)),
				rangeKV("a", "f", 1, MVCCValue{}),
				rangeKV("f", "g", 2, MVCCValue{}),
				rangeKV("f", "g", 1, MVCCValue{}),
				rangeKV("g", "h", 1, MVCCValue{}),
			}, scanRangeKeys(t, eng))
		}
	})
}

// TODO(erikgrinaker): The below test helpers should be moved to
// testutils/storageutils instead, but that requires storage tests to be in the
// storage_test package to avoid import cycles.

// scanRangeKeys scans all range keys from the reader.
func scanRangeKeys(t *testing.T, r Reader) []MVCCRangeKeyValue {
	t.Helper()

	iter, err := r.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
		KeyTypes:   IterKeyTypeRangesOnly,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	})
	require.NoError(t, err)
	defer iter.Close()

	var rangeKeys []MVCCRangeKeyValue
	for iter.SeekGE(MVCCKey{Key: keys.LocalMax}); ; iter.Next() {
		ok, err := iter.Valid()
		require.NoError(t, err)
		if !ok {
			break
		}
		for _, rkv := range iter.RangeKeys().AsRangeKeyValues() {
			rangeKeys = append(rangeKeys, rkv.Clone())
		}
	}
	return rangeKeys
}

// scanPointKeys scans all point keys from the reader, excluding intents.
func scanPointKeys(t *testing.T, r Reader) []MVCCKey {
	t.Helper()

	iter, err := r.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	})
	require.NoError(t, err)
	defer iter.Close()

	var pointKeys []MVCCKey
	for iter.SeekGE(MVCCKey{Key: keys.LocalMax}); ; iter.Next() {
		ok, err := iter.Valid()
		require.NoError(t, err)
		if !ok {
			break
		}
		pointKeys = append(pointKeys, iter.UnsafeKey().Clone())
	}
	return pointKeys
}

// scanLockKeys scans all locks from the reader, ignoring the provisional values
// of intents.
func scanLockKeys(t *testing.T, r Reader) []roachpb.Key {
	t.Helper()

	var lockKeys []roachpb.Key
	locks, err := ScanLocks(context.Background(), r, keys.LocalMax, keys.MaxKey, 0, 0)
	require.NoError(t, err)
	for _, l := range locks {
		lockKeys = append(lockKeys, l.Key)
	}
	return lockKeys
}

// scanIter scans all point/range keys from the iterator, and returns a combined
// slice of MVCCRangeKeyValue and MVCCKeyValue in order.
func scanIter(t *testing.T, iter SimpleMVCCIterator) []interface{} {
	t.Helper()

	iter.SeekGE(MVCCKey{Key: keys.LocalMax})

	var keys []interface{}
	var prevRangeStart roachpb.Key
	for {
		ok, err := iter.Valid()
		require.NoError(t, err)
		if !ok {
			break
		}
		hasPoint, hasRange := iter.HasPointAndRange()
		if hasRange {
			if bounds := iter.RangeBounds(); !bounds.Key.Equal(prevRangeStart) {
				for _, rkv := range iter.RangeKeys().AsRangeKeyValues() {
					keys = append(keys, rkv.Clone())
				}
				prevRangeStart = bounds.Key.Clone()
			}
		}
		if hasPoint {
			v, err := iter.UnsafeValue()
			require.NoError(t, err)
			keys = append(keys, MVCCKeyValue{
				Key:   iter.UnsafeKey().Clone(),
				Value: append([]byte{}, v...),
			})
		}
		iter.Next()
	}
	return keys
}
