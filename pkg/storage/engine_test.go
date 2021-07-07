// Copyright 2014 The Cockroach Authors.
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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
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
							val, err := e.MVCCGet(key)
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
		})
	}
}

func TestEngineBatchStaleCachedIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Prevent regression of a bug which caused spurious MVCC errors due to an
	// invalid optimization which let an iterator return key-value pairs which
	// had since been deleted from the underlying engine.
	// Discovered in #6878.

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			eng := engineImpl.create()
			defer eng.Close()

			// Focused failure mode: highlights the actual bug.
			{
				batch := eng.NewBatch()
				defer batch.Close()
				iter := batch.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
				key := MVCCKey{Key: roachpb.Key("b")}

				if err := batch.PutUnversioned(key.Key, []byte("foo")); err != nil {
					t.Fatal(err)
				}

				iter.SeekGE(key)

				if err := batch.ClearUnversioned(key.Key); err != nil {
					t.Fatal(err)
				}

				// MVCCIterator should not reuse its cached result.
				iter.SeekGE(key)

				if ok, err := iter.Valid(); err != nil {
					t.Fatal(err)
				} else if ok {
					t.Fatalf("iterator unexpectedly valid: %v -> %v",
						iter.UnsafeKey(), iter.UnsafeValue())
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
				if err := MVCCPut(context.Background(), batch, nil, key, hlc.Timestamp{},
					roachpb.MakeValueFromString("x"), nil); err != nil {
					t.Fatal(err)
				}

				// Seek the iterator to `key` and clear the value (but without
				// telling the iterator about that).
				if err := MVCCDelete(context.Background(), batch, nil, key,
					hlc.Timestamp{}, nil); err != nil {
					t.Fatal(err)
				}

				// Trigger a seek on the cached iterator by seeking to the (now
				// absent) key.
				// The underlying iterator will already be in the right position
				// due to a seek in MVCCDelete (followed by a Clear, which does not
				// invalidate the iterator's cache), and if it reports its cached
				// result back, we'll see the (newly deleted) value (due to the
				// failure mode above).
				if v, _, err := MVCCGet(context.Background(), batch, key,
					hlc.Timestamp{}, MVCCGetOptions{}); err != nil {
					t.Fatal(err)
				} else if v != nil {
					t.Fatalf("expected no value, got %+v", v)
				}
			}
		})
	}
}

func TestEngineBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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
					return rw.ClearUnversioned(d.key.Key)
				} else if d.merge {
					return rw.Merge(d.key, d.value)
				}
				return rw.PutUnversioned(d.key.Key, d.value)
			}

			get := func(rw ReadWriter, key MVCCKey) []byte {
				b, err := rw.MVCCGet(key)
				if err != nil {
					t.Fatal(err)
				}
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
				if err := engine.ClearUnversioned(key.Key); err != nil {
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
				if err := b.ClearUnversioned(key.Key); err != nil {
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
				iter := b.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
				iter.SeekGE(key)
				if ok, err := iter.Valid(); !ok {
					if currentBatch[len(currentBatch)-1].value != nil {
						t.Errorf("%d: batch seek invalid, err=%v", i, err)
					}
				} else if !iter.Key().Equal(key) {
					t.Errorf("%d: batch seek expected key %s, but got %s", i, key, iter.Key())
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
		})
	}
}

func TestEnginePutGetDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			// Test for correct handling of empty keys, which should produce errors.
			for i, err := range []error{
				engine.PutUnversioned(mvccKey("").Key, []byte("")),
				engine.PutUnversioned(NilKey.Key, []byte("")),
				func() error {
					_, err := engine.MVCCGet(mvccKey(""))
					return err
				}(),
				engine.ClearUnversioned(NilKey.Key),
				func() error {
					_, err := engine.MVCCGet(NilKey)
					return err
				}(),
				engine.ClearUnversioned(NilKey.Key),
				engine.ClearUnversioned(mvccKey("").Key),
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
				val, err := engine.MVCCGet(c.key)
				if err != nil {
					t.Errorf("get: expected no error, but got %s", err)
				}
				if len(val) != 0 {
					t.Errorf("expected key %q value.Bytes to be nil: got %+v", c.key, val)
				}
				if err := engine.PutUnversioned(c.key.Key, c.value); err != nil {
					t.Errorf("put: expected no error, but got %s", err)
				}
				val, err = engine.MVCCGet(c.key)
				if err != nil {
					t.Errorf("get: expected no error, but got %s", err)
				}
				if !bytes.Equal(val, c.value) {
					t.Errorf("expected key value %s to be %+v: got %+v", c.key, c.value, val)
				}
				if err := engine.ClearUnversioned(c.key.Key); err != nil {
					t.Errorf("delete: expected no error, but got %s", err)
				}
				val, err = engine.MVCCGet(c.key)
				if err != nil {
					t.Errorf("get: expected no error, but got %s", err)
				}
				if len(val) != 0 {
					t.Errorf("expected key %s value.Bytes to be nil: got %+v", c.key, val)
				}
			}
		})
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

	engineBytes := make([][][]byte, len(mvccEngineImpls))
	for engineIndex, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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
			engineBytes[engineIndex] = make([][]byte, len(testcases))
			for tcIndex, tc := range testcases {
				for i, update := range tc.merges {
					if err := engine.Merge(tc.testKey, update); err != nil {
						t.Fatalf("%d: %+v", i, err)
					}
				}
				result, _ := engine.MVCCGet(tc.testKey)
				engineBytes[engineIndex][tcIndex] = result
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
		})
	}
	for i := 0; i < len(engineBytes); i++ {
		// Pair-wise comparison of bytes since difference in serialization
		// can trigger replica consistency checker failures #45811
		if i+1 == len(engineBytes) {
			break
		}
		eng1 := i
		eng2 := i + 1
		for j := 0; j < len(engineBytes[eng1]); j++ {
			if !bytes.Equal(engineBytes[eng1][j], engineBytes[eng2][j]) {
				t.Errorf("engines %d, %d differ at test %d:\n%s\n != \n%s\n", eng1, eng2, j,
					hex.Dump(engineBytes[eng1][j]), hex.Dump(engineBytes[eng2][j]))
			}
		}
	}
}

func TestEngineMustExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	test := func(errStr string) {
		tempDir, dirCleanupFn := testutils.TempDir(t)
		defer dirCleanupFn()

		_, err := NewEngine(0, base.StorageConfig{
			Dir:       tempDir,
			MustExist: true,
		})
		if err == nil {
			t.Fatal("expected error related to missing directory")
		}
		if !strings.Contains(fmt.Sprint(err), errStr) {
			t.Fatal(err)
		}
	}

	test("no such file or directory")
}

func TestEngineTimeBound(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			var minTimestamp = hlc.Timestamp{WallTime: 1, Logical: 0}
			var maxTimestamp = hlc.Timestamp{WallTime: 3, Logical: 0}
			times := []hlc.Timestamp{
				{WallTime: 2, Logical: 0},
				minTimestamp,
				maxTimestamp,
				{WallTime: 2, Logical: 0},
			}

			for i, time := range times {
				s := fmt.Sprintf("%02d", i)
				key := MVCCKey{Key: roachpb.Key(s), Timestamp: time}
				if err := engine.PutMVCC(key, []byte(s)); err != nil {
					t.Fatal(err)
				}
			}
			if err := engine.Flush(); err != nil {
				t.Fatal(err)
			}

			batch := engine.NewBatch()
			defer batch.Close()

			check := func(t *testing.T, tbi MVCCIterator, keys, ssts int) {
				defer tbi.Close()
				tbi.SeekGE(NilKey)

				var count int
				for ; ; tbi.Next() {
					ok, err := tbi.Valid()
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					count++
				}

				// Make sure the iterator sees no writes.
				if keys != count {
					t.Fatalf("saw %d values in time bounded iterator, but expected %d", count, keys)
				}
				stats := tbi.Stats()
				if a := stats.TimeBoundNumSSTs; a != ssts {
					t.Fatalf("touched %d SSTs, expected %d", a, ssts)
				}
			}

			testCases := []struct {
				iter       MVCCIterator
				keys, ssts int
			}{
				// Completely to the right, not touching.
				{
					iter: batch.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
						MinTimestampHint: maxTimestamp.Next(),
						MaxTimestampHint: maxTimestamp.Next().Next(),
						UpperBound:       roachpb.KeyMax,
						WithStats:        true,
					}),
					keys: 0,
					ssts: 0,
				},
				// Completely to the left, not touching.
				{
					iter: batch.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
						MinTimestampHint: minTimestamp.Prev().Prev(),
						MaxTimestampHint: minTimestamp.Prev(),
						UpperBound:       roachpb.KeyMax,
						WithStats:        true,
					}),
					keys: 0,
					ssts: 0,
				},
				// Touching on the right.
				{
					iter: batch.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
						MinTimestampHint: maxTimestamp,
						MaxTimestampHint: maxTimestamp,
						UpperBound:       roachpb.KeyMax,
						WithStats:        true,
					}),
					keys: len(times),
					ssts: 1,
				},
				// Touching on the left.
				{
					iter: batch.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
						MinTimestampHint: minTimestamp,
						MaxTimestampHint: minTimestamp,
						UpperBound:       roachpb.KeyMax,
						WithStats:        true,
					}),
					keys: len(times),
					ssts: 1,
				},
				// Copy of last case, but confirm that we don't get SST stats if we don't
				// ask for them.
				{
					iter: batch.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
						MinTimestampHint: minTimestamp,
						MaxTimestampHint: minTimestamp,
						UpperBound:       roachpb.KeyMax,
						WithStats:        false,
					}),
					keys: len(times),
					ssts: 0,
				},
				// Copy of last case, but confirm that upper bound is respected.
				{
					iter: batch.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
						MinTimestampHint: minTimestamp,
						MaxTimestampHint: minTimestamp,
						UpperBound:       []byte("02"),
						WithStats:        false,
					}),
					keys: 2,
					ssts: 0,
				},
			}

			for _, test := range testCases {
				t.Run("", func(t *testing.T) {
					check(t, test.iter, test.keys, test.ssts)
				})
			}

			// Make a regular iterator. Before #21721, this would accidentally pick up the
			// time bounded iterator instead.
			iter := batch.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
			defer iter.Close()
			iter.SeekGE(MVCCKey{Key: keys.LocalMax})

			var count int
			for ; ; iter.Next() {
				ok, err := iter.Valid()
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				count++
			}

			// Make sure the iterator sees the writes (i.e. it's not the time bounded iterator).
			if expCount := len(times); expCount != count {
				t.Fatalf("saw %d values in regular iterator, but expected %d", count, expCount)
			}
		})
	}
}

func TestFlushNumSSTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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
		})
	}
}

func TestEngineScan1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

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

			keyvals, err := Scan(engine, roachpb.Key("chinese"), roachpb.Key("german"), 0)
			if err != nil {
				t.Fatalf("could not run scan: %+v", err)
			}
			ensureRangeEqual(t, sortedKeys[1:4], keyMap, keyvals)

			// Check an end of range which does not equal an existing key.
			keyvals, err = Scan(engine, roachpb.Key("chinese"), roachpb.Key("german1"), 0)
			if err != nil {
				t.Fatalf("could not run scan: %+v", err)
			}
			ensureRangeEqual(t, sortedKeys[1:5], keyMap, keyvals)

			keyvals, err = Scan(engine, roachpb.Key("chinese"), roachpb.Key("german"), 2)
			if err != nil {
				t.Fatalf("could not run scan: %+v", err)
			}
			ensureRangeEqual(t, sortedKeys[1:3], keyMap, keyvals)

			// Should return all key/value pairs in lexicographic order. Note that
			// LocalMax is the lowest possible global key.
			startKeys := []roachpb.Key{roachpb.Key("cat"), keys.LocalMax}
			for _, startKey := range startKeys {
				keyvals, err = Scan(engine, startKey, roachpb.KeyMax, 0)
				if err != nil {
					t.Fatalf("could not run scan: %+v", err)
				}
				ensureRangeEqual(t, sortedKeys, keyMap, keyvals)
			}

			// Test iterator stats.
			ro := engine.NewReadOnly()
			iter := ro.NewMVCCIterator(MVCCKeyIterKind,
				IterOptions{LowerBound: roachpb.Key("cat"), UpperBound: roachpb.Key("server")})
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
			require.Equal(t, "(interface (dir, seek, step): (fwd, 1, 5), (rev, 0, 0)), "+
				"(internal (dir, seek, step): (fwd, 1, 5), (rev, 0, 0))", stats.String())
			iter.Close()
			iter = ro.NewMVCCIterator(MVCCKeyIterKind,
				IterOptions{LowerBound: roachpb.Key("cat"), UpperBound: roachpb.Key("server")})
			// pebble.Iterator is reused, but stats are reset.
			stats = iter.Stats().Stats
			require.Equal(t, "(interface (dir, seek, step): (fwd, 0, 0), (rev, 0, 0)), "+
				"(internal (dir, seek, step): (fwd, 0, 0), (rev, 0, 0))", stats.String())
			iter.SeekGE(MVCCKey{Key: roachpb.Key("french")})
			iter.SeekLT(MVCCKey{Key: roachpb.Key("server")})
			stats = iter.Stats().Stats
			require.Equal(t, "(interface (dir, seek, step): (fwd, 1, 0), (rev, 1, 0)), "+
				"(internal (dir, seek, step): (fwd, 1, 0), (rev, 1, 1))", stats.String())
			iter.Close()
			ro.Close()
		})
	}
}

func verifyScan(start, end roachpb.Key, max int64, expKeys []MVCCKey, engine Engine, t *testing.T) {
	kvs, err := Scan(engine, start, end, max)
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

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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
		})
	}
}

func testEngineDeleteRange(t *testing.T, clearRange func(engine Engine, start, end MVCCKey) error) {
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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

			// Delete a range of keys
			if err := clearRange(engine, mvccKey("aa"), mvccKey("abc")); err != nil {
				t.Fatal(err)
			}
			// Verify what's left
			verifyScan(localMax, roachpb.KeyMax, 10,
				[]MVCCKey{mvccKey("a"), mvccKey("abc")}, engine, t)
		})
	}
}

func TestEngineDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testEngineDeleteRange(t, func(engine Engine, start, end MVCCKey) error {
		return engine.ClearMVCCRange(start, end)
	})
}

func TestEngineDeleteRangeBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testEngineDeleteRange(t, func(engine Engine, start, end MVCCKey) error {
		batch := engine.NewUnindexedBatch(true /* writeOnly */)
		defer batch.Close()
		if err := batch.ClearMVCCRange(start, end); err != nil {
			return err
		}
		batch2 := engine.NewUnindexedBatch(true /* writeOnly */)
		defer batch2.Close()
		if err := batch2.ApplyBatchRepr(batch.Repr(), false); err != nil {
			return err
		}
		return batch2.Commit(false)
	})
}

func TestEngineDeleteIterRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testEngineDeleteRange(t, func(engine Engine, start, end MVCCKey) error {
		iter := engine.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
		defer iter.Close()
		return engine.ClearIterRange(iter, start.Key, end.Key)
	})
}

func TestSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			key := mvccKey("a")
			val1 := []byte("1")
			if err := engine.PutUnversioned(key.Key, val1); err != nil {
				t.Fatal(err)
			}
			val, _ := engine.MVCCGet(key)
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
			val, _ = engine.MVCCGet(key)
			valSnapshot, error := snap.MVCCGet(key)
			if error != nil {
				t.Fatalf("error : %s", error)
			}
			if !bytes.Equal(val, val2) {
				t.Fatalf("the value %s in get result does not match the value %s in request",
					val, val2)
			}
			if !bytes.Equal(valSnapshot, val1) {
				t.Fatalf("the value %s in get result does not match the value %s in request",
					valSnapshot, val1)
			}

			keyvals, _ := Scan(engine, key.Key, roachpb.KeyMax, 0)
			keyvalsSnapshot, error := Scan(snap, key.Key, roachpb.KeyMax, 0)
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
		})
	}
}

// TestSnapshotMethods verifies that snapshots allow only read-only
// engine operations.
func TestSnapshotMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			keys := []MVCCKey{mvccKey("a"), mvccKey("b")}
			vals := [][]byte{[]byte("1"), []byte("2")}
			for i := range keys {
				if err := engine.PutUnversioned(keys[i].Key, vals[i]); err != nil {
					t.Fatal(err)
				}
				if val, err := engine.MVCCGet(keys[i]); err != nil {
					t.Fatal(err)
				} else if !bytes.Equal(vals[i], val) {
					t.Fatalf("expected %s, but found %s", vals[i], val)
				}
			}
			snap := engine.NewSnapshot()
			defer snap.Close()

			// Verify Get.
			for i := range keys {
				valSnapshot, err := snap.MVCCGet(keys[i])
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(vals[i], valSnapshot) {
					t.Fatalf("the value %s in get result does not match the value %s in snapshot",
						vals[i], valSnapshot)
				}
			}

			// Verify Scan.
			keyvals, _ := Scan(engine, localMax, roachpb.KeyMax, 0)
			keyvalsSnapshot, err := Scan(snap, localMax, roachpb.KeyMax, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(keyvals, keyvalsSnapshot) {
				t.Fatalf("the key/values %v in scan result does not match the value %s in snapshot",
					keyvals, keyvalsSnapshot)
			}

			// Verify MVCCIterate.
			index := 0
			if err := snap.MVCCIterate(localMax, roachpb.KeyMax, MVCCKeyAndIntentsIterKind, func(kv MVCCKeyValue) error {
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
			iter := snap.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
			iter.SeekGE(newKey)
			if ok, err := iter.Valid(); err != nil {
				t.Fatal(err)
			} else if ok {
				t.Error("expected invalid iterator when seeking to element which shouldn't be visible to snapshot")
			}
			iter.Close()
		})
	}
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

	opts := DefaultPebbleOptions()
	db, err := NewPebble(
		context.Background(),
		PebbleConfig{
			StorageConfig: base.StorageConfig{
				Settings: cluster.MakeTestingClusterSettings(),
				Dir:      dir,
			},
			Opts: opts,
		},
	)
	assert.NoError(t, err)
	defer db.Close()

	dir = filepath.Join(dir, "checkpoint")

	assert.NoError(t, err)
	assert.NoError(t, db.CreateCheckpoint(dir))
	assert.DirExists(t, dir)
	m, err := filepath.Glob(dir + "/*")
	assert.NoError(t, err)
	assert.True(t, len(m) > 0)
	if err := db.CreateCheckpoint(dir); !testutils.IsError(err, "exists") {
		t.Fatal(err)
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

type stringSorter []string

func (s stringSorter) Len() int               { return len(s) }
func (s stringSorter) Swap(i int, j int)      { s[i], s[j] = s[j], s[i] }
func (s stringSorter) Less(i int, j int) bool { return strings.Compare(s[i], s[j]) < 0 }

func TestEngineFS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

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
				"9a: create-dir /dir1",
				"9b: create /dir1/bar",
				"9c: list-dir /dir1 == bar",
				"9d: create /dir1/baz",
				"9e: list-dir /dir1 == bar,baz",
				"9f: delete /dir1/bar",
				"9g: delete /dir1/baz",
				"9h: delete-dir /dir1",
			}

			var f fs.File
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
					g   fs.File
					err error
				)
				switch s[0] {
				case "create":
					g, err = e.Create(s[1])
				case "create-with-sync":
					g, err = e.CreateWithSync(s[1], 1)
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
					err = e.MkdirAll(s[1])
				case "delete-dir":
					err = e.RemoveDir(s[1])
				case "list-dir":
					result, err := e.List(s[1])
					if err != nil {
						break
					}
					sort.Sort(stringSorter(result))
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
		})
	}
}

type engineImpl struct {
	name   string
	create func(*testing.T, string) Engine
}

// These FS implementations are not in-memory.
var engineRealFSImpls = []engineImpl{
	{"pebble", func(t *testing.T, dir string) Engine {

		opts := DefaultPebbleOptions()
		opts.FS = vfs.Default
		opts.Cache = pebble.NewCache(testCacheSize)
		defer opts.Cache.Unref()

		db, err := NewPebble(
			context.Background(),
			PebbleConfig{
				StorageConfig: base.StorageConfig{
					Dir: dir,
				},
				Opts: opts,
			})
		if err != nil {
			t.Fatalf("could not create new pebble instance at %s: %+v", dir, err)
		}
		return db
	}},
}

func TestEngineFSFileNotFoundError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, engineImpl := range engineRealFSImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			dir, dirCleanup := testutils.TempDir(t)
			defer dirCleanup()
			db := engineImpl.create(t, dir)
			defer db.Close()

			// Verify Remove returns os.ErrNotExist if file does not exist.
			if err := db.Remove("/non/existent/file"); !oserror.IsNotExist(err) {
				t.Fatalf("expected IsNotExist, but got %v (%T)", err, err)
			}

			// Verify RemoveAll returns nil if path does not exist.
			if err := db.RemoveAll("/non/existent/file"); err != nil {
				t.Fatalf("expected nil, but got %v (%T)", err, err)
			}

			fname := filepath.Join(dir, "random.file")
			data := "random data"
			if f, err := db.Create(fname); err != nil {
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

			if b, err := db.ReadFile(fname); err != nil {
				t.Errorf("unable to read file with filename %s, got err %v", fname, err)
			} else if string(b) != data {
				t.Errorf("expected content in %s is '%s', got '%s'", fname, data, string(b))
			}

			if err := db.Remove(fname); err != nil {
				t.Errorf("unable to delete file with filename %s, got err %v", fname, err)
			}

			// Verify ReadFile returns os.ErrNotExist if reading an already deleted file.
			if _, err := db.ReadFile(fname); !oserror.IsNotExist(err) {
				t.Fatalf("expected IsNotExist, but got %v (%T)", err, err)
			}

			// Verify Remove returns os.ErrNotExist if deleting an already deleted file.
			if err := db.Remove(fname); !oserror.IsNotExist(err) {
				t.Fatalf("expected IsNotExist, but got %v (%T)", err, err)
			}
		})
	}
}

// TestSupportPrev tests that SupportsPrev works as expected.
func TestSupportsPrev(t *testing.T) {
	defer leaktest.AfterTest(t)()

	opts := IterOptions{LowerBound: keys.LocalMax, UpperBound: keys.MaxKey}
	type engineTest struct {
		engineIterSupportsPrev   bool
		batchIterSupportsPrev    bool
		snapshotIterSupportsPrev bool
	}
	runTest := func(t *testing.T, eng Engine, et engineTest) {
		t.Run("engine", func(t *testing.T) {
			it := eng.NewMVCCIterator(MVCCKeyAndIntentsIterKind, opts)
			defer it.Close()
			require.Equal(t, et.engineIterSupportsPrev, it.SupportsPrev())
		})
		t.Run("batch", func(t *testing.T) {
			batch := eng.NewBatch()
			defer batch.Close()
			batchIt := batch.NewMVCCIterator(MVCCKeyAndIntentsIterKind, opts)
			defer batchIt.Close()
			require.Equal(t, et.batchIterSupportsPrev, batchIt.SupportsPrev())
		})
		t.Run("snapshot", func(t *testing.T) {
			snap := eng.NewSnapshot()
			defer snap.Close()
			snapIt := snap.NewMVCCIterator(MVCCKeyAndIntentsIterKind, opts)
			defer snapIt.Close()
			require.Equal(t, et.snapshotIterSupportsPrev, snapIt.SupportsPrev())
		})
	}
	t.Run("pebble", func(t *testing.T) {

		eng := NewInMem(
			context.Background(),
			roachpb.Attributes{},
			1<<20,   /* cacheSize */
			512<<20, /* storeSize */
			nil,     /* settings */
		)
		defer eng.Close()
		runTest(t, eng, engineTest{
			engineIterSupportsPrev:   true,
			batchIterSupportsPrev:    true,
			snapshotIterSupportsPrev: true,
		})
	})
}

func TestFS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var engineImpls []engineImpl
	engineImpls = append(engineImpls, engineRealFSImpls...)
	engineImpls = append(engineImpls,
		engineImpl{
			name: "pebble_mem",
			create: func(_ *testing.T, _ string) Engine {
				return createTestPebbleEngine()
			},
		})

	for _, impl := range engineImpls {
		t.Run(impl.name, func(t *testing.T) {
			dir, cleanupDir := testutils.TempDir(t)
			defer cleanupDir()
			fs := impl.create(t, dir)
			defer fs.Close()

			path := func(rel string) string {
				return filepath.Join(dir, rel)
			}
			expectLS := func(dir string, want []string) {
				t.Helper()

				got, err := fs.List(dir)
				require.NoError(t, err)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("fs.List(%q) = %#v, want %#v", dir, got, want)
				}
			}

			// Create a/ and assert that it's empty.
			require.NoError(t, fs.MkdirAll(path("a")))
			expectLS(path("a"), []string{})
			if _, err := fs.Stat(path("a/b/c")); !oserror.IsNotExist(err) {
				t.Fatal(`fs.Stat("a/b/c") should not exist`)
			}

			// Create a/b/ and a/b/c/ in a single MkdirAll call.
			// Then ensure that a duplicate call returns a nil error.
			require.NoError(t, fs.MkdirAll(path("a/b/c")))
			require.NoError(t, fs.MkdirAll(path("a/b/c")))
			expectLS(path("a"), []string{"b"})
			expectLS(path("a/b"), []string{"c"})
			expectLS(path("a/b/c"), []string{})
			_, err := fs.Stat(path("a/b/c"))
			require.NoError(t, err)

			// Create a file at a/b/c/foo.
			f, err := fs.Create(path("a/b/c/foo"))
			require.NoError(t, err)
			require.NoError(t, f.Close())
			expectLS(path("a/b/c"), []string{"foo"})

			// Create a file at a/b/c/bar.
			f, err = fs.Create(path("a/b/c/bar"))
			require.NoError(t, err)
			require.NoError(t, f.Close())
			expectLS(path("a/b/c"), []string{"bar", "foo"})
			_, err = fs.Stat(path("a/b/c/bar"))
			require.NoError(t, err)

			// RemoveAll a file.
			require.NoError(t, fs.RemoveAll(path("a/b/c/bar")))
			expectLS(path("a/b/c"), []string{"foo"})

			// RemoveAll a directory that contains subdirectories and
			// descendant files.
			require.NoError(t, fs.RemoveAll(path("a/b")))
			expectLS(path("a"), []string{})
		})
	}
}

func TestScanSeparatedIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	maxKey := keys.MaxKey

	keys := []roachpb.Key{
		roachpb.Key("a"),
		roachpb.Key("b"),
		roachpb.Key("c"),
	}
	testcases := map[string]struct {
		from          roachpb.Key
		to            roachpb.Key
		max           int64
		targetBytes   int64
		expectIntents []roachpb.Key
	}{
		"no keys":         {keys[0], keys[0], 0, 0, keys[:0]},
		"one key":         {keys[0], keys[1], 0, 0, keys[0:1]},
		"two keys":        {keys[0], keys[2], 0, 0, keys[0:2]},
		"all keys":        {keys[0], maxKey, 0, 0, keys},
		"offset mid":      {keys[1], keys[2], 0, 0, keys[1:2]},
		"offset last":     {keys[2], maxKey, 0, 0, keys[2:]},
		"offset post":     {roachpb.Key("x"), maxKey, 0, 0, []roachpb.Key{}},
		"nil end":         {keys[0], nil, 0, 0, []roachpb.Key{}},
		"limit keys":      {keys[0], maxKey, 2, 0, keys[0:2]},
		"one byte":        {keys[0], maxKey, 0, 1, keys[0:1]},
		"80 bytes":        {keys[0], maxKey, 0, 80, keys[0:2]},
		"80 bytes or one": {keys[0], maxKey, 1, 80, keys[0:1]},
		"1000 bytes":      {keys[0], maxKey, 0, 1000, keys},
	}

	for name, enableSeparatedIntents := range map[string]bool{"interleaved": false, "separated": true} {
		t.Run(name, func(t *testing.T) {
			settings := makeSettingsForSeparatedIntents(false, enableSeparatedIntents)
			eng := NewInMem(
				ctx,
				roachpb.Attributes{},
				1<<20,   /* cacheSize */
				512<<20, /* storeSize */
				settings,
			)
			defer eng.Close()

			for _, key := range keys {
				err := MVCCPut(ctx, eng, nil, key, txn1.ReadTimestamp, roachpb.Value{RawBytes: key}, txn1)
				require.NoError(t, err)
			}

			for name, tc := range testcases {
				tc := tc
				t.Run(name, func(t *testing.T) {
					intents, err := ScanSeparatedIntents(eng, tc.from, tc.to, tc.max, tc.targetBytes)
					require.NoError(t, err)
					if enableSeparatedIntents {
						require.Len(t, intents, len(tc.expectIntents), "unexpected number of separated intents")
						for i, intent := range intents {
							require.Equal(t, tc.expectIntents[i], intent.Key)
						}
					} else {
						require.Empty(t, intents)
					}
				})
			}
		})
	}
}
