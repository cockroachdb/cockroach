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
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
							val, err := e.Get(key)
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
				if err := batch.Put(key, []byte(strconv.Itoa(i))); err != nil {
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
				iter := batch.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
				key := MVCCKey{Key: roachpb.Key("b")}

				if err := batch.Put(key, []byte("foo")); err != nil {
					t.Fatal(err)
				}

				iter.SeekGE(key)

				if err := batch.Clear(key); err != nil {
					t.Fatal(err)
				}

				// Iterator should not reuse its cached result.
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
					return rw.Clear(d.key)
				} else if d.merge {
					return rw.Merge(d.key, d.value)
				}
				return rw.Put(d.key, d.value)
			}

			get := func(rw ReadWriter, key MVCCKey) []byte {
				b, err := rw.Get(key)
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
				if err := engine.Clear(key); err != nil {
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
				if err := b.Clear(key); err != nil {
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
				iter := b.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
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

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			// Test for correct handling of empty keys, which should produce errors.
			for i, err := range []error{
				engine.Put(mvccKey(""), []byte("")),
				engine.Put(NilKey, []byte("")),
				func() error {
					_, err := engine.Get(mvccKey(""))
					return err
				}(),
				engine.Clear(NilKey),
				func() error {
					_, err := engine.Get(NilKey)
					return err
				}(),
				engine.Clear(NilKey),
				engine.Clear(mvccKey("")),
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
				val, err := engine.Get(c.key)
				if err != nil {
					t.Errorf("get: expected no error, but got %s", err)
				}
				if len(val) != 0 {
					t.Errorf("expected key %q value.Bytes to be nil: got %+v", c.key, val)
				}
				if err := engine.Put(c.key, c.value); err != nil {
					t.Errorf("put: expected no error, but got %s", err)
				}
				val, err = engine.Get(c.key)
				if err != nil {
					t.Errorf("get: expected no error, but got %s", err)
				}
				if !bytes.Equal(val, c.value) {
					t.Errorf("expected key value %s to be %+v: got %+v", c.key, c.value, val)
				}
				if err := engine.Clear(c.key); err != nil {
					t.Errorf("delete: expected no error, but got %s", err)
				}
				val, err = engine.Get(c.key)
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

// TestEngineMerge tests that the passing through of engine merge operations
// to the goMerge function works as expected. The semantics are tested more
// exhaustively in the merge tests themselves.
func TestEngineMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
				result, _ := engine.Get(tc.testKey)
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

	test := func(engineType enginepb.EngineType, errStr string) {
		tempDir, dirCleanupFn := testutils.TempDir(t)
		defer dirCleanupFn()

		_, err := NewEngine(
			engineType,
			0, base.StorageConfig{
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

	test(enginepb.EngineTypeRocksDB, "does not exist (create_if_missing is false)")
	test(enginepb.EngineTypePebble, "no such file or directory")
}

func TestEngineTimeBound(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
				if err := engine.Put(key, []byte(s)); err != nil {
					t.Fatal(err)
				}
			}
			if err := engine.Flush(); err != nil {
				t.Fatal(err)
			}

			batch := engine.NewBatch()
			defer batch.Close()

			check := func(t *testing.T, tbi Iterator, keys, ssts int) {
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
				iter       Iterator
				keys, ssts int
			}{
				// Completely to the right, not touching.
				{
					iter: batch.NewIterator(IterOptions{
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
					iter: batch.NewIterator(IterOptions{
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
					iter: batch.NewIterator(IterOptions{
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
					iter: batch.NewIterator(IterOptions{
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
					iter: batch.NewIterator(IterOptions{
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
					iter: batch.NewIterator(IterOptions{
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
			iter := batch.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
			defer iter.Close()
			iter.SeekGE(NilKey)

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

func TestFlushWithSSTables(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			batch := engine.NewBatch()
			for i := 0; i < 10000; i++ {
				key := make([]byte, 4)
				binary.BigEndian.PutUint32(key, uint32(i))
				err := batch.Put(MVCCKey{Key: key}, []byte("foobar"))
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

			ssts := engine.GetSSTables()
			if len(ssts) == 0 {
				t.Fatal("expected non-zero sstables, got 0")
			}
		})
	}
}

func TestEngineScan1(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
				if err := engine.Put(c.key, c.value); err != nil {
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

			// Should return all key/value pairs in lexicographic order. Note that ""
			// is the lowest key possible and is a special case in engine.scan, that's
			// why we test it here.
			startKeys := []roachpb.Key{roachpb.Key("cat"), roachpb.Key("")}
			for _, startKey := range startKeys {
				keyvals, err = Scan(engine, startKey, roachpb.KeyMax, 0)
				if err != nil {
					t.Fatalf("could not run scan: %+v", err)
				}
				ensureRangeEqual(t, sortedKeys, keyMap, keyvals)
			}
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
			verifyScan(roachpb.KeyMin, roachpb.KeyMax, 10, keys[:5], engine, t)
			verifyScan(roachpb.Key("a"), roachpb.KeyMax, 10, keys[:5], engine, t)

			// Scan sub range.
			verifyScan(roachpb.Key("aab"), roachpb.Key("abcc"), 10, keys[3:5], engine, t)
			verifyScan(roachpb.Key("aa0"), roachpb.Key("abcc"), 10, keys[2:5], engine, t)

			// Scan with max values.
			verifyScan(roachpb.KeyMin, roachpb.KeyMax, 3, keys[:3], engine, t)
			verifyScan(roachpb.Key("a0"), roachpb.KeyMax, 3, keys[1:4], engine, t)

			// Scan with max value 0 gets all values.
			verifyScan(roachpb.KeyMin, roachpb.KeyMax, 0, keys[:5], engine, t)
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
			verifyScan(roachpb.KeyMin, roachpb.KeyMax, 10, keys[:5], engine, t)

			// Delete a range of keys
			if err := clearRange(engine, mvccKey("aa"), mvccKey("abc")); err != nil {
				t.Fatal(err)
			}
			// Verify what's left
			verifyScan(roachpb.KeyMin, roachpb.KeyMax, 10,
				[]MVCCKey{mvccKey("a"), mvccKey("abc")}, engine, t)
		})
	}
}

func TestEngineDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testEngineDeleteRange(t, func(engine Engine, start, end MVCCKey) error {
		return engine.ClearRange(start, end)
	})
}

func TestEngineDeleteRangeBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testEngineDeleteRange(t, func(engine Engine, start, end MVCCKey) error {
		batch := engine.NewWriteOnlyBatch()
		defer batch.Close()
		if err := batch.ClearRange(start, end); err != nil {
			return err
		}
		batch2 := engine.NewWriteOnlyBatch()
		defer batch2.Close()
		if err := batch2.ApplyBatchRepr(batch.Repr(), false); err != nil {
			return err
		}
		return batch2.Commit(false)
	})
}

func TestEngineDeleteIterRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testEngineDeleteRange(t, func(engine Engine, start, end MVCCKey) error {
		iter := engine.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
		defer iter.Close()
		return engine.ClearIterRange(iter, start.Key, end.Key)
	})
}

func TestSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			key := mvccKey("a")
			val1 := []byte("1")
			if err := engine.Put(key, val1); err != nil {
				t.Fatal(err)
			}
			val, _ := engine.Get(key)
			if !bytes.Equal(val, val1) {
				t.Fatalf("the value %s in get result does not match the value %s in request",
					val, val1)
			}

			snap := engine.NewSnapshot()
			defer snap.Close()

			val2 := []byte("2")
			if err := engine.Put(key, val2); err != nil {
				t.Fatal(err)
			}
			val, _ = engine.Get(key)
			valSnapshot, error := snap.Get(key)
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

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			keys := []MVCCKey{mvccKey("a"), mvccKey("b")}
			vals := [][]byte{[]byte("1"), []byte("2")}
			for i := range keys {
				if err := engine.Put(keys[i], vals[i]); err != nil {
					t.Fatal(err)
				}
				if val, err := engine.Get(keys[i]); err != nil {
					t.Fatal(err)
				} else if !bytes.Equal(vals[i], val) {
					t.Fatalf("expected %s, but found %s", vals[i], val)
				}
			}
			snap := engine.NewSnapshot()
			defer snap.Close()

			// Verify Get.
			for i := range keys {
				valSnapshot, err := snap.Get(keys[i])
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(vals[i], valSnapshot) {
					t.Fatalf("the value %s in get result does not match the value %s in snapshot",
						vals[i], valSnapshot)
				}
			}

			// Verify Scan.
			keyvals, _ := Scan(engine, roachpb.KeyMin, roachpb.KeyMax, 0)
			keyvalsSnapshot, err := Scan(snap, roachpb.KeyMin, roachpb.KeyMax, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(keyvals, keyvalsSnapshot) {
				t.Fatalf("the key/values %v in scan result does not match the value %s in snapshot",
					keyvals, keyvalsSnapshot)
			}

			// Verify Iterate.
			index := 0
			if err := snap.Iterate(roachpb.KeyMin, roachpb.KeyMax, func(kv MVCCKeyValue) (bool, error) {
				if !kv.Key.Equal(keys[index]) || !bytes.Equal(kv.Value, vals[index]) {
					t.Errorf("%d: key/value not equal between expected and snapshot: %s/%s, %s/%s",
						index, keys[index], vals[index], kv.Key, kv.Value)
				}
				index++
				return false, nil
			}); err != nil {
				t.Fatal(err)
			}

			// Write a new key to engine.
			newKey := mvccKey("c")
			newVal := []byte("3")
			if err := engine.Put(newKey, newVal); err != nil {
				t.Fatal(err)
			}

			// Verify NewIterator still iterates over original snapshot.
			iter := snap.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
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
		if err := engine.Put(keys[idx], val); err != nil {
			t.Errorf("put: expected no error, but got %s", err)
		}
	}
}

func TestCreateCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	rocksDB, err := NewRocksDB(
		RocksDBConfig{
			StorageConfig: base.StorageConfig{
				Settings: cluster.MakeTestingClusterSettings(),
				Dir:      dir,
			},
		},
		RocksDBCache{},
	)

	db := Engine(rocksDB) // be impl neutral from now on
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
	s := cluster.MakeTestingClusterSettings()

	max, ramp := time.Second*5, time.Second*5/10

	for _, tc := range []struct {
		exp   time.Duration
		stats Stats
	}{
		{0, Stats{}},
		{0, Stats{L0FileCount: 19, L0SublevelCount: -1}},
		{0, Stats{L0FileCount: 20, L0SublevelCount: -1}},
		{ramp, Stats{L0FileCount: 21, L0SublevelCount: -1}},
		{ramp * 2, Stats{L0FileCount: 22, L0SublevelCount: -1}},
		{ramp * 2, Stats{L0FileCount: 22, L0SublevelCount: 22}},
		{ramp * 2, Stats{L0FileCount: 55, L0SublevelCount: 22}},
		{max, Stats{L0FileCount: 55, L0SublevelCount: -1}},
	} {
		require.Equal(t, tc.exp, calculatePreIngestDelay(s, &tc.stats))
	}
}

type stringSorter []string

func (s stringSorter) Len() int               { return len(s) }
func (s stringSorter) Swap(i int, j int)      { s[i], s[j] = s[j], s[i] }
func (s stringSorter) Less(i int, j int) bool { return strings.Compare(s[i], s[j]) < 0 }

func TestEngineFS(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
					if os.IsExist(err) {
						actualErrStr = "exists"
					} else if os.IsNotExist(err) {
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
	{"rocksdb", func(t *testing.T, dir string) Engine {
		db, err := NewRocksDB(
			RocksDBConfig{
				StorageConfig: base.StorageConfig{
					Settings: cluster.MakeTestingClusterSettings(),
					Dir:      dir,
				},
			},
			RocksDBCache{},
		)
		if err != nil {
			t.Fatalf("could not create new rocksdb instance at %s: %+v", dir, err)
		}
		return db
	}},
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

	for _, engineImpl := range engineRealFSImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			dir, dirCleanup := testutils.TempDir(t)
			defer dirCleanup()
			db := engineImpl.create(t, dir)
			defer db.Close()

			// Verify Remove returns os.ErrNotExist if file does not exist.
			if err := db.Remove("/non/existent/file"); !os.IsNotExist(err) {
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
			if _, err := db.ReadFile(fname); !os.IsNotExist(err) {
				t.Fatalf("expected IsNotExist, but got %v (%T)", err, err)
			}

			// Verify Remove returns os.ErrNotExist if deleting an already deleted file.
			if err := db.Remove(fname); !os.IsNotExist(err) {
				t.Fatalf("expected IsNotExist, but got %v (%T)", err, err)
			}
		})
	}
}

func TestFS(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var engineImpls []engineImpl
	engineImpls = append(engineImpls, engineRealFSImpls...)
	engineImpls = append(engineImpls,
		engineImpl{
			name: "rocksdb_mem",
			create: func(_ *testing.T, _ string) Engine {
				return createTestRocksDBEngine()
			},
		},
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
			if _, err := fs.Stat(path("a/b/c")); !os.IsNotExist(err) {
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
