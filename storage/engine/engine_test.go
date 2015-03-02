// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package engine

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

func ensureRangeEqual(t *testing.T, sortedKeys []string, keyMap map[string][]byte, keyvals []proto.RawKeyValue) {
	if len(keyvals) != len(sortedKeys) {
		t.Errorf("length mismatch. expected %s, got %s", sortedKeys, keyvals)
	}
	t.Log("---")
	for i, kv := range keyvals {
		t.Logf("index: %d\tk: %q\tv: %q\n", i, kv.Key, kv.Value)
		if sortedKeys[i] != string(kv.Key) {
			t.Errorf("key mismatch at index %d: expected %q, got %q", i, sortedKeys[i], kv.Key)
		}
		if !bytes.Equal(keyMap[sortedKeys[i]], kv.Value) {
			t.Errorf("value mismatch at index %d: expected %q, got %q", i, keyMap[sortedKeys[i]], kv.Value)
		}
	}
}

var (
	inMemAttrs   = proto.Attributes{Attrs: []string{"mem"}}
	rocksDBAttrs = proto.Attributes{Attrs: []string{"ssd"}}
)

// runWithAllEngines creates a new engine of each supported type and
// invokes the supplied test func with each instance.
func runWithAllEngines(test func(e Engine, t *testing.T), t *testing.T) {
	inMem := NewInMem(inMemAttrs, 10<<20)

	loc := fmt.Sprintf("%s/data_%d", os.TempDir(), time.Now().UnixNano())
	rocksdb := NewRocksDB(rocksDBAttrs, loc, testCacheSize)
	err := rocksdb.Start()
	if err != nil {
		t.Fatalf("could not create new rocksdb db instance at %s: %v", loc, err)
	}
	defer func(t *testing.T) {
		rocksdb.Stop()
		if err := rocksdb.Destroy(); err != nil {
			t.Errorf("could not delete rocksdb db at %s: %v", loc, err)
		}
	}(t)

	test(inMem, t)
	test(rocksdb, t)
}

// TestEngineWriteBatch writes a batch containing 10K rows (all the
// same key) and concurrently attempts to read the value in a tight
// loop. The test verifies that either there is no value for the key
// or it contains the final value, but never a value in between.
func TestEngineWriteBatch(t *testing.T) {
	numWrites := 10000
	key := proto.EncodedKey("a")
	finalVal := []byte(strconv.Itoa(numWrites - 1))

	runWithAllEngines(func(e Engine, t *testing.T) {
		// Start a concurrent read operation in a busy loop.
		readsBegun := make(chan struct{})
		readsDone := make(chan struct{})
		writesDone := make(chan struct{})
		go func() {
			for i := 0; ; i++ {
				select {
				case <-writesDone:
					close(readsDone)
					return
				default:
					val, err := e.Get(key)
					if err != nil {
						t.Fatal(err)
					}
					if val != nil && bytes.Compare(val, finalVal) != 0 {
						close(readsDone)
						t.Fatalf("key value should be empty or %q; got %q", string(finalVal), string(val))
					}
					if i == 0 {
						close(readsBegun)
					}
				}
			}
		}()
		// Wait until we've succeeded with first read.
		<-readsBegun

		// Create key/values and put them in a batch to engine.
		puts := make([]interface{}, numWrites, numWrites)
		for i := 0; i < numWrites; i++ {
			puts[i] = BatchPut{proto.RawKeyValue{Key: key, Value: []byte(strconv.Itoa(i))}}
		}
		if err := e.WriteBatch(puts); err != nil {
			t.Fatal(err)
		}
		close(writesDone)
		<-readsDone
	}, t)
}

func TestEngineBatch(t *testing.T) {
	runWithAllEngines(func(engine Engine, t *testing.T) {
		numShuffles := 100
		key := proto.EncodedKey("a")
		// Those are randomized below.
		batch := []interface{}{
			BatchPut{proto.RawKeyValue{Key: key, Value: appender("~ockroachDB")}},
			BatchPut{proto.RawKeyValue{Key: key, Value: appender("C~ckroachDB")}},
			BatchPut{proto.RawKeyValue{Key: key, Value: appender("Co~kroachDB")}},
			BatchPut{proto.RawKeyValue{Key: key, Value: appender("Coc~roachDB")}},
			BatchPut{proto.RawKeyValue{Key: key, Value: appender("Cock~oachDB")}},
			BatchPut{proto.RawKeyValue{Key: key, Value: appender("Cockr~achDB")}},
			BatchPut{proto.RawKeyValue{Key: key, Value: appender("Cockro~chDB")}},
			BatchPut{proto.RawKeyValue{Key: key, Value: appender("Cockroa~hDB")}},
			BatchPut{proto.RawKeyValue{Key: key, Value: appender("Cockroac~DB")}},
			BatchPut{proto.RawKeyValue{Key: key, Value: appender("Cockroach~B")}},
			BatchPut{proto.RawKeyValue{Key: key, Value: appender("CockroachD~")}},
			BatchDelete{proto.RawKeyValue{Key: key}},
			BatchMerge{proto.RawKeyValue{Key: key, Value: appender("C")}},
			BatchMerge{proto.RawKeyValue{Key: key, Value: appender(" o")}},
			BatchMerge{proto.RawKeyValue{Key: key, Value: appender("  c")}},
			BatchMerge{proto.RawKeyValue{Key: key, Value: appender(" k")}},
			BatchMerge{proto.RawKeyValue{Key: key, Value: appender("r")}},
			BatchMerge{proto.RawKeyValue{Key: key, Value: appender(" o")}},
			BatchMerge{proto.RawKeyValue{Key: key, Value: appender("  a")}},
			BatchMerge{proto.RawKeyValue{Key: key, Value: appender(" c")}},
			BatchMerge{proto.RawKeyValue{Key: key, Value: appender("h")}},
			BatchMerge{proto.RawKeyValue{Key: key, Value: appender(" D")}},
			BatchMerge{proto.RawKeyValue{Key: key, Value: appender("  B")}},
		}

		for i := 0; i < numShuffles; i++ {
			// In each run, create an array of shuffled operations.
			shuffledIndices := rand.Perm(len(batch))
			currentBatch := make([]interface{}, len(batch))
			for k := range currentBatch {
				currentBatch[k] = batch[shuffledIndices[k]]
			}
			// Reset the key
			engine.Clear(key)
			// Run it once with individual operations and remember the result.
			for i, op := range currentBatch {
				if err := engine.WriteBatch([]interface{}{op}); err != nil {
					t.Errorf("batch test: %d: op %v: %v", i, op, err)
					continue
				}
			}
			correctValue, _ := engine.Get(key)
			// Run the whole thing as a batch and compare.
			if err := engine.WriteBatch(currentBatch); err != nil {
				t.Errorf("batch test: %d: %v", i, err)
				continue
			}
			actualValue, _ := engine.Get(key)
			if !bytes.Equal(actualValue, correctValue) {
				t.Errorf("batch test: %d: result inconsistent", i)
			}
		}
	}, t)
}

func TestEnginePutGetDelete(t *testing.T) {
	runWithAllEngines(func(engine Engine, t *testing.T) {
		// Test for correct handling of empty keys, which should produce errors.
		for i, err := range []error{
			engine.Put([]byte(""), []byte("")),
			engine.Put(nil, []byte("")),
			func() error {
				_, err := engine.Get([]byte(""))
				return err
			}(),
			engine.Clear(nil),
			func() error {
				_, err := engine.Get(nil)
				return err
			}(),
			engine.Clear(nil),
			engine.Clear([]byte("")),
		} {
			if err == nil {
				t.Fatalf("%d: illegal handling of empty key", i)
			}
		}

		// Test for allowed keys, which should go through.
		testCases := []struct {
			key, value []byte
		}{
			{[]byte("dog"), []byte("woof")},
			{[]byte("cat"), []byte("meow")},
			{[]byte("emptyval"), nil},
			{[]byte("emptyval2"), []byte("")},
			{[]byte("server"), []byte("42")},
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
	}, t)
}

// TestEngineMerge tests that the passing through of engine merge operations
// to the goMerge function works as expected. The semantics are tested more
// exhaustively in the merge tests themselves.
func TestEngineMerge(t *testing.T) {
	runWithAllEngines(func(engine Engine, t *testing.T) {
		testcases := []struct {
			testKey  proto.EncodedKey
			merges   [][]byte
			expected []byte
		}{
			{
				proto.EncodedKey("haste not in life"),
				[][]byte{
					appender("x"),
					appender("y"),
					appender("z"),
				},
				appender("xyz"),
			},
			{
				proto.EncodedKey("timeseriesmerged"),
				[][]byte{
					timeSeriesInt(testtime, 1000, []tsIntSample{
						{1, 1, 5, 5, 5},
					}...),
					timeSeriesInt(testtime, 1000, []tsIntSample{
						{2, 1, 5, 5, 5},
						{1, 2, 10, 7, 3},
					}...),
					timeSeriesInt(testtime, 1000, []tsIntSample{
						{10, 1, 5, 5, 5},
					}...),
					timeSeriesInt(testtime, 1000, []tsIntSample{
						{5, 1, 5, 5, 5},
						{3, 1, 5, 5, 5},
					}...),
				},
				timeSeriesInt(testtime, 1000, []tsIntSample{
					{1, 3, 15, 7, 3},
					{2, 1, 5, 5, 5},
					{3, 1, 5, 5, 5},
					{5, 1, 5, 5, 5},
					{10, 1, 5, 5, 5},
				}...),
			},
		}
		for _, tc := range testcases {
			for i, update := range tc.merges {
				if err := engine.Merge(tc.testKey, update); err != nil {
					t.Fatalf("%d: %v", i, err)
				}
			}
			result, _ := engine.Get(tc.testKey)
			var resultV, expectedV proto.MVCCMetadata
			gogoproto.Unmarshal(result, &resultV)
			gogoproto.Unmarshal(tc.expected, &expectedV)
			if !reflect.DeepEqual(resultV, expectedV) {
				t.Errorf("unexpected append-merge result: %v != %v", resultV, expectedV)
			}
		}
	}, t)
}

func TestEngineScan1(t *testing.T) {
	runWithAllEngines(func(engine Engine, t *testing.T) {
		testCases := []struct {
			key, value []byte
		}{
			{[]byte("dog"), []byte("woof")},
			{[]byte("cat"), []byte("meow")},
			{[]byte("server"), []byte("42")},
			{[]byte("french"), []byte("Allô?")},
			{[]byte("german"), []byte("hallo")},
			{[]byte("chinese"), []byte("你好")},
		}
		keyMap := map[string][]byte{}
		for _, c := range testCases {
			if err := engine.Put(c.key, c.value); err != nil {
				t.Errorf("could not put key %q: %v", c.key, err)
			}
			keyMap[string(c.key)] = c.value
		}
		sortedKeys := make([]string, len(testCases))
		for i, t := range testCases {
			sortedKeys[i] = string(t.key)
		}
		sort.Strings(sortedKeys)

		keyvals, err := Scan(engine, []byte("chinese"), []byte("german"), 0)
		if err != nil {
			t.Fatalf("could not run scan: %v", err)
		}
		ensureRangeEqual(t, sortedKeys[1:4], keyMap, keyvals)

		// Check an end of range which does not equal an existing key.
		keyvals, err = Scan(engine, []byte("chinese"), []byte("german1"), 0)
		if err != nil {
			t.Fatalf("could not run scan: %v", err)
		}
		ensureRangeEqual(t, sortedKeys[1:5], keyMap, keyvals)

		keyvals, err = Scan(engine, []byte("chinese"), []byte("german"), 2)
		if err != nil {
			t.Fatalf("could not run scan: %v", err)
		}
		ensureRangeEqual(t, sortedKeys[1:3], keyMap, keyvals)

		// Should return all key/value pairs in lexicographic order.
		// Note that []byte("") is the lowest key possible and is
		// a special case in engine.scan, that's why we test it here.
		startKeys := []proto.EncodedKey{proto.EncodedKey("cat"), proto.EncodedKey("")}
		for _, startKey := range startKeys {
			keyvals, err := Scan(engine, startKey, proto.EncodedKey(KeyMax), 0)
			if err != nil {
				t.Fatalf("could not run scan: %v", err)
			}
			ensureRangeEqual(t, sortedKeys, keyMap, keyvals)
		}
	}, t)
}

func TestEngineIncrement(t *testing.T) {
	runWithAllEngines(func(engine Engine, t *testing.T) {
		// Start with increment of an empty key.
		val, err := Increment(engine, proto.EncodedKey("a"), 1)
		if err != nil {
			t.Fatal(err)
		}
		if val != 1 {
			t.Errorf("expected increment to be %d; got %d", 1, val)
		}
		// Increment same key by 1.
		if val, err = Increment(engine, proto.EncodedKey("a"), 1); err != nil {
			t.Fatal(err)
		}
		if val != 2 {
			t.Errorf("expected increment to be %d; got %d", 2, val)
		}
		// Increment same key by 2.
		if val, err = Increment(engine, proto.EncodedKey("a"), 2); err != nil {
			t.Fatal(err)
		}
		if val != 4 {
			t.Errorf("expected increment to be %d; got %d", 4, val)
		}
		// Decrement same key by -1.
		if val, err = Increment(engine, proto.EncodedKey("a"), -1); err != nil {
			t.Fatal(err)
		}
		if val != 3 {
			t.Errorf("expected increment to be %d; got %d", 3, val)
		}
		// Increment same key by max int64 value to cause overflow; should return error.
		if val, err = Increment(engine, proto.EncodedKey("a"), math.MaxInt64); err == nil {
			t.Error("expected an overflow error")
		}
		if val, err = Increment(engine, proto.EncodedKey("a"), 0); err != nil {
			t.Fatal(err)
		}
		if val != 3 {
			t.Errorf("expected increment to be %d; got %d", 3, val)
		}
	}, t)
}

func verifyScan(start, end proto.EncodedKey, max int64, expKeys []proto.EncodedKey, engine Engine, t *testing.T) {
	kvs, err := Scan(engine, start, end, max)
	if err != nil {
		t.Errorf("scan %q-%q: expected no error, but got %s", string(start), string(end), err)
	}
	if len(kvs) != len(expKeys) {
		t.Errorf("scan %q-%q: expected scanned keys mismatch %d != %d: %v",
			start, end, len(kvs), len(expKeys), kvs)
	}
	for i, kv := range kvs {
		if !bytes.Equal(kv.Key, expKeys[i]) {
			t.Errorf("scan %q-%q: expected keys equal %q != %q", string(start), string(end),
				string(kv.Key), string(expKeys[i]))
		}
	}
}

func TestEngineScan2(t *testing.T) {
	// TODO(Tobias): Merge this with TestEngineScan1 and remove
	// either verifyScan or the other helper function.
	runWithAllEngines(func(engine Engine, t *testing.T) {
		keys := []proto.EncodedKey{
			proto.EncodedKey("a"),
			proto.EncodedKey("aa"),
			proto.EncodedKey("aaa"),
			proto.EncodedKey("ab"),
			proto.EncodedKey("abc"),
			proto.EncodedKey(KeyMax),
		}

		insertKeys(keys, engine, t)

		// Scan all keys (non-inclusive of final key).
		verifyScan(proto.EncodedKey(KeyMin), proto.EncodedKey(KeyMax), 10, keys[0:5], engine, t)
		verifyScan(proto.EncodedKey("a"), proto.EncodedKey(KeyMax), 10, keys[0:5], engine, t)

		// Scan sub range.
		verifyScan(proto.EncodedKey("aab"), proto.EncodedKey("abcc"), 10, keys[3:5], engine, t)
		verifyScan(proto.EncodedKey("aa0"), proto.EncodedKey("abcc"), 10, keys[2:5], engine, t)

		// Scan with max values.
		verifyScan(proto.EncodedKey(KeyMin), proto.EncodedKey(KeyMax), 3, keys[0:3], engine, t)
		verifyScan(proto.EncodedKey("a0"), proto.EncodedKey(KeyMax), 3, keys[1:4], engine, t)

		// Scan with max value 0 gets all values.
		verifyScan(proto.EncodedKey(KeyMin), proto.EncodedKey(KeyMax), 0, keys[0:5], engine, t)
	}, t)
}

func TestEngineDeleteRange(t *testing.T) {
	runWithAllEngines(func(engine Engine, t *testing.T) {
		keys := []proto.EncodedKey{
			proto.EncodedKey("a"),
			proto.EncodedKey("aa"),
			proto.EncodedKey("aaa"),
			proto.EncodedKey("ab"),
			proto.EncodedKey("abc"),
			proto.EncodedKey(KeyMax),
		}

		insertKeys(keys, engine, t)

		// Scan all keys (non-inclusive of final key).
		verifyScan(proto.EncodedKey(KeyMin), proto.EncodedKey(KeyMax), 10, keys[0:5], engine, t)

		// Delete a range of keys
		numDeleted, err := ClearRange(engine, proto.EncodedKey("aa"), proto.EncodedKey("abc"))
		// Verify what was deleted
		if err != nil {
			t.Error("Not expecting an error")
		}
		if numDeleted != 3 {
			t.Errorf("Expected to delete 3 entries; was %v", numDeleted)
		}
		// Verify what's left
		verifyScan(proto.EncodedKey(KeyMin), proto.EncodedKey(KeyMax), 10,
			[]proto.EncodedKey{proto.EncodedKey("a"), proto.EncodedKey("abc")}, engine, t)
	}, t)
}

func TestSnapshot(t *testing.T) {
	runWithAllEngines(func(engine Engine, t *testing.T) {
		key := []byte("a")
		val1 := []byte("1")
		engine.Put(key, val1)
		val, _ := engine.Get(key)
		if !bytes.Equal(val, val1) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				val, val1)
		}

		snap := engine.NewSnapshot()
		defer snap.Stop()

		val2 := []byte("2")
		engine.Put(key, val2)
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

		keyvals, _ := Scan(engine, key, proto.EncodedKey(KeyMax), 0)
		keyvalsSnapshot, error := Scan(snap, key, proto.EncodedKey(KeyMax), 0)
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
	}, t)
}

// TestSnapshotMethods verifies that snapshots allow only read-only
// engine operations.
func TestSnapshotMethods(t *testing.T) {
	runWithAllEngines(func(engine Engine, t *testing.T) {
		keys := [][]byte{[]byte("a"), []byte("b")}
		vals := [][]byte{[]byte("1"), []byte("2")}
		for i := range keys {
			engine.Put(keys[i], vals[i])
		}
		snap := engine.NewSnapshot()
		defer snap.Stop()

		// Verify Attrs.
		var attrs proto.Attributes
		switch engine.(type) {
		case *InMem:
			attrs = inMemAttrs
		case *RocksDB:
			attrs = rocksDBAttrs
		}
		if !reflect.DeepEqual(engine.Attrs(), attrs) {
			t.Errorf("attrs mismatch; expected %+v, got %+v", attrs, engine.Attrs())
		}

		// Verify Put is error.
		if err := snap.Put([]byte("c"), []byte("3")); err == nil {
			t.Error("expected error on Put to snapshot")
		}

		// Verify Get.
		valSnapshot, err := snap.Get(keys[0])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(vals[0], valSnapshot) {
			t.Fatalf("the value %s in get result does not match the value %s in snapshot",
				vals[0], valSnapshot)
		}

		// Verify Scan.
		keyvals, _ := Scan(engine, proto.EncodedKey(KeyMin), proto.EncodedKey(KeyMax), 0)
		keyvalsSnapshot, err := Scan(snap, proto.EncodedKey(KeyMin), proto.EncodedKey(KeyMax), 0)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(keyvals, keyvalsSnapshot) {
			t.Fatalf("the key/values %v in scan result does not match the value %s in snapshot",
				keyvals, keyvalsSnapshot)
		}

		// Verify Iterate.
		index := 0
		if err := snap.Iterate(proto.EncodedKey(KeyMin), proto.EncodedKey(KeyMax), func(kv proto.RawKeyValue) (bool, error) {
			if !bytes.Equal(kv.Key, keys[index]) || !bytes.Equal(kv.Value, vals[index]) {
				t.Errorf("%d: key/value not equal between expected and snapshot: %s/%s, %s/%s",
					index, keys[index], vals[index], kv.Key, kv.Value)
			}
			index++
			return false, nil
		}); err != nil {
			t.Fatal(err)
		}

		// Verify Clear is error.
		if err := snap.Clear(keys[0]); err == nil {
			t.Error("expected error on Clear to snapshot")
		}

		// Verify WriteBatch is error.
		if err := snap.WriteBatch([]interface{}{BatchDelete{proto.RawKeyValue{Key: keys[0]}}}); err == nil {
			t.Error("expected error on WriteBatch to snapshot")
		}

		// Verify Merge is error.
		if err := snap.Merge([]byte("merge-key"), appender("x")); err == nil {
			t.Error("expected error on Merge to snapshot")
		}

		// Verify Capacity.
		capacity, err := engine.Capacity()
		if err != nil {
			t.Fatal(err)
		}
		capacitySnapshot, err := snap.Capacity()
		if err != nil {
			t.Fatal(err)
		}
		// The Available fields of capacity may differ due to processes beyond our control.
		if capacity.Capacity != capacitySnapshot.Capacity {
			t.Errorf("expected capacities to be equal: %v != %v",
				capacity.Capacity, capacitySnapshot.Capacity)
		}

		// Verify ApproximateSize.
		approx, err := engine.ApproximateSize(proto.EncodedKey(KeyMin), proto.EncodedKey(KeyMax))
		if err != nil {
			t.Fatal(err)
		}
		approxSnapshot, err := snap.ApproximateSize(proto.EncodedKey(KeyMin), proto.EncodedKey(KeyMax))
		if err != nil {
			t.Fatal(err)
		}
		if approx != approxSnapshot {
			t.Errorf("expected approx sizes to be equal: %d != %d", approx, approxSnapshot)
		}

		// Write a new key to engine.
		newKey := []byte("c")
		newVal := []byte("3")
		if err := engine.Put(newKey, newVal); err != nil {
			t.Fatal(err)
		}

		// Verify NewIterator still iterates over original snapshot.
		iter := snap.NewIterator()
		iter.Seek(newKey)
		if iter.Valid() {
			t.Error("expected invalid iterator when seeking to element which shouldn't be visible to snapshot")
		}
		iter.Close()

		// Verify Commit is error.
		if err := snap.Commit(); err == nil {
			t.Error("expected error on Commit to snapshot")
		}
	}, t)
}

// TestSnapshotNewSnapshot panics.
func TestSnapshotNewSnapshot(t *testing.T) {
	runWithAllEngines(func(engine Engine, t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic")
			}
		}()
		snap := engine.NewSnapshot()
		defer snap.Stop()
		snap.NewSnapshot()
	}, t)
}

// TestSnapshotNewBatch panics.
func TestSnapshotNewBatch(t *testing.T) {
	runWithAllEngines(func(engine Engine, t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic")
			}
		}()
		snap := engine.NewSnapshot()
		defer snap.Stop()
		snap.NewBatch()
	}, t)
}

func TestApproximateSize(t *testing.T) {
	runWithAllEngines(func(engine Engine, t *testing.T) {
		var (
			count    = 10000
			keys     = make([]proto.EncodedKey, count)
			values   = make([][]byte, count) // Random values to prevent compression
			rand     = util.NewPseudoRand()
			valueLen = 10
		)
		for i := 0; i < count; i++ {
			keys[i] = []byte(fmt.Sprintf("key%8d", i))
			values[i] = []byte(util.RandString(rand, valueLen))
		}

		insertKeysAndValues(keys, values, engine, t)
		if rocksdb, ok := engine.(*RocksDB); ok {
			err := rocksdb.Flush()
			if err != nil {
				t.Fatalf("Error flushing RocksDB: %s", err.Error())
			}
		}

		sizePerRecord := (len([]byte(keys[0])) + valueLen)
		verifyApproximateSize(keys, engine, sizePerRecord, 0.15, t)
		verifyApproximateSize(keys[:count/2], engine, sizePerRecord, 0.15, t)
		verifyApproximateSize(keys[:count/4], engine, sizePerRecord, 0.15, t)
	}, t)
}

func insertKeys(keys []proto.EncodedKey, engine Engine, t *testing.T) {
	insertKeysAndValues(keys, nil, engine, t)
}

func insertKeysAndValues(keys []proto.EncodedKey, values [][]byte, engine Engine, t *testing.T) {
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

func verifyApproximateSize(keys []proto.EncodedKey, engine Engine, sizePerRecord int, ratio float64, t *testing.T) {
	sz, err := engine.ApproximateSize(keys[0], keys[len(keys)-1])
	if err != nil {
		t.Errorf("Error from ApproximateSize(): %s", err)
	}

	uncompressedTotalSize := uint64(sizePerRecord * len(keys))
	// On-disk size may be lower than expected total size due to compression.
	// If compression is disabled (e.g. snappy auto-detects uncompressable
	// input and disables compression), the total size will be higher due to
	// storage overhead.
	minSize := uint64(float64(uncompressedTotalSize) * (float64(1) - ratio))
	maxSize := uint64(float64(uncompressedTotalSize) * (float64(1) + ratio))
	if sz < minSize || sz > maxSize {
		t.Errorf("ApproximateSize %d outside of acceptable bounds %d - %d", sz, minSize, maxSize)
	}
}
