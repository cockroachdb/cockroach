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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package engine

import (
	"bytes"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func ensureRangeEqual(
	t *testing.T, sortedKeys []string, keyMap map[string][]byte, keyvals []MVCCKeyValue,
) {
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

var (
	inMemAttrs = roachpb.Attributes{Attrs: []string{"mem"}}
)

// runWithAllEngines creates a new engine of each supported type and
// invokes the supplied test func with each instance.
func runWithAllEngines(test func(e Engine, t *testing.T), t *testing.T) {
	stopper := stop.NewStopper()
	defer stopper.Stop()
	inMem := NewInMem(inMemAttrs, testCacheSize)
	stopper.AddCloser(inMem)
	test(inMem, t)
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

	runWithAllEngines(func(e Engine, t *testing.T) {
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
		if err := batch.Commit(false /* !sync */); err != nil {
			t.Fatal(err)
		}
		close(writesDone)
		if err := <-readsDone; err != nil {
			t.Fatal(err)
		}
	}, t)
}

func TestEngineBatchStaleCachedIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Prevent regression of a bug which caused spurious MVCC errors due to an
	// invalid optimization which let an iterator return key-value pairs which
	// had since been deleted from the underlying engine.
	// Discovered in #6878.
	runWithAllEngines(func(eng Engine, t *testing.T) {
		// Focused failure mode: highlights the actual bug.
		{
			batch := eng.NewBatch()
			defer batch.Close()
			iter := batch.NewIterator(false)
			key := MVCCKey{Key: roachpb.Key("b")}

			if err := batch.Put(key, []byte("foo")); err != nil {
				t.Fatal(err)
			}

			iter.Seek(key)

			if err := batch.Clear(key); err != nil {
				t.Fatal(err)
			}

			// Iterator should not reuse its cached result.
			iter.Seek(key)

			if iter.Valid() {
				t.Fatalf("iterator unexpectedly valid: %v -> %v",
					iter.unsafeKey(), iter.unsafeValue())
			}
		}

		// Higher-level failure mode. Mostly for documentation.
		{
			batch := eng.NewBatch().(*rocksDBBatch)
			defer batch.Close()

			key := roachpb.Key("z")

			// Put a value so that the deletion below finds a value to seek
			// to.
			if err := MVCCPut(context.Background(), batch, nil, key, hlc.ZeroTimestamp,
				roachpb.MakeValueFromString("x"), nil); err != nil {
				t.Fatal(err)
			}

			// Seek the iterator to `key` and clear the value (but without
			// telling the iterator about that).
			if err := MVCCDelete(context.Background(), batch, nil, key,
				hlc.ZeroTimestamp, nil); err != nil {
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
				hlc.ZeroTimestamp, true, nil); err != nil {
				t.Fatal(err)
			} else if v != nil {
				t.Fatalf("expected no value, got %+v", v)
			}
		}
	}, t)
}

func TestEngineBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runWithAllEngines(func(engine Engine, t *testing.T) {
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

		apply := func(eng ReadWriter, d data) error {
			if d.value == nil {
				return eng.Clear(d.key)
			} else if d.merge {
				return eng.Merge(d.key, d.value)
			}
			return eng.Put(d.key, d.value)
		}

		get := func(eng ReadWriter, key MVCCKey) []byte {
			b, err := eng.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			var m enginepb.MVCCMetadata
			if err := proto.Unmarshal(b, &m); err != nil {
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
					t.Errorf("%d: op %v: %v", i, op, err)
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
			iter := b.NewIterator(false)
			iter.Seek(key)
			if !iter.Valid() {
				if currentBatch[len(currentBatch)-1].value != nil {
					t.Errorf("%d: batch seek invalid", i)
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
			if err := b.Commit(false /* !sync */); err != nil {
				t.Errorf("%d: %v", i, err)
				continue
			}
			actualValue = get(engine, key)
			if !bytes.Equal(actualValue, expectedValue) {
				t.Errorf("%d: expected %s, but got %s", i, expectedValue, actualValue)
			}
		}
	}, t)
}

func TestEnginePutGetDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runWithAllEngines(func(engine Engine, t *testing.T) {
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
	}, t)
}

// TestEngineMerge tests that the passing through of engine merge operations
// to the goMerge function works as expected. The semantics are tested more
// exhaustively in the merge tests themselves.
func TestEngineMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runWithAllEngines(func(engine Engine, t *testing.T) {
		testcases := []struct {
			testKey  MVCCKey
			merges   [][]byte
			expected []byte
		}{
			{
				mvccKey("haste not in life"),
				[][]byte{
					appender("x"),
					appender("y"),
					appender("z"),
				},
				appender("xyz"),
			},
			{
				mvccKey("timeseriesmerged"),
				[][]byte{
					timeSeries(testtime, 1000, []tsSample{
						{1, 1, 5, 5, 5},
					}...),
					timeSeries(testtime, 1000, []tsSample{
						{2, 1, 5, 5, 5},
						{1, 2, 10, 7, 3},
					}...),
					timeSeries(testtime, 1000, []tsSample{
						{10, 1, 5, 5, 5},
					}...),
					timeSeries(testtime, 1000, []tsSample{
						{5, 1, 5, 5, 5},
						{3, 1, 5, 5, 5},
					}...),
				},
				timeSeries(testtime, 1000, []tsSample{
					{1, 2, 10, 7, 3},
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
			var resultV, expectedV enginepb.MVCCMetadata
			if err := proto.Unmarshal(result, &resultV); err != nil {
				t.Fatal(err)
			}
			if err := proto.Unmarshal(tc.expected, &expectedV); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(resultV, expectedV) {
				t.Errorf("unexpected append-merge result: %v != %v", resultV, expectedV)
			}
		}
	}, t)
}

func TestEngineScan1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runWithAllEngines(func(engine Engine, t *testing.T) {
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
				t.Errorf("could not put key %q: %v", c.key, err)
			}
			keyMap[string(c.key.Key)] = c.value
		}
		sortedKeys := make([]string, len(testCases))
		for i, t := range testCases {
			sortedKeys[i] = string(t.key.Key)
		}
		sort.Strings(sortedKeys)

		keyvals, err := Scan(engine, mvccKey("chinese"), mvccKey("german"), 0)
		if err != nil {
			t.Fatalf("could not run scan: %v", err)
		}
		ensureRangeEqual(t, sortedKeys[1:4], keyMap, keyvals)

		// Check an end of range which does not equal an existing key.
		keyvals, err = Scan(engine, mvccKey("chinese"), mvccKey("german1"), 0)
		if err != nil {
			t.Fatalf("could not run scan: %v", err)
		}
		ensureRangeEqual(t, sortedKeys[1:5], keyMap, keyvals)

		keyvals, err = Scan(engine, mvccKey("chinese"), mvccKey("german"), 2)
		if err != nil {
			t.Fatalf("could not run scan: %v", err)
		}
		ensureRangeEqual(t, sortedKeys[1:3], keyMap, keyvals)

		// Should return all key/value pairs in lexicographic order. Note that ""
		// is the lowest key possible and is a special case in engine.scan, that's
		// why we test it here.
		startKeys := []MVCCKey{mvccKey("cat"), mvccKey("")}
		for _, startKey := range startKeys {
			keyvals, err = Scan(engine, startKey, mvccKey(roachpb.RKeyMax), 0)
			if err != nil {
				t.Fatalf("could not run scan: %v", err)
			}
			ensureRangeEqual(t, sortedKeys, keyMap, keyvals)
		}
	}, t)
}

func verifyScan(start, end MVCCKey, max int64, expKeys []MVCCKey, engine Engine, t *testing.T) {
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
	runWithAllEngines(func(engine Engine, t *testing.T) {
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
		verifyScan(mvccKey(roachpb.RKeyMin), mvccKey(roachpb.RKeyMax), 10, keys[:5], engine, t)
		verifyScan(mvccKey("a"), mvccKey(roachpb.RKeyMax), 10, keys[:5], engine, t)

		// Scan sub range.
		verifyScan(mvccKey("aab"), mvccKey("abcc"), 10, keys[3:5], engine, t)
		verifyScan(mvccKey("aa0"), mvccKey("abcc"), 10, keys[2:5], engine, t)

		// Scan with max values.
		verifyScan(mvccKey(roachpb.RKeyMin), mvccKey(roachpb.RKeyMax), 3, keys[:3], engine, t)
		verifyScan(mvccKey("a0"), mvccKey(roachpb.RKeyMax), 3, keys[1:4], engine, t)

		// Scan with max value 0 gets all values.
		verifyScan(mvccKey(roachpb.RKeyMin), mvccKey(roachpb.RKeyMax), 0, keys[:5], engine, t)
	}, t)
}

func testEngineDeleteRange(t *testing.T, clearRange func(engine Engine, start, end MVCCKey) error) {
	runWithAllEngines(func(engine Engine, t *testing.T) {
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
		verifyScan(mvccKey(roachpb.RKeyMin), mvccKey(roachpb.RKeyMax), 10, keys[:5], engine, t)

		// Delete a range of keys
		if err := clearRange(engine, mvccKey("aa"), mvccKey("abc")); err != nil {
			t.Error("Not expecting an error")
		}
		// Verify what's left
		verifyScan(mvccKey(roachpb.RKeyMin), mvccKey(roachpb.RKeyMax), 10,
			[]MVCCKey{mvccKey("a"), mvccKey("abc")}, engine, t)
	}, t)

}

func TestEngineDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testEngineDeleteRange(t, func(engine Engine, start, end MVCCKey) error {
		return engine.ClearRange(start, end)
	})
}

func TestEngineDeleteIterRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testEngineDeleteRange(t, func(engine Engine, start, end MVCCKey) error {
		iter := engine.NewIterator(false)
		defer iter.Close()
		return engine.ClearIterRange(iter, start, end)
	})
}

func TestSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runWithAllEngines(func(engine Engine, t *testing.T) {
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

		keyvals, _ := Scan(engine, key, mvccKey(roachpb.RKeyMax), 0)
		keyvalsSnapshot, error := Scan(snap, key, mvccKey(roachpb.RKeyMax), 0)
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
	defer leaktest.AfterTest(t)()
	runWithAllEngines(func(engine Engine, t *testing.T) {
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

		// Verify Attrs.
		var attrs roachpb.Attributes
		switch engine.(type) {
		case InMem:
			attrs = inMemAttrs
		}
		if !reflect.DeepEqual(engine.Attrs(), attrs) {
			t.Errorf("attrs mismatch; expected %+v, got %+v", attrs, engine.Attrs())
		}

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
		keyvals, _ := Scan(engine, mvccKey(roachpb.RKeyMin), mvccKey(roachpb.RKeyMax), 0)
		keyvalsSnapshot, err := Scan(snap, mvccKey(roachpb.RKeyMin), mvccKey(roachpb.RKeyMax), 0)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(keyvals, keyvalsSnapshot) {
			t.Fatalf("the key/values %v in scan result does not match the value %s in snapshot",
				keyvals, keyvalsSnapshot)
		}

		// Verify Iterate.
		index := 0
		if err := snap.Iterate(mvccKey(roachpb.RKeyMin), mvccKey(roachpb.RKeyMax), func(kv MVCCKeyValue) (bool, error) {
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
		iter := snap.NewIterator(false)
		iter.Seek(newKey)
		if iter.Valid() {
			t.Error("expected invalid iterator when seeking to element which shouldn't be visible to snapshot")
		}
		iter.Close()
	}, t)
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
