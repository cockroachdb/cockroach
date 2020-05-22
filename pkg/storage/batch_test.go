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
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func mvccKey(k interface{}) MVCCKey {
	switch k := k.(type) {
	case string:
		return MakeMVCCMetadataKey(roachpb.Key(k))
	case []byte:
		return MakeMVCCMetadataKey(roachpb.Key(k))
	case roachpb.Key:
		return MakeMVCCMetadataKey(k)
	case roachpb.RKey:
		return MakeMVCCMetadataKey(roachpb.Key(k))
	default:
		panic(fmt.Sprintf("unsupported type: %T", k))
	}
}

func testBatchBasics(t *testing.T, writeOnly bool, commit func(e Engine, b Batch) error) {
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			var b Batch
			if writeOnly {
				b = e.NewWriteOnlyBatch()
			} else {
				b = e.NewBatch()
			}
			defer b.Close()

			if err := b.Put(mvccKey("a"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			// Write an engine value to be deleted.
			if err := e.Put(mvccKey("b"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err := b.Clear(mvccKey("b")); err != nil {
				t.Fatal(err)
			}
			// Write an engine value to be merged.
			if err := e.Put(mvccKey("c"), appender("foo")); err != nil {
				t.Fatal(err)
			}
			if err := b.Merge(mvccKey("c"), appender("bar")); err != nil {
				t.Fatal(err)
			}
			// Write a key with an empty value.
			if err := b.Put(mvccKey("e"), nil); err != nil {
				t.Fatal(err)
			}
			// Write an engine value to be single deleted.
			if err := e.Put(mvccKey("d"), []byte("before")); err != nil {
				t.Fatal(err)
			}
			if err := b.SingleClear(mvccKey("d")); err != nil {
				t.Fatal(err)
			}

			// Check all keys are in initial state (nothing from batch has gone
			// through to engine until commit).
			expValues := []MVCCKeyValue{
				{Key: mvccKey("b"), Value: []byte("value")},
				{Key: mvccKey("c"), Value: appender("foo")},
				{Key: mvccKey("d"), Value: []byte("before")},
			}
			kvs, err := Scan(e, roachpb.KeyMin, roachpb.KeyMax, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(expValues, kvs) {
				t.Fatalf("%v != %v", kvs, expValues)
			}

			// Now, merged values should be:
			expValues = []MVCCKeyValue{
				{Key: mvccKey("a"), Value: []byte("value")},
				{Key: mvccKey("c"), Value: appender("foobar")},
				{Key: mvccKey("e"), Value: []byte{}},
			}
			if !writeOnly {
				// Scan values from batch directly.
				kvs, err = Scan(b, roachpb.KeyMin, roachpb.KeyMax, 0)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(expValues, kvs) {
					t.Errorf("%v != %v", kvs, expValues)
				}
			}

			// Commit batch and verify direct engine scan yields correct values.
			if err := commit(e, b); err != nil {
				t.Fatal(err)
			}
			kvs, err = Scan(e, roachpb.KeyMin, roachpb.KeyMax, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(expValues, kvs) {
				t.Errorf("%v != %v", kvs, expValues)
			}
		})
	}
}

// TestBatchBasics verifies that all commands work in a batch, aren't
// visible until commit, and then are all visible after commit.
func TestBatchBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBatchBasics(t, false /* writeOnly */, func(e Engine, b Batch) error {
		return b.Commit(false /* sync */)
	})
}

func shouldPanic(t *testing.T, f func(), funcName string, expectedPanicStr string) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("%v: test did not panic", funcName)
		} else if r != expectedPanicStr {
			t.Fatalf("%v: unexpected panic: %v", funcName, r)
		}
	}()
	f()
}

func shouldNotPanic(t *testing.T, f func(), funcName string) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("%v: unexpected panic: %v", funcName, r)
		}
	}()
	f()
}

// TestReadOnlyBasics verifies that for a read-only ReadWriter (obtained via
// engine.NewReadOnly()) all Reader methods work, and all Writer methods panic
// as "not implemented". Also basic iterating functionality is verified.
func TestReadOnlyBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			ro := e.NewReadOnly()
			if ro.Closed() {
				t.Fatal("read-only is expectedly found to be closed")
			}
			a := mvccKey("a")
			getVal := &roachpb.Value{}
			successTestCases := []func(){
				func() { _, _ = ro.Get(a) },
				func() { _, _, _, _ = ro.GetProto(a, getVal) },
				func() { _ = ro.Iterate(a.Key, a.Key, func(MVCCKeyValue) (bool, error) { return true, nil }) },
				func() { ro.NewIterator(IterOptions{UpperBound: roachpb.KeyMax}).Close() },
				func() {
					ro.NewIterator(IterOptions{
						MinTimestampHint: hlc.MinTimestamp,
						MaxTimestampHint: hlc.MaxTimestamp,
						UpperBound:       roachpb.KeyMax,
					}).Close()
				},
			}
			defer func() {
				ro.Close()
				if !ro.Closed() {
					t.Fatal("even after calling Close, a read-only should not be closed")
				}
				name := "rocksDBReadOnly"
				if engineImpl.name == "pebble" {
					name = "pebbleReadOnly"
				}
				shouldPanic(t, func() { ro.Close() }, "Close", "closing an already-closed "+name)
				for i, f := range successTestCases {
					shouldPanic(t, f, string(i), "using a closed "+name)
				}
			}()

			for i, f := range successTestCases {
				shouldNotPanic(t, f, string(i))
			}

			// For a read-only ReadWriter, all Writer methods should panic.
			failureTestCases := []func(){
				func() { _ = ro.ApplyBatchRepr(nil, false) },
				func() { _ = ro.Clear(a) },
				func() { _ = ro.SingleClear(a) },
				func() { _ = ro.ClearRange(a, a) },
				func() { _ = ro.Merge(a, nil) },
				func() { _ = ro.Put(a, nil) },
			}
			for i, f := range failureTestCases {
				shouldPanic(t, f, string(i), "not implemented")
			}

			if err := e.Put(mvccKey("a"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err := e.Put(mvccKey("b"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err := e.Clear(mvccKey("b")); err != nil {
				t.Fatal(err)
			}
			if err := e.Put(mvccKey("c"), appender("foo")); err != nil {
				t.Fatal(err)
			}
			if err := e.Merge(mvccKey("c"), appender("bar")); err != nil {
				t.Fatal(err)
			}
			if err := e.Put(mvccKey("d"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err := e.SingleClear(mvccKey("d")); err != nil {
				t.Fatal(err)
			}

			// Now, merged values should be:
			expValues := []MVCCKeyValue{
				{Key: mvccKey("a"), Value: []byte("value")},
				{Key: mvccKey("c"), Value: appender("foobar")},
			}

			kvs, err := Scan(e, roachpb.KeyMin, roachpb.KeyMax, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(expValues, kvs) {
				t.Errorf("%v != %v", kvs, expValues)
			}
		})
	}
}

func TestBatchRepr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBatchBasics(t, false /* writeOnly */, func(e Engine, b Batch) error {
		repr := b.Repr()

		r, err := NewRocksDBBatchReader(repr)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		const expectedCount = 5
		if count := r.Count(); count != expectedCount {
			t.Fatalf("bad count: RocksDBBatchReader.Count expected %d, but found %d", expectedCount, count)
		}
		if count, err := RocksDBBatchCount(repr); err != nil {
			t.Fatal(err)
		} else if count != expectedCount {
			t.Fatalf("bad count: RocksDBBatchCount expected %d, but found %d", expectedCount, count)
		}

		var ops []string
		for i := 0; i < r.Count(); i++ {
			if ok := r.Next(); !ok {
				t.Fatalf("%d: unexpected end of batch", i)
			}
			switch r.BatchType() {
			case BatchTypeDeletion:
				ops = append(ops, fmt.Sprintf("delete(%s)", string(r.Key())))
			case BatchTypeValue:
				ops = append(ops, fmt.Sprintf("put(%s,%s)", string(r.Key()), string(r.Value())))
			case BatchTypeMerge:
				// The merge value is a protobuf and not easily displayable.
				ops = append(ops, fmt.Sprintf("merge(%s)", string(r.Key())))
			case BatchTypeSingleDeletion:
				ops = append(ops, fmt.Sprintf("single_delete(%s)", string(r.Key())))
			}
		}
		if err != nil {
			t.Fatalf("unexpected err during iteration: %+v", err)
		}
		if ok := r.Next(); ok {
			t.Errorf("expected end of batch")
		}

		// The keys in the batch have the internal MVCC encoding applied which for
		// this test implies an appended 0 byte.
		expOps := []string{
			"put(a\x00,value)",
			"delete(b\x00)",
			"merge(c\x00)",
			"put(e\x00,)",
			"single_delete(d\x00)",
		}
		if !reflect.DeepEqual(expOps, ops) {
			t.Fatalf("expected %v, but found %v", expOps, ops)
		}

		return e.ApplyBatchRepr(repr, false /* sync */)
	})
}

func TestWriteBatchBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBatchBasics(t, true /* writeOnly */, func(e Engine, b Batch) error {
		return b.Commit(false /* sync */)
	})
}

// Regression test for flush issue which caused
// b2.ApplyBatchRepr(b1.Repr()).Repr() to not equal a noop.
func TestApplyBatchRepr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			// Failure to represent the absorbed Batch again.
			{
				b1 := e.NewBatch()
				defer b1.Close()

				if err := b1.Put(mvccKey("lost"), []byte("update")); err != nil {
					t.Fatal(err)
				}

				repr1 := b1.Repr()

				b2 := e.NewBatch()
				defer b2.Close()
				if err := b2.ApplyBatchRepr(repr1, false /* sync */); err != nil {
					t.Fatal(err)
				}
				repr2 := b2.Repr()

				if !reflect.DeepEqual(repr1, repr2) {
					t.Fatalf("old batch represents to:\n%q\nrestored batch to:\n%q", repr1, repr2)
				}
			}

			// Failure to commit what was absorbed.
			{
				b3 := e.NewBatch()
				defer b3.Close()

				key := mvccKey("phantom")
				val := []byte("phantom")

				if err := b3.Put(key, val); err != nil {
					t.Fatal(err)
				}

				repr := b3.Repr()

				b4 := e.NewBatch()
				defer b4.Close()
				if err := b4.ApplyBatchRepr(repr, false /* sync */); err != nil {
					t.Fatal(err)
				}
				// Intentionally don't call Repr() because the expected user wouldn't.
				if err := b4.Commit(false /* sync */); err != nil {
					t.Fatal(err)
				}

				if b, err := e.Get(key); err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(b, val) {
					t.Fatalf("read %q from engine, expected %q", b, val)
				}
			}
		})
	}
}

func TestBatchGet(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			b := e.NewBatch()
			defer b.Close()

			// Write initial values, then write to batch.
			if err := e.Put(mvccKey("b"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err := e.Put(mvccKey("c"), appender("foo")); err != nil {
				t.Fatal(err)
			}
			// Write batch values.
			if err := b.Put(mvccKey("a"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err := b.Clear(mvccKey("b")); err != nil {
				t.Fatal(err)
			}
			if err := b.Merge(mvccKey("c"), appender("bar")); err != nil {
				t.Fatal(err)
			}
			if err := b.Put(mvccKey("d"), []byte("before")); err != nil {
				t.Fatal(err)
			}
			if err := b.SingleClear(mvccKey("d")); err != nil {
				t.Fatal(err)
			}
			if err := b.Put(mvccKey("d"), []byte("after")); err != nil {
				t.Fatal(err)
			}

			expValues := []MVCCKeyValue{
				{Key: mvccKey("a"), Value: []byte("value")},
				{Key: mvccKey("b"), Value: nil},
				{Key: mvccKey("c"), Value: appender("foobar")},
				{Key: mvccKey("d"), Value: []byte("after")},
			}
			for i, expKV := range expValues {
				kv, err := b.Get(expKV.Key)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(kv, expKV.Value) {
					t.Errorf("%d: expected \"value\", got %q", i, kv)
				}
			}
		})
	}
}

func compareMergedValues(t *testing.T, result, expected []byte) bool {
	var resultV, expectedV enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(result, &resultV); err != nil {
		t.Fatal(err)
	}
	if err := protoutil.Unmarshal(expected, &expectedV); err != nil {
		t.Fatal(err)
	}
	return reflect.DeepEqual(resultV, expectedV)
}

func TestBatchMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			b := e.NewBatch()
			defer b.Close()

			// Write batch put, delete & merge.
			if err := b.Put(mvccKey("a"), appender("a-value")); err != nil {
				t.Fatal(err)
			}
			if err := b.Clear(mvccKey("b")); err != nil {
				t.Fatal(err)
			}
			if err := b.Merge(mvccKey("c"), appender("c-value")); err != nil {
				t.Fatal(err)
			}

			// Now, merge to all three keys.
			if err := b.Merge(mvccKey("a"), appender("append")); err != nil {
				t.Fatal(err)
			}
			if err := b.Merge(mvccKey("b"), appender("append")); err != nil {
				t.Fatal(err)
			}
			if err := b.Merge(mvccKey("c"), appender("append")); err != nil {
				t.Fatal(err)
			}

			// Verify values.
			val, err := b.Get(mvccKey("a"))
			if err != nil {
				t.Fatal(err)
			}
			if !compareMergedValues(t, val, appender("a-valueappend")) {
				t.Error("mismatch of \"a\"")
			}

			val, err = b.Get(mvccKey("b"))
			if err != nil {
				t.Fatal(err)
			}
			if !compareMergedValues(t, val, appender("append")) {
				t.Error("mismatch of \"b\"")
			}

			val, err = b.Get(mvccKey("c"))
			if err != nil {
				t.Fatal(err)
			}
			if !compareMergedValues(t, val, appender("c-valueappend")) {
				t.Error("mismatch of \"c\"")
			}
		})
	}
}

func TestBatchProto(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			b := e.NewBatch()
			defer b.Close()

			val := roachpb.MakeValueFromString("value")
			if _, _, err := PutProto(b, mvccKey("proto"), &val); err != nil {
				t.Fatal(err)
			}
			getVal := &roachpb.Value{}
			ok, keySize, valSize, err := b.GetProto(mvccKey("proto"), getVal)
			if !ok || err != nil {
				t.Fatalf("expected GetProto to success ok=%t: %+v", ok, err)
			}
			if keySize != 6 {
				t.Errorf("expected key size 6; got %d", keySize)
			}
			data, err := protoutil.Marshal(&val)
			if err != nil {
				t.Fatal(err)
			}
			if valSize != int64(len(data)) {
				t.Errorf("expected value size %d; got %d", len(data), valSize)
			}
			if !proto.Equal(getVal, &val) {
				t.Errorf("expected %v; got %v", &val, getVal)
			}
			// Before commit, proto will not be available via engine.
			fmt.Printf("before\n")
			if ok, _, _, err := e.GetProto(mvccKey("proto"), getVal); ok || err != nil {
				fmt.Printf("after\n")
				t.Fatalf("expected GetProto to fail ok=%t: %+v", ok, err)
			}
			// Commit and verify the proto can be read directly from the engine.
			if err := b.Commit(false /* sync */); err != nil {
				t.Fatal(err)
			}
			if ok, _, _, err := e.GetProto(mvccKey("proto"), getVal); !ok || err != nil {
				t.Fatalf("expected GetProto to success ok=%t: %+v", ok, err)
			}
			if !proto.Equal(getVal, &val) {
				t.Errorf("expected %v; got %v", &val, getVal)
			}
		})
	}
}

func TestBatchScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			b := e.NewBatch()
			defer b.Close()

			existingVals := []MVCCKeyValue{
				{Key: mvccKey("a"), Value: []byte("1")},
				{Key: mvccKey("b"), Value: []byte("2")},
				{Key: mvccKey("c"), Value: []byte("3")},
				{Key: mvccKey("d"), Value: []byte("4")},
				{Key: mvccKey("e"), Value: []byte("5")},
				{Key: mvccKey("f"), Value: []byte("6")},
				{Key: mvccKey("g"), Value: []byte("7")},
				{Key: mvccKey("h"), Value: []byte("8")},
				{Key: mvccKey("i"), Value: []byte("9")},
				{Key: mvccKey("j"), Value: []byte("10")},
				{Key: mvccKey("k"), Value: []byte("11")},
				{Key: mvccKey("l"), Value: []byte("12")},
				{Key: mvccKey("m"), Value: []byte("13")},
			}
			for _, kv := range existingVals {
				if err := e.Put(kv.Key, kv.Value); err != nil {
					t.Fatal(err)
				}
			}

			batchVals := []MVCCKeyValue{
				{Key: mvccKey("a"), Value: []byte("b1")},
				{Key: mvccKey("bb"), Value: []byte("b2")},
				{Key: mvccKey("c"), Value: []byte("b3")},
				{Key: mvccKey("dd"), Value: []byte("b4")},
				{Key: mvccKey("e"), Value: []byte("b5")},
				{Key: mvccKey("ff"), Value: []byte("b6")},
				{Key: mvccKey("g"), Value: []byte("b7")},
				{Key: mvccKey("hh"), Value: []byte("b8")},
				{Key: mvccKey("i"), Value: []byte("b9")},
				{Key: mvccKey("jj"), Value: []byte("b10")},
			}
			for _, kv := range batchVals {
				if err := b.Put(kv.Key, kv.Value); err != nil {
					t.Fatal(err)
				}
			}

			scans := []struct {
				start, end roachpb.Key
				max        int64
			}{
				// Full monty.
				{start: roachpb.Key("a"), end: roachpb.Key("z"), max: 0},
				// Select ~half.
				{start: roachpb.Key("a"), end: roachpb.Key("z"), max: 9},
				// Select one.
				{start: roachpb.Key("a"), end: roachpb.Key("z"), max: 1},
				// Select half by end key.
				{start: roachpb.Key("a"), end: roachpb.Key("f0"), max: 0},
				// Start at half and select rest.
				{start: roachpb.Key("f"), end: roachpb.Key("z"), max: 0},
				// Start at last and select max=10.
				{start: roachpb.Key("m"), end: roachpb.Key("z"), max: 10},
			}

			// Scan each case using the batch and store the results.
			results := map[int][]MVCCKeyValue{}
			for i, scan := range scans {
				kvs, err := Scan(b, scan.start, scan.end, scan.max)
				if err != nil {
					t.Fatal(err)
				}
				results[i] = kvs
			}

			// Now, commit batch and re-scan using engine direct to compare results.
			if err := b.Commit(false /* sync */); err != nil {
				t.Fatal(err)
			}
			for i, scan := range scans {
				kvs, err := Scan(e, scan.start, scan.end, scan.max)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(kvs, results[i]) {
					t.Errorf("%d: expected %v; got %v", i, results[i], kvs)
				}
			}
		})
	}
}

// TestBatchScanWithDelete verifies that a scan containing
// a single deleted value returns nothing.
func TestBatchScanWithDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			b := e.NewBatch()
			defer b.Close()

			// Write initial value, then delete via batch.
			if err := e.Put(mvccKey("a"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err := b.Clear(mvccKey("a")); err != nil {
				t.Fatal(err)
			}
			kvs, err := Scan(b, roachpb.KeyMin, roachpb.KeyMax, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(kvs) != 0 {
				t.Errorf("expected empty scan with batch-deleted value; got %v", kvs)
			}
		})
	}
}

// TestBatchScanMaxWithDeleted verifies that if a deletion
// in the updates map shadows an entry from the engine, the
// max on a scan is still reached.
func TestBatchScanMaxWithDeleted(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			b := e.NewBatch()
			defer b.Close()

			// Write two values.
			if err := e.Put(mvccKey("a"), []byte("value1")); err != nil {
				t.Fatal(err)
			}
			if err := e.Put(mvccKey("b"), []byte("value2")); err != nil {
				t.Fatal(err)
			}
			// Now, delete "a" in batch.
			if err := b.Clear(mvccKey("a")); err != nil {
				t.Fatal(err)
			}
			// A scan with max=1 should scan "b".
			kvs, err := Scan(b, roachpb.KeyMin, roachpb.KeyMax, 1)
			if err != nil {
				t.Fatal(err)
			}
			if len(kvs) != 1 || !bytes.Equal(kvs[0].Key.Key, []byte("b")) {
				t.Errorf("expected scan of \"b\"; got %v", kvs)
			}
		})
	}
}

// TestBatchConcurrency verifies operation of batch when the
// underlying engine has concurrent modifications to overlapping
// keys. This should never happen with the way Cockroach uses
// batches, but worth verifying.
func TestBatchConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			b := e.NewBatch()
			defer b.Close()

			// Write a merge to the batch.
			if err := b.Merge(mvccKey("a"), appender("bar")); err != nil {
				t.Fatal(err)
			}
			val, err := b.Get(mvccKey("a"))
			if err != nil {
				t.Fatal(err)
			}
			if !compareMergedValues(t, val, appender("bar")) {
				t.Error("mismatch of \"a\"")
			}
			// Write an engine value.
			if err := e.Put(mvccKey("a"), appender("foo")); err != nil {
				t.Fatal(err)
			}
			// Now, read again and verify that the merge happens on top of the mod.
			val, err = b.Get(mvccKey("a"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(val, appender("foobar")) {
				t.Error("mismatch of \"a\"")
			}
		})
	}
}

func TestBatchBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	e := newRocksDBInMem(roachpb.Attributes{}, 1<<20)
	stopper.AddCloser(e)

	batch := e.NewBatch().(*rocksDBBatch)
	batch.ensureBatch()
	// Ensure that, even though we reach into the batch's internals with
	// dbPut etc, asking for the batch's Repr will get data from C++ and
	// not its unused builder.
	batch.flushes++
	defer batch.Close()

	builder := &RocksDBBatchBuilder{}

	testData := []struct {
		key string
		ts  hlc.Timestamp
	}{
		{"a", hlc.Timestamp{}},
		{"b", hlc.Timestamp{WallTime: 1}},
		{"c", hlc.Timestamp{WallTime: 1, Logical: 1}},
	}
	for _, data := range testData {
		key := MVCCKey{roachpb.Key(data.key), data.ts}
		if err := dbPut(batch.batch, key, []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err := dbClear(batch.batch, key); err != nil {
			t.Fatal(err)
		}
		// TODO(itsbilal): Uncomment this when pebble.Batch supports SingleDeletion.
		//if err := dbSingleClear(batch.batch, key); err != nil {
		//	t.Fatal(err)
		//}
		if err := dbMerge(batch.batch, key, appender("bar")); err != nil {
			t.Fatal(err)
		}

		builder.Put(key, []byte("value"))
		builder.Clear(key)
		// TODO(itsbilal): Uncomment this when pebble.Batch supports SingleDeletion.
		//builder.SingleClear(key)
		builder.Merge(key, appender("bar"))
	}

	batchRepr := batch.Repr()
	builderRepr := builder.Finish()
	if !bytes.Equal(batchRepr, builderRepr) {
		t.Fatalf("expected [% x], but got [% x]", batchRepr, builderRepr)
	}
}

func TestBatchBuilderStress(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	e := newRocksDBInMem(roachpb.Attributes{}, 1<<20)
	stopper.AddCloser(e)

	rng, _ := randutil.NewPseudoRand()

	for i := 0; i < 1000; i++ {
		count := 1 + rng.Intn(1000)

		func() {
			batch := e.NewBatch().(*rocksDBBatch)
			batch.ensureBatch()
			// Ensure that, even though we reach into the batch's internals with
			// dbPut etc, asking for the batch's Repr will get data from C++ and
			// not its unused builder.
			batch.flushes++
			defer batch.Close()

			builder := &RocksDBBatchBuilder{}

			for j := 0; j < count; j++ {
				var ts hlc.Timestamp
				if rng.Float32() <= 0.9 {
					// Give 90% of keys timestamps.
					ts.WallTime = rng.Int63()
					if rng.Float32() <= 0.1 {
						// Give 10% of timestamps a non-zero logical component.
						ts.Logical = rng.Int31()
					}
				}
				key := MVCCKey{
					Key:       []byte(fmt.Sprintf("%d", rng.Intn(10000))),
					Timestamp: ts,
				}
				// Generate a random mixture of puts, deletes, single deletes, and merges.
				switch rng.Intn(3) {
				case 0:
					if err := dbPut(batch.batch, key, []byte("value")); err != nil {
						t.Fatal(err)
					}
					builder.Put(key, []byte("value"))
				case 1:
					if err := dbClear(batch.batch, key); err != nil {
						t.Fatal(err)
					}
					builder.Clear(key)
				case 2:
					// TODO(itsbilal): Don't test SingleClears matching up until
					// pebble.Batch supports them.
					//if err := dbSingleClear(batch.batch, key); err != nil {
					//	t.Fatal(err)
					//}
					//builder.SingleClear(key)
				case 3:
					if err := dbMerge(batch.batch, key, appender("bar")); err != nil {
						t.Fatal(err)
					}
					builder.Merge(key, appender("bar"))
				}
			}

			batchRepr := batch.Repr()
			builderRepr := builder.Finish()
			if !bytes.Equal(batchRepr, builderRepr) {
				t.Fatalf("expected [% x], but got [% x]", batchRepr, builderRepr)
			}
		}()
	}
}

func TestBatchDistinctAfterApplyBatchRepr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			wb := func() []byte {
				batch := e.NewBatch()
				defer batch.Close()

				if err := batch.Put(mvccKey("batchkey"), []byte("b")); err != nil {
					t.Fatal(err)
				}

				return batch.Repr()
			}()

			batch := e.NewBatch()
			defer batch.Close()

			assert.NoError(t, batch.ApplyBatchRepr(wb, false /* sync */))

			distinct := batch.Distinct()
			defer distinct.Close()

			// The distinct batch can see the earlier write to the batch.
			v, err := distinct.Get(mvccKey("batchkey"))
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, []byte("b"), v)
		})
	}
}

func TestBatchDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			if err := e.Put(mvccKey("b"), []byte("b")); err != nil {
				t.Fatal(err)
			}

			batch := e.NewBatch()
			defer batch.Close()

			if err := batch.Put(mvccKey("a"), []byte("a")); err != nil {
				t.Fatal(err)
			}
			if err := batch.Clear(mvccKey("b")); err != nil {
				t.Fatal(err)
			}

			// The original batch can see the writes to the batch.
			if v, err := batch.Get(mvccKey("a")); err != nil {
				t.Fatal(err)
			} else if string(v) != "a" {
				t.Fatalf("expected a, but got %s", v)
			}

			// The distinct batch will see previous writes to the batch.
			distinct := batch.Distinct()
			if v, err := distinct.Get(mvccKey("a")); err != nil {
				t.Fatal(err)
			} else if string(v) != "a" {
				t.Fatalf("expected a, but got %s", v)
			}
			if v, err := distinct.Get(mvccKey("b")); err != nil {
				t.Fatal(err)
			} else if v != nil {
				t.Fatalf("expected nothing, but got %s", v)
			}

			// Similarly, for distinct batch iterators we will see previous writes to the
			// batch.
			iter := distinct.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
			iter.SeekGE(mvccKey("a"))
			if ok, err := iter.Valid(); !ok {
				t.Fatalf("expected iterator to be valid; err=%v", err)
			}
			if string(iter.Key().Key) != "a" {
				t.Fatalf("expected a, but got %s", iter.Key())
			}
			iter.Close()

			if err := distinct.Put(mvccKey("c"), []byte("c")); err != nil {
				t.Fatal(err)
			}
			if v, err := distinct.Get(mvccKey("c")); err != nil {
				t.Fatal(err)
			} else {
				switch engineImpl.name {
				case "pebble":
					// With Pebble, writes to the distinct batch are readable by the
					// distinct batch. This semantic difference is due to not buffering
					// writes in a builder.
					if v == nil {
						t.Fatalf("expected success, but got %s", v)
					}
				default:
					// Writes to the distinct batch are not readable by the distinct
					// batch.
					if v != nil {
						t.Fatalf("expected nothing, but got %s", v)
					}
				}
			}
			distinct.Close()

			// Writes to the distinct batch are reflected in the original batch.
			if v, err := batch.Get(mvccKey("c")); err != nil {
				t.Fatal(err)
			} else if string(v) != "c" {
				t.Fatalf("expected c, but got %s", v)
			}
		})
	}
}

func TestWriteOnlyBatchDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			if err := e.Put(mvccKey("b"), []byte("b")); err != nil {
				t.Fatal(err)
			}
			if _, _, err := PutProto(e, mvccKey("c"), &roachpb.Value{}); err != nil {
				t.Fatal(err)
			}

			b := e.NewWriteOnlyBatch()
			defer b.Close()

			distinct := b.Distinct()
			defer distinct.Close()

			// Verify that reads on the distinct batch go to the underlying engine, not
			// to the write-only batch.
			iter := distinct.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
			iter.SeekGE(mvccKey("a"))
			if ok, err := iter.Valid(); !ok {
				t.Fatalf("expected iterator to be valid, err=%v", err)
			}
			if string(iter.Key().Key) != "b" {
				t.Fatalf("expected b, but got %s", iter.Key())
			}
			iter.Close()

			if v, err := distinct.Get(mvccKey("b")); err != nil {
				t.Fatal(err)
			} else if string(v) != "b" {
				t.Fatalf("expected b, but got %s", v)
			}

			val := &roachpb.Value{}
			if _, _, _, err := distinct.GetProto(mvccKey("c"), val); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBatchDistinctPanics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			batch := e.NewBatch()
			defer batch.Close()

			distinct := batch.Distinct()
			defer distinct.Close()

			// The various Reader and Writer methods on the original batch should panic
			// while the distinct batch is open.
			a := mvccKey("a")
			testCases := []func(){
				func() { _ = batch.Put(a, nil) },
				func() { _ = batch.Merge(a, nil) },
				func() { _ = batch.Clear(a) },
				func() { _ = batch.SingleClear(a) },
				func() { _ = batch.ApplyBatchRepr(nil, false) },
				func() { _, _ = batch.Get(a) },
				func() { _, _, _, _ = batch.GetProto(a, nil) },
				func() { _ = batch.Iterate(a.Key, a.Key, nil) },
				func() { _ = batch.NewIterator(IterOptions{UpperBound: roachpb.KeyMax}) },
			}
			for i, f := range testCases {
				func() {
					defer func() {
						if r := recover(); r == nil {
							t.Fatalf("%d: test did not panic", i)
						} else if r != "distinct batch open" {
							t.Fatalf("%d: unexpected panic: %v", i, r)
						}
					}()
					f()
				}()
			}
		})
	}
}

func TestBatchIteration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			b := e.NewBatch()
			defer b.Close()

			k1 := MakeMVCCMetadataKey(roachpb.Key("c"))
			k2 := MakeMVCCMetadataKey(roachpb.Key("d"))
			k3 := MakeMVCCMetadataKey(roachpb.Key("e"))
			v1 := []byte("value1")
			v2 := []byte("value2")

			if err := b.Put(k1, v1); err != nil {
				t.Fatal(err)
			}
			if err := b.Put(k2, v2); err != nil {
				t.Fatal(err)
			}
			if err := b.Put(k3, []byte("doesn't matter")); err != nil {
				t.Fatal(err)
			}

			iterOpts := IterOptions{UpperBound: k3.Key}
			iter := b.NewIterator(iterOpts)
			defer iter.Close()

			// Forward iteration,
			iter.SeekGE(k1)
			if ok, err := iter.Valid(); !ok {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(iter.Key(), k1) {
				t.Fatalf("expected %s, got %s", k1, iter.Key())
			}
			if !reflect.DeepEqual(iter.Value(), v1) {
				t.Fatalf("expected %s, got %s", v1, iter.Value())
			}
			iter.Next()
			if ok, err := iter.Valid(); !ok {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(iter.Key(), k2) {
				t.Fatalf("expected %s, got %s", k2, iter.Key())
			}
			if !reflect.DeepEqual(iter.Value(), v2) {
				t.Fatalf("expected %s, got %s", v2, iter.Value())
			}
			iter.Next()
			if ok, err := iter.Valid(); err != nil {
				t.Fatal(err)
			} else if ok {
				t.Fatalf("expected invalid, got valid at key %s", iter.Key())
			}

			// Reverse iteration.
			switch engineImpl.name {
			case "pebble":
				// Reverse iteration in batches works on Pebble.
				iter.SeekLT(k3)
				if ok, err := iter.Valid(); !ok {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(iter.Key(), k2) {
					t.Fatalf("expected %s, got %s", k2, iter.Key())
				}
				if !reflect.DeepEqual(iter.Value(), v2) {
					t.Fatalf("expected %s, got %s", v2, iter.Value())
				}

				iter.Prev()
				if ok, err := iter.Valid(); !ok || err != nil {
					t.Fatalf("expected success, but got invalid: %v", err)
				}
				if !reflect.DeepEqual(iter.Key(), k1) {
					t.Fatalf("expected %s, got %s", k1, iter.Key())
				}
				if !reflect.DeepEqual(iter.Value(), v1) {
					t.Fatalf("expected %s, got %s", v1, iter.Value())
				}
			default:
				// Reverse iteration in batches is not supported with RocksDB.
				iter.SeekLT(k3)
				if ok, err := iter.Valid(); ok {
					t.Fatalf("expected invalid, got valid at key %s", iter.Key())
				} else if !testutils.IsError(err, "SeekForPrev\\(\\) not supported") {
					t.Fatalf("expected 'SeekForPrev() not supported', got %s", err)
				}

				iter.Prev()
				if ok, err := iter.Valid(); ok {
					t.Fatalf("expected invalid, got valid at key %s", iter.Key())
				} else if !testutils.IsError(err, "Prev\\(\\) not supported") {
					t.Fatalf("expected 'Prev() not supported', got %s", err)
				}
			}
		})
	}
}

// Test combining of concurrent commits of write-only batches, verifying that
// all of the keys written by the individual batches are subsequently readable.
func TestBatchCombine(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			var n uint32
			const count = 10000

			errs := make(chan error, 10)
			for i := 0; i < cap(errs); i++ {
				go func() {
					for {
						v := atomic.AddUint32(&n, 1) - 1
						if v >= count {
							break
						}
						k := fmt.Sprint(v)

						b := e.NewWriteOnlyBatch()
						if err := b.Put(mvccKey(k), []byte(k)); err != nil {
							errs <- errors.Wrap(err, "put failed")
							return
						}
						if err := b.Commit(false); err != nil {
							errs <- errors.Wrap(err, "commit failed")
							return
						}

						// Verify we can read the key we just wrote immediately.
						if v, err := e.Get(mvccKey(k)); err != nil {
							errs <- errors.Wrap(err, "get failed")
							return
						} else if string(v) != k {
							errs <- errors.Errorf("read %q from engine, expected %q", v, k)
							return
						}
					}
					errs <- nil
				}()
			}

			for i := 0; i < cap(errs); i++ {
				if err := <-errs; err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func TestDecodeKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	e := newRocksDBInMem(roachpb.Attributes{}, 1<<20)
	defer e.Close()

	tests := []MVCCKey{
		{Key: []byte{}},
		{Key: []byte("foo")},
		{Key: []byte("foo"), Timestamp: hlc.Timestamp{WallTime: 1}},
		{Key: []byte("foo"), Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1}},
	}
	for _, test := range tests {
		t.Run(test.String(), func(t *testing.T) {
			b := e.NewBatch()
			defer b.Close()
			if err := b.Put(test, nil); err != nil {
				t.Fatalf("%+v", err)
			}
			repr := b.Repr()

			r, err := NewRocksDBBatchReader(repr)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			if !r.Next() {
				t.Fatalf("could not get the first entry: %+v", r.Error())
			}
			decodedKey, err := DecodeMVCCKey(r.Key())
			if err != nil {
				t.Fatalf("unexpected err: %+v", err)
			}
			if !reflect.DeepEqual(test, decodedKey) {
				t.Errorf("expected %+v got %+v", test, decodedKey)
			}
		})
	}
}
