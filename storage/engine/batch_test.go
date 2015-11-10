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

package engine

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

// TestBatchBasics verifies that all commands work in a batch, aren't
// visible until commit, and then are all visible after commit.
func TestBatchBasics(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	b := e.NewBatch()
	defer b.Close()

	if err := b.Put(MVCCKey("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	// Write an engine value to be deleted.
	if err := e.Put(MVCCKey("b"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(MVCCKey("b")); err != nil {
		t.Fatal(err)
	}
	// Write an engine value to be merged.
	if err := e.Put(MVCCKey("c"), appender("foo")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(MVCCKey("c"), appender("bar")); err != nil {
		t.Fatal(err)
	}

	// Check all keys are in initial state (nothing from batch has gone
	// through to engine until commit).
	expValues := []MVCCKeyValue{
		{Key: MVCCKey("b"), Value: []byte("value")},
		{Key: MVCCKey("c"), Value: appender("foo")},
	}
	kvs, err := Scan(e, MVCCKey(roachpb.RKeyMin), MVCCKey(roachpb.RKeyMax), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expValues, kvs) {
		t.Errorf("%v != %v", kvs, expValues)
	}

	// Now, merged values should be:
	expValues = []MVCCKeyValue{
		{Key: MVCCKey("a"), Value: []byte("value")},
		{Key: MVCCKey("c"), Value: appender("foobar")},
	}
	// Scan values from batch directly.
	kvs, err = Scan(b, MVCCKey(roachpb.RKeyMin), MVCCKey(roachpb.RKeyMax), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expValues, kvs) {
		t.Errorf("%v != %v", kvs, expValues)
	}

	// Commit batch and verify direct engine scan yields correct values.
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	kvs, err = Scan(e, MVCCKey(roachpb.RKeyMin), MVCCKey(roachpb.RKeyMax), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expValues, kvs) {
		t.Errorf("%v != %v", kvs, expValues)
	}
}

func TestBatchGet(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	b := e.NewBatch()
	defer b.Close()

	// Write initial values, then write to batch.
	if err := e.Put(MVCCKey("b"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := e.Put(MVCCKey("c"), appender("foo")); err != nil {
		t.Fatal(err)
	}
	// Write batch values.
	if err := b.Put(MVCCKey("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(MVCCKey("b")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(MVCCKey("c"), appender("bar")); err != nil {
		t.Fatal(err)
	}

	expValues := []MVCCKeyValue{
		{Key: MVCCKey("a"), Value: []byte("value")},
		{Key: MVCCKey("b"), Value: nil},
		{Key: MVCCKey("c"), Value: appender("foobar")},
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
}

func compareMergedValues(t *testing.T, result, expected []byte) bool {
	var resultV, expectedV MVCCMetadata
	if err := proto.Unmarshal(result, &resultV); err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(expected, &expectedV); err != nil {
		t.Fatal(err)
	}
	return reflect.DeepEqual(resultV, expectedV)
}

func TestBatchMerge(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	b := e.NewBatch()
	defer b.Close()

	// Write batch put, delete & merge.
	if err := b.Put(MVCCKey("a"), appender("a-value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(MVCCKey("b")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(MVCCKey("c"), appender("c-value")); err != nil {
		t.Fatal(err)
	}

	// Now, merge to all three keys.
	if err := b.Merge(MVCCKey("a"), appender("append")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(MVCCKey("b"), appender("append")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(MVCCKey("c"), appender("append")); err != nil {
		t.Fatal(err)
	}

	// Verify values.
	val, err := b.Get(MVCCKey("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !compareMergedValues(t, val, appender("a-valueappend")) {
		t.Error("mismatch of \"a\"")
	}

	val, err = b.Get(MVCCKey("b"))
	if err != nil {
		t.Fatal(err)
	}
	if !compareMergedValues(t, val, appender("append")) {
		t.Error("mismatch of \"b\"")
	}

	val, err = b.Get(MVCCKey("c"))
	if err != nil {
		t.Fatal(err)
	}
	if !compareMergedValues(t, val, appender("c-valueappend")) {
		t.Error("mismatch of \"c\"")
	}
}

func TestBatchProto(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	b := e.NewBatch()
	defer b.Close()

	val := roachpb.MakeValueFromString("value")
	if _, _, err := PutProto(b, MVCCKey("proto"), &val); err != nil {
		t.Fatal(err)
	}
	getVal := &roachpb.Value{}
	ok, keySize, valSize, err := b.GetProto(MVCCKey("proto"), getVal)
	if !ok || err != nil {
		t.Fatalf("expected GetProto to success ok=%t: %s", ok, err)
	}
	if keySize != 5 {
		t.Errorf("expected key size 5; got %d", keySize)
	}
	data, err := val.Marshal()
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
	if ok, _, _, err := e.GetProto(MVCCKey("proto"), getVal); ok || err != nil {
		t.Fatalf("expected GetProto to fail ok=%t: %s", ok, err)
	}
	// Commit and verify the proto can be read directly from the engine.
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if ok, _, _, err := e.GetProto(MVCCKey("proto"), getVal); !ok || err != nil {
		t.Fatalf("expected GetProto to success ok=%t: %s", ok, err)
	}
	if !proto.Equal(getVal, &val) {
		t.Errorf("expected %v; got %v", &val, getVal)
	}
}

func TestBatchScan(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	b := e.NewBatch()
	defer b.Close()

	existingVals := []MVCCKeyValue{
		{Key: MVCCKey("a"), Value: []byte("1")},
		{Key: MVCCKey("b"), Value: []byte("2")},
		{Key: MVCCKey("c"), Value: []byte("3")},
		{Key: MVCCKey("d"), Value: []byte("4")},
		{Key: MVCCKey("e"), Value: []byte("5")},
		{Key: MVCCKey("f"), Value: []byte("6")},
		{Key: MVCCKey("g"), Value: []byte("7")},
		{Key: MVCCKey("h"), Value: []byte("8")},
		{Key: MVCCKey("i"), Value: []byte("9")},
		{Key: MVCCKey("j"), Value: []byte("10")},
		{Key: MVCCKey("k"), Value: []byte("11")},
		{Key: MVCCKey("l"), Value: []byte("12")},
		{Key: MVCCKey("m"), Value: []byte("13")},
	}
	for _, kv := range existingVals {
		if err := e.Put(kv.Key, kv.Value); err != nil {
			t.Fatal(err)
		}
	}

	batchVals := []MVCCKeyValue{
		{Key: MVCCKey("a"), Value: []byte("b1")},
		{Key: MVCCKey("bb"), Value: []byte("b2")},
		{Key: MVCCKey("c"), Value: []byte("b3")},
		{Key: MVCCKey("dd"), Value: []byte("b4")},
		{Key: MVCCKey("e"), Value: []byte("b5")},
		{Key: MVCCKey("ff"), Value: []byte("b6")},
		{Key: MVCCKey("g"), Value: []byte("b7")},
		{Key: MVCCKey("hh"), Value: []byte("b8")},
		{Key: MVCCKey("i"), Value: []byte("b9")},
		{Key: MVCCKey("jj"), Value: []byte("b10")},
	}
	for _, kv := range batchVals {
		if err := b.Put(kv.Key, kv.Value); err != nil {
			t.Fatal(err)
		}
	}

	scans := []struct {
		start, end MVCCKey
		max        int64
	}{
		// Full monty.
		{start: MVCCKey("a"), end: MVCCKey("z"), max: 0},
		// Select ~half.
		{start: MVCCKey("a"), end: MVCCKey("z"), max: 9},
		// Select one.
		{start: MVCCKey("a"), end: MVCCKey("z"), max: 1},
		// Select half by end key.
		{start: MVCCKey("a"), end: MVCCKey("f0"), max: 0},
		// Start at half and select rest.
		{start: MVCCKey("f"), end: MVCCKey("z"), max: 0},
		// Start at last and select max=10.
		{start: MVCCKey("m"), end: MVCCKey("z"), max: 10},
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
	if err := b.Commit(); err != nil {
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
}

// TestBatchScanWithDelete verifies that a scan containing
// a single deleted value returns nothing.
func TestBatchScanWithDelete(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	b := e.NewBatch()
	defer b.Close()

	// Write initial value, then delete via batch.
	if err := e.Put(MVCCKey("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(MVCCKey("a")); err != nil {
		t.Fatal(err)
	}
	kvs, err := Scan(b, MVCCKey(roachpb.RKeyMin), MVCCKey(roachpb.RKeyMax), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 0 {
		t.Errorf("expected empty scan with batch-deleted value; got %v", kvs)
	}
}

// TestBatchScanMaxWithDeleted verifies that if a deletion
// in the updates map shadows an entry from the engine, the
// max on a scan is still reached.
func TestBatchScanMaxWithDeleted(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	b := e.NewBatch()
	defer b.Close()

	// Write two values.
	if err := e.Put(MVCCKey("a"), []byte("value1")); err != nil {
		t.Fatal(err)
	}
	if err := e.Put(MVCCKey("b"), []byte("value2")); err != nil {
		t.Fatal(err)
	}
	// Now, delete "a" in batch.
	if err := b.Clear(MVCCKey("a")); err != nil {
		t.Fatal(err)
	}
	// A scan with max=1 should scan "b".
	kvs, err := Scan(b, MVCCKey(roachpb.RKeyMin), MVCCKey(roachpb.RKeyMax), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 || !bytes.Equal(kvs[0].Key, []byte("b")) {
		t.Errorf("expected scan of \"b\"; got %v", kvs)
	}
}

// TestBatchConcurrency verifies operation of batch when the
// underlying engine has concurrent modifications to overlapping
// keys. This should never happen with the way Cockroach uses
// batches, but worth verifying.
func TestBatchConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	b := e.NewBatch()
	defer b.Close()

	// Write a merge to the batch.
	if err := b.Merge(MVCCKey("a"), appender("bar")); err != nil {
		t.Fatal(err)
	}
	val, err := b.Get(MVCCKey("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !compareMergedValues(t, val, appender("bar")) {
		t.Error("mismatch of \"a\"")
	}
	// Write an engine value.
	if err := e.Put(MVCCKey("a"), appender("foo")); err != nil {
		t.Fatal(err)
	}
	// Now, read again and verify that the merge happens on top of the mod.
	val, err = b.Get(MVCCKey("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, appender("foobar")) {
		t.Error("mismatch of \"a\"")
	}
}

func TestBatchDefer(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	b := e.NewBatch()
	defer b.Close()

	list := []string{}

	b.Defer(func() {
		list = append(list, "one")
	})
	b.Defer(func() {
		list = append(list, "two")
	})

	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}

	// Order was reversed when the defers were run.
	if !reflect.DeepEqual(list, []string{"two", "one"}) {
		t.Errorf("expected [two, one]; got %v", list)
	}
}
