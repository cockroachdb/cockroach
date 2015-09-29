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

	if err := b.Put(roachpb.EncodedKey("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	// Write an engine value to be deleted.
	if err := e.Put(roachpb.EncodedKey("b"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(roachpb.EncodedKey("b")); err != nil {
		t.Fatal(err)
	}
	// Write an engine value to be merged.
	if err := e.Put(roachpb.EncodedKey("c"), appender("foo")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(roachpb.EncodedKey("c"), appender("bar")); err != nil {
		t.Fatal(err)
	}

	// Check all keys are in initial state (nothing from batch has gone
	// through to engine until commit).
	expValues := []roachpb.RawKeyValue{
		{Key: roachpb.EncodedKey("b"), Value: []byte("value")},
		{Key: roachpb.EncodedKey("c"), Value: appender("foo")},
	}
	kvs, err := Scan(e, roachpb.EncodedKey(roachpb.KeyMin), roachpb.EncodedKey(roachpb.KeyMax), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expValues, kvs) {
		t.Errorf("%v != %v", kvs, expValues)
	}

	// Now, merged values should be:
	expValues = []roachpb.RawKeyValue{
		{Key: roachpb.EncodedKey("a"), Value: []byte("value")},
		{Key: roachpb.EncodedKey("c"), Value: appender("foobar")},
	}
	// Scan values from batch directly.
	kvs, err = Scan(b, roachpb.EncodedKey(roachpb.KeyMin), roachpb.EncodedKey(roachpb.KeyMax), 0)
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
	kvs, err = Scan(e, roachpb.EncodedKey(roachpb.KeyMin), roachpb.EncodedKey(roachpb.KeyMax), 0)
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
	if err := e.Put(roachpb.EncodedKey("b"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := e.Put(roachpb.EncodedKey("c"), appender("foo")); err != nil {
		t.Fatal(err)
	}
	// Write batch values.
	if err := b.Put(roachpb.EncodedKey("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(roachpb.EncodedKey("b")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(roachpb.EncodedKey("c"), appender("bar")); err != nil {
		t.Fatal(err)
	}

	expValues := []roachpb.RawKeyValue{
		{Key: roachpb.EncodedKey("a"), Value: []byte("value")},
		{Key: roachpb.EncodedKey("b"), Value: nil},
		{Key: roachpb.EncodedKey("c"), Value: appender("foobar")},
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
	if err := b.Put(roachpb.EncodedKey("a"), appender("a-value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(roachpb.EncodedKey("b")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(roachpb.EncodedKey("c"), appender("c-value")); err != nil {
		t.Fatal(err)
	}

	// Now, merge to all three keys.
	if err := b.Merge(roachpb.EncodedKey("a"), appender("append")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(roachpb.EncodedKey("b"), appender("append")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(roachpb.EncodedKey("c"), appender("append")); err != nil {
		t.Fatal(err)
	}

	// Verify values.
	val, err := b.Get(roachpb.EncodedKey("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !compareMergedValues(t, val, appender("a-valueappend")) {
		t.Error("mismatch of \"a\"")
	}

	val, err = b.Get(roachpb.EncodedKey("b"))
	if err != nil {
		t.Fatal(err)
	}
	if !compareMergedValues(t, val, appender("append")) {
		t.Error("mismatch of \"b\"")
	}

	val, err = b.Get(roachpb.EncodedKey("c"))
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

	kv := &roachpb.RawKeyValue{Key: roachpb.EncodedKey("a"), Value: []byte("value")}
	if _, _, err := PutProto(b, roachpb.EncodedKey("proto"), kv); err != nil {
		t.Fatal(err)
	}
	getKV := &roachpb.RawKeyValue{}
	ok, keySize, valSize, err := b.GetProto(roachpb.EncodedKey("proto"), getKV)
	if !ok || err != nil {
		t.Fatalf("expected GetProto to success ok=%t: %s", ok, err)
	}
	if keySize != 5 {
		t.Errorf("expected key size 5; got %d", keySize)
	}
	var data []byte
	if data, err = proto.Marshal(kv); err != nil {
		t.Fatal(err)
	}
	if valSize != int64(len(data)) {
		t.Errorf("expected value size %d; got %d", len(data), valSize)
	}
	if !reflect.DeepEqual(getKV, kv) {
		t.Errorf("expected %v; got %v", kv, getKV)
	}
	// Before commit, proto will not be available via engine.
	if ok, _, _, err = e.GetProto(roachpb.EncodedKey("proto"), getKV); ok || err != nil {
		t.Fatalf("expected GetProto to fail ok=%t: %s", ok, err)
	}
	// Commit and verify the proto can be read directly from the engine.
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if ok, _, _, err = e.GetProto(roachpb.EncodedKey("proto"), getKV); !ok || err != nil {
		t.Fatalf("expected GetProto to success ok=%t: %s", ok, err)
	}
	if !reflect.DeepEqual(getKV, kv) {
		t.Errorf("expected %v; got %v", kv, getKV)
	}
}

func TestBatchScan(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	b := e.NewBatch()
	defer b.Close()

	existingVals := []roachpb.RawKeyValue{
		{Key: roachpb.EncodedKey("a"), Value: []byte("1")},
		{Key: roachpb.EncodedKey("b"), Value: []byte("2")},
		{Key: roachpb.EncodedKey("c"), Value: []byte("3")},
		{Key: roachpb.EncodedKey("d"), Value: []byte("4")},
		{Key: roachpb.EncodedKey("e"), Value: []byte("5")},
		{Key: roachpb.EncodedKey("f"), Value: []byte("6")},
		{Key: roachpb.EncodedKey("g"), Value: []byte("7")},
		{Key: roachpb.EncodedKey("h"), Value: []byte("8")},
		{Key: roachpb.EncodedKey("i"), Value: []byte("9")},
		{Key: roachpb.EncodedKey("j"), Value: []byte("10")},
		{Key: roachpb.EncodedKey("k"), Value: []byte("11")},
		{Key: roachpb.EncodedKey("l"), Value: []byte("12")},
		{Key: roachpb.EncodedKey("m"), Value: []byte("13")},
	}
	for _, kv := range existingVals {
		if err := e.Put(kv.Key, kv.Value); err != nil {
			t.Fatal(err)
		}
	}

	batchVals := []roachpb.RawKeyValue{
		{Key: roachpb.EncodedKey("a"), Value: []byte("b1")},
		{Key: roachpb.EncodedKey("bb"), Value: []byte("b2")},
		{Key: roachpb.EncodedKey("c"), Value: []byte("b3")},
		{Key: roachpb.EncodedKey("dd"), Value: []byte("b4")},
		{Key: roachpb.EncodedKey("e"), Value: []byte("b5")},
		{Key: roachpb.EncodedKey("ff"), Value: []byte("b6")},
		{Key: roachpb.EncodedKey("g"), Value: []byte("b7")},
		{Key: roachpb.EncodedKey("hh"), Value: []byte("b8")},
		{Key: roachpb.EncodedKey("i"), Value: []byte("b9")},
		{Key: roachpb.EncodedKey("jj"), Value: []byte("b10")},
	}
	for _, kv := range batchVals {
		if err := b.Put(kv.Key, kv.Value); err != nil {
			t.Fatal(err)
		}
	}

	scans := []struct {
		start, end roachpb.EncodedKey
		max        int64
	}{
		// Full monty.
		{start: roachpb.EncodedKey("a"), end: roachpb.EncodedKey("z"), max: 0},
		// Select ~half.
		{start: roachpb.EncodedKey("a"), end: roachpb.EncodedKey("z"), max: 9},
		// Select one.
		{start: roachpb.EncodedKey("a"), end: roachpb.EncodedKey("z"), max: 1},
		// Select half by end key.
		{start: roachpb.EncodedKey("a"), end: roachpb.EncodedKey("f0"), max: 0},
		// Start at half and select rest.
		{start: roachpb.EncodedKey("f"), end: roachpb.EncodedKey("z"), max: 0},
		// Start at last and select max=10.
		{start: roachpb.EncodedKey("m"), end: roachpb.EncodedKey("z"), max: 10},
	}

	// Scan each case using the batch and store the results.
	results := map[int][]roachpb.RawKeyValue{}
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
	if err := e.Put(roachpb.EncodedKey("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(roachpb.EncodedKey("a")); err != nil {
		t.Fatal(err)
	}
	kvs, err := Scan(b, roachpb.EncodedKey(roachpb.KeyMin), roachpb.EncodedKey(roachpb.KeyMax), 0)
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
	if err := e.Put(roachpb.EncodedKey("a"), []byte("value1")); err != nil {
		t.Fatal(err)
	}
	if err := e.Put(roachpb.EncodedKey("b"), []byte("value2")); err != nil {
		t.Fatal(err)
	}
	// Now, delete "a" in batch.
	if err := b.Clear(roachpb.EncodedKey("a")); err != nil {
		t.Fatal(err)
	}
	// A scan with max=1 should scan "b".
	kvs, err := Scan(b, roachpb.EncodedKey(roachpb.KeyMin), roachpb.EncodedKey(roachpb.KeyMax), 1)
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
	if err := b.Merge(roachpb.EncodedKey("a"), appender("bar")); err != nil {
		t.Fatal(err)
	}
	val, err := b.Get(roachpb.EncodedKey("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !compareMergedValues(t, val, appender("bar")) {
		t.Error("mismatch of \"a\"")
	}
	// Write an engine value.
	if err := e.Put(roachpb.EncodedKey("a"), appender("foo")); err != nil {
		t.Fatal(err)
	}
	// Now, read again and verify that the merge happens on top of the mod.
	val, err = b.Get(roachpb.EncodedKey("a"))
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
