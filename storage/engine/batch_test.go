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

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/leaktest"
	gogoproto "github.com/gogo/protobuf/proto"
)

// TestBatchBasics verifies that all commands work in a batch, aren't
// visible until commit, and then are all visible after commit.
func TestBatchBasics(t *testing.T) {
	defer leaktest.AfterTest(t)
	e := NewInMem(proto.Attributes{}, 1<<20)
	defer e.Close()

	b := e.NewBatch()
	defer b.Close()

	if err := b.Put(proto.EncodedKey("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	// Write an engine value to be deleted.
	if err := e.Put(proto.EncodedKey("b"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(proto.EncodedKey("b")); err != nil {
		t.Fatal(err)
	}
	// Write an engine value to be merged.
	if err := e.Put(proto.EncodedKey("c"), appender("foo")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(proto.EncodedKey("c"), appender("bar")); err != nil {
		t.Fatal(err)
	}

	// Check all keys are in initial state (nothing from batch has gone
	// through to engine until commit).
	expValues := []proto.RawKeyValue{
		{Key: proto.EncodedKey("b"), Value: []byte("value")},
		{Key: proto.EncodedKey("c"), Value: appender("foo")},
	}
	kvs, err := Scan(e, proto.EncodedKey(proto.KeyMin), proto.EncodedKey(proto.KeyMax), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expValues, kvs) {
		t.Errorf("%v != %v", kvs, expValues)
	}

	// Now, merged values should be:
	expValues = []proto.RawKeyValue{
		{Key: proto.EncodedKey("a"), Value: []byte("value")},
		{Key: proto.EncodedKey("c"), Value: appender("foobar")},
	}
	// Scan values from batch directly.
	kvs, err = Scan(b, proto.EncodedKey(proto.KeyMin), proto.EncodedKey(proto.KeyMax), 0)
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
	kvs, err = Scan(e, proto.EncodedKey(proto.KeyMin), proto.EncodedKey(proto.KeyMax), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expValues, kvs) {
		t.Errorf("%v != %v", kvs, expValues)
	}
}

func TestBatchGet(t *testing.T) {
	defer leaktest.AfterTest(t)
	e := NewInMem(proto.Attributes{}, 1<<20)
	defer e.Close()

	b := e.NewBatch()
	defer b.Close()

	// Write initial values, then write to batch.
	if err := e.Put(proto.EncodedKey("b"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := e.Put(proto.EncodedKey("c"), appender("foo")); err != nil {
		t.Fatal(err)
	}
	// Write batch values.
	if err := b.Put(proto.EncodedKey("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(proto.EncodedKey("b")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(proto.EncodedKey("c"), appender("bar")); err != nil {
		t.Fatal(err)
	}

	expValues := []proto.RawKeyValue{
		{Key: proto.EncodedKey("a"), Value: []byte("value")},
		{Key: proto.EncodedKey("b"), Value: nil},
		{Key: proto.EncodedKey("c"), Value: appender("foobar")},
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
	var resultV, expectedV proto.MVCCMetadata
	if err := gogoproto.Unmarshal(result, &resultV); err != nil {
		t.Fatal(err)
	}
	if err := gogoproto.Unmarshal(expected, &expectedV); err != nil {
		t.Fatal(err)
	}
	return reflect.DeepEqual(resultV, expectedV)
}

func TestBatchMerge(t *testing.T) {
	defer leaktest.AfterTest(t)
	e := NewInMem(proto.Attributes{}, 1<<20)
	defer e.Close()

	b := e.NewBatch()
	defer b.Close()

	// Write batch put, delete & merge.
	if err := b.Put(proto.EncodedKey("a"), appender("a-value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(proto.EncodedKey("b")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(proto.EncodedKey("c"), appender("c-value")); err != nil {
		t.Fatal(err)
	}

	// Now, merge to all three keys.
	if err := b.Merge(proto.EncodedKey("a"), appender("append")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(proto.EncodedKey("b"), appender("append")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(proto.EncodedKey("c"), appender("append")); err != nil {
		t.Fatal(err)
	}

	// Verify values.
	val, err := b.Get(proto.EncodedKey("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !compareMergedValues(t, val, appender("a-valueappend")) {
		t.Error("mismatch of \"a\"")
	}

	val, err = b.Get(proto.EncodedKey("b"))
	if err != nil {
		t.Fatal(err)
	}
	if !compareMergedValues(t, val, appender("append")) {
		t.Error("mismatch of \"b\"")
	}

	val, err = b.Get(proto.EncodedKey("c"))
	if err != nil {
		t.Fatal(err)
	}
	if !compareMergedValues(t, val, appender("c-valueappend")) {
		t.Error("mismatch of \"c\"")
	}
}

func TestBatchProto(t *testing.T) {
	defer leaktest.AfterTest(t)
	e := NewInMem(proto.Attributes{}, 1<<20)
	defer e.Close()

	b := e.NewBatch()
	defer b.Close()

	kv := &proto.RawKeyValue{Key: proto.EncodedKey("a"), Value: []byte("value")}
	if _, _, err := PutProto(b, proto.EncodedKey("proto"), kv); err != nil {
		t.Fatal(err)
	}
	getKV := &proto.RawKeyValue{}
	ok, keySize, valSize, err := b.GetProto(proto.EncodedKey("proto"), getKV)
	if !ok || err != nil {
		t.Fatalf("expected GetProto to success ok=%t: %s", ok, err)
	}
	if keySize != 5 {
		t.Errorf("expected key size 5; got %d", keySize)
	}
	var data []byte
	if data, err = gogoproto.Marshal(kv); err != nil {
		t.Fatal(err)
	}
	if valSize != int64(len(data)) {
		t.Errorf("expected value size %d; got %d", len(data), valSize)
	}
	if !reflect.DeepEqual(getKV, kv) {
		t.Errorf("expected %v; got %v", kv, getKV)
	}
	// Before commit, proto will not be available via engine.
	if ok, _, _, err = e.GetProto(proto.EncodedKey("proto"), getKV); ok || err != nil {
		t.Fatalf("expected GetProto to fail ok=%t: %s", ok, err)
	}
	// Commit and verify the proto can be read directly from the engine.
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if ok, _, _, err = e.GetProto(proto.EncodedKey("proto"), getKV); !ok || err != nil {
		t.Fatalf("expected GetProto to success ok=%t: %s", ok, err)
	}
	if !reflect.DeepEqual(getKV, kv) {
		t.Errorf("expected %v; got %v", kv, getKV)
	}
}

func TestBatchScan(t *testing.T) {
	defer leaktest.AfterTest(t)
	e := NewInMem(proto.Attributes{}, 1<<20)
	defer e.Close()

	b := e.NewBatch()
	defer b.Close()

	existingVals := []proto.RawKeyValue{
		{Key: proto.EncodedKey("a"), Value: []byte("1")},
		{Key: proto.EncodedKey("b"), Value: []byte("2")},
		{Key: proto.EncodedKey("c"), Value: []byte("3")},
		{Key: proto.EncodedKey("d"), Value: []byte("4")},
		{Key: proto.EncodedKey("e"), Value: []byte("5")},
		{Key: proto.EncodedKey("f"), Value: []byte("6")},
		{Key: proto.EncodedKey("g"), Value: []byte("7")},
		{Key: proto.EncodedKey("h"), Value: []byte("8")},
		{Key: proto.EncodedKey("i"), Value: []byte("9")},
		{Key: proto.EncodedKey("j"), Value: []byte("10")},
		{Key: proto.EncodedKey("k"), Value: []byte("11")},
		{Key: proto.EncodedKey("l"), Value: []byte("12")},
		{Key: proto.EncodedKey("m"), Value: []byte("13")},
	}
	for _, kv := range existingVals {
		if err := e.Put(kv.Key, kv.Value); err != nil {
			t.Fatal(err)
		}
	}

	batchVals := []proto.RawKeyValue{
		{Key: proto.EncodedKey("a"), Value: []byte("b1")},
		{Key: proto.EncodedKey("bb"), Value: []byte("b2")},
		{Key: proto.EncodedKey("c"), Value: []byte("b3")},
		{Key: proto.EncodedKey("dd"), Value: []byte("b4")},
		{Key: proto.EncodedKey("e"), Value: []byte("b5")},
		{Key: proto.EncodedKey("ff"), Value: []byte("b6")},
		{Key: proto.EncodedKey("g"), Value: []byte("b7")},
		{Key: proto.EncodedKey("hh"), Value: []byte("b8")},
		{Key: proto.EncodedKey("i"), Value: []byte("b9")},
		{Key: proto.EncodedKey("jj"), Value: []byte("b10")},
	}
	for _, kv := range batchVals {
		if err := b.Put(kv.Key, kv.Value); err != nil {
			t.Fatal(err)
		}
	}

	scans := []struct {
		start, end proto.EncodedKey
		max        int64
	}{
		// Full monty.
		{start: proto.EncodedKey("a"), end: proto.EncodedKey("z"), max: 0},
		// Select ~half.
		{start: proto.EncodedKey("a"), end: proto.EncodedKey("z"), max: 9},
		// Select one.
		{start: proto.EncodedKey("a"), end: proto.EncodedKey("z"), max: 1},
		// Select half by end key.
		{start: proto.EncodedKey("a"), end: proto.EncodedKey("f0"), max: 0},
		// Start at half and select rest.
		{start: proto.EncodedKey("f"), end: proto.EncodedKey("z"), max: 0},
		// Start at last and select max=10.
		{start: proto.EncodedKey("m"), end: proto.EncodedKey("z"), max: 10},
	}

	// Scan each case using the batch and store the results.
	results := map[int][]proto.RawKeyValue{}
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
	e := NewInMem(proto.Attributes{}, 1<<20)
	defer e.Close()

	b := e.NewBatch()
	defer b.Close()

	// Write initial value, then delete via batch.
	if err := e.Put(proto.EncodedKey("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(proto.EncodedKey("a")); err != nil {
		t.Fatal(err)
	}
	kvs, err := Scan(b, proto.EncodedKey(proto.KeyMin), proto.EncodedKey(proto.KeyMax), 0)
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
	e := NewInMem(proto.Attributes{}, 1<<20)
	defer e.Close()

	b := e.NewBatch()
	defer b.Close()

	// Write two values.
	if err := e.Put(proto.EncodedKey("a"), []byte("value1")); err != nil {
		t.Fatal(err)
	}
	if err := e.Put(proto.EncodedKey("b"), []byte("value2")); err != nil {
		t.Fatal(err)
	}
	// Now, delete "a" in batch.
	if err := b.Clear(proto.EncodedKey("a")); err != nil {
		t.Fatal(err)
	}
	// A scan with max=1 should scan "b".
	kvs, err := Scan(b, proto.EncodedKey(proto.KeyMin), proto.EncodedKey(proto.KeyMax), 1)
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
	e := NewInMem(proto.Attributes{}, 1<<20)
	defer e.Close()

	b := e.NewBatch()
	defer b.Close()

	// Write a merge to the batch.
	if err := b.Merge(proto.EncodedKey("a"), appender("bar")); err != nil {
		t.Fatal(err)
	}
	val, err := b.Get(proto.EncodedKey("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !compareMergedValues(t, val, appender("bar")) {
		t.Error("mismatch of \"a\"")
	}
	// Write an engine value.
	if err := e.Put(proto.EncodedKey("a"), appender("foo")); err != nil {
		t.Fatal(err)
	}
	// Now, read again and verify that the merge happens on top of the mod.
	val, err = b.Get(proto.EncodedKey("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, appender("foobar")) {
		t.Error("mismatch of \"a\"")
	}
}
