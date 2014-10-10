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

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// TestBatchBasics verifies that all commands work in a batch, aren't
// visible until commit, and then are all visible after commit.
func TestBatchBasics(t *testing.T) {
	e := NewInMem(proto.Attributes{}, 1<<20)
	b := NewBatch(e)
	if err := b.Put(Key("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	// Write an engine value to be deleted.
	if err := e.Put(Key("b"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(Key("b")); err != nil {
		t.Fatal(err)
	}
	// Write an engine value to be merged.
	if err := e.Put(Key("c"), encoding.MustGobEncode(Appender("foo"))); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(Key("c"), encoding.MustGobEncode(Appender("bar"))); err != nil {
		t.Fatal(err)
	}

	// Check all keys are in initial state (nothing from batch has gone
	// through to engine until commit).
	expValues := []proto.RawKeyValue{
		{Key: Key("b"), Value: []byte("value")},
		{Key: Key("c"), Value: encoding.MustGobEncode(Appender("foo"))},
	}
	kvs, err := e.Scan(KeyMin, KeyMax, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expValues, kvs) {
		t.Errorf("%v != %v", kvs, expValues)
	}

	// Now, merged values should be:
	expValues = []proto.RawKeyValue{
		{Key: Key("a"), Value: []byte("value")},
		{Key: Key("c"), Value: encoding.MustGobEncode(Appender("foobar"))},
	}
	// Scan values from batch directly.
	kvs, err = b.Scan(KeyMin, KeyMax, 0)
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
	kvs, err = e.Scan(KeyMin, KeyMax, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expValues, kvs) {
		t.Errorf("%v != %v", kvs, expValues)
	}
}

func TestBatchGet(t *testing.T) {
	e := NewInMem(proto.Attributes{}, 1<<20)
	b := NewBatch(e)
	// Write initial values, then write to batch.
	if err := e.Put(Key("b"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := e.Put(Key("c"), encoding.MustGobEncode(Appender("foo"))); err != nil {
		t.Fatal(err)
	}
	// Write batch values.
	if err := b.Put(Key("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(Key("b")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(Key("c"), encoding.MustGobEncode(Appender("bar"))); err != nil {
		t.Fatal(err)
	}

	expValues := []proto.RawKeyValue{
		{Key: Key("a"), Value: []byte("value")},
		{Key: Key("b"), Value: nil},
		{Key: Key("c"), Value: encoding.MustGobEncode(Appender("foobar"))},
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

func TestBatchMerge(t *testing.T) {
	b := NewBatch(NewInMem(proto.Attributes{}, 1<<20))

	// Write batch put, delete & merge.
	if err := b.Put(Key("a"), encoding.MustGobEncode(Appender("a-value"))); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(Key("b")); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(Key("c"), encoding.MustGobEncode(Appender("c-value"))); err != nil {
		t.Fatal(err)
	}

	// Now, merge to all three keys.
	if err := b.Merge(Key("a"), encoding.MustGobEncode(Appender("append"))); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(Key("b"), encoding.MustGobEncode(Appender("append"))); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(Key("c"), encoding.MustGobEncode(Appender("append"))); err != nil {
		t.Fatal(err)
	}

	// Verify values.
	val, err := b.Get(Key("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, encoding.MustGobEncode(Appender("a-valueappend"))) {
		t.Error("mismatch of \"a\"")
	}

	val, err = b.Get(Key("b"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, encoding.MustGobEncode(Appender("append"))) {
		t.Error("mismatch of \"b\"")
	}

	val, err = b.Get(Key("c"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, encoding.MustGobEncode(Appender("c-valueappend"))) {
		t.Error("mismatch of \"c\"")
	}
}

func TestBatchProto(t *testing.T) {
	e := NewInMem(proto.Attributes{}, 1<<20)
	b := NewBatch(e)
	kv := &proto.RawKeyValue{Key: Key("a"), Value: []byte("value")}
	b.PutProto(Key("proto"), kv)
	getKV := &proto.RawKeyValue{}
	ok, kSize, vSize, err := b.GetProto(Key("proto"), getKV)
	if !ok || err != nil {
		t.Fatalf("expected GetProto to success ok=%t: %s", ok, err)
	}
	if kSize != 5 {
		t.Errorf("expected key size 5; got %d", kSize)
	}
	var data []byte
	if data, err = gogoproto.Marshal(kv); err != nil {
		t.Fatal(err)
	}
	if vSize != int64(len(data)) {
		t.Errorf("expected value size %d; got %d", len(data), vSize)
	}
	if !reflect.DeepEqual(getKV, kv) {
		t.Errorf("expected %v; got %v", kv, getKV)
	}
	// Before commit, proto will not be available via engine.
	if ok, err := GetProto(e, Key("proto"), getKV); ok || err != nil {
		t.Fatalf("expected GetProto to fail ok=%t: %s", ok, err)
	}
	// Commit and verify the proto can be read directly from the engine.
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if ok, err := GetProto(e, Key("proto"), getKV); !ok || err != nil {
		t.Fatalf("expected GetProto to success ok=%t: %s", ok, err)
	}
	if !reflect.DeepEqual(getKV, kv) {
		t.Errorf("expected %v; got %v", kv, getKV)
	}
}

func TestBatchScan(t *testing.T) {
	e := NewInMem(proto.Attributes{}, 1<<20)
	b := NewBatch(e)
	existingVals := []proto.RawKeyValue{
		{Key: Key("a"), Value: []byte("1")},
		{Key: Key("b"), Value: []byte("2")},
		{Key: Key("c"), Value: []byte("3")},
		{Key: Key("d"), Value: []byte("4")},
		{Key: Key("e"), Value: []byte("5")},
		{Key: Key("f"), Value: []byte("6")},
		{Key: Key("g"), Value: []byte("7")},
		{Key: Key("h"), Value: []byte("8")},
		{Key: Key("i"), Value: []byte("9")},
		{Key: Key("j"), Value: []byte("10")},
		{Key: Key("k"), Value: []byte("11")},
		{Key: Key("l"), Value: []byte("12")},
		{Key: Key("m"), Value: []byte("13")},
	}
	for _, kv := range existingVals {
		if err := e.Put(kv.Key, kv.Value); err != nil {
			t.Fatal(err)
		}
	}

	batchVals := []proto.RawKeyValue{
		{Key: Key("a"), Value: []byte("b1")},
		{Key: Key("bb"), Value: []byte("b2")},
		{Key: Key("c"), Value: []byte("b3")},
		{Key: Key("dd"), Value: []byte("b4")},
		{Key: Key("e"), Value: []byte("b5")},
		{Key: Key("ff"), Value: []byte("b6")},
		{Key: Key("g"), Value: []byte("b7")},
		{Key: Key("hh"), Value: []byte("b8")},
		{Key: Key("i"), Value: []byte("b9")},
		{Key: Key("jj"), Value: []byte("b10")},
	}
	for _, kv := range batchVals {
		if err := b.Put(kv.Key, kv.Value); err != nil {
			t.Fatal(err)
		}
	}

	scans := []struct {
		start, end Key
		max        int64
	}{
		// Full monty.
		{start: Key("a"), end: Key("z"), max: 0},
		// Select ~half.
		{start: Key("a"), end: Key("z"), max: 9},
		// Select one.
		{start: Key("a"), end: Key("z"), max: 1},
		// Select half by end key.
		{start: Key("a"), end: Key("f0"), max: 0},
		// Start at half and select rest.
		{start: Key("f"), end: Key("z"), max: 0},
		// Start at last and select max=10.
		{start: Key("m"), end: Key("z"), max: 10},
	}

	// Scan each case using the batch and store the results.
	results := map[int][]proto.RawKeyValue{}
	for i, scan := range scans {
		kvs, err := b.Scan(scan.start, scan.end, scan.max)
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
		kvs, err := e.Scan(scan.start, scan.end, scan.max)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(kvs, results[i]) {
			t.Errorf("%d: expected %v; got %v", i, results[i], kvs)
		}
	}
}

// TestBatchConcurrency verifies operation of batch when the
// underlying engine has concurrent modifications to overlapping
// keys. This should never happen with the way Cockroach uses
// batches, but worth verifying.
func TestBatchConcurrency(t *testing.T) {
	e := NewInMem(proto.Attributes{}, 1<<20)
	b := NewBatch(e)
	// Write a merge to the batch.
	if err := b.Merge(Key("a"), encoding.MustGobEncode(Appender("bar"))); err != nil {
		t.Fatal(err)
	}
	val, err := b.Get(Key("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, encoding.MustGobEncode(Appender("bar"))) {
		t.Error("mismatch of \"a\"")
	}
	// Write an engine value.
	if err := e.Put(Key("a"), encoding.MustGobEncode(Appender("foo"))); err != nil {
		t.Fatal(err)
	}
	// Now, read again and verify that the merge happens on top of the mod.
	val, err = b.Get(Key("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, encoding.MustGobEncode(Appender("foobar"))) {
		t.Error("mismatch of \"a\"")
	}
}
