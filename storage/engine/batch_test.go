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

package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/stop"
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

func testBatchBasics(t *testing.T, commit func(e Engine, b Batch) error) {
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	b := e.NewBatch()
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

	// Check all keys are in initial state (nothing from batch has gone
	// through to engine until commit).
	expValues := []MVCCKeyValue{
		{Key: mvccKey("b"), Value: []byte("value")},
		{Key: mvccKey("c"), Value: appender("foo")},
	}
	kvs, err := Scan(e, mvccKey(roachpb.RKeyMin), mvccKey(roachpb.RKeyMax), 0)
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
	}
	// Scan values from batch directly.
	kvs, err = Scan(b, mvccKey(roachpb.RKeyMin), mvccKey(roachpb.RKeyMax), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expValues, kvs) {
		t.Errorf("%v != %v", kvs, expValues)
	}

	// Commit batch and verify direct engine scan yields correct values.
	if err := commit(e, b); err != nil {
		t.Fatal(err)
	}
	kvs, err = Scan(e, mvccKey(roachpb.RKeyMin), mvccKey(roachpb.RKeyMax), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expValues, kvs) {
		t.Errorf("%v != %v", kvs, expValues)
	}
}

// TestBatchBasics verifies that all commands work in a batch, aren't
// visible until commit, and then are all visible after commit.
func TestBatchBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBatchBasics(t, func(e Engine, b Batch) error {
		return b.Commit()
	})
}

func TestBatchRepr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBatchBasics(t, func(e Engine, b Batch) error {
		repr := b.Repr()

		// Simple sanity checks about the format of the batch representation. This
		// is inefficient decoding code. Do not move outside of test code.
		buf := bytes.NewReader(repr)
		var seq uint64
		var count uint32
		if err := binary.Read(buf, binary.LittleEndian, &seq); err != nil {
			t.Fatal(err)
		}
		if seq != 0 {
			// The sequence number is set when the batch is written.
			t.Fatalf("bad sequence: expected 0, but found %d", seq)
		}
		if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
			t.Fatal(err)
		}
		if expected := uint32(3); expected != count {
			t.Fatalf("bad count: expected %d, but found %d", expected, count)
		}

		const (
			// These constants come from rocksdb/db/dbformat.h.
			TypeDeletion = 0x0
			TypeValue    = 0x1
			TypeMerge    = 0x2
			// TypeLogData                    = 0x3
			// TypeColumnFamilyDeletion       = 0x4
			// TypeColumnFamilyValue          = 0x5
			// TypeColumnFamilyMerge          = 0x6
			// TypeSingleDeletion             = 0x7
			// TypeColumnFamilySingleDeletion = 0x8
		)

		varstring := func(buf *bytes.Reader) (string, error) {
			n, err := binary.ReadUvarint(buf)
			if err != nil {
				return "", err
			}
			s := make([]byte, n)
			c, err := buf.Read(s)
			if err != nil {
				return "", err
			}
			if c != int(n) {
				return "", fmt.Errorf("expected %d, but found %d", n, c)
			}
			return string(s), nil
		}

		var ops []string
		for i := uint32(0); i < count; i++ {
			typ, err := buf.ReadByte()
			if err != nil {
				t.Fatal(err)
			}
			switch typ {
			case TypeDeletion:
				k, err := varstring(buf)
				if err != nil {
					t.Fatal(err)
				}
				ops = append(ops, fmt.Sprintf("delete(%s)", k))
			case TypeValue:
				k, err := varstring(buf)
				if err != nil {
					t.Fatal(err)
				}
				v, err := varstring(buf)
				if err != nil {
					t.Fatal(err)
				}
				ops = append(ops, fmt.Sprintf("put(%s,%s)", k, v))
			case TypeMerge:
				k, err := varstring(buf)
				if err != nil {
					t.Fatal(err)
				}
				// The merge value is a protobuf and not easily displayable.
				if _, err = varstring(buf); err != nil {
					t.Fatal(err)
				}
				ops = append(ops, fmt.Sprintf("merge(%s)", k))
			default:
				t.Fatalf("%d: unexpected type %d", i, typ)
			}
		}

		// The keys in the batch have the internal MVCC encoding applied which for
		// this test implies an appended 0 byte.
		expOps := []string{"put(a\x00,value)", "delete(b\x00)", "merge(c\x00)"}
		if !reflect.DeepEqual(expOps, ops) {
			t.Fatalf("expected %v, but found %v", expOps, ops)
		}

		return e.ApplyBatchRepr(repr)
	})
}

func TestBatchGet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

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

	expValues := []MVCCKeyValue{
		{Key: mvccKey("a"), Value: []byte("value")},
		{Key: mvccKey("b"), Value: nil},
		{Key: mvccKey("c"), Value: appender("foobar")},
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
	var resultV, expectedV enginepb.MVCCMetadata
	if err := proto.Unmarshal(result, &resultV); err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(expected, &expectedV); err != nil {
		t.Fatal(err)
	}
	return reflect.DeepEqual(resultV, expectedV)
}

func TestBatchMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

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
}

func TestBatchProto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	b := e.NewBatch()
	defer b.Close()

	val := roachpb.MakeValueFromString("value")
	if _, _, err := PutProto(b, mvccKey("proto"), &val); err != nil {
		t.Fatal(err)
	}
	getVal := &roachpb.Value{}
	ok, keySize, valSize, err := b.GetProto(mvccKey("proto"), getVal)
	if !ok || err != nil {
		t.Fatalf("expected GetProto to success ok=%t: %s", ok, err)
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
	if ok, _, _, err := e.GetProto(mvccKey("proto"), getVal); ok || err != nil {
		t.Fatalf("expected GetProto to fail ok=%t: %s", ok, err)
	}
	// Commit and verify the proto can be read directly from the engine.
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if ok, _, _, err := e.GetProto(mvccKey("proto"), getVal); !ok || err != nil {
		t.Fatalf("expected GetProto to success ok=%t: %s", ok, err)
	}
	if !proto.Equal(getVal, &val) {
		t.Errorf("expected %v; got %v", &val, getVal)
	}
}

func TestBatchScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

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
		start, end MVCCKey
		max        int64
	}{
		// Full monty.
		{start: mvccKey("a"), end: mvccKey("z"), max: 0},
		// Select ~half.
		{start: mvccKey("a"), end: mvccKey("z"), max: 9},
		// Select one.
		{start: mvccKey("a"), end: mvccKey("z"), max: 1},
		// Select half by end key.
		{start: mvccKey("a"), end: mvccKey("f0"), max: 0},
		// Start at half and select rest.
		{start: mvccKey("f"), end: mvccKey("z"), max: 0},
		// Start at last and select max=10.
		{start: mvccKey("m"), end: mvccKey("z"), max: 10},
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
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	b := e.NewBatch()
	defer b.Close()

	// Write initial value, then delete via batch.
	if err := e.Put(mvccKey("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(mvccKey("a")); err != nil {
		t.Fatal(err)
	}
	kvs, err := Scan(b, mvccKey(roachpb.RKeyMin), mvccKey(roachpb.RKeyMax), 0)
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
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

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
	kvs, err := Scan(b, mvccKey(roachpb.RKeyMin), mvccKey(roachpb.RKeyMax), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 || !bytes.Equal(kvs[0].Key.Key, []byte("b")) {
		t.Errorf("expected scan of \"b\"; got %v", kvs)
	}
}

// TestBatchConcurrency verifies operation of batch when the
// underlying engine has concurrent modifications to overlapping
// keys. This should never happen with the way Cockroach uses
// batches, but worth verifying.
func TestBatchConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

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
}

func TestBatchBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	batch := e.NewBatch().(*rocksDBBatch)
	defer batch.Close()

	builder := &rocksDBBatchBuilder{}

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
		if err := dbMerge(batch.batch, key, appender("bar")); err != nil {
			t.Fatal(err)
		}

		builder.Put(key, []byte("value"))
		builder.Clear(key)
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
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	rng, _ := randutil.NewPseudoRand()

	for i := 0; i < 1000; i++ {
		count := 1 + rng.Intn(1000)

		func() {
			batch := e.NewBatch().(*rocksDBBatch)
			defer batch.Close()

			builder := &rocksDBBatchBuilder{}

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
				// Generate a random mixture of puts, deletes and merges.
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

func TestBatchDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

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
	iter := distinct.NewIterator(false)
	iter.Seek(mvccKey("a"))
	if !iter.Valid() {
		t.Fatalf("expected iterator to be valid")
	}
	if string(iter.Key().Key) != "a" {
		t.Fatalf("expected a, but got %s", iter.Key())
	}

	// Writes to the distinct batch are not readable by the distinct batch.
	if err := distinct.Put(mvccKey("c"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	if v, err := distinct.Get(mvccKey("c")); err != nil {
		t.Fatal(err)
	} else if v != nil {
		t.Fatalf("expected nothing, but got %s", v)
	}
	distinct.Close()

	// Writes to the distinct batch are reflected in the original batch.
	if v, err := batch.Get(mvccKey("c")); err != nil {
		t.Fatal(err)
	} else if string(v) != "c" {
		t.Fatalf("expected c, but got %s", v)
	}
}

func TestBatchDistinctPanics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := NewInMem(roachpb.Attributes{}, 1<<20, stopper)

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
		func() { _ = batch.ApplyBatchRepr(nil) },
		func() { _, _ = batch.Get(a) },
		func() { _, _, _, _ = batch.GetProto(a, nil) },
		func() { _ = batch.Iterate(a, a, nil) },
		func() { _ = batch.NewIterator(false) },
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
}
