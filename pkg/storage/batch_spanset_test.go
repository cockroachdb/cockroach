// Copyright 2017 The Cockroach Authors.
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

package storage

import (
	"bytes"
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func TestSpanSetBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	eng := engine.NewInMem(roachpb.Attributes{}, 10<<20)
	defer eng.Close()

	var ss spanset.SpanSet
	ss.Add(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("g")})
	outsideKey := engine.MakeMVCCMetadataKey(roachpb.Key("a"))
	outsideKey2 := engine.MakeMVCCMetadataKey(roachpb.Key("b"))
	outsideKey3 := engine.MakeMVCCMetadataKey(roachpb.Key("m"))
	insideKey := engine.MakeMVCCMetadataKey(roachpb.Key("c"))
	insideKey2 := engine.MakeMVCCMetadataKey(roachpb.Key("d"))

	// Write values outside the range that we can try to read later.
	if err := eng.Put(outsideKey, []byte("value")); err != nil {
		t.Fatalf("direct write failed: %s", err)
	}
	if err := eng.Put(outsideKey3, []byte("value")); err != nil {
		t.Fatalf("direct write failed: %s", err)
	}

	batch := spanset.NewBatch(eng.NewBatch(), &ss)
	defer batch.Close()

	// Writes inside the range work. Write twice for later read testing.
	if err := batch.Put(insideKey, []byte("value")); err != nil {
		t.Fatalf("failed to write inside the range: %s", err)
	}
	if err := batch.Put(insideKey2, []byte("value2")); err != nil {
		t.Fatalf("failed to write inside the range: %s", err)
	}

	// Writes outside the range fail. We try to cover all write methods
	// in the failure case to make sure the checkAllowed call is
	// present, but we don't attempt successful versions of all
	// methods since those are harder to set up.
	isWriteSpanErr := func(err error) bool {
		return testutils.IsError(err, "cannot write undeclared span")
	}
	if err := batch.Clear(outsideKey); !isWriteSpanErr(err) {
		t.Errorf("Clear: unexpected error %v", err)
	}
	if err := batch.ClearRange(outsideKey, outsideKey2); !isWriteSpanErr(err) {
		t.Errorf("ClearRange: unexpected error %v", err)
	}
	{
		iter := batch.NewIterator(false)
		err := batch.ClearIterRange(iter, outsideKey, outsideKey2)
		iter.Close()
		if !isWriteSpanErr(err) {
			t.Errorf("ClearIterRange: unexpected error %v", err)
		}
	}
	if err := batch.Merge(outsideKey, nil); !isWriteSpanErr(err) {
		t.Errorf("Merge: unexpected error %v", err)
	}
	if err := batch.Put(outsideKey, nil); !isWriteSpanErr(err) {
		t.Errorf("Put: unexpected error %v", err)
	}

	// Reads inside the range work.
	if value, err := batch.Get(insideKey); err != nil {
		t.Errorf("failed to read inside the range: %s", err)
	} else if !bytes.Equal(value, []byte("value")) {
		t.Errorf("failed to read previously written value, got %q", value)
	}

	// Reads outside the range fail.
	isReadSpanErr := func(err error) bool {
		return testutils.IsError(err, "cannot read undeclared span")
	}
	if _, err := batch.Get(outsideKey); !isReadSpanErr(err) {
		t.Errorf("Get: unexpected error %v", err)
	}
	if _, _, _, err := batch.GetProto(outsideKey, nil); !isReadSpanErr(err) {
		t.Errorf("GetProto: unexpected error %v", err)
	}
	if err := batch.Iterate(outsideKey, outsideKey2,
		func(v engine.MVCCKeyValue) (bool, error) {
			return false, errors.Errorf("unexpected callback: %v", v)
		},
	); !isReadSpanErr(err) {
		t.Errorf("Iterate: unexpected error %v", err)
	}

	func() {
		iter := batch.NewIterator(false)
		defer iter.Close()

		// Iterators check boundaries on seek and next/prev
		iter.Seek(outsideKey)
		if _, err := iter.Valid(); !isReadSpanErr(err) {
			t.Fatalf("Seek: unexpected error %v", err)
		}
		// Seeking back in bounds restores validity.
		iter.Seek(insideKey)
		if ok, err := iter.Valid(); !ok {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.Key(), insideKey) {
			t.Fatalf("expected key %s, got %s", insideKey, iter.Key())
		}
		iter.Next()
		if ok, err := iter.Valid(); !ok {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.Key(), insideKey2) {
			t.Fatalf("expected key %s, got %s", insideKey2, iter.Key())
		}
		// Scan out of bounds.
		iter.Next()
		if ok, err := iter.Valid(); ok {
			t.Fatalf("expected invalid iterator; found valid at key %s", iter.Key())
		} else if err != nil {
			// Scanning out of bounds sets Valid() to false but is not an error.
			t.Errorf("unexpected error on iterator: %s", err)
		}
	}()

	// Same test in reverse. We commit the batch and wrap an iterator on
	// the raw engine because we don't support bidirectional iteration
	// over a pending batch.
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	iter := spanset.NewIterator(eng.NewIterator(false), &ss)
	defer iter.Close()
	iter.SeekReverse(outsideKey)
	if _, err := iter.Valid(); !isReadSpanErr(err) {
		t.Fatalf("SeekReverse: unexpected error %v", err)
	}
	// Seeking back in bounds restores validity.
	iter.SeekReverse(insideKey2)
	if ok, err := iter.Valid(); !ok {
		t.Fatalf("expected valid iterator, err=%v", err)
	}
	if !reflect.DeepEqual(iter.Key(), insideKey2) {
		t.Fatalf("expected key %s, got %s", insideKey2, iter.Key())
	}
	iter.Prev()
	if ok, err := iter.Valid(); !ok {
		t.Fatalf("expected valid iterator, err=%v", err)
	}
	if !reflect.DeepEqual(iter.Key(), insideKey) {
		t.Fatalf("expected key %s, got %s", insideKey, iter.Key())
	}
	// Scan out of bounds.
	iter.Prev()
	if ok, err := iter.Valid(); ok {
		t.Fatalf("expected invalid iterator; found valid at key %s", iter.Key())
	} else if err != nil {
		t.Errorf("unexpected error on iterator: %s", err)
	}
	// Seeking back in bounds restores validity.
	iter.SeekReverse(insideKey)
	if ok, err := iter.Valid(); !ok {
		t.Fatalf("expected valid iterator, err=%v", err)
	}
}

// TestSpanSetMVCCResolveWriteIntentRangeUsingIter verifies that
// MVCCResolveWriteIntentRangeUsingIter does not stray outside of the passed-in
// key range (which it only used to do in this corner case tested here).
//
// See #20894.
func TestSpanSetMVCCResolveWriteIntentRangeUsingIter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	eng := engine.NewInMem(roachpb.Attributes{}, 10<<20)
	defer eng.Close()

	ctx := context.Background()

	value := roachpb.MakeValueFromString("irrelevant")

	if err := engine.MVCCPut(
		ctx,
		eng,
		nil, // ms
		roachpb.Key("b"),
		hlc.Timestamp{WallTime: 10}, // irrelevant
		value,
		nil, // txn
	); err != nil {
		t.Fatal(err)
	}

	var ss spanset.SpanSet
	ss.Add(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b\x00")})

	batch := spanset.NewBatch(eng.NewBatch(), &ss)
	defer batch.Close()

	iterAndBuf := engine.GetIterAndBuf(batch)
	defer iterAndBuf.Cleanup()

	intent := roachpb.Intent{
		Span:   roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b\x00")},
		Txn:    enginepb.TxnMeta{}, // unused
		Status: roachpb.PENDING,
	}

	if _, _, err := engine.MVCCResolveWriteIntentRangeUsingIter(
		ctx, batch, iterAndBuf, nil /* ms */, intent, math.MaxInt64,
	); err != nil {
		t.Fatal(err)
	}
}
