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
//
// Author: Ben Darnell

package storage

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

// Test that spans are properly classified as global or local and that
// getSpans respects the scope argument.
func TestSpanSetGetSpansScope(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var ss SpanSet
	ss.Add(SpanReadOnly, roachpb.Span{Key: roachpb.Key("a")})
	ss.Add(SpanReadOnly, roachpb.Span{Key: keys.RangeLastGCKey(1)})
	ss.Add(SpanReadOnly, roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")})

	exp := []roachpb.Span{{Key: keys.RangeLastGCKey(1)}}
	if act := ss.getSpans(SpanReadOnly, spanLocal); !reflect.DeepEqual(act, exp) {
		t.Errorf("get local spans: got %v, expected %v", act, exp)
	}

	exp = []roachpb.Span{
		{Key: roachpb.Key("a")},
		{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
	}

	if act := ss.getSpans(SpanReadOnly, spanGlobal); !reflect.DeepEqual(act, exp) {
		t.Errorf("get global spans: got %v, expected %v", act, exp)
	}
}

// Test that checkAllowed properly enforces boundaries.
func TestSpanSetCheckAllowedBoundaries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var ss SpanSet
	ss.Add(SpanReadOnly, roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")})
	ss.Add(SpanReadOnly, roachpb.Span{Key: roachpb.Key("g")})
	ss.Add(SpanReadOnly, roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("q")})

	allowed := []roachpb.Span{
		// Exactly as declared.
		{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")},
		{Key: roachpb.Key("g")},
		{Key: roachpb.Key("k"), EndKey: roachpb.Key("q")},

		// Points within the non-zero-length spans.
		{Key: roachpb.Key("c")},
		{Key: roachpb.Key("l")},

		// Sub-spans.
		{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
		{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
		{Key: roachpb.Key("l"), EndKey: roachpb.Key("m")},
	}
	for _, span := range allowed {
		if err := ss.checkAllowed(SpanReadOnly, span); err != nil {
			t.Errorf("expected %s to be allowed, but got error: %s", span, err)
		}
	}

	disallowed := []roachpb.Span{
		// Points outside the declared spans, and on the endpoints.
		{Key: roachpb.Key("a")},
		{Key: roachpb.Key("d")},
		{Key: roachpb.Key("h")},
		{Key: roachpb.Key("v")},
		{Key: roachpb.Key("q")},

		// Spans outside the declared spans.
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
		{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")},
		{Key: roachpb.Key("q"), EndKey: roachpb.Key("z")},

		// Partial overlap.
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
		{Key: roachpb.Key("c"), EndKey: roachpb.Key("m")},
		{Key: roachpb.Key("g"), EndKey: roachpb.Key("k")},

		// Just past the end.
		{Key: roachpb.Key("b"), EndKey: roachpb.Key("d").Next()},
		{Key: roachpb.Key("g"), EndKey: roachpb.Key("g").Next()},
		{Key: roachpb.Key("k"), EndKey: roachpb.Key("q").Next()},
	}
	for _, span := range disallowed {
		if err := ss.checkAllowed(SpanReadOnly, span); err == nil {
			t.Errorf("expected %s to be disallowed", span)
		}
	}
}

// Test that a span declared for write access also implies read
// access, but not vice-versa.
func TestSpanSetWriteImpliesRead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var ss SpanSet
	roSpan := roachpb.Span{Key: roachpb.Key("read-only")}
	rwSpan := roachpb.Span{Key: roachpb.Key("read-write")}
	ss.Add(SpanReadOnly, roSpan)
	ss.Add(SpanReadWrite, rwSpan)

	if err := ss.checkAllowed(SpanReadOnly, roSpan); err != nil {
		t.Errorf("expected to be allowed to read roSpan, error: %s", err)
	}
	if err := ss.checkAllowed(SpanReadWrite, roSpan); err == nil {
		t.Errorf("expected not to be allowed to write roSpan")
	}
	if err := ss.checkAllowed(SpanReadOnly, rwSpan); err != nil {
		t.Errorf("expected to be allowed to read rwSpan, error: %s", err)
	}
	if err := ss.checkAllowed(SpanReadWrite, rwSpan); err != nil {
		t.Errorf("expected to be allowed to read rwSpan, error: %s", err)
	}
}

func TestSpanSetBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	eng := engine.NewInMem(roachpb.Attributes{}, 10<<20)
	defer eng.Close()

	var ss SpanSet
	ss.Add(SpanReadWrite, roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("g")})
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

	batch := makeSpanSetBatch(eng.NewBatch(), &ss)
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
	iter := &SpanSetIterator{eng.NewIterator(false), &ss, nil, false}
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
