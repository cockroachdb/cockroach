// Copyright 2015 The Cockroach Authors.
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
// Author: Tobias Schottdorf

package kv

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestBatchPrevNext tests batch.{Prev,Next}.
func TestBatchPrevNext(t *testing.T) {
	defer leaktest.AfterTest(t)
	span := func(strs ...string) []roachpb.Span {
		var r []roachpb.Span
		for i, str := range strs {
			if i%2 == 0 {
				r = append(r, roachpb.Span{Key: roachpb.Key(str)})
			} else {
				r[len(r)-1].EndKey = roachpb.Key(str)
			}
		}
		return r
	}
	max, min := string(roachpb.RKeyMax), string(roachpb.RKeyMin)
	abc := span("a", "", "b", "", "c", "")
	testCases := []struct {
		spans             []roachpb.Span
		key, expFW, expBW string
	}{
		{spans: span("a", "c", "b", ""), key: "b", expFW: "b", expBW: "b"},
		{spans: span("a", "c", "b", ""), key: "a", expFW: "a", expBW: "a"},
		{spans: span("a", "c", "d", ""), key: "c", expFW: "d", expBW: "c"},
		{spans: span("a", "c\x00", "d", ""), key: "c", expFW: "c", expBW: "c"},
		{spans: abc, key: "b", expFW: "b", expBW: "b"},
		{spans: abc, key: "b\x00", expFW: "c", expBW: "b\x00"},
		{spans: abc, key: "bb", expFW: "c", expBW: "b"},
		{spans: span(), key: "whatevs", expFW: max, expBW: min},
	}

	for i, test := range testCases {
		var ba roachpb.BatchRequest
		for _, span := range test.spans {
			args := &roachpb.ScanRequest{}
			args.Key, args.EndKey = span.Key, span.EndKey
			ba.Add(args)
		}
		if next := next(ba, roachpb.RKey(test.key)); !bytes.Equal(next, roachpb.Key(test.expFW)) {
			t.Errorf("%d: next: expected %q, got %q", i, test.expFW, next)
		}
		if prev := prev(ba, roachpb.RKey(test.key)); !bytes.Equal(prev, roachpb.Key(test.expBW)) {
			t.Errorf("%d: prev: expected %q, got %q", i, test.expBW, prev)
		}
	}
}

// TestRSpanIntersect verifies rSpan.intersect.
func TestRSpanIntersect(t *testing.T) {
	defer leaktest.AfterTest(t)
	rs := rSpan{key: roachpb.RKey("b"), endKey: roachpb.RKey("e")}

	testData := []struct {
		startKey, endKey roachpb.RKey
		expected         rSpan
	}{
		// Partially overlapping.
		{roachpb.RKey("a"), roachpb.RKey("c"), rSpan{key: roachpb.RKey("b"), endKey: roachpb.RKey("c")}},
		{roachpb.RKey("d"), roachpb.RKey("f"), rSpan{key: roachpb.RKey("d"), endKey: roachpb.RKey("e")}},
		// Descriptor surrounds the span.
		{roachpb.RKey("a"), roachpb.RKey("f"), rSpan{key: roachpb.RKey("b"), endKey: roachpb.RKey("e")}},
		// Span surrounds the descriptor.
		{roachpb.RKey("c"), roachpb.RKey("d"), rSpan{key: roachpb.RKey("c"), endKey: roachpb.RKey("d")}},
		// Descriptor has the same range as the span.
		{roachpb.RKey("b"), roachpb.RKey("e"), rSpan{key: roachpb.RKey("b"), endKey: roachpb.RKey("e")}},
	}

	for i, test := range testData {
		desc := roachpb.RangeDescriptor{}
		desc.StartKey = test.startKey
		desc.EndKey = test.endKey

		actual, err := rs.intersect(&desc)
		if err != nil {
			t.Error(err)
			continue
		}
		if bytes.Compare(actual.key, test.expected.key) != 0 ||
			bytes.Compare(actual.endKey, test.expected.endKey) != 0 {
			t.Errorf("%d: expected RSpan [%q,%q) but got [%q,%q)",
				i, test.expected.key, test.expected.endKey,
				actual.key, actual.endKey)
		}
	}

	// Error scenarios
	errorTestData := []struct {
		startKey, endKey roachpb.RKey
	}{
		{roachpb.RKey("a"), roachpb.RKey("b")},
		{roachpb.RKey("e"), roachpb.RKey("f")},
		{roachpb.RKey("f"), roachpb.RKey("g")},
	}
	for i, test := range errorTestData {
		desc := roachpb.RangeDescriptor{}
		desc.StartKey = test.startKey
		desc.EndKey = test.endKey
		if _, err := rs.intersect(&desc); err == nil {
			t.Errorf("%d: unexpected sucess", i)
		}
	}

}
