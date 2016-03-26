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
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf

package kv

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestBatchPrevNext tests batch.{Prev,Next}.
func TestBatchPrevNext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	loc := func(s string) string {
		return string(keys.RangeDescriptorKey(roachpb.RKey(s)))
	}
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
		{spans: span(loc("a"), loc("c")), key: "c", expFW: "c", expBW: "c"},
		{spans: span(loc("a"), loc("c")), key: "c\x00", expFW: max, expBW: "c\x00"},
	}

	for i, test := range testCases {
		var ba roachpb.BatchRequest
		for _, span := range test.spans {
			args := &roachpb.ScanRequest{}
			args.Key, args.EndKey = span.Key, span.EndKey
			ba.Add(args)
		}
		if next, err := next(ba, roachpb.RKey(test.key)); err != nil {
			t.Errorf("%d: %v", i, err)
		} else if !bytes.Equal(next, roachpb.Key(test.expFW)) {
			t.Errorf("%d: next: expected %q, got %q", i, test.expFW, next)
		}
		if prev, err := prev(ba, roachpb.RKey(test.key)); err != nil {
			t.Errorf("%d: %v", i, err)
		} else if !bytes.Equal(prev, roachpb.Key(test.expBW)) {
			t.Errorf("%d: prev: expected %q, got %q", i, test.expBW, prev)
		}
	}
}
