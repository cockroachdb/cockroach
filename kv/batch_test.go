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

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestBatchPrevNext tests batch.{Prev,Next}.
func TestBatchPrevNext(t *testing.T) {
	defer leaktest.AfterTest(t)
	span := func(strs ...string) []keys.Span {
		var r []keys.Span
		for i, str := range strs {
			if i%2 == 0 {
				r = append(r, keys.Span{Start: proto.Key(str)})
			} else {
				r[len(r)-1].End = proto.Key(str)
			}
		}
		return r
	}
	max, min := string(proto.KeyMax), string(proto.KeyMin)
	abc := span("a", "", "b", "", "c", "")
	testCases := []struct {
		spans             []keys.Span
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
		var ba proto.BatchRequest
		for _, span := range test.spans {
			args := &proto.ScanRequest{}
			args.Key, args.EndKey = span.Start, span.End
			ba.Add(args)
		}
		if next := next(ba, proto.Key(test.key)); !bytes.Equal(next, proto.Key(test.expFW)) {
			t.Errorf("%d: next: expected %q, got %q", i, test.expFW, next)
		}
		if prev := prev(ba, proto.Key(test.key)); !bytes.Equal(prev, proto.Key(test.expBW)) {
			t.Errorf("%d: prev: expected %q, got %q", i, test.expBW, prev)
		}
	}
}
