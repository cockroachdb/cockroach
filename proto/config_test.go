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

package proto

import (
	"bytes"
	"testing"
)

// TestRangeContains verifies methods to check whether a key or key range
// is contained within the range.
func TestRangeContains(t *testing.T) {
	desc := RangeDescriptor{}
	desc.StartKey = []byte("a")
	desc.EndKey = []byte("b")

	testData := []struct {
		start, end []byte
		contains   bool
	}{
		// Single keys.
		{[]byte("a"), []byte("a"), true},
		{[]byte("aa"), []byte("aa"), true},
		{[]byte("`"), []byte("`"), false},
		{[]byte("b"), []byte("b"), false},
		{[]byte("c"), []byte("c"), false},
		// Key ranges.
		{[]byte("a"), []byte("b"), true},
		{[]byte("a"), []byte("aa"), true},
		{[]byte("aa"), []byte("b"), true},
		{[]byte("0"), []byte("9"), false},
		{[]byte("`"), []byte("a"), false},
		{[]byte("b"), []byte("bb"), false},
		{[]byte("0"), []byte("bb"), false},
		{[]byte("aa"), []byte("bb"), false},
	}
	for _, test := range testData {
		if bytes.Compare(test.start, test.end) == 0 {
			if desc.ContainsKey(test.start) != test.contains {
				t.Errorf("expected key %q within range", test.start)
			}
		} else {
			if desc.ContainsKeyRange(test.start, test.end) != test.contains {
				t.Errorf("expected key range %q-%q within range", test.start, test.end)
			}
		}
	}
}
