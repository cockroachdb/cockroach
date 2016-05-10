// Copyright 2016 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package roachpb

import (
	"reflect"
	"strings"
	"testing"
)

func TestMergeSpans(t *testing.T) {
	makeSpan := func(s string) Span {
		parts := strings.Split(s, "-")
		if len(parts) == 2 {
			return Span{Key: Key(parts[0]), EndKey: Key(parts[1])}
		}
		return Span{Key: Key(s)}
	}
	makeSpans := func(s string) []Span {
		var spans []Span
		if len(s) > 0 {
			for _, p := range strings.Split(s, ",") {
				spans = append(spans, makeSpan(p))
			}
		}
		return spans
	}

	testCases := []struct {
		spans    string
		expected string
	}{
		{"", ""},
		{"a", "a"},
		{"a,b", "a,b"},
		{"b,a", "a,b"},
		{"a,a", "a"},
		{"a-b", "a-b"},
		{"a-b,b-c", "a-c"},
		{"a-c,a-b", "a-c"},
		{"a,b-c", "a,b-c"},
		{"a,a-c", "a-c"},
		{"a-c,b", "a-c"},
		{"a-c,c", "a-c\x00"},
		{"a-c,b-bb", "a-c"},
		{"a-c,b-c", "a-c"},
	}
	for i, c := range testCases {
		spans := makeSpans(c.spans)
		MergeSpans(&spans)
		expected := makeSpans(c.expected)
		if !reflect.DeepEqual(expected, spans) {
			t.Fatalf("%d: expected\n%s\n, but found:\n%s", i, expected, spans)
		}
	}
}
