// Copyright 2018 The Cockroach Authors.
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

package pgdate

import (
	"reflect"
	"testing"
	"time"
)

func TestExtractRelative(t *testing.T) {
	tests := []struct {
		s   string
		rel int
	}{
		{
			s:   keywordYesterday,
			rel: -1,
		},
		{
			s:   keywordToday,
			rel: 0,
		},
		{
			s:   keywordTomorrow,
			rel: 1,
		},
	}

	now := time.Date(2018, 10, 17, 0, 0, 0, 0, time.UTC)
	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {

			ts, err := ParseDate(now, ParseModeYMD, tc.s)
			if err != nil {
				t.Fatal(err)
			}
			exp := now.AddDate(0, 0, tc.rel)
			if ts != exp {
				t.Fatalf("expected %v, got %v", exp, ts)
			}
		})
	}
}

func TestExtractSentinels(t *testing.T) {
	now := time.Unix(42, 56)
	tests := []struct {
		s        string
		expected time.Time
		err      bool
	}{
		{
			s:        keywordEpoch,
			expected: TimeEpoch,
		},
		{
			s:        keywordInfinity,
			expected: TimeInfinity,
		},
		{
			s:        "-" + keywordInfinity,
			expected: TimeNegativeInfinity,
		},
		{
			s:        keywordNow,
			expected: now,
		},
		{
			s:   keywordNow + " tomorrow",
			err: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			fe, err := extract(fieldExtract{now: now}, tc.s)
			if tc.err {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			} else {
				if err != nil {
					t.Fatal(err)
				}
			}
			if fe != tc.expected {
				t.Fatal("did not get expected sentinel value")
			}
		})
	}
}

func TestFieldExtractSet(t *testing.T) {
	p := fieldExtract{wanted: dateFields}
	if err := p.Set(fieldYear, 2018); err != nil {
		t.Fatal(err)
	}
	if err := p.Set(fieldMonth, 1); err != nil {
		t.Fatal(err)
	}
	if p.Wants(fieldSecond) {
		t.Fatal("should not want RelativeDate")
	}
	t.Log(p.String())
}

func TestFilterSplitString(t *testing.T) {
	tests := []struct {
		s        string
		expected []stringChunk
		tail     string
	}{
		{
			s: "@@ foo!bar baz %%",
			expected: []stringChunk{
				{"@@ ", "foo"},
				{"!", "bar"},
				{" ", "baz"},
			},
			tail: " %%",
		},
		{
			s:        "Εργαστήρια κατσαρίδων", /* Cockroach Labs */
			expected: []stringChunk{{"", "Εργαστήρια"}, {" ", "κατσαρίδων"}},
		},
		{
			s:        "!@#$%^",
			expected: []stringChunk{},
			tail:     "!@#$%^",
		},
	}

	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			got, tail := chunk(tc.s)
			if !reflect.DeepEqual(tc.expected, got) {
				t.Errorf("expected %v, got %v", tc.expected, got)
			}
			if tail != tc.tail {
				t.Errorf("expected tail %s, got %s", tail, tc.tail)
			}
		})
	}
}

func BenchmarkFilterSplitString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		chunk("foo bar baz")
	}
}
