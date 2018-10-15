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
	"unicode"
)

func TestExtractRelative(t *testing.T) {
	tests := []struct {
		s   string
		exp int
	}{
		{
			s:   keywordYesterday,
			exp: 16,
		},
		{
			s:   keywordToday,
			exp: 17,
		},
		{
			s:   keywordTomorrow,
			exp: 18,
		},
	}

	ctx := WithFixedNow(ISOContext(), time.Date(2018, 10, 17, 0, 0, 0, 0, time.UTC))
	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {

			p, err := newFieldExtract(ctx, tc.s, dateFields, dateRequiredFields)
			if err != nil {
				t.Fatal(err)
			}
			v, ok := p.Get(fieldDay)
			if !ok {
				t.Fatal("expecting day field")
			}
			if v != tc.exp {
				t.Fatalf("expected %d, got %d", tc.exp, v)
			}
		})
	}
}

func TestExtractSentinels(t *testing.T) {
	tests := []struct {
		s        string
		expected *fieldExtract
		err      bool
	}{
		{
			s:        keywordEpoch,
			expected: sentinelEpoch,
		},
		{
			s:        keywordInfinity,
			expected: sentinelInfinity,
		},
		{
			s:        "-" + keywordInfinity,
			expected: sentinelNegativeInfinity,
		},
		{
			s:        keywordNow,
			expected: sentinelNow,
		},
		{
			s:   keywordNow + " tomorrow",
			err: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			fe, err := newFieldExtract(ISOContext(), tc.s, dateTimeFields, dateTimeRequiredFields)
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
		expected []splitChunk
		tail     string
	}{
		{
			s: "99 foo!bar baz 55",
			expected: []splitChunk{
				{"99 ", "foo"},
				{"!", "bar"},
				{" ", "baz"},
			},
			tail: " 55",
		},
		{
			s:        "Εργαστήρια κατσαρίδων", /* Cockroach Labs */
			expected: []splitChunk{{"", "Εργαστήρια"}, {" ", "κατσαρίδων"}},
		},
		{
			s:        "123456",
			expected: []splitChunk{},
			tail:     "123456",
		},
	}

	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			got, tail := filterSplitString(tc.s, unicode.IsLetter)
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
		filterSplitString("foo bar baz", unicode.IsLetter)
	}
}
