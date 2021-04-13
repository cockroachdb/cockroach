// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgdate

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
			d, depOnCtx, err := ParseDate(now, ParseModeYMD, tc.s)
			if err != nil {
				t.Fatal(err)
			}
			if !depOnCtx {
				t.Fatalf("relative dates should depend on context")
			}
			ts, err := d.ToTime()
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
	now := timeutil.Unix(42, 56)
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
		{
			s:   "j66001",
			err: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			fe := fieldExtract{currentTime: now}
			err := fe.Extract(tc.s)
			if tc.err {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if fe.MakeTimestamp() != tc.expected {
				t.Fatal("did not get expected sentinel value")
			}
		})
	}
}

func TestExtractInvalidJulianDate(t *testing.T) {
	testCases := []struct {
		wanted   fieldSet
		str      string
		expError string
	}{
		// Expect only a time, reject a year.
		{timeFields, "j69001", `value -4524 for field Year already present or not wanted
Wanted: [ Hour Minute Second Nanos Meridian TZHour TZMinute TZSecond ]
Already found in input: [ ]`},
		// Expect a date, reject when the year is specified twice.
		{dateFields, "j69001 j69002", `value -4524 for field Year already present or not wanted
Wanted: [ Era ]
Already found in input: [ Year Month Day ]`},
		/// Expect a date+time, reject when the date/month/day is specified twice.
		{dateTimeFields, "2010-10-10 j69002", `could not parse field: -10
Wanted: [ Era Nanos Meridian ]
Already found in input: [ Year Month Day Hour Minute Second TZHour TZMinute TZSecond ]`},
	}

	// TODO(knz): This would benefit from datadriven testing.

	now := timeutil.Unix(42, 56)
	for _, tc := range testCases {
		fe := fieldExtract{
			currentTime: now,
			wanted:      tc.wanted,
		}
		err := fe.Extract(tc.str)
		if err == nil {
			t.Errorf("%+v: expected error, got no error", tc)
			continue
		}

		msg := err.Error()
		msg += "\n" + errors.FlattenDetails(err)
		if msg != tc.expError {
			t.Errorf("expected error:\n  %v\ngot:\n  %v",
				strings.ReplaceAll(tc.expError, "\n", "\n  "),
				strings.ReplaceAll(msg, "\n", "\n  "))
		}
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

func TestChunking(t *testing.T) {
	// Using an over-long UTF-8 sequence from:
	// https://www.cl.cam.ac.uk/~mgk25/ucs/examples/UTF-8-test.txt
	badString := string([]byte{0xe0, 0x80, 0xaf})

	tests := []struct {
		s        string
		count    int
		expected []stringChunk
		tail     string
	}{
		{
			// Empty input.
			s:        "",
			expected: []stringChunk{},
		},
		{
			s:     "@@ foo!bar baz %%",
			count: 3,
			expected: []stringChunk{
				{"@@ ", "foo"},
				{"!", "bar"},
				{" ", "baz"},
			},
			tail: " %%",
		},
		{
			s:        "Εργαστήρια κατσαρίδων", /* Cockroach Labs */
			count:    2,
			expected: []stringChunk{{"", "Εργαστήρια"}, {" ", "κατσαρίδων"}},
		},
		{
			s:        "!@#$%^",
			expected: []stringChunk{},
			tail:     "!@#$%^",
		},
		// Check cases where we see bad UTF-8 inputs.  We should
		// try to keep scanning until a reasonable value reappears.
		{
			s:     "foo bar baz" + badString + "boom",
			count: 4,
			expected: []stringChunk{
				{"", "foo"},
				{" ", "bar"},
				{" ", "baz"},
				{badString, "boom"},
			},
		},
		{
			s:     badString + "boom",
			count: 1,
			expected: []stringChunk{
				{string([]byte{0xe0, 0x80, 0xaf}), "boom"},
			},
		},
		{
			s:     "boom" + badString,
			count: 1,
			expected: []stringChunk{
				{"", "boom"},
			},
			tail: badString,
		},
		{
			s:        badString,
			expected: []stringChunk{},
			tail:     badString,
		},
		{
			// This should be too long to fit in the slice.
			s:     "1 2 3 4 5 6 7 8 9 10",
			count: -1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			textChunks := make([]stringChunk, 8)
			count, tail := chunk(tc.s, textChunks)
			if count != tc.count {
				t.Errorf("expected %d, got %d", len(tc.expected), count)
			}
			if count < 0 {
				return
			}
			if !reflect.DeepEqual(tc.expected, textChunks[:count]) {
				t.Errorf("expected %v, got %v", tc.expected, textChunks[:count])
			}
			if tail != tc.tail {
				t.Errorf("expected tail %s, got %s", tail, tc.tail)
			}
		})
	}
}

func BenchmarkChunking(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := make([]stringChunk, 8)
		chunk("foo bar baz", buf)
	}
}
