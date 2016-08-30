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

package parser

import (
	"testing"
	"time"
)

func TestCategory(t *testing.T) {
	if expected, actual := categoryString, Builtins["lower"][0].Category(); expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categoryString, Builtins["length"][0].Category(); expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categoryDateAndTime, Builtins["now"][0].Category(); expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categorySystemInfo, Builtins["version"][0].Category(); expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categoryDateAndTime, Builtins["to_date"][0].Category(); expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categoryDateAndTime, Builtins["to_timestamp"][0].Category(); expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categoryDateAndTime, Builtins["to_timestamp"][1].Category(); expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
}

func TestToDateAndTimestampBuildin(t *testing.T) {
	testCases := []struct {
		buildin  string
		idx      int
		ctx      *EvalContext
		args     DTuple
		expected Datum
	}{
		{
			"to_date",
			0,
			nil,
			DTuple{NewDString("2016-06-12"), NewDString("YYYY-MM-DD")},
			NewDDateFromTime(time.Date(2016, time.Month(06), 12, 0, 0, 0, 0, time.UTC), time.UTC),
		},
		{
			"to_date",
			0,
			nil,
			DTuple{NewDString("2016-122"), NewDString("YYYY-DDD")},
			NewDDateFromTime(time.Date(2016, time.Month(5), 1, 0, 0, 0, 0, time.UTC), time.UTC),
		},
		{
			"to_timestamp",
			0,
			&EvalContext{Location: &(time.UTC)},
			DTuple{NewDString("2016-06-12 11:12:30"), NewDString("YYYY-MM-DD HH:MI:SS")},
			MakeDTimestampTZ(time.Date(2016, time.Month(06), 12, 11, 12, 30, 0, time.UTC), time.Nanosecond),
		},
		{
			"to_timestamp",
			0,
			&EvalContext{Location: &(time.UTC)},
			DTuple{NewDString("2016-06-12 16:12:30"), NewDString("YYYY-MM-DD HH24:MI:SS")},
			MakeDTimestampTZ(time.Date(2016, time.Month(06), 12, 16, 12, 30, 0, time.UTC), time.Nanosecond),
		},
		{
			"to_timestamp",
			0,
			&EvalContext{Location: &(time.UTC)},
			DTuple{NewDString("2016Jun12 16:12:30"), NewDString("YYYYMonDD HH24:MI:SS")},
			MakeDTimestampTZ(time.Date(2016, time.Month(06), 12, 16, 12, 30, 0, time.UTC), time.Nanosecond),
		},
		{
			"to_timestamp",
			1,
			&EvalContext{Location: &(time.UTC)},
			DTuple{NewDFloat(DFloat(200120400))},
			MakeDTimestampTZ(time.Date(1976, time.Month(05), 5, 5, 0, 0, 0, time.UTC), time.Nanosecond),
		},
	}

	for _, c := range testCases {
		r, err := Builtins[c.buildin][c.idx].fn(c.ctx, c.args)
		if err != nil || r.Compare(c.expected) != 0 {
			t.Fatalf("result \"%s\" is not equal to expected \"%v\"", r, c.expected)
		}
	}
}
