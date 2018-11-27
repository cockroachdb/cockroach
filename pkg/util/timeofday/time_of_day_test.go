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
// permissions and limitations under the License.

package timeofday

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

func TestString(t *testing.T) {
	expected := "01:02:03.456789"
	actual := New(1, 2, 3, 456789).String()
	if actual != expected {
		t.Errorf("expected %s, got %s", expected, actual)
	}
	testData := []struct {
		t   TimeOfDay
		exp string
	}{
		{New(1, 2, 3, 0), "01:02:03"},
		{New(1, 2, 3, 456000), "01:02:03.456"},
		{New(1, 2, 3, 456789), "01:02:03.456789"},
	}
	for i, td := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			actual := td.t.String()
			if actual != td.exp {
				t.Errorf("expected %s, got %s", td.exp, actual)
			}
		})
	}
}

func TestFromAndToTime(t *testing.T) {
	testData := []struct {
		s   string
		exp string
	}{
		{"0000-01-01T00:00:00Z", "1970-01-01T00:00:00Z"},
		{"2017-01-01T12:00:00.5Z", "1970-01-01T12:00:00.5Z"},
		{"9999-12-31T23:59:59.999999Z", "1970-01-01T23:59:59.999999Z"},
		{"2017-01-01T12:00:00-05:00", "1970-01-01T12:00:00Z"},
	}
	for _, td := range testData {
		t.Run(td.s, func(t *testing.T) {
			fromTime, err := time.Parse(time.RFC3339Nano, td.s)
			if err != nil {
				t.Fatal(err)
			}
			actual := FromTime(fromTime).ToTime().Format(time.RFC3339Nano)
			if actual != td.exp {
				t.Errorf("expected %s, got %s", td.exp, actual)
			}
		})
	}
}

func TestAdd(t *testing.T) {
	testData := []struct {
		t      TimeOfDay
		micros int64
		exp    TimeOfDay
	}{
		{New(12, 0, 0, 0), 1, New(12, 0, 0, 1)},
		{New(12, 0, 0, 0), microsecondsPerDay, New(12, 0, 0, 0)},
		{Max, 1, Min},
		{Min, -1, Max},
	}
	for _, td := range testData {
		d := duration.Duration{Nanos: td.micros * nanosPerMicro}
		t.Run(fmt.Sprintf("%s,%s", td.t, d), func(t *testing.T) {
			actual := td.t.Add(d)
			if actual != td.exp {
				t.Errorf("expected %s, got %s", td.exp, actual)
			}
		})
	}
}

func TestDifference(t *testing.T) {
	testData := []struct {
		t1        TimeOfDay
		t2        TimeOfDay
		expMicros int64
	}{
		{New(0, 0, 0, 0), New(0, 0, 0, 0), 0},
		{New(0, 0, 0, 1), New(0, 0, 0, 0), 1},
		{New(0, 0, 0, 0), New(0, 0, 0, 1), -1},
		{Max, Min, microsecondsPerDay - 1},
		{Min, Max, -1 * (microsecondsPerDay - 1)},
	}
	for _, td := range testData {
		t.Run(fmt.Sprintf("%s,%s", td.t1, td.t2), func(t *testing.T) {
			actual := Difference(td.t1, td.t2).Nanos / nanosPerMicro
			if actual != td.expMicros {
				t.Errorf("expected %d, got %d", td.expMicros, actual)
			}
		})
	}
}
