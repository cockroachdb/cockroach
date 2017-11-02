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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

func TestFromAndToTime(t *testing.T) {
	testData := []struct {
		s   string
		exp string
	}{
		{"0000-01-01T00:00:00Z", "1970-01-01T00:00:00Z"},
		{"2017-01-01T12:00:00.5Z", "1970-01-01T12:00:00.5Z"},
		{"9999-12-31T23:59:59.999999Z", "1970-01-01T23:59:59.999999Z"},
	}
	for _, td := range testData {
		fromTime, err := time.Parse(time.RFC3339Nano, td.s)
		if err != nil {
			panic(err)
		}
		actual := ToTime(FromTime(fromTime)).Format(time.RFC3339Nano)
		if actual != td.exp {
			t.Errorf("FromTime(%s): expected %s, got %s", td.s, td.exp, actual)
		}
	}
}

func TestAdd(t *testing.T) {
	testData := []struct {
		t      TimeOfDay
		micros int64
		exp    TimeOfDay
	}{
		{NewTimeOfDay(12, 0, 0, 0), 1, NewTimeOfDay(12, 0, 0, 1)},
		{NewTimeOfDay(12, 0, 0, 0), microsecondsPerDay, NewTimeOfDay(12, 0, 0, 0)},
		{Max, 1, Min},
		{Min, -1, Max},
	}
	for _, td := range testData {
		d := duration.Duration{Nanos: td.micros * nanosPerMicro}
		actual := Add(td.t, d)
		if actual != td.exp {
			t.Errorf("Add(%s, %s): expected %s, got %s", td.t, d, td.exp, actual)
		}
	}
}

func TestDifference(t *testing.T) {
	testData := []struct {
		t1        TimeOfDay
		t2        TimeOfDay
		expMicros int64
	}{
		{NewTimeOfDay(0, 0, 0, 0), NewTimeOfDay(0, 0, 0, 0), 0},
		{NewTimeOfDay(0, 0, 0, 1), NewTimeOfDay(0, 0, 0, 0), 1},
		{NewTimeOfDay(0, 0, 0, 0), NewTimeOfDay(0, 0, 0, 1), -1},
		{Max, Min, microsecondsPerDay - 1},
		{Min, Max, -1 * (microsecondsPerDay - 1)},
	}
	for _, td := range testData {
		actual := Difference(td.t1, td.t2).Nanos / nanosPerMicro
		if actual != td.expMicros {
			t.Errorf("Difference(%s, %s): expected %d, got %d", td.t1, td.t2, td.expMicros, actual)
		}
	}
}
