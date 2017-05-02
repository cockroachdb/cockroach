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
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf

package util

import (
	"testing"
	"time"
)

func TestTruncateDuration(t *testing.T) {
	zero := time.Duration(0).String()
	testCases := []struct {
		d, r time.Duration
		s    string
	}{
		{0, 1, zero},
		{0, 1, zero},
		{time.Second, 1, "1s"},
		{time.Second, 2 * time.Second, zero},
		{time.Second + 1, time.Second, "1s"},
		{11 * time.Nanosecond, 10 * time.Nanosecond, "10ns"},
		{time.Hour + time.Nanosecond + 3*time.Millisecond + time.Second, time.Millisecond, "1h0m1.003s"},
	}
	for i, tc := range testCases {
		if s := TruncateDuration(tc.d, tc.r).String(); s != tc.s {
			t.Errorf("%d: (%s,%s) should give %s, but got %s", i, tc.d, tc.r, tc.s, s)
		}
	}
}
