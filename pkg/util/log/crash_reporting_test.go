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

package log

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"golang.org/x/net/context"
)

func TestCrashReportingSafeError(t *testing.T) {
	type testCase struct {
		format  string
		rs      []interface{}
		expType string
		expErr  string
	}

	runtimeErr := &runtime.TypeAssertionError{}

	testCases := []testCase{
		{
			// Intended result of panic(context.DeadlineExceeded).
			format: "", rs: []interface{}{context.DeadlineExceeded},
			expType: "*log.safeError", expErr: "?:0: <context.deadlineExceededError>",
		},
		{
			// Intended result of panic(runtimeErr) which exhibits special case of known safe error.
			format: "", rs: []interface{}{runtimeErr},
			expType: "*runtime.TypeAssertionError", expErr: "interface conversion: interface is nil, not ",
		},
		{
			// Special-casing switched off when format string present.
			format: "%s", rs: []interface{}{runtimeErr},
			expType: "*log.safeError", expErr: "?:0: %s | interface conversion: interface is nil, not ",
		},
		{
			// Special-casing switched off when more than one reportable present.
			format: "", rs: []interface{}{runtimeErr, "foo"},
			expType: "*log.safeError", expErr: "?:0: interface conversion: interface is nil, not ; <string>",
		},
		{
			format: "I like %s and %q and my pin code is %d", rs: []interface{}{Safe("A"), &SafeType{V: "B"}, 1234},
			expType: "*log.safeError", expErr: "?:0: I like %s and %q and my pin code is %d | A; B; <int>",
		},
	}

	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			err := reportablesToSafeError(0, test.format, test.rs)
			if err == nil {
				t.Fatal(err)
			}
			if typStr := fmt.Sprintf("%T", err); typStr != test.expType {
				t.Errorf("expected %s, got %s", test.expType, typStr)
			}
			if errStr := err.Error(); errStr != test.expErr {
				t.Errorf("expected %q, got %q", test.expErr, errStr)
			}
		})
	}
}

func TestingSetCrashReportingURL(url string) func() {
	oldCrashReportURL := crashReportURL
	crashReportURL = url
	return func() { crashReportURL = oldCrashReportURL }
}

func TestUptimeTag(t *testing.T) {
	startTime = time.Unix(0, 0)
	testCases := []struct {
		crashTime time.Time
		expected  string
	}{
		{time.Unix(0, 0), "<1s"},
		{time.Unix(0, 0), "<1s"},
		{time.Unix(1, 0), "<10s"},
		{time.Unix(9, 0), "<10s"},
		{time.Unix(10, 0), "<1m"},
		{time.Unix(59, 0), "<1m"},
		{time.Unix(60, 0), "<10m"},
		{time.Unix(9*60, 0), "<10m"},
		{time.Unix(10*60, 0), "<1h"},
		{time.Unix(59*60, 0), "<1h"},
		{time.Unix(60*60, 0), "<10h"},
		{time.Unix(9*60*60, 0), "<10h"},
		{time.Unix(10*60*60, 0), "<1d"},
		{time.Unix(23*60*60, 0), "<1d"},
		{time.Unix(24*60*60, 0), "<2d"},
		{time.Unix(47*60*60, 0), "<2d"},
		{time.Unix(119*60*60, 0), "<5d"},
		{time.Unix(10*24*60*60, 0), "<11d"},
		{time.Unix(365*24*60*60, 0), "<366d"},
	}
	for _, tc := range testCases {
		if a, e := uptimeTag(tc.crashTime), tc.expected; a != e {
			t.Errorf("uptimeTag(%v) got %v, want %v)", tc.crashTime, a, e)
		}
	}
}
