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
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

type safeErrorTestCase struct {
	format string
	rs     []interface{}
	expErr string
}

// Exposed globally so it can be injected with platform-specific tests.
var safeErrorTestCases = func() []safeErrorTestCase {
	var errSentinel = struct{ error }{} // explodes if Error() called
	var errFundamental = errors.Errorf("%s", "not recoverable :(")
	var errWrapped1 = errors.Wrap(errFundamental, "not recoverable :(")
	var errWrapped2 = errors.Wrapf(errWrapped1, "not recoverable :(")
	var errWrapped3 = errors.Wrap(errWrapped2, "not recoverable :(")
	var errWrappedSentinel = errors.Wrap(errors.Wrapf(errSentinel, "unseen"), "unsung")

	runtimeErr := &runtime.TypeAssertionError{}

	return []safeErrorTestCase{
		{
			// Intended result of panic(context.DeadlineExceeded). Note that this is a known sentinel
			// error but a safeError is returned.
			format: "", rs: []interface{}{context.DeadlineExceeded},
			expErr: "?:0: context.deadlineExceededError: context deadline exceeded",
		},
		{
			// Intended result of panic(runtimeErr) which exhibits special case of known safe error.
			format: "", rs: []interface{}{runtimeErr},
			expErr: "?:0: *runtime.TypeAssertionError: interface conversion: interface is nil, not ",
		},
		{
			// Same as last, but skipping through to the cause: panic(errors.Wrap(safeErr, "gibberish")).
			format: "", rs: []interface{}{errors.Wrap(runtimeErr, "unseen")},
			expErr: "?:0: crash_reporting_test.go:60: caused by *errors.withMessage: caused by *runtime.TypeAssertionError: interface conversion: interface is nil, not ",
		},
		{
			// Special-casing switched off when format string present.
			format: "%s", rs: []interface{}{runtimeErr},
			expErr: "?:0: %s | *runtime.TypeAssertionError: interface conversion: interface is nil, not ",
		},
		{
			// Special-casing switched off when more than one reportable present.
			format: "", rs: []interface{}{runtimeErr, "foo"},
			expErr: "?:0: *runtime.TypeAssertionError: interface conversion: interface is nil, not ; string",
		},
		{
			format: "I like %s and %q and my pin code is %d", rs: []interface{}{Safe("A"), &SafeType{V: "B"}, 1234},
			expErr: "?:0: I like %s and %q and my pin code is %d | A; B; int",
		},
		{
			format: "outer %+v", rs: []interface{}{
				errors.Wrapf(context.Canceled, "this will unfortunately be lost: %d", Safe(6)),
			},
			expErr: "?:0: outer %+v | crash_reporting_test.go:79: caused by *errors.withMessage: caused by *errors.errorString: context canceled",
		},
		{
			// Verify that the special case still scrubs inside of the error.
			format: "", rs: []interface{}{&os.LinkError{Op: "moo", Old: "sec", New: "cret", Err: errors.New("assumed safe")}},
			expErr: "?:0: *os.LinkError: moo <redacted> <redacted>: assumed safe",
		},
		{
			// Verify that unknown sentinel errors print at least their type (regression test).
			// Also, that its Error() is never called (since it would panic).
			format: "%s", rs: []interface{}{errWrappedSentinel},
			expErr: "?:0: %s | crash_reporting_test.go:42: caused by *errors.withMessage: caused by crash_reporting_test.go:42: caused by *errors.withMessage: caused by struct { error }",
		},
		{
			format: "", rs: []interface{}{errWrapped3},
			expErr: "?:0: crash_reporting_test.go:41: caused by *errors.withMessage: caused by crash_reporting_test.go:40: caused by *errors.withMessage: caused by crash_reporting_test.go:39: caused by *errors.withMessage: caused by crash_reporting_test.go:38",
		},
	}
}()

func TestCrashReportingSafeError(t *testing.T) {
	for _, test := range safeErrorTestCases {
		t.Run("", func(t *testing.T) {
			err := ReportablesToSafeError(0, test.format, test.rs)
			if err == nil {
				t.Fatal(err)
			}
			const expType = "*log.safeError"
			if typStr := fmt.Sprintf("%T", err); typStr != expType {
				t.Errorf("expected type:\n%s\ngot type:\n%s", expType, typStr)
			}
			if errStr := err.Error(); errStr != test.expErr {
				t.Errorf("expected:\n%q\ngot:\n%q", test.expErr, errStr)
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
	startTime = timeutil.Unix(0, 0)
	testCases := []struct {
		crashTime time.Time
		expected  string
	}{
		{timeutil.Unix(0, 0), "<1s"},
		{timeutil.Unix(0, 0), "<1s"},
		{timeutil.Unix(1, 0), "<10s"},
		{timeutil.Unix(9, 0), "<10s"},
		{timeutil.Unix(10, 0), "<1m"},
		{timeutil.Unix(59, 0), "<1m"},
		{timeutil.Unix(60, 0), "<10m"},
		{timeutil.Unix(9*60, 0), "<10m"},
		{timeutil.Unix(10*60, 0), "<1h"},
		{timeutil.Unix(59*60, 0), "<1h"},
		{timeutil.Unix(60*60, 0), "<10h"},
		{timeutil.Unix(9*60*60, 0), "<10h"},
		{timeutil.Unix(10*60*60, 0), "<1d"},
		{timeutil.Unix(23*60*60, 0), "<1d"},
		{timeutil.Unix(24*60*60, 0), "<2d"},
		{timeutil.Unix(47*60*60, 0), "<2d"},
		{timeutil.Unix(119*60*60, 0), "<5d"},
		{timeutil.Unix(10*24*60*60, 0), "<11d"},
		{timeutil.Unix(365*24*60*60, 0), "<366d"},
	}
	for _, tc := range testCases {
		if a, e := uptimeTag(tc.crashTime), tc.expected; a != e {
			t.Errorf("uptimeTag(%v) got %v, want %v)", tc.crashTime, a, e)
		}
	}
}

func TestWithCause(t *testing.T) {
	parent := Safe("roses are safe").
		WithCause(
			Safe("violets").
				WithCause("are").
				WithCause(Safe("too")),
		).WithCause("bugs sure do suck").
		WithCause("and so do you").
		WithCause(Safe("j/k ❤"))

	if a, e := parent.SafeMessage(), "roses are safe"; a != e {
		t.Fatalf("expected %s, got %s", e, a)
	}

	act := ReportablesToSafeError(0, "", []interface{}{parent}).Error()
	const exp = "?:0: roses are safe: caused by violets: caused by <redacted>: " +
		"caused by too: caused by <redacted>: caused by <redacted>: caused by j/k ❤"

	if act != exp {
		t.Fatalf("wanted %s, got %s", exp, act)
	}
}
