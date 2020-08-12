// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"context"
	"net"
	"os"
	"regexp"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/pmezard/go-difflib/difflib"
)

// Renumber lines so they're stable no matter what changes above. (We
// could make the regexes accept any string of digits, but we also
// want to make sure that the correct line numbers get captured).
//
//line crash_reporting_test.go:1000

type safeErrorTestCase struct {
	err    error
	expErr string
}

// Exposed globally so it can be injected with platform-specific tests.
var safeErrorTestCases = func() []safeErrorTestCase {
	var errSentinel = struct{ error }{} // explodes if Error() called
	var errFundamental = errors.Errorf("%s", "not recoverable :(")
	var errWrapped1 = errors.Wrap(errFundamental, "not recoverable :(")
	var errWrapped2 = errors.Wrapf(errWrapped1, "this is reportable")
	var errWrapped3 = errors.Wrap(errWrapped2, "not recoverable :(")
	var errWrappedSentinel = errors.Wrap(errors.Wrapf(errSentinel, "this is reportable"), "seecret")

	runtimeErr := makeTypeAssertionErr()

	return []safeErrorTestCase{
		{
			// Special case in errors.Redact().
			err:    context.DeadlineExceeded,
			expErr: "context.deadlineExceededError: context deadline exceeded",
		},
		{
			// Special case in errors.Redact().
			err:    runtimeErr,
			expErr: "*runtime.TypeAssertionError: interface conversion: interface {} is nil, not int",
		},
		{
			// Same as last, but skipping through to the cause: panic(errors.Wrap(safeErr, "gibberish")).
			err: errors.Wrap(runtimeErr, "unseen"),
			expErr: `...crash_reporting_test.go:NN: *runtime.TypeAssertionError: interface conversion: interface {} is nil, not int
wrapper: <*errutil.withMessage>
wrapper: <*withstack.withStack>
(more details:)
github.com/cockroachdb/cockroach/pkg/util/log.glob..func4
	...crash_reporting_test.go:NN
github.com/cockroachdb/cockroach/pkg/util/log.init
	...crash_reporting_test.go:NN
runtime.doInit
	...proc.go:NN
runtime.doInit
	...proc.go:NN
runtime.main
	...proc.go:NN
runtime.goexit
	...asm_amd64.s:NN`,
		},
		{
			// Safe errors revealed in safe details of error wraps/objects.
			err: errors.Newf("%s", runtimeErr),
			expErr: `...crash_reporting_test.go:NN: <*errors.errorString>
wrapper: <*safedetails.withSafeDetails>
(more details:)
%s
-- arg 1: *runtime.TypeAssertionError: interface conversion: interface {} is nil, not int
wrapper: <*withstack.withStack>
(more details:)
github.com/cockroachdb/cockroach/pkg/util/log.glob..func4
	...crash_reporting_test.go:NN
github.com/cockroachdb/cockroach/pkg/util/log.init
	...crash_reporting_test.go:NN
runtime.doInit
	...proc.go:NN
runtime.doInit
	...proc.go:NN
runtime.main
	...proc.go:NN
runtime.goexit
	...asm_amd64.s:NN`,
		},
		{
			// More embedding of safe details.
			err: errors.WithSafeDetails(runtimeErr, "foo"),
			expErr: `*runtime.TypeAssertionError: interface conversion: interface {} is nil, not int
wrapper: <*safedetails.withSafeDetails>
(more details:)
foo`,
		},
		{
			err: errors.Newf("I like %s and my pin code is %d or %d", Safe("A"), 1234, Safe(9999)),
			expErr: `...crash_reporting_test.go:NN: <*errors.errorString>
wrapper: <*safedetails.withSafeDetails>
(more details:)
I like %s and my pin code is %d or %d
-- arg 1: A
-- arg 2: <int>
-- arg 3: 9999
wrapper: <*withstack.withStack>
(more details:)
github.com/cockroachdb/cockroach/pkg/util/log.glob..func4
	...crash_reporting_test.go:NN
github.com/cockroachdb/cockroach/pkg/util/log.init
	...crash_reporting_test.go:NN
runtime.doInit
	...proc.go:NN
runtime.doInit
	...proc.go:NN
runtime.main
	...proc.go:NN
runtime.goexit
	...asm_amd64.s:NN`,
		},
		{
			err: errors.Wrapf(context.Canceled, "this is preserved: %d", Safe(6)),
			expErr: `...crash_reporting_test.go:NN: *errors.errorString: context canceled
wrapper: <*errutil.withMessage>
wrapper: <*safedetails.withSafeDetails>
(more details:)
this is preserved: %d
-- arg 1: 6
wrapper: <*withstack.withStack>
(more details:)
github.com/cockroachdb/cockroach/pkg/util/log.glob..func4
	...crash_reporting_test.go:NN
github.com/cockroachdb/cockroach/pkg/util/log.init
	...crash_reporting_test.go:NN
runtime.doInit
	...proc.go:NN
runtime.doInit
	...proc.go:NN
runtime.main
	...proc.go:NN
runtime.goexit
	...asm_amd64.s:NN`,
		},
		{
			// Verify that the special case still scrubs inside of the error.
			err: &os.LinkError{Op: "moo", Old: "sec", New: "cret", Err: errors.WithSafeDetails(leafErr{}, "assumed safe")},
			expErr: `<log.leafErr>
wrapper: <*safedetails.withSafeDetails>
(more details:)
assumed safe
wrapper: *os.LinkError: moo <redacted> <redacted>`,
		},
		{
			// Verify that unknown sentinel errors print at least their type (regression test).
			// Also, that its Error() is never called (since it would panic).
			err: errWrappedSentinel,
			expErr: `...crash_reporting_test.go:NN: <struct { error }>
wrapper: <*errutil.withMessage>
wrapper: <*safedetails.withSafeDetails>
(more details:)
this is reportable
wrapper: <*withstack.withStack>
(more details:)
github.com/cockroachdb/cockroach/pkg/util/log.glob..func4
	...crash_reporting_test.go:NN
github.com/cockroachdb/cockroach/pkg/util/log.init
	...crash_reporting_test.go:NN
runtime.doInit
	...proc.go:NN
runtime.doInit
	...proc.go:NN
runtime.main
	...proc.go:NN
runtime.goexit
	...asm_amd64.s:NN
wrapper: <*errutil.withMessage>
wrapper: <*withstack.withStack>
(more details:)
github.com/cockroachdb/cockroach/pkg/util/log.glob..func4
	...crash_reporting_test.go:NN
github.com/cockroachdb/cockroach/pkg/util/log.init
	...crash_reporting_test.go:NN
runtime.doInit
	...proc.go:NN
runtime.doInit
	...proc.go:NN
runtime.main
	...proc.go:NN
runtime.goexit
	...asm_amd64.s:NN`,
		},
		{
			err: errWrapped3,
			expErr: `...crash_reporting_test.go:NN: <*errors.errorString>
wrapper: <*safedetails.withSafeDetails>
(more details:)
%s
-- arg 1: <string>
wrapper: <*withstack.withStack>
(more details:)
github.com/cockroachdb/cockroach/pkg/util/log.glob..func4
	...crash_reporting_test.go:NN
github.com/cockroachdb/cockroach/pkg/util/log.init
	...crash_reporting_test.go:NN
runtime.doInit
	...proc.go:NN
runtime.doInit
	...proc.go:NN
runtime.main
	...proc.go:NN
runtime.goexit
	...asm_amd64.s:NN
wrapper: <*errutil.withMessage>
wrapper: <*withstack.withStack>
(more details:)
github.com/cockroachdb/cockroach/pkg/util/log.glob..func4
	...crash_reporting_test.go:NN
github.com/cockroachdb/cockroach/pkg/util/log.init
	...crash_reporting_test.go:NN
runtime.doInit
	...proc.go:NN
runtime.doInit
	...proc.go:NN
runtime.main
	...proc.go:NN
runtime.goexit
	...asm_amd64.s:NN
wrapper: <*errutil.withMessage>
wrapper: <*safedetails.withSafeDetails>
(more details:)
this is reportable
wrapper: <*withstack.withStack>
(more details:)
github.com/cockroachdb/cockroach/pkg/util/log.glob..func4
	...crash_reporting_test.go:NN
github.com/cockroachdb/cockroach/pkg/util/log.init
	...crash_reporting_test.go:NN
runtime.doInit
	...proc.go:NN
runtime.doInit
	...proc.go:NN
runtime.main
	...proc.go:NN
runtime.goexit
	...asm_amd64.s:NN
wrapper: <*errutil.withMessage>
wrapper: <*withstack.withStack>
(more details:)
github.com/cockroachdb/cockroach/pkg/util/log.glob..func4
	...crash_reporting_test.go:NN
github.com/cockroachdb/cockroach/pkg/util/log.init
	...crash_reporting_test.go:NN
runtime.doInit
	...proc.go:NN
runtime.doInit
	...proc.go:NN
runtime.main
	...proc.go:NN
runtime.goexit
	...asm_amd64.s:NN`,
		},
		{
			err: &net.OpError{
				Op:     "write",
				Net:    "tcp",
				Source: &util.UnresolvedAddr{AddressField: "sensitive-source"},
				Addr:   &util.UnresolvedAddr{AddressField: "sensitive-addr"},
				Err:    leafErr{},
			},
			expErr: `<log.leafErr>
wrapper: *net.OpError: write tcp<redacted>-><redacted>`,
		},
	}
}()

func TestCrashReportingSafeError(t *testing.T) {
	fileref := regexp.MustCompile(`((?:[a-zA-Z0-9\._@-]*/)*)([a-zA-Z0-9._@-]*\.(?:go|s)):\d+`)

	for _, test := range safeErrorTestCases {
		t.Run("safeErr", func(t *testing.T) {
			errStr := errors.Redact(test.err)
			errStr = fileref.ReplaceAllString(errStr, "...$2:NN")
			if errStr != test.expErr {
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A:        difflib.SplitLines(test.expErr),
					B:        difflib.SplitLines(errStr),
					FromFile: "Expected",
					FromDate: "",
					ToFile:   "Actual",
					ToDate:   "",
					Context:  1,
				})
				t.Errorf("Diff:\n%s", diff)
			}
		})
	}
}

type leafErr struct{}

func (leafErr) Error() string { return "error" }

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

// makeTypeAssertionErr returns a runtime.Error with the message:
//     interface conversion: interface {} is nil, not int
func makeTypeAssertionErr() (result runtime.Error) {
	defer func() {
		e := recover()
		result = e.(runtime.Error)
	}()
	var x interface{}
	_ = x.(int)
	return nil
}
