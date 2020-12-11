// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logcrash

import (
	"context"
	"net"
	"os"
	"regexp"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
	var errWrapped1 = errors.Wrap(errFundamental, "this is reportable")
	var errWrapped2 = errors.Wrapf(errWrapped1, "this is reportable too")
	var errWrapped3 = errors.Wrap(errWrapped2, "this is reportable as well")
	var errFormatted = errors.Newf("this embed an error: %v", errWrapped2)
	var errWrappedSentinel = errors.Wrap(
		errors.Wrapf(errSentinel,
			"this is reportable"),
		"this is also reportable")

	// rm is the redaction mark, what remains after redaction when
	// the redaction markers are removed.
	rm := string(redact.RedactableBytes(redact.RedactedMarker()).StripMarkers())

	runtimeErr := makeTypeAssertionErr()

	return []safeErrorTestCase{
		{
			// Special case in redaction.
			err: context.DeadlineExceeded,
			expErr: `context deadline exceeded
(1) context deadline exceeded
Error types: (1) context.deadlineExceededError`,
		},
		{
			// Special case in redaction.
			err: runtimeErr,
			expErr: `interface conversion: interface {} is nil, not int
(1) interface conversion: interface {} is nil, not int
Error types: (1) *runtime.TypeAssertionError`,
		},
		{
			// Same as last, but skipping through to the cause: panic(errors.Wrap(safeErr, "gibberish")).
			err: errors.Wrap(runtimeErr, "some visible detail"),
			expErr: `some visible detail: interface conversion: interface {} is nil, not int
(1) attached stack trace
  -- stack trace:
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.glob..func2
  | 	...crash_reporting_test.go:NN
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.init
  | 	...crash_reporting_test.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.main
  | 	...proc.go:NN
  | runtime.goexit
  | 	...asm_amd64.s:NN
Wraps: (2) some visible detail
Wraps: (3) interface conversion: interface {} is nil, not int
Error types: (1) *withstack.withStack (2) *errutil.withPrefix (3) *runtime.TypeAssertionError`,
		},
		{
			// Safe errors revealed in safe details of error wraps/objects.
			err: errors.Newf("%s", runtimeErr),
			expErr: `interface conversion: interface {} is nil, not int
(1) attached stack trace
  -- stack trace:
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.glob..func2
  | 	...crash_reporting_test.go:NN
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.init
  | 	...crash_reporting_test.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.main
  | 	...proc.go:NN
  | runtime.goexit
  | 	...asm_amd64.s:NN
Wraps: (2) secondary error attachment
  | interface conversion: interface {} is nil, not int
  | (1) interface conversion: interface {} is nil, not int
  | Error types: (1) *runtime.TypeAssertionError
Wraps: (3) interface conversion: interface {} is nil, not int
Error types: (1) *withstack.withStack (2) *secondary.withSecondaryError (3) *errutil.leafError`,
		},
		{
			// More embedding of safe details.
			err: errors.WithSafeDetails(runtimeErr, "foo"),
			expErr: `interface conversion: interface {} is nil, not int
(1) foo
Wraps: (2) interface conversion: interface {} is nil, not int
Error types: (1) *safedetails.withSafeDetails (2) *runtime.TypeAssertionError`,
		},
		{
			err: errors.Newf("I like %s and my pin code is %v or %v", log.Safe("A"), "1234", log.Safe("9999")),
			expErr: `I like A and my pin code is ` + rm + ` or 9999
(1) attached stack trace
  -- stack trace:
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.glob..func2
  | 	...crash_reporting_test.go:NN
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.init
  | 	...crash_reporting_test.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.main
  | 	...proc.go:NN
  | runtime.goexit
  | 	...asm_amd64.s:NN
Wraps: (2) I like A and my pin code is ` + rm + ` or 9999
Error types: (1) *withstack.withStack (2) *errutil.leafError`,
		},
		{
			err: errors.Wrapf(context.Canceled, "this is preserved: %d", log.Safe(6)),
			expErr: `this is preserved: 6: context canceled
(1) attached stack trace
  -- stack trace:
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.glob..func2
  | 	...crash_reporting_test.go:NN
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.init
  | 	...crash_reporting_test.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.main
  | 	...proc.go:NN
  | runtime.goexit
  | 	...asm_amd64.s:NN
Wraps: (2) this is preserved: 6
Wraps: (3) context canceled
Error types: (1) *withstack.withStack (2) *errutil.withPrefix (3) *errors.errorString`,
		},
		{
			// Verify that the special case still scrubs inside of the error.
			err: &os.LinkError{Op: "moo", Old: "sec", New: "cret", Err: errors.WithSafeDetails(leafErr{}, "assumed safe")},
			expErr: `moo ` + rm + ` ` + rm + `: ` + rm + `
(1) moo ` + rm + ` ` + rm + `
Wraps: (2) assumed safe
Wraps: (3) ` + rm + `
Error types: (1) *os.LinkError (2) *safedetails.withSafeDetails (3) logcrash.leafErr`,
		},
		{
			// Verify that invalid sentinel errors print something and don't
			// crash the test.
			err:    errWrappedSentinel,
			expErr: `%!v(PANIC=SafeFormatter method: runtime error: invalid memory address or nil pointer dereference)`,
		},
		{
			err: errWrapped3,
			expErr: `this is reportable as well: this is reportable too: this is reportable: ` + rm + `
(1) attached stack trace
  -- stack trace:
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.glob..func2
  | 	...crash_reporting_test.go:NN
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.init
  | 	...crash_reporting_test.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.main
  | 	...proc.go:NN
Wraps: (2) this is reportable as well
Wraps: (3) attached stack trace
  -- stack trace:
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.glob..func2
  | 	...crash_reporting_test.go:NN
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.init
  | 	...crash_reporting_test.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.main
  | 	...proc.go:NN
Wraps: (4) this is reportable too
Wraps: (5) attached stack trace
  -- stack trace:
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.glob..func2
  | 	...crash_reporting_test.go:NN
  | [...repeated from below...]
Wraps: (6) this is reportable
Wraps: (7) attached stack trace
  -- stack trace:
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.glob..func2
  | 	...crash_reporting_test.go:NN
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.init
  | 	...crash_reporting_test.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.main
  | 	...proc.go:NN
  | runtime.goexit
  | 	...asm_amd64.s:NN
Wraps: (8) ` + rm + `
Error types: (1) *withstack.withStack (2) *errutil.withPrefix (3) *withstack.withStack (4) *errutil.withPrefix (5) *withstack.withStack (6) *errutil.withPrefix (7) *withstack.withStack (8) *errutil.leafError`,
		},
		{
			err: &net.OpError{
				Op:     "write",
				Net:    "tcp",
				Source: &util.UnresolvedAddr{AddressField: "sensitive-source"},
				Addr:   &util.UnresolvedAddr{AddressField: "sensitive-addr"},
				Err:    leafErr{},
			},
			expErr: `write tcp ` + rm + ` -> ` + rm + `: ` + rm + `
(1) write tcp ` + rm + ` -> ` + rm + `
Wraps: (2) ` + rm + `
Error types: (1) *net.OpError (2) logcrash.leafErr`,
		},
		{
			err: errFormatted,
			expErr: `this embed an error: this is reportable too: this is reportable: ` + rm + `
(1) attached stack trace
  -- stack trace:
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.glob..func2
  | 	...crash_reporting_test.go:NN
  | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.init
  | 	...crash_reporting_test.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.doInit
  | 	...proc.go:NN
  | runtime.main
  | 	...proc.go:NN
  | runtime.goexit
  | 	...asm_amd64.s:NN
Wraps: (2) secondary error attachment
  | this is reportable too: this is reportable: ` + rm + `
  | (1) attached stack trace
  |   -- stack trace:
  |   | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.glob..func2
  |   | 	...crash_reporting_test.go:NN
  |   | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.init
  |   | 	...crash_reporting_test.go:NN
  |   | runtime.doInit
  |   | 	...proc.go:NN
  |   | runtime.doInit
  |   | 	...proc.go:NN
  |   | runtime.main
  |   | 	...proc.go:NN
  | Wraps: (2) this is reportable too
  | Wraps: (3) attached stack trace
  |   -- stack trace:
  |   | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.glob..func2
  |   | 	...crash_reporting_test.go:NN
  |   | [...repeated from below...]
  | Wraps: (4) this is reportable
  | Wraps: (5) attached stack trace
  |   -- stack trace:
  |   | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.glob..func2
  |   | 	...crash_reporting_test.go:NN
  |   | github.com/cockroachdb/cockroach/pkg/util/log/logcrash.init
  |   | 	...crash_reporting_test.go:NN
  |   | runtime.doInit
  |   | 	...proc.go:NN
  |   | runtime.doInit
  |   | 	...proc.go:NN
  |   | runtime.main
  |   | 	...proc.go:NN
  |   | runtime.goexit
  |   | 	...asm_amd64.s:NN
  | Wraps: (6) ` + rm + `
  | Error types: (1) *withstack.withStack (2) *errutil.withPrefix (3) *withstack.withStack (4) *errutil.withPrefix (5) *withstack.withStack (6) *errutil.leafError
Wraps: (3) this embed an error: this is reportable too: this is reportable: ` + rm + `
Error types: (1) *withstack.withStack (2) *secondary.withSecondaryError (3) *errutil.leafError`,
		},
	}
}()

func TestCrashReportingSafeError(t *testing.T) {
	fileref := regexp.MustCompile(`((?:[a-zA-Z0-9\._@-]*/)*)([a-zA-Z0-9._@-]*\.(?:go|s)):\d+`)

	for _, test := range safeErrorTestCases {
		t.Run("safeErr", func(t *testing.T) {
			errStr := redact.Sprintf("%+v", test.err).Redact().StripMarkers()
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
