// Copyright 2019 The Cockroach Authors.
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

package pgerror

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// f formats an error.
func f(format string, e *Error) string {
	return fmt.Sprintf(format, e)
}

// m checks a match against a single regexp.
func m(t *testing.T, s, re string) {
	matched, err := regexp.MatchString(re, s)
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Errorf("string does not match pattern %q:\n%s", re, s)
	}
}

// ml checks a match against an array of regexps.
func ml(t *testing.T, s string, re []string) {
	lines := strings.Split(s, "\n")
	for i := 0; i < len(lines) && i < len(re); i++ {
		matched, err := regexp.MatchString(re[i], lines[i])
		if err != nil {
			t.Fatal(err)
		}
		if !matched {
			t.Errorf("line %d: %q: does not match pattern %q; s:\n%s", i+1, lines[i], re[i], s)
		}
	}
	if len(lines) < len(re) {
		t.Errorf("too few lines in message: expected %d, got %d; s:\n%s", len(re), len(lines), s)
	}
}

// eq checks a string equality.
func eq(t *testing.T, s, exp string) {
	if s != exp {
		t.Errorf("got %q, expected %q", s, exp)
	}
}

func TestInternalError(t *testing.T) {
	type pred struct {
		name string
		fn   func(*testing.T, *Error)
	}

	const ie = internalErrorPrefix

	testData := []struct {
		err   error
		preds []pred
	}{
		{
			AssertionFailedf("woo %s", "waa"),
			[]pred{
				// Verify that the information is captured.
				{"basefields", func(t *testing.T, e *Error) {
					eq(t, e.Message, ie+"woo waa")
					eq(t, e.Code, CodeInternalError)
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
					ml(t, e.Detail, []string{"stack trace:",
						".*internal_errors_test.go.*TestInternalError.*",
						".*testing.go.*tRunner()"})
				}},

				// Verify that internal errors carry safe details.
				{"safedetail", func(t *testing.T, e *Error) {
					m(t, e.SafeDetail[0].SafeMessage, `internal_errors_test.go:\d+: woo %s \| string$`)
					st, _ := log.DecodeStackTrace(e.SafeDetail[0].EncodedStackTrace)
					sst := log.PrintStackTrace(st)
					ml(t, sst, []string{
						`.*internal_errors_test.go.*TestInternalError\(\)`,
						".*testing.go.*tRunner"})
				}},

				// Verify that formatting works.
				{"format", func(t *testing.T, e *Error) {
					eq(t, f("%v", e), ie+"woo waa")
					eq(t, f("%s", e), ie+"woo waa")
					eq(t, f("%#v", e), "(XX000) "+ie+"woo waa")
					ml(t,
						f("%+v", e),
						[]string{
							// Heading line: source, code, error message.
							`.*internal_errors_test.go:\d+ in TestInternalError\(\): \(XX000\) ` +
								ie + "woo waa",
							// Safe details follow.
							"-- detail --",
							// Safe message.
							`internal_errors_test.go:\d+: woo %s \| string`,
							// Stack trace.
							".*internal_errors_test.go.*TestInternalError.*",
							".*testing.go.*tRunner.*"})
				}},
			},
		},
		{
			AssertionFailedf("safe %s", log.Safe("waa")),
			[]pred{
				// Verify that safe details are preserved.
				{"safedetail", func(t *testing.T, e *Error) {
					m(t, e.SafeDetail[0].SafeMessage, `internal_errors_test.go:\d+: safe %s \| waa$`)
				}},
			},
		},
		{
			// Check that the non-formatting constructor preserves the
			// format spec without making a mess.
			New(CodeSyntaxError, "syn %s"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					eq(t, e.Message, "syn %s")
					eq(t, e.Code, CodeSyntaxError)
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"no-details", func(t *testing.T, e *Error) {
					// Simple errors do not store stack trace details.
					eq(t, e.Detail, "")
					if len(e.SafeDetail) > 0 {
						t.Errorf("expected no details, got %+v", e.SafeDetail)
					}
				}},
			},
		},
		{
			// Check that a wrapped pgerr with errors.Wrap is wrapped properly.
			Wrap(errors.Wrap(makeNormal(), "wrapA"), CodeSyntaxError, "wrapB"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					eq(t, e.Code, CodeSyntaxError)
					eq(t, e.Message, "wrapB: wrapA: syn")
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "makeNormal")
				}},
				{"no-details", func(t *testing.T, e *Error) {
					// Simple errors do not store stack trace details.
					eq(t, e.Detail, "")
					if len(e.SafeDetail) > 0 {
						t.Errorf("expected no details, got %+v", e.SafeDetail)
					}
				}},
			},
		},
		{
			// Check that a wrapped internal error with errors.Wrap is wrapped properly.
			Wrap(errors.Wrap(makeBoo(), "wrapA"), CodeSyntaxError, "wrapB"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					eq(t, e.Code, CodeInternalError)
					// errors.Wrap does not strip the "internal error" prefix
					// inside the wrapped error. But we're still adding on the
					// outside.  This is expected.
					eq(t, e.Message, ie+"wrapB: wrapA: "+ie+"boo")
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "makeBoo")
				}},
				{"no-details", func(t *testing.T, e *Error) {
					// Simple errors do not store stack trace details.
					ml(t, e.Detail, []string{
						`stack trace:`,
						`.*makeBoo.*`,
					})
					m(t, e.SafeDetail[0].SafeMessage, `internal_errors_test.go:\d+: boo \| string`)
					m(t, e.SafeDetail[1].SafeMessage, `.*errors.withMessage`)
				}},
			},
		},
		{
			// Check that Wrapf respects the original code for regular errors.
			Wrapf(New(CodeSyntaxError, "syn foo"), CodeAdminShutdownError, "wrap %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, "wrap waa: syn foo")
					// Original code is preserved.
					eq(t, e.Code, CodeSyntaxError)
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"no-details", func(t *testing.T, e *Error) {
					// Simple errors do not store stack trace details.
					eq(t, e.Detail, "")
					if len(e.SafeDetail) > 0 {
						t.Errorf("expected no details, got %+v", e.SafeDetail)
					}
				}},
			},
		},
		{
			// Check that Wrapf around a regular fmt.Error makes sense.
			Wrapf(fmt.Errorf("fmt err"), CodeAdminShutdownError, "wrap %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, "wrap waa: fmt err")
					// New code was added.
					eq(t, e.Code, CodeAdminShutdownError)
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"no-details", func(t *testing.T, e *Error) {
					// Simple errors do not store stack trace details.
					eq(t, e.Detail, "")
					if len(e.SafeDetail) > 0 {
						t.Errorf("expected no details, got %+v", e.SafeDetail)
					}
				}},
			},
		},
		{
			// Check that Wrap does the same thing
			Wrap(fmt.Errorf("fmt err"), CodeAdminShutdownError, "wrapx waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, "wrapx waa: fmt err")
					// New code was added.
					eq(t, e.Code, CodeAdminShutdownError)
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"no-details", func(t *testing.T, e *Error) {
					// Simple errors do not store stack trace details.
					eq(t, e.Detail, "")
					if len(e.SafeDetail) > 0 {
						t.Errorf("expected no details, got %+v", e.SafeDetail)
					}
				}},
			},
		},
		{
			// Check that Wrapf around an error.Wrap extracts something useful.
			Wrapf(errors.Wrap(fmt.Errorf("fmt"), "wrap1"), CodeAdminShutdownError, "wrap2 %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, "wrap2 waa: wrap1: fmt")
					// New code was added.
					eq(t, e.Code, CodeAdminShutdownError)
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"no-details", func(t *testing.T, e *Error) {
					// Simple errors do not store stack trace details.
					eq(t, e.Detail, "")
					if len(e.SafeDetail) > 0 {
						t.Errorf("expected no details, got %+v", e.SafeDetail)
					}
				}},
			},
		},
		{
			// Check that a Wrap around an internal error preserves the internal error.
			doWrap(makeBoo()),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, ie+"wrap woo: boo")
					// Internal error was preserved.
					eq(t, e.Code, CodeInternalError)
					// Source info is preserved from original error.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "makeBoo")
				}},
				{"retained-details", func(t *testing.T, e *Error) {
					ml(t, e.Detail,
						[]string{
							// Ensure that the original stack trace remains at the top.
							"stack trace:",
							".*makeBoo.*",
							".*TestInternalError.*",
							".*tRunner.*",
							"",
							// Followed by the wrap stack trace underneath.
							"stack trace:",
							".*doWrap",
							".*TestInternalError",
						},
					)
					m(t, e.SafeDetail[0].SafeMessage, `internal_errors_test.go.*boo \| string`)
					m(t, e.SafeDetail[0].EncodedStackTrace, "makeBoo")
					m(t, e.SafeDetail[1].SafeMessage, `internal_errors_test.go.*wrap %s \| string`)
					m(t, e.SafeDetail[1].EncodedStackTrace, "doWrap")
				}},
			},
		},
		{
			// Check that an internal error Wrap around a regular error
			// creates internal error details.
			NewAssertionErrorWithWrappedErrf(New(CodeSyntaxError, "syn err"), "iewrap %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, ie+"iewrap waa: syn err")
					// Internal error was preserved.
					eq(t, e.Code, CodeInternalError)
					// Source info is preserved from original error.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"retained-details", func(t *testing.T, e *Error) {
					ml(t, e.Detail,
						[]string{
							// Ensure that the assertion catcher is captured in details.
							"stack trace:",
							".*TestInternalError.*",
							".*tRunner.*",
						},
					)
					m(t, e.SafeDetail[0].SafeMessage, `internal_errors_test.go.*iewrap %s \| string`)
					m(t, e.SafeDetail[0].EncodedStackTrace, "TestInternalError")
				}},
			},
		},
		{
			// Check that an internal error Wrap around another internal
			// error creates internal error details and a sane error
			// message.
			NewAssertionErrorWithWrappedErrf(
				makeBoo(), "iewrap2 %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					// Ensure the "internal error" prefix only occurs once.
					eq(t, e.Message, ie+"iewrap2 waa: boo")
					// Internal error was preserved.
					eq(t, e.Code, CodeInternalError)
					// Source info is preserved from original error.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "makeBoo")
				}},
				{"retained-details", func(t *testing.T, e *Error) {
					ml(t, e.Detail,
						[]string{
							// Ensure that the assertion catcher is captured in details.
							"stack trace:",
							".*makeBoo.*",
							".*TestInternalError.*",
							".*tRunner.*",
							"",
							// Followed by the wrap stack trace underneath.
							"stack trace:",
							".*TestInternalError",
						},
					)
					m(t, e.SafeDetail[0].SafeMessage, `internal_errors_test.go.*boo \| string`)
					m(t, e.SafeDetail[0].EncodedStackTrace, "makeBoo")
					m(t, e.SafeDetail[1].SafeMessage, `internal_errors_test.go.*iewrap2 %s \| string`)
					m(t, e.SafeDetail[1].EncodedStackTrace, "TestInternalError")
				}},
			},
		},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.err), func(t *testing.T) {
			for _, pred := range test.preds {
				t.Run(pred.name, func(t *testing.T) {
					pred.fn(t, test.err.(*Error))
				})
			}
		})
	}
}

func makeNormal() error {
	return New(CodeSyntaxError, "syn")
}

func doWrap(err error) error {
	return Wrapf(err, CodeAdminShutdownError, "wrap %s", "woo")
}

func makeBoo() error {
	return AssertionFailedf("boo")
}
