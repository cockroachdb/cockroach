// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgerror

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

// f formats an error.
func f(format string, e *Error) string {
	return fmt.Sprintf(format, e)
}

// m checks a match against a single regexp.
func m(t *testing.T, s, re string) {
	t.Helper()
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
	t.Helper()
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
	t.Helper()
	if s != exp {
		t.Errorf("got %q, expected %q", s, exp)
	}
}

func TestInternalError(t *testing.T) {
	type pred struct {
		name string
		fn   func(*testing.T, *Error)
	}

	const ie = "internal error: "

	testData := []struct {
		err   error
		preds []pred
	}{
		{
			errors.AssertionFailedf("woo %s", "waa"),
			[]pred{
				// Verify that the information is captured.
				{"basefields", func(t *testing.T, e *Error) {
					eq(t, e.Message, ie+"woo waa")
					eq(t, e.Code, pgcode.Internal.String())
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
					ml(t, e.Detail, []string{"stack trace:",
						".*internal_errors_test.go.*TestInternalError.*",
						".*testing.go.*tRunner()"})
				}},

				// Verify that formatting works.
				{"format", func(t *testing.T, e *Error) {
					eq(t, f("%v", e), ie+"woo waa")
					eq(t, f("%s", e), ie+"woo waa")
					eq(t, f("%#v", e), "(XX000) "+ie+"woo waa")
					m(t, f("%+v", e),
						// Heading line: source, code, error message.
						`.*internal_errors_test.go:\d+ in TestInternalError\(\): \(XX000\) `+
							ie+"woo waa")
				}},
			},
		},
		{
			// Check that the non-formatting constructor preserves the
			// format spec without making a mess.
			New(pgcode.Syntax, "syn %s"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					eq(t, e.Message, "syn %s")
					eq(t, e.Code, pgcode.Syntax.String())
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
			},
		},
		{
			// Check that a wrapped pgerr with errors.Wrap is wrapped properly.
			Wrap(errors.Wrap(makeNormal(), "wrapA"), pgcode.Syntax, "wrapB"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					eq(t, e.Code, pgcode.Syntax.String())
					eq(t, e.Message, "wrapB: wrapA: syn")
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "makeNormal")
				}},
			},
		},
		{
			// Check that a wrapped internal error with errors.Wrap is wrapped properly.
			Wrap(errors.Wrap(makeBoo(), "wrapA"), pgcode.Syntax, "wrapB"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					eq(t, e.Code, pgcode.Internal.String())
					eq(t, e.Message, ie+"wrapB: wrapA: boo")
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
				}},
			},
		},
		{
			// Check that Wrapf respects the original code for regular errors.
			Wrapf(New(pgcode.Syntax, "syn foo"), pgcode.AdminShutdown, "wrap %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, "wrap waa: syn foo")
					// Original code is preserved.
					eq(t, e.Code, pgcode.Syntax.String())
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
			},
		},
		{
			// Check that Wrapf around a regular fmt.Error makes sense.
			Wrapf(fmt.Errorf("fmt err"), pgcode.AdminShutdown, "wrap %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, "wrap waa: fmt err")
					// New code was added.
					eq(t, e.Code, pgcode.AdminShutdown.String())
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
			},
		},
		{
			// Check that Wrap does the same thing
			Wrap(fmt.Errorf("fmt err"), pgcode.AdminShutdown, "wrapx waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, "wrapx waa: fmt err")
					// New code was added.
					eq(t, e.Code, pgcode.AdminShutdown.String())
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"no-details", func(t *testing.T, e *Error) {
					eq(t, e.Detail, "")
				}},
			},
		},
		{
			// Check that Wrapf around an error.Wrap extracts something useful.
			Wrapf(errors.Wrap(fmt.Errorf("fmt"), "wrap1"), pgcode.AdminShutdown, "wrap2 %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, "wrap2 waa: wrap1: fmt")
					// New code was added.
					eq(t, e.Code, pgcode.AdminShutdown.String())
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"no-details", func(t *testing.T, e *Error) {
					// Simple errors do not store stack trace details.
					eq(t, e.Detail, "")
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
					eq(t, e.Code, pgcode.Internal.String())
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
						},
					)
				}},
			},
		},
		{
			// Check that an internal error Wrap around a regular error
			// creates internal error details.
			errors.NewAssertionErrorWithWrappedErrf(New(pgcode.Syntax, "syn err"), "iewrap %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, ie+"iewrap waa: syn err")
					// Internal error was preserved.
					eq(t, e.Code, pgcode.Internal.String())
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
				}},
			},
		},
		{
			// Check that an internal error Wrap around another internal
			// error creates internal error details and a sane error
			// message.
			errors.NewAssertionErrorWithWrappedErrf(
				makeBoo(), "iewrap2 %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *Error) {
					// Ensure the "internal error" prefix only occurs once.
					eq(t, e.Message, ie+"iewrap2 waa: boo")
					// Internal error was preserved.
					eq(t, e.Code, pgcode.Internal.String())
					// Source info is preserved from original error.
					m(t, e.Source.File, ".*internal_errors_test.go")
					// The original cause is masked by the barrier.
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"retained-details", func(t *testing.T, e *Error) {
					ml(t, e.Detail,
						[]string{
							// Ensure that the assertion catcher is captured in details.
							// Also makeBoo() is masked here.
							"stack trace:",
							".*TestInternalError.*",
							".*tRunner.*",
						},
					)
				}},
			},
		},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.err), func(t *testing.T) {
			for _, pred := range test.preds {
				t.Run(pred.name, func(t *testing.T) {
					pgErr := Flatten(test.err)
					pred.fn(t, pgErr)
					if t.Failed() {
						t.Logf("input error: %# v", pretty.Formatter(test.err))
						t.Logf("pg error: %# v", pretty.Formatter(pgErr))
					}
				})
			}
		})
	}
}

func makeNormal() error {
	return New(pgcode.Syntax, "syn")
}

func doWrap(err error) error {
	return Wrapf(err, pgcode.AdminShutdown, "wrap %s", "woo")
}

func makeBoo() error {
	return errors.AssertionFailedf("boo")
}
