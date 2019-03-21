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

package pgerror_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sqlerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// f formats an error.
func f(format string, e *pgerror.Error) string {
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
		fn   func(*testing.T, *pgerror.Error, []pgerror.SafeDetailPayload)
	}

	const ie = pgerror.InternalErrorPrefix

	testData := []struct {
		err   error
		preds []pred
	}{
		// 0
		{
			pgerror.NewAssertionErrorf("woo %s", "waa"),
			[]pred{
				// Verify that the information is captured.
				{"basefields", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					eq(t, e.Message, ie+"woo waa")
					eq(t, e.Code, pgerror.CodeInternalError)
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
					ml(t, e.Detail, []string{"stack trace:",
						".*internal_errors_test.go.*TestInternalError.*",
						".*testing.go.*tRunner()"})
				}},

				// Verify that internal errors carry safe details.
				{"safedetail", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					m(t, safeDetails[0].SafeMessage, `internal_errors_test.go:\d+: woo %s \| string$`)
					st, _ := log.DecodeStackTrace(safeDetails[0].EncodedStackTrace)
					sst := log.PrintStackTrace(st)
					ml(t, sst, []string{
						`.*internal_errors_test.go.*TestInternalError\(\)`,
						".*testing.go.*tRunner"})
				}},

				// Verify that formatting works.
				{"format", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					eq(t, f("%v", e), ie+"woo waa")
					eq(t, f("%s", e), ie+"woo waa")
					eq(t, f("%#v", e), "(XX000) "+ie+"woo waa")
					ml(t,
						f("%+v", e),
						[]string{
							// Heading line: source, code, error message.
							`.*internal_errors_test.go:\d+ in TestInternalError\(\): \(XX000\) ` +
								ie + "woo waa",
						})
				}},
			},
		},
		// 1
		{
			pgerror.NewAssertionErrorf("safe %s", log.Safe("waa")),
			[]pred{
				// Verify that safe details are preserved.
				{"safedetail", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					m(t, safeDetails[0].SafeMessage, `internal_errors_test.go:\d+: safe %s \| waa$`)
				}},
			},
		},
		// 2
		{
			// Check that the non-formatting constructor preserves the
			// format spec without making a mess.
			pgerror.NewError(pgerror.CodeSyntaxError, "syn %s"),
			[]pred{
				{"basefields", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					eq(t, e.Message, "syn %s")
					eq(t, e.Code, pgerror.CodeSyntaxError)
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"no-details", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Simple errors do not store stack trace details.
					eq(t, e.Detail, "")
					if len(safeDetails) > 0 {
						t.Errorf("expected no details, got %+v", safeDetails)
					}
				}},
			},
		},
		// 3
		{
			// Check that a wrapped pgerr with errors.Wrap is wrapped properly.
			pgerror.Wrap(errors.Wrap(makeNormal(), "wrapA"), pgerror.CodeSyntaxError, "wrapB"),
			[]pred{
				{"basefields", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					eq(t, e.Code, pgerror.CodeSyntaxError)
					eq(t, e.Message, "wrapB: wrapA: syn")
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "makeNormal")
				}},
				{"no-details", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Simple errors do not store stack trace details.
					eq(t, e.Detail, "")
					m(t, safeDetails[0].SafeMessage, `\*errors.withMessage: caused by \*pgerror.Error`)
				}},
			},
		},
		// 4
		{
			// Check that a wrapped internal error with errors.Wrap is wrapped properly.
			pgerror.Wrap(errors.Wrap(makeBoo(), "wrapA"), pgerror.CodeSyntaxError, "wrapB"),
			[]pred{
				{"basefields", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					eq(t, e.Code, pgerror.CodeInternalError)
					eq(t, e.Message, ie+"wrapB: wrapA: boo")
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "makeBoo")
				}},
				{"details", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Simple errors do not store stack trace details.
					ml(t, e.Detail, []string{
						`stack trace:`,
						`.*makeBoo.*`,
					})
					m(t, safeDetails[0].SafeMessage, `internal_errors_test.go:\d+: boo \| string`)
					m(t, safeDetails[1].SafeMessage, `.*errors.withMessage`)
				}},
			},
		},
		// 5
		{
			// Check that Wrapf respects the original code for regular errors.
			pgerror.Wrapf(pgerror.NewError(pgerror.CodeSyntaxError, "syn foo"),
				pgerror.CodeAdminShutdownError, "wrap %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, "wrap waa: syn foo")
					// Original code is preserved.
					eq(t, e.Code, pgerror.CodeSyntaxError)
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"no-details", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Simple errors do not store stack trace details.
					eq(t, e.Detail, "")
					m(t, safeDetails[0].SafeMessage, `\*pgerror.withMessage: caused by \*pgerror.Error`)
					m(t, safeDetails[1].SafeMessage, `.*TestInternalError`)
				}},
			},
		},
		// 6
		{
			// Check that Wrapf around a regular fmt.Error makes sense.
			pgerror.Wrapf(fmt.Errorf("fmt err"), pgerror.CodeAdminShutdownError, "wrap %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, "wrap waa: fmt err")
					// New code was added.
					eq(t, e.Code, pgerror.CodeAdminShutdownError)
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"no-details", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Simple errors do not store stack trace details.
					eq(t, e.Detail, "")
					m(t, safeDetails[0].SafeMessage, `\*errors.errorString`)
					m(t, safeDetails[1].SafeMessage, `\*pgerror.withMessage: caused by \*errors.errorString`)
				}},
			},
		},
		// 7
		{
			// Check that Wrap does the same thing.
			pgerror.Wrap(fmt.Errorf("fmt err"), pgerror.CodeAdminShutdownError, "wrapx waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, "wrapx waa: fmt err")
					// New code was added.
					eq(t, e.Code, pgerror.CodeAdminShutdownError)
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"no-details", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Simple errors do not store stack trace details.
					eq(t, e.Detail, "")
					m(t, safeDetails[0].SafeMessage, `\*errors.errorString`)
					m(t, safeDetails[1].SafeMessage, `\*pgerror.withMessage: caused by \*errors.errorString`)
				}},
			},
		},
		// 8
		{
			// Check that Wrapf around an error.Wrap extracts something useful.
			pgerror.Wrapf(errors.Wrap(fmt.Errorf("fmt"), "wrap1"), pgerror.CodeAdminShutdownError, "wrap2 %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, "wrap2 waa: wrap1: fmt")
					// New code was added.
					eq(t, e.Code, pgerror.CodeAdminShutdownError)
					// Source info is stored.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"no-details", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Simple errors do not store stack trace details.
					eq(t, e.Detail, "")
					m(t, safeDetails[0].SafeMessage, `\*errors.errorString`)
					m(t, safeDetails[1].SafeMessage, `\*errors.withMessage: caused by \*errors.errorString`)
					m(t, safeDetails[2].SafeMessage, `internal_errors_test.go.*`)
				}},
			},
		},
		// 9
		{
			// Check that a Wrap around an internal error preserves the internal error.
			doWrap(makeBoo()),
			[]pred{
				{"basefields", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, ie+"wrap woo: boo")
					// Internal error was preserved.
					eq(t, e.Code, pgerror.CodeInternalError)
					// Source info is preserved from original error.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "makeBoo")
				}},
				{"retained-details", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
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
					m(t, safeDetails[0].SafeMessage, `internal_errors_test.go.*boo \| string`)
					m(t, safeDetails[0].EncodedStackTrace, "makeBoo")
					m(t, safeDetails[1].SafeMessage, `\*pgerror.withMessage`)
					m(t, safeDetails[2].SafeMessage, `internal_errors_test.go.*wrap %s \| string`)
					m(t, safeDetails[2].EncodedStackTrace, "doWrap")
				}},
			},
		},
		// 10
		{
			// Check that an internal error Wrap around a regular error
			// creates internal error details.
			pgerror.NewAssertionErrorWithWrappedErrf(pgerror.NewError(pgerror.CodeSyntaxError, "syn err"), "iewrap %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Wrap adds a prefix to the message.
					eq(t, e.Message, ie+"iewrap waa: syn err")
					// Internal error was preserved.
					eq(t, e.Code, pgerror.CodeInternalError)
					// Source info is preserved from original error.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "TestInternalError")
				}},
				{"retained-details", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					ml(t, e.Detail,
						[]string{
							// Ensure that the assertion catcher is captured in details.
							"stack trace:",
							".*TestInternalError.*",
							".*tRunner.*",
						},
					)
					m(t, safeDetails[0].SafeMessage, `internal_errors_test.go.*iewrap %s \| string`)
					m(t, safeDetails[0].EncodedStackTrace, "TestInternalError")
				}},
			},
		},
		// 11
		{
			// Check that an internal error Wrap around another internal
			// error creates internal error details and a sane error
			// message.
			pgerror.NewAssertionErrorWithWrappedErrf(
				makeBoo(), "iewrap2 %s", "waa"),
			[]pred{
				{"basefields", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Ensure the "internal error" prefix only occurs once.
					eq(t, e.Message, ie+"iewrap2 waa: boo")
					// Internal error was preserved.
					eq(t, e.Code, pgerror.CodeInternalError)
					// Source info is preserved from original error.
					m(t, e.Source.File, ".*internal_errors_test.go")
					eq(t, e.Source.Function, "makeBoo")
				}},
				{"retained-details", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
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
					m(t, safeDetails[0].SafeMessage, `internal_errors_test.go.*boo \| string`)
					m(t, safeDetails[0].EncodedStackTrace, "makeBoo")
					m(t, safeDetails[1].SafeMessage, `internal_errors_test.go.*iewrap2 %s \| string`)
					m(t, safeDetails[1].EncodedStackTrace, "TestInternalError")
				}},
			},
		},
		// 12
		{
			// Check that retry errors are wrapped properly.
			pgerror.Wrapf(&roachpb.UnhandledRetryableError{}, "", "rwrap"),
			[]pred{
				{"basefields", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Ensure the restart prefix occurs in the message.
					m(t, e.Message, pgerror.TxnRetryMsgPrefix+": rwrap: ")
					eq(t, e.Code, pgerror.CodeSerializationFailureError)
				}},
			},
		},
		// 13
		{
			// Check that retry errors are wrapped properly.
			pgerror.Wrapf(&roachpb.TransactionRetryWithProtoRefreshError{}, "", "rwrap"),
			[]pred{
				{"basefields", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Ensure the restart prefix occurs in the message.
					m(t, e.Message, pgerror.TxnRetryMsgPrefix+": rwrap: ")
					eq(t, e.Code, pgerror.CodeSerializationFailureError)
				}},
			},
		},
		// 14
		{
			// Check that retry errors are wrapped properly.
			pgerror.Wrapf(&roachpb.AmbiguousResultError{}, "", "rwrap"),
			[]pred{
				{"basefields", func(t *testing.T, e *pgerror.Error, safeDetails []pgerror.SafeDetailPayload) {
					// Ensure the restart prefix occurs in the message.
					m(t, e.Message, "rwrap: ")
					eq(t, e.Code, pgerror.CodeStatementCompletionUnknownError)
				}},
			},
		},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.err), func(t *testing.T) {
			for _, pred := range test.preds {
				t.Run(pred.name, func(t *testing.T) {
					// t.Logf("input error:\n%+v", test.err)
					pgErr, details, _ := pgerror.Flatten(test.err)
					// for i, d := range details {
					//	t.Logf("details %d: %+v", i, d)
					// }
					pred.fn(t, pgErr, details)
				})
			}
		})
	}
}

func makeNormal() error {
	return pgerror.NewError(pgerror.CodeSyntaxError, "syn")
}

func doWrap(err error) error {
	return pgerror.Wrapf(err, pgerror.CodeAdminShutdownError, "wrap %s", "woo")
}

func makeBoo() error {
	return pgerror.NewAssertionErrorf("boo")
}
