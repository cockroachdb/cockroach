// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestParseIdent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		input                   string
		expectedStrictOutput    []string
		expectedStrictErr       string
		expectedNonStrictOutput []string
		expectedNonStrictErr    string
	}

	fullError := func(err error) string {
		if err != nil {
			return pgerror.FullError(err)
		}
		return ""
	}

	for i, tc := range []testCase{
		{
			input:                   "a.b.c",
			expectedStrictOutput:    []string{"a", "b", "c"},
			expectedNonStrictOutput: []string{"a", "b", "c"},
		},
		{
			input:                   `"SomeSchema".someTable`,
			expectedStrictOutput:    []string{"SomeSchema", "sometable"},
			expectedNonStrictOutput: []string{"SomeSchema", "sometable"},
		},
		{
			input:                   `" SomeSchema  "  .  someTable  `,
			expectedStrictOutput:    []string{" SomeSchema  ", "sometable"},
			expectedNonStrictOutput: []string{" SomeSchema  ", "sometable"},
		},
		{
			input:                "",
			expectedStrictErr:    "string is not a valid identifier: \"\"",
			expectedNonStrictErr: "string is not a valid identifier: \"\"",
		},
		{
			input:                " ",
			expectedStrictErr:    "string is not a valid identifier: \" \"",
			expectedNonStrictErr: "string is not a valid identifier: \" \"",
		},
		{
			input:                "a.b.c.",
			expectedStrictErr:    "No valid identifier after \".\"",
			expectedNonStrictErr: "No valid identifier after \".\"",
		},
		{
			input:                "a. .b.c.",
			expectedStrictErr:    "No valid identifier before \".\"",
			expectedNonStrictErr: "No valid identifier before \".\"",
		},
		{
			input:                ".b.c.",
			expectedStrictErr:    "No valid identifier before \".\"",
			expectedNonStrictErr: "No valid identifier before \".\"",
		},
		{
			input:                " .b.c.",
			expectedStrictErr:    "No valid identifier before \".\"",
			expectedNonStrictErr: "No valid identifier before \".\"",
		},
		{
			input:                "a.\"b.c.",
			expectedStrictErr:    "String has unclosed double quotes.",
			expectedNonStrictErr: "String has unclosed double quotes.",
		},
		{
			input:                "a. \"\" .b.c.",
			expectedStrictErr:    "Quoted identifier must not be empty.",
			expectedNonStrictErr: "Quoted identifier must not be empty.",
		},
		{
			input:                   "a. \" \" .b.c",
			expectedStrictOutput:    []string{"a", " ", "b", "c"},
			expectedNonStrictOutput: []string{"a", " ", "b", "c"},
		},
		{
			input:                   `a. "x""y" .b.c`,
			expectedStrictOutput:    []string{"a", `x"y`, "b", "c"},
			expectedNonStrictOutput: []string{"a", `x"y`, "b", "c"},
		},
		{
			input:                   `a."x\"y".c`,
			expectedStrictErr:       "Extra characters after last identifier.",
			expectedNonStrictOutput: []string{"a", `x\`},
		},
		{
			input:                   `a."x\y".c`,
			expectedStrictOutput:    []string{"a", `x\y`, "c"},
			expectedNonStrictOutput: []string{"a", `x\y`, "c"},
		},
		{
			input:                   `a."x\\y".c`,
			expectedStrictOutput:    []string{"a", `x\\y`, "c"},
			expectedNonStrictOutput: []string{"a", `x\\y`, "c"},
		},
		{
			input:                   "世界.b.c",
			expectedStrictOutput:    []string{"世界", "b", "c"},
			expectedNonStrictOutput: []string{"世界", "b", "c"},
		},
		{
			input:                   "\"世界\".b.c",
			expectedStrictOutput:    []string{"世界", "b", "c"},
			expectedNonStrictOutput: []string{"世界", "b", "c"},
		},
		{
			input:                "0a.b.c",
			expectedStrictErr:    "string is not a valid identifier",
			expectedNonStrictErr: "string is not a valid identifier",
		},
		{
			input:                   "\"0a\".b.c",
			expectedStrictOutput:    []string{"0a", "b", "c"},
			expectedNonStrictOutput: []string{"0a", "b", "c"},
		},
		{
			input:                ",a.b.c",
			expectedStrictErr:    "string is not a valid identifier",
			expectedNonStrictErr: "string is not a valid identifier",
		},
		{
			input:                   "\",a\".b.c",
			expectedStrictOutput:    []string{",a", "b", "c"},
			expectedNonStrictOutput: []string{",a", "b", "c"},
		},
		{
			input:                   "a.b.c()",
			expectedStrictErr:       "Extra characters after last identifier",
			expectedNonStrictOutput: []string{"a", "b", "c"},
		},
		{
			input:                   "a.b.\"c()\"",
			expectedStrictOutput:    []string{"a", "b", "c()"},
			expectedNonStrictOutput: []string{"a", "b", "c()"},
		},
	} {
		testutils.RunTrueAndFalse(t, strconv.Itoa(i), func(t *testing.T, strict bool) {
			res, err := parseIdent(tc.input, strict)
			if strict {
				require.Equal(t, tc.expectedStrictOutput, res)
				require.Truef(
					t, testutils.IsError(err, tc.expectedStrictErr),
					"got error: %s\nexpected error: %s", fullError(err), tc.expectedStrictErr,
				)
			} else {
				require.Equal(t, tc.expectedNonStrictOutput, res)
				require.Truef(
					t, testutils.IsError(err, tc.expectedNonStrictErr),
					"got error: %s\nexpected error: %s", fullError(err), tc.expectedNonStrictErr,
				)
			}
		})
	}
}
