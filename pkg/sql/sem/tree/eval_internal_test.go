// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestUnescapePattern(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		pattern     string
		expected    string
		escapeToken string
	}{
		{``, ``, `\`},
		{``, ``, `\\`},
		{`ABC\\`, `ABC\`, `\`},
		{`ABC\\\\`, `ABC\\`, `\\`},
		{`A\\\\BC`, `A\\BC`, `\`},
		{`A\\\\\\\\C`, `A\\\\C`, `\\`},
		{`A\\B\\C`, `A\B\C`, `\`},
		{`A\\\\B\\\\C`, `A\\B\\C`, `\\`},
		{`ABC`, `ABC`, `\`},
		{`ABC`, `ABC`, `\\`},
		{`A\BC`, `ABC`, `\`},
		{`A\BC`, `A\BC`, `\\`},
		{`A\\\BC`, `A\BC`, `\`},
		{`A\\\\\\BC`, `A\\BC`, `\\`},
		{`\漢\字`, `漢字`, `\`},
		{`\ \\A\B`, ` \AB`, `\`},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s-->%s Escape=%s", tc.pattern, tc.expected, tc.escapeToken), func(t *testing.T) {
			actual, err := unescapePattern(tc.pattern, tc.escapeToken, true /* emitEscapeCharacterLastError */)
			if err != nil {
				t.Fatal(err)
			}

			if tc.expected != actual {
				t.Errorf("expected unescaped pattern: %s, got %s\n", tc.expected, actual)
			}
		})
	}
}

func TestUnescapePatternError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		pattern     string
		escapeToken string
	}{
		{`\`, `\`},
		{`\\`, `\\`},
		{`ABC\`, `\`},
		{`ABC\\`, `\\`},
		{`ABC\\\`, `\`},
		{`ABC\\\\\\`, `\\`},
	}

	const errorMessage = "LIKE pattern must not end with escape character"

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Pattern=%s Escape=%s", tc.pattern, tc.escapeToken), func(t *testing.T) {
			actual, err := unescapePattern(tc.pattern, tc.escapeToken, true /* emitEscapeCharacterLastError */)
			if err == nil {
				t.Fatalf("error not raised. expected error message: %s\ngot unescaped pattern: %s\n", errorMessage, actual)
			}

			if err.Error() != errorMessage {
				t.Errorf("expected error message: %s\ngot error message: %s\n", errorMessage, err.Error())
			}
		})
	}
}

func TestReplaceUnescaped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		pattern     string
		old         string
		new         string
		escapeToken string
		expected    string
	}{
		{``, `B`, `DEF`, `\`, ``},
		{`ABC`, `B`, `DEF`, `\`, `ADEFC`},
		{`A\BC`, `B`, `DEF`, `\`, `A\BC`},
		{`A\\BC`, `B`, `DEF`, `\`, `A\\DEFC`},
		{`\\\\BC`, `B`, `DEF`, `\`, `\\\\DEFC`},
		{`\\\\\BC`, `B`, `DEF`, `\`, `\\\\\BC`},
		{`A\\BC`, `B`, `DEF`, `\\`, `A\\BC`},
		{`A\\\BC`, `B`, `DEF`, `\\`, `A\\\BC`},
		{`ACE`, `B`, `DEF`, `\`, `ACE`},
		{`B\\B\\\B`, `B`, `DEF`, `\`, `DEF\\DEF\\\B`},
		{`漢字\\漢\字\\\漢`, `漢`, `字`, `\`, `字字\\字\字\\\漢`},
		{`ABCABC`, `ABC`, `D`, `\`, `DD`},
		{`ABC\ABCABC`, `ABC`, `D`, `\`, `D\ABCD`},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s-->%s Escape=%s", tc.pattern, tc.expected, tc.escapeToken), func(t *testing.T) {
			actual := replaceUnescaped(tc.pattern, tc.old, tc.new, tc.escapeToken)

			if tc.expected != actual {
				t.Errorf("expected replaced pattern: %s, got %s\n", tc.expected, actual)
			}
		})
	}
}

// TestEvalContextCopy verifies that EvalContext.Copy() produces a deep copy of
// EvalContext.
func TestEvalContextCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Note: the test relies on "parent" EvalContext having non-nil and non-empty
	// iVarContainerStack.
	ctx := EvalContext{iVarContainerStack: make([]IndexedVarContainer, 1)}

	cpy := ctx.Copy()
	if &ctx.iVarContainerStack[0] == &cpy.iVarContainerStack[0] {
		t.Fatal("iVarContainerStacks are the same")
	}
}
