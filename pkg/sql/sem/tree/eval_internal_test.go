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

package tree

import (
	"fmt"
	"testing"
)

func TestUnescapePattern(t *testing.T) {
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
			actual, err := unescapePattern(tc.pattern, tc.escapeToken)
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

	const errorMessage = "pattern ends with escape character"

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Pattern=%s Escape=%s", tc.pattern, tc.escapeToken), func(t *testing.T) {
			actual, err := unescapePattern(tc.pattern, tc.escapeToken)
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
