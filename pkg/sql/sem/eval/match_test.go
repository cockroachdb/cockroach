// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"fmt"
	"testing"
)

// TestOptimizedLike checks that for certain patterns we are using optimized
// evaluation of LIKE and ILIKE rather than regex-powered evaluation.
func TestOptimizedLike(t *testing.T) {
	// All of these patterns should use optimizedLikeFunc rather than
	// CovertLikeToRegexp.
	testCases := []struct {
		str             string
		pattern         string
		escape          rune
		caseInsensitive bool
		expected        bool
		errorExpected   bool
	}{
		/* The following test cases were generated using:

		   WITH c(c) AS (VALUES ('a'), ('%'), ('_'), ('\\'))
		   SELECT e'\t\t{"' || str || '", "' || pat || e'", \'' || esc || e'\', ' || ins || ', true, false},'
		   FROM (
		   	SELECT 'a' str, c1.c pat, c2.c esc, 'false' ins FROM c c1, c c2
		   	UNION ALL
		   	SELECT 'A', c1.c, c2.c, 'true' FROM c c1, c c2
		   	UNION ALL
		   	SELECT 'aa', c1.c || c2.c pat, c3.c esc, 'false' FROM c c1, c c2, c c3
		   	UNION ALL
		   	SELECT 'aaa', c1.c || 'a' || c2.c pat, c3.c esc, 'false' FROM c c1, c c2, c c3
		   ) x
		   ORDER BY str, pat, esc;
		*/
		{"A", "%", '%', true, false, true},
		{"A", "%", '\\', true, true, false},
		{"A", "%", '_', true, true, false},
		{"A", "%", 'a', true, true, false},
		{"A", "\\", '%', true, false, false},
		{"A", "\\", '\\', true, false, true},
		{"A", "\\", '_', true, false, false},
		{"A", "\\", 'a', true, false, false},
		{"A", "_", '%', true, true, false},
		{"A", "_", '\\', true, true, false},
		{"A", "_", '_', true, false, true},
		{"A", "_", 'a', true, true, false},
		{"A", "a", '%', true, true, false},
		{"A", "a", '\\', true, true, false},
		{"A", "a", '_', true, true, false},
		{"A", "a", 'a', true, false, true},
		{"a", "%", '%', false, false, true},
		{"a", "%", '\\', false, true, false},
		{"a", "%", '_', false, true, false},
		{"a", "%", 'a', false, true, false},
		{"a", "\\", '%', false, false, false},
		{"a", "\\", '\\', false, false, true},
		{"a", "\\", '_', false, false, false},
		{"a", "\\", 'a', false, false, false},
		{"a", "_", '%', false, true, false},
		{"a", "_", '\\', false, true, false},
		{"a", "_", '_', false, false, true},
		{"a", "_", 'a', false, true, false},
		{"a", "a", '%', false, true, false},
		{"a", "a", '\\', false, true, false},
		{"a", "a", '_', false, true, false},
		{"a", "a", 'a', false, false, true},
		{"aa", "%%", '%', false, false, false},
		{"aa", "%%", '\\', false, true, false},
		{"aa", "%%", '_', false, true, false},
		{"aa", "%%", 'a', false, true, false},
		{"aa", "%\\", '%', false, false, false},
		{"aa", "%\\", '\\', false, false, true},
		{"aa", "%\\", '_', false, false, false},
		{"aa", "%\\", 'a', false, false, false},
		{"aa", "%_", '%', false, false, false},
		{"aa", "%_", '\\', false, true, false},
		{"aa", "%_", '_', false, false, true},
		{"aa", "%_", 'a', false, true, false},
		{"aa", "%a", '%', false, false, false},
		{"aa", "%a", '\\', false, true, false},
		{"aa", "%a", '_', false, true, false},
		{"aa", "%a", 'a', false, false, true},
		{"aa", "\\%", '%', false, false, true},
		{"aa", "\\%", '\\', false, false, false},
		{"aa", "\\%", '_', false, false, false},
		{"aa", "\\%", 'a', false, false, false},
		{"aa", "\\\\", '%', false, false, false},
		{"aa", "\\\\", '\\', false, false, false},
		{"aa", "\\\\", '_', false, false, false},
		{"aa", "\\\\", 'a', false, false, false},
		{"aa", "\\_", '%', false, false, false},
		{"aa", "\\_", '\\', false, false, false},
		{"aa", "\\_", '_', false, false, true},
		{"aa", "\\_", 'a', false, false, false},
		{"aa", "\\a", '%', false, false, false},
		{"aa", "\\a", '\\', false, false, false},
		{"aa", "\\a", '_', false, false, false},
		{"aa", "\\a", 'a', false, false, true},
		{"aa", "_%", '%', false, false, true},
		{"aa", "_%", '\\', false, true, false},
		{"aa", "_%", '_', false, false, false},
		{"aa", "_%", 'a', false, true, false},
		{"aa", "_\\", '%', false, false, false},
		{"aa", "_\\", '\\', false, false, true},
		{"aa", "_\\", '_', false, false, false},
		{"aa", "_\\", 'a', false, false, false},
		{"aa", "__", '%', false, true, false},
		{"aa", "__", '\\', false, true, false},
		{"aa", "__", '_', false, false, false},
		{"aa", "__", 'a', false, true, false},
		{"aa", "_a", '%', false, true, false},
		{"aa", "_a", '\\', false, true, false},
		{"aa", "_a", '_', false, false, false},
		{"aa", "_a", 'a', false, false, true},
		{"aa", "a%", '%', false, false, true},
		{"aa", "a%", '\\', false, true, false},
		{"aa", "a%", '_', false, true, false},
		{"aa", "a%", 'a', false, false, false},
		{"aa", "a\\", '%', false, false, false},
		{"aa", "a\\", '\\', false, false, true},
		{"aa", "a\\", '_', false, false, false},
		{"aa", "a\\", 'a', false, false, false},
		{"aa", "a_", '%', false, true, false},
		{"aa", "a_", '\\', false, true, false},
		{"aa", "a_", '_', false, false, true},
		{"aa", "a_", 'a', false, false, false},
		{"aa", "aa", '%', false, true, false},
		{"aa", "aa", '\\', false, true, false},
		{"aa", "aa", '_', false, true, false},
		{"aa", "aa", 'a', false, false, false},
		{"aaa", "%a%", '%', false, false, true},
		{"aaa", "%a%", '\\', false, true, false},
		{"aaa", "%a%", '_', false, true, false},
		{"aaa", "%a%", 'a', false, false, false},
		{"aaa", "%a\\", '%', false, false, false},
		{"aaa", "%a\\", '\\', false, false, true},
		{"aaa", "%a\\", '_', false, false, false},
		{"aaa", "%a\\", 'a', false, false, false},
		{"aaa", "%a_", '%', false, false, false},
		{"aaa", "%a_", '\\', false, true, false},
		{"aaa", "%a_", '_', false, false, true},
		{"aaa", "%a_", 'a', false, false, false},
		{"aaa", "%aa", '%', false, false, false},
		{"aaa", "%aa", '\\', false, true, false},
		{"aaa", "%aa", '_', false, true, false},
		{"aaa", "%aa", 'a', false, true, false},
		{"aaa", "\\a%", '%', false, false, true},
		{"aaa", "\\a%", '\\', false, true, false},
		{"aaa", "\\a%", '_', false, false, false},
		{"aaa", "\\a%", 'a', false, false, false},
		{"aaa", "\\a\\", '%', false, false, false},
		{"aaa", "\\a\\", '\\', false, false, true},
		{"aaa", "\\a\\", '_', false, false, false},
		{"aaa", "\\a\\", 'a', false, false, false},
		{"aaa", "\\a_", '%', false, false, false},
		{"aaa", "\\a_", '\\', false, false, false},
		{"aaa", "\\a_", '_', false, false, true},
		{"aaa", "\\a_", 'a', false, false, false},
		{"aaa", "\\aa", '%', false, false, false},
		{"aaa", "\\aa", '\\', false, false, false},
		{"aaa", "\\aa", '_', false, false, false},
		{"aaa", "\\aa", 'a', false, false, false},
		{"aaa", "_a%", '%', false, false, true},
		{"aaa", "_a%", '\\', false, true, false},
		{"aaa", "_a%", '_', false, true, false},
		{"aaa", "_a%", 'a', false, false, false},
		{"aaa", "_a\\", '%', false, false, false},
		{"aaa", "_a\\", '\\', false, false, true},
		{"aaa", "_a\\", '_', false, false, false},
		{"aaa", "_a\\", 'a', false, false, false},
		{"aaa", "_a_", '%', false, true, false},
		{"aaa", "_a_", '\\', false, true, false},
		{"aaa", "_a_", '_', false, false, true},
		{"aaa", "_a_", 'a', false, false, false},
		{"aaa", "_aa", '%', false, true, false},
		{"aaa", "_aa", '\\', false, true, false},
		{"aaa", "_aa", '_', false, false, false},
		{"aaa", "_aa", 'a', false, false, false},
		{"aaa", "aa%", '%', false, false, true},
		{"aaa", "aa%", '\\', false, true, false},
		{"aaa", "aa%", '_', false, true, false},
		{"aaa", "aa%", 'a', false, true, false},
		{"aaa", "aa\\", '%', false, false, false},
		{"aaa", "aa\\", '\\', false, false, true},
		{"aaa", "aa\\", '_', false, false, false},
		{"aaa", "aa\\", 'a', false, false, false},
		{"aaa", "aa_", '%', false, true, false},
		{"aaa", "aa_", '\\', false, true, false},
		{"aaa", "aa_", '_', false, false, true},
		{"aaa", "aa_", 'a', false, false, false},
		{"aaa", "aaa", '%', false, true, false},
		{"aaa", "aaa", '\\', false, true, false},
		{"aaa", "aaa", '_', false, true, false},
		{"aaa", "aaa", 'a', false, false, true},
	}

	for i, tc := range testCases {
		op := "LIKE"
		if tc.caseInsensitive {
			op = "ILIKE"
		}
		name := fmt.Sprintf("%d-%s-%s-%s-ESCAPE-%s", i, tc.str, op, tc.pattern, string(tc.escape))
		t.Run(name, func(t *testing.T) {
			like, err := optimizedLikeFunc(tc.pattern, tc.caseInsensitive, tc.escape)
			if err != nil {
				if !tc.errorExpected {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if tc.errorExpected {
				t.Errorf("expected error")
				return
			}
			if like == nil {
				t.Errorf("did not use optimized like evaluation")
				return
			}
			matches, err := like(tc.str)
			if err != nil {
				t.Error(err)
				return
			}
			if matches != tc.expected {
				t.Errorf("matches (%v) != tc.expected (%v)", matches, tc.expected)
			}
		})
	}
}
