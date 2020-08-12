// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// This file implements data structures used by index constraints generation.

package constraint

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestSpans(t *testing.T) {
	var s Spans
	check := func(exp string) {
		if actual := s.String(); actual != exp {
			t.Errorf("expected %s, got %s", exp, actual)
		}
	}
	add := func(x int) {
		k := MakeKey(tree.NewDInt(tree.DInt(x)))
		var span Span
		span.Init(k, IncludeBoundary, k, IncludeBoundary)
		s.Append(&span)
	}
	check("")
	add(1)
	check("[/1 - /1]")
	add(2)
	check("[/1 - /1] [/2 - /2]")
	add(3)
	check("[/1 - /1] [/2 - /2] [/3 - /3]")

	// Verify that Alloc doesn't lose spans.
	s.Alloc(10)
	check("[/1 - /1] [/2 - /2] [/3 - /3]")

	s.Truncate(2)
	check("[/1 - /1] [/2 - /2]")
	s.Truncate(1)
	check("[/1 - /1]")
	s.Truncate(0)
	check("")
}

func TestSpansSortAndMerge(t *testing.T) {
	keyCtx := testKeyContext(1)
	evalCtx := keyCtx.EvalCtx

	// To test SortAndMerge, we note that the result can also be obtained by
	// creating a constraint per span and calculating the union. We generate
	// random cases and cross-check.
	for testIdx := 0; testIdx < 100; testIdx++ {
		rng, _ := randutil.NewPseudoRand()
		n := 1 + rng.Intn(10)
		var spans Spans
		for i := 0; i < n; i++ {
			x, y := rng.Intn(20), rng.Intn(20)
			if x > y {
				x, y = y, x
			}
			xk, yk := EmptyKey, EmptyKey
			if x > 0 {
				xk = MakeKey(tree.NewDInt(tree.DInt(x)))
			}
			if y > 0 {
				yk = MakeKey(tree.NewDInt(tree.DInt(y)))
			}

			xb, yb := IncludeBoundary, IncludeBoundary
			if x != 0 && x != y && rng.Intn(2) == 0 {
				xb = ExcludeBoundary
			}
			if y != 0 && x != y && rng.Intn(2) == 0 {
				yb = ExcludeBoundary
			}
			var sp Span
			if x != 0 || y != 0 {
				sp.Init(xk, xb, yk, yb)
			}
			spans.Append(&sp)
		}
		origStr := spans.String()

		// Calculate via constraints.
		var c Constraint
		c.InitSingleSpan(keyCtx, spans.Get(0))
		for i := 1; i < spans.Count(); i++ {
			var d Constraint
			d.InitSingleSpan(keyCtx, spans.Get(i))
			c.UnionWith(evalCtx, &d)
		}
		expected := c.Spans.String()

		spans.SortAndMerge(keyCtx)
		if actual := spans.String(); actual != expected {
			t.Fatalf("%s : expected  %s  got  %s", origStr, expected, actual)
		}
	}
}

func TestSpans_KeyCount(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	kcAscAsc := testKeyContext(1, 2)
	kcDescDesc := testKeyContext(-1, -2)

	testCases := []struct {
		s        Spans
		keyCtx   *KeyContext
		expected string
	}{
		{ // 0
			// Single span with single key.
			s:        parseSpans(&evalCtx, "[/1/0 - /1/0]"),
			keyCtx:   kcAscAsc,
			expected: "1",
		},
		{ // 1
			// Two spans each with single key.
			s:        parseSpans(&evalCtx, "[/1/0 - /1/0] [/1/2 - /1/2]"),
			keyCtx:   kcAscAsc,
			expected: "2",
		},
		{ // 2
			// One span with 4 keys.
			s:        parseSpans(&evalCtx, "[/1/1 - /1/4]"),
			keyCtx:   kcAscAsc,
			expected: "4",
		},
		{ // 3
			// Descending span with 3 keys.
			s:        parseSpans(&evalCtx, "[/1/1 - /1/-1]"),
			keyCtx:   kcDescDesc,
			expected: "3",
		},
		{ // 4
			// Fails due to descending span with ascending columns.
			s:        parseSpans(&evalCtx, "[/1/1 - /1/-1]"),
			keyCtx:   kcAscAsc,
			expected: "FAIL",
		},
		{ // 5
			// One descending span with 3 keys and one single-key span. Spans are out
			// of order.
			s:        parseSpans(&evalCtx, "[/1/1 - /1/-1] [/1/2 - /1/2]"),
			keyCtx:   kcDescDesc,
			expected: "4",
		},
		{ // 6
			// Fails because the keys are not all the same length within a span.
			s:        parseSpans(&evalCtx, "[/2 - /2/1] [/3 - /3]"),
			keyCtx:   kcAscAsc,
			expected: "FAIL",
		},
		{ // 7
			// Case where keys have different lengths between spans. (This is allowed
			// by KeyCount, but not by ExtractSingleKeySpans).
			s:        parseSpans(&evalCtx, "[/2 - /2] [/1/0 - /1/0]"),
			keyCtx:   kcDescDesc,
			expected: "2",
		},
		{ // 8
			// Case with 3 spans with 4 keys each.
			s:        parseSpans(&evalCtx, "[/1/1 - /1/4] [/2/1 - /2/4] [/3/1 - /3/4]"),
			keyCtx:   kcAscAsc,
			expected: "12",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			toStr := func(keyCount int64, ok bool) string {
				if !ok {
					return "FAIL"
				}
				return strconv.FormatInt(keyCount, 10 /* base */)
			}

			if res := toStr(tc.s.KeyCount(tc.keyCtx)); res != tc.expected {
				t.Errorf("expected: %s, actual: %s", tc.expected, res)
			}
		})
	}
}

func TestSpans_ExtractSingleKeySpans(t *testing.T) {
	const maxKeyCount = 10

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	kcAscAsc := testKeyContext(1, 2)
	kcDescDesc := testKeyContext(-1, -2)

	testCases := []struct {
		s        Spans
		keyCtx   *KeyContext
		expected string
	}{
		{ // 0
			// Single span with single key; no-op.
			s:        parseSpans(&evalCtx, "[/1/0 - /1/0]"),
			keyCtx:   kcAscAsc,
			expected: "[/1/0 - /1/0]",
		},
		{ // 1
			// Two spans each with single key; no-op.
			s:        parseSpans(&evalCtx, "[/1/0 - /1/0] [/1/2 - /1/2]"),
			keyCtx:   kcAscAsc,
			expected: "[/1/0 - /1/0] [/1/2 - /1/2]",
		},
		{ // 2
			// One span with four keys.
			s:        parseSpans(&evalCtx, "[/1/1 - /1/4]"),
			keyCtx:   kcAscAsc,
			expected: "[/1/1 - /1/1] [/1/2 - /1/2] [/1/3 - /1/3] [/1/4 - /1/4]",
		},
		{ // 3
			// Descending span.
			s:        parseSpans(&evalCtx, "[/1/1 - /1/-1]"),
			keyCtx:   kcDescDesc,
			expected: "[/1/1 - /1/1] [/1/0 - /1/0] [/1/-1 - /1/-1]",
		},
		{ // 4
			// Fails due to descending span with ascending columns.
			s:        parseSpans(&evalCtx, "[/1/1 - /1/-1]"),
			keyCtx:   kcAscAsc,
			expected: "FAIL",
		},
		{ // 5
			// One descending span and one single-key span. Spans are out of order.
			s:        parseSpans(&evalCtx, "[/1/1 - /1/-1] [/1/2 - /1/2]"),
			keyCtx:   kcDescDesc,
			expected: "[/1/1 - /1/1] [/1/0 - /1/0] [/1/-1 - /1/-1] [/1/2 - /1/2]",
		},
		{ // 6
			// Fails because the keys are not all the same length.
			s:        parseSpans(&evalCtx, "[/1/0 - /1/0] [/2 - /2]"),
			keyCtx:   kcAscAsc,
			expected: "FAIL",
		},
		{ // 7
			// Fails because the keys are not all the same length.
			s:        parseSpans(&evalCtx, "[/2 - /2] [/1/0 - /1/0]"),
			keyCtx:   kcDescDesc,
			expected: "FAIL",
		},
		{ // 8
			// Fails because the span has 16 keys, which is greater than maxKeyCount.
			s:        parseSpans(&evalCtx, "[/1/0 - /1/15]"),
			keyCtx:   kcAscAsc,
			expected: "FAIL",
		},
		{ // 9
			// Fails because 3 spans with 4 keys each exceeds maxKeyCount.
			s:        parseSpans(&evalCtx, "[/1/1 - /1/4] [/2/1 - /2/4] [/3/1 - /3/4]"),
			keyCtx:   kcAscAsc,
			expected: "FAIL",
		},
		{ // 10
			// Fails because 11 spans exceed maxKeyCount.
			s: parseSpans(&evalCtx, ""+
				"[/0 - /0] [/1 - /1] [/2 - /2] [/3 - /3] [/4 - /4] [/5 - /5] "+
				"[/6 - /6] [/7 - /7] [/8 - /8] [/9 - /9] [/10 - /10]"),
			keyCtx:   kcAscAsc,
			expected: "FAIL",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			toStr := func(spans *Spans, ok bool) string {
				if !ok {
					return "FAIL"
				}
				return spans.String()
			}

			if res := toStr(tc.s.ExtractSingleKeySpans(tc.keyCtx, maxKeyCount)); res != tc.expected {
				t.Errorf("expected: %s, actual: %s", tc.expected, res)
			}
		})
	}
}
