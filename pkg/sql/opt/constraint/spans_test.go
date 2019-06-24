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
	"testing"

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
