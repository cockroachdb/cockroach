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

package opt

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// parses a span with integer column values in the format of LogicalSpan.String.
func parseSpan(str string) LogicalSpan {
	if len(str) < len("[ - ]") {
		panic(str)
	}
	var start, end LogicalKey
	switch str[0] {
	case '[':
		start.Inclusive = true
	case '(':
	default:
		panic(str)
	}
	switch str[len(str)-1] {
	case ']':
		end.Inclusive = true
	case ')':
	default:
		panic(str)
	}
	sepIdx := strings.Index(str, " - ")
	if sepIdx == -1 {
		panic(str)
	}
	startVal := str[1:sepIdx]
	endVal := str[sepIdx+len(" - ") : len(str)-1]

	parseVals := func(str string) tree.Datums {
		if str == "" {
			return nil
		}
		if str[0] != '/' {
			panic(str)
		}
		var res tree.Datums
		for i := 1; i < len(str); {
			length := strings.Index(str[i:], "/")
			if length == -1 {
				length = len(str) - i
			}
			val, err := strconv.Atoi(str[i : i+length])
			if err != nil {
				panic(err)
			}
			res = append(res, tree.NewDInt(tree.DInt(val)))
			i += length + 1
		}
		return res
	}

	start.Vals = parseVals(startVal)
	end.Vals = parseVals(endVal)
	return LogicalSpan{Start: start, End: end}
}

// parseSpans parses a list of spans with integer values
// like:
//   [/1 - /2], [/5 - /6]
func parseSpans(str string) LogicalSpans {
	if str == "" {
		return nil
	}
	var result LogicalSpans
	for _, s := range strings.Split(str, ", ") {
		result = append(result, parseSpan(s))
	}
	return result
}

func spansStr(spans LogicalSpans) string {
	var str string
	for _, sp := range spans {
		if str != "" {
			str += ", "
		}
		str += sp.String()
	}
	return str
}

func TestIntersectSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		a, b string
		// expected value
		e string
	}{
		{
			a: "[ - ]",
			b: "(/5 - /6]",
			e: "(/5 - /6]",
		},
		{
			// Equal values but exclusive vs inclusive keys.
			a: "[/1 - /2)",
			b: "(/1 - /2]",
			e: "(/1 - /2)",
		},
		{
			// Equal prefix values (inclusive).
			a: "[/1/2 - /5]",
			b: "[/1 - /5/6]",
			e: "[/1/2 - /5/6]",
		},
		{
			// Equal prefix values (exclusive).
			a: "(/1/2 - /5)",
			b: "(/1 - /5/6)",
			e: "(/1 - /5)",
		},
		{
			// Equal prefix values (mixed).
			a: "[/1/2 - /5)",
			b: "(/1 - /5/6]",
			e: "(/1 - /5)",
		},
		{
			a: "[/1 - /2]",
			b: "[/2 - /5]",
			e: "[/2 - /2]",
		},
		{
			a: "[/1 - /2)",
			b: "[/2 - /5]",
			e: "",
		},
		{
			a: "[/1 - /2/4)",
			b: "[/2 - /5]",
			e: "[/2 - /2/4)",
		},
	}

	evalCtx := tree.MakeTestingEvalContext()

	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			sp1 := parseSpan(tc.a)
			sp2 := parseSpan(tc.b)
			c := indexConstraintCtx{
				// Only the directions from colInfos are used by intersectSpan
				colInfos: []IndexColumnInfo{
					{Direction: encoding.Ascending},
					{Direction: encoding.Ascending},
				},
				evalCtx: &evalCtx,
			}
			res := ""
			if sp3, ok := c.intersectSpan(0 /* offset */, &sp1, &sp2); ok {
				res = sp3.String()
			}
			if res != tc.e {
				t.Errorf("expected  %s  got  %s", tc.e, res)
			}
		})
	}
}

// randSpans is a set of randomly generated LogicalSpans with
// integer endpoints, along with a bitmap representation of the
// intervals.
type randSpans struct {
	spans  LogicalSpans
	bitmap []bool
}

const randSpansBitmapSize = 1000

func makeRandSpans(rng *rand.Rand, allowExclusive bool) randSpans {
	spans := make(LogicalSpans, rng.Intn(5))
	bitmap := make([]bool, randSpansBitmapSize)
	last := 0
	for i := range spans {
		// Generate an integer interval [start, end].
		// We multiply by two to make sure we don't generate intervals
		// like [0, 1] and [2, 3] which seem disjoint but are not disjoint
		// in the bitmap.
		start := last + 2 + 2*rng.Intn(10)
		end := start + 2 + 2*rng.Intn(10)
		last = end
		for k := start; k <= end; k++ {
			bitmap[k] = true
		}

		sp := &spans[i]
		*sp = MakeFullSpan()
		// Randomly make the endpoints inclusive or exclusive (while
		// effectively indicating the same interval).
		if allowExclusive && rng.Intn(2) == 0 {
			sp.Start.Inclusive = false
			start--
		}
		if allowExclusive && rng.Intn(2) == 0 {
			sp.End.Inclusive = false
			end++
		}
		sp.Start.Vals = tree.Datums{tree.NewDInt(tree.DInt(start))}
		sp.End.Vals = tree.Datums{tree.NewDInt(tree.DInt(end))}
	}
	return randSpans{spans: spans, bitmap: bitmap}
}

// mergeRandSpans merges the bitmaps of two randSpans and returns
// the result as a list of intervals.
func mergeRandSpans(a randSpans, b randSpans) [][2]int {
	var result [][2]int

	for i := 0; i < randSpansBitmapSize; i++ {
		if !a.bitmap[i] && !b.bitmap[i] {
			continue
		}
		start := i
		for i++; a.bitmap[i] || b.bitmap[i]; i++ {
		}
		end := i - 1
		result = append(result, [2]int{start, end})
	}
	return result
}

func isRandSpanSubset(a randSpans, b randSpans) bool {
	for i := 0; i < randSpansBitmapSize; i++ {
		if a.bitmap[i] && !b.bitmap[i] {
			return false
		}
	}
	return true
}

func TestMergeSpanSets(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		a, b string
		// expected value
		e string
	}{
		{
			a: "[/1 - /3]",
			b: "[/2 - /4]",
			e: "[/1 - /4]",
		},
		{
			a: "",
			b: "[/1 - /2]",
			e: "[/1 - /2]",
		},
		{
			a: "[ - ]",
			b: "[/1 - /2]",
			e: "[ - ]",
		},
		{
			a: "[/1 - /2], [/3 - /5]",
			b: "[/2 - /4]",
			e: "[/1 - /5]",
		},
		{
			a: "[/1 - /2], [/3 - /5]",
			b: "[/2/5 - /4]",
			e: "[/1 - /5]",
		},
		{
			a: "[/1 - /2), [/3 - /5]",
			b: "[/2/5 - /4]",
			e: "[/1 - /2), [/2/5 - /5]",
		},
		{
			a: "[/1 - /2], [/4 - /5]",
			b: "(/2 - /4)",
			e: "[/1 - /5]",
		},
		{
			a: "[/1 - /2), (/4 - /5]",
			b: "[/2 - /4]",
			e: "[/1 - /5]",
		},
		{
			a: "[/1 - /3], [/8 - /10], (/13 - /15)",
			b: "[/0 - /0], (/4 - /5], (/9 - /14]",
			e: "[/0 - /0], [/1 - /3], (/4 - /5], [/8 - /15)",
		},
	}

	evalCtx := tree.MakeTestingEvalContext()

	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			a := parseSpans(tc.a)
			b := parseSpans(tc.b)
			c := indexConstraintCtx{
				colInfos: []IndexColumnInfo{
					{Direction: encoding.Ascending},
					{Direction: encoding.Ascending},
				},
				evalCtx: &evalCtx,
			}
			res := spansStr(c.mergeSpanSets(0 /* offset */, a, b))
			if res != tc.e {
				t.Errorf("expected  %s  got  %s", tc.e, res)
			}
		})
	}

	t.Run("rand", func(t *testing.T) {
		rng, _ := randutil.NewPseudoRand()
		for n := 0; n < 500; n++ {
			// We generate two random sets of intervals with small integer values and
			// cross-check mergeSpanSets with a simple bitmap-based implementation.
			a := makeRandSpans(rng, true /* allowExclusive */)
			b := makeRandSpans(rng, true /* allowExclusive */)
			c := indexConstraintCtx{
				colInfos: []IndexColumnInfo{{Direction: encoding.Ascending}},
				evalCtx:  &evalCtx,
			}
			mergedSpans := c.mergeSpanSets(0 /* offset */, a.spans, b.spans)
			// Convert back to integer intervals and check against the bitmap.
			var result [][2]int
			for _, sp := range mergedSpans {
				start := int(*sp.Start.Vals[0].(*tree.DInt))
				if !sp.Start.Inclusive {
					start++
				}
				end := int(*sp.End.Vals[0].(*tree.DInt))
				if !sp.End.Inclusive {
					end--
				}
				result = append(result, [2]int{start, end})
			}
			expected := mergeRandSpans(a, b)
			if !reflect.DeepEqual(expected, result) {
				t.Errorf(`
       a: %v
       b: %v
  merged: %v
  result: %v
expected: %v
`,
					a.spans, b.spans, mergedSpans, result, expected)
			}
		}
	})
}

func TestIsSpanSubset(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := tree.MakeTestingEvalContext()

	rng, _ := randutil.NewPseudoRand()
	for n := 0; n < 500; n++ {
		// We generate two random sets of intervals with small integer values and
		// cross-check mergeSpanSets with a simple bitmap-based implementation.
		// We don't allow exclusive spans because of cases like:
		//  (/5 - /10]
		//  [/6 - /20]
		// which doesn't look like a subset but it is according to the bitmap.
		a := makeRandSpans(rng, false /* allowExclusive */)
		b := makeRandSpans(rng, false /* allowExclusive */)
		c := indexConstraintCtx{
			colInfos: []IndexColumnInfo{{Direction: encoding.Ascending}},
			evalCtx:  &evalCtx,
		}
		result := c.isSpanSubset(0 /* offset */, a.spans, b.spans)
		expected := isRandSpanSubset(a, b)

		if result != expected {
			t.Errorf(`
a: %v
b: %v
result: %t
`,
				a.spans, b.spans, result,
			)
		}
	}
}
