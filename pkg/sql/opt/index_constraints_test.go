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
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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

func TestIntersectSpan(t *testing.T) {
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
			c := indexConstraintCalc{
				// Only the directions from colInfos are used by intersectSpan
				colInfos: []IndexColumnInfo{
					{direction: encoding.Ascending},
					{direction: encoding.Ascending},
				},
				evalCtx: &evalCtx,
			}
			res := ""
			if sp3, ok := c.intersectSpan(0 /* depth */, &sp1, &sp2); ok {
				res = sp3.String()
			}
			if res != tc.e {
				t.Errorf("expected  %s  got  %s", tc.e, res)
			}
		})
	}
}
