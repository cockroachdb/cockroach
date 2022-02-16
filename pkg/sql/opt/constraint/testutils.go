// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package constraint

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// ParseConstraint parses a constraint in the format of Constraint.String, e.g:
//   "/1/2/3: [/1 - /2]".
func ParseConstraint(evalCtx *tree.EvalContext, str string) Constraint {
	s := strings.SplitN(str, ": ", 2)
	if len(s) != 2 {
		panic(errors.AssertionFailedf("invalid constraint format: %s", str))
	}
	var cols []opt.OrderingColumn
	for _, v := range parseIntPath(s[0]) {
		cols = append(cols, opt.OrderingColumn(v))
	}
	var c Constraint
	c.Columns.Init(cols)
	c.Spans = parseSpans(evalCtx, s[1])
	return c
}

// parseSpans parses a list of spans with integer values like:
//   "[/1 - /2] [/5 - /6]".
func parseSpans(evalCtx *tree.EvalContext, str string) Spans {
	if str == "" || str == "contradiction" {
		return Spans{}
	}
	if str == "unconstrained" {
		s := Spans{}
		s.InitSingleSpan(&UnconstrainedSpan)
		return s
	}
	s := strings.Split(str, " ")
	// Each span has three pieces.
	if len(s)%3 != 0 {
		panic(errors.AssertionFailedf("invalid span format: %s", str))
	}
	var result Spans
	for i := 0; i < len(s)/3; i++ {
		sp := ParseSpan(evalCtx, strings.Join(s[i*3:i*3+3], " "))
		result.Append(&sp)
	}
	return result
}

// ParseSpan parses a span in the format of Span.String, e.g: [/1 - /2].
// If no types are passed in, the type is inferred as being an int if possible;
// otherwise a string. If any types are specified, they must be specified for
// every datum.
func ParseSpan(evalCtx *tree.EvalContext, str string, typs ...types.Family) Span {
	if len(str) < len("[ - ]") {
		panic(str)
	}
	boundary := map[byte]SpanBoundary{
		'[': IncludeBoundary,
		']': IncludeBoundary,
		'(': ExcludeBoundary,
		')': ExcludeBoundary,
	}
	s, e := str[0], str[len(str)-1]
	if (s != '[' && s != '(') || (e != ']' && e != ')') {
		panic(str)
	}
	keys := strings.Split(str[1:len(str)-1], " - ")
	if len(keys) != 2 {
		panic(str)
	}

	// Retrieve the values of the longest key.
	longestKey := tree.ParsePath(keys[0])
	endDatums := tree.ParsePath(keys[1])
	if len(longestKey) < len(endDatums) {
		longestKey = endDatums
	}
	if len(longestKey) > 0 && len(typs) == 0 {
		// Infer the datum types and populate typs accordingly.
		typs = tree.InferTypes(longestKey)
	}

	var sp Span
	startVals := tree.ParseDatumPath(evalCtx, keys[0], typs)
	endVals := tree.ParseDatumPath(evalCtx, keys[1], typs)
	sp.Init(
		MakeCompositeKey(startVals...), boundary[s],
		MakeCompositeKey(endVals...), boundary[e],
	)
	return sp
}

// parseIntPath parses a string like "/1/2/3" into a list of integers.
func parseIntPath(str string) []int {
	var res []int
	for _, valStr := range tree.ParsePath(str) {
		val, err := strconv.Atoi(valStr)
		if err != nil {
			panic(err)
		}
		res = append(res, val)
	}
	return res
}
