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
	"time"

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
		panic(str)
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
	if str == "" {
		return Spans{}
	}
	s := strings.Split(str, " ")
	// Each span has three pieces.
	if len(s)%3 != 0 {
		panic(str)
	}
	var result Spans
	for i := 0; i < len(s)/3; i++ {
		sp := ParseSpan(evalCtx, strings.Join(s[i*3:i*3+3], " "), types.IntFamily)
		result.Append(&sp)
	}
	return result
}

// ParseSpan parses a span in the format of Span.String, e.g: [/1 - /2].
func ParseSpan(evalCtx *tree.EvalContext, str string, typ types.Family) Span {
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
	var sp Span
	startVals := parseDatumPath(evalCtx, keys[0], typ)
	endVals := parseDatumPath(evalCtx, keys[1], typ)
	sp.Init(
		MakeCompositeKey(startVals...), boundary[s],
		MakeCompositeKey(endVals...), boundary[e],
	)
	return sp
}

// parseIntPath parses a string like "/1/2/3" into a list of integers.
func parseIntPath(str string) []int {
	var res []int
	for _, valStr := range parsePath(str) {
		val, err := strconv.Atoi(valStr)
		if err != nil {
			panic(err)
		}
		res = append(res, val)
	}
	return res
}

// parseDatumPath parses a span key string like "/1/2/3".
// Only NULL and a subset of types are currently supported.
func parseDatumPath(evalCtx *tree.EvalContext, str string, typ types.Family) []tree.Datum {
	var res []tree.Datum
	for _, valStr := range parsePath(str) {
		if valStr == "NULL" {
			res = append(res, tree.DNull)
			continue
		}
		var val tree.Datum
		var err error
		switch typ {
		case types.BoolFamily:
			val, err = tree.ParseDBool(valStr)
		case types.IntFamily:
			val, err = tree.ParseDInt(valStr)
		case types.FloatFamily:
			val, err = tree.ParseDFloat(valStr)
		case types.DecimalFamily:
			val, err = tree.ParseDDecimal(valStr)
		case types.DateFamily:
			val, err = tree.ParseDDate(evalCtx, valStr)
		case types.TimestampFamily:
			val, err = tree.ParseDTimestamp(evalCtx, valStr, time.Microsecond)
		case types.TimestampTZFamily:
			val, err = tree.ParseDTimestampTZ(evalCtx, valStr, time.Microsecond)
		case types.StringFamily:
			val = tree.NewDString(valStr)
		default:
			panic(errors.AssertionFailedf("type %s not supported", typ.String()))
		}
		if err != nil {
			panic(err)
		}
		res = append(res, val)
	}
	return res
}

// parsePath splits a string of the form "/foo/bar" into strings ["foo", "bar"].
// An empty string is allowed, otherwise the string must start with /.
func parsePath(str string) []string {
	if str == "" {
		return nil
	}
	if str[0] != '/' {
		panic(str)
	}
	return strings.Split(str, "/")[1:]
}
