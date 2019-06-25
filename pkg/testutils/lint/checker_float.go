// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// TODO(radu): re-enable the checkers using the new staticcheck interfaces.
// +build lint_todo

package lint

import (
	"go/ast"
	"go/types"

	"honnef.co/go/tools/lint"
)

// @ianlancetaylor via golang-nuts[0]:
//
// For the record, the spec says, in https://golang.org/ref/spec#Conversions:
// "In all non-constant conversions involving floating-point or complex
// values, if the result type cannot represent the value the conversion
// succeeds but the result value is implementation-dependent."  That is the
// case that applies here: you are converting a negative floating point number
// to uint64, which can not represent a negative value, so the result is
// implementation-dependent.  The conversion to int64 works, of course. And
// the conversion to int64 and then to uint64 succeeds in converting to int64,
// and when converting to uint64 follows a different rule: "When converting
// between integer types, if the value is a signed integer, it is sign
// extended to implicit infinite precision; otherwise it is zero extended. It
// is then truncated to fit in the result type's size."
//
// So, basically, don't convert a negative floating point number to an
// unsigned integer type.
//
// [0] https://groups.google.com/d/msg/golang-nuts/LH2AO1GAIZE/PyygYRwLAwAJ
//
// TODO(tamird): upstream this.
func checkConvertFloatToUnsigned(j *lint.Job) {
	forAllFiles(j, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		castType, ok := j.Program.Info.TypeOf(call.Fun).(*types.Basic)
		if !ok {
			return true
		}
		if castType.Info()&types.IsUnsigned == 0 {
			return true
		}
		for _, arg := range call.Args {
			argType, ok := j.Program.Info.TypeOf(arg).(*types.Basic)
			if !ok {
				continue
			}
			if argType.Info()&types.IsFloat == 0 {
				continue
			}
			j.Errorf(arg, "do not convert a floating point number to an unsigned integer type")
		}
		return true
	})
}
