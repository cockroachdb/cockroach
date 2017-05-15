// Copyright 2016 The Cockroach Authors.
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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package main

import (
	"go/ast"
	"go/types"
	"log"
	"os"

	"honnef.co/go/tools/lint"
	"honnef.co/go/tools/lint/lintutil"
	"honnef.co/go/tools/simple"
	"honnef.co/go/tools/staticcheck"
	"honnef.co/go/tools/unused"
)

type metaChecker struct {
	checkers []lint.Checker
}

func (m *metaChecker) Init(program *lint.Program) {
	for _, checker := range m.checkers {
		checker.Init(program)
	}
}

func (m *metaChecker) Funcs() map[string]lint.Func {
	funcs := map[string]lint.Func{
		"FloatToUnsigned": checkConvertFloatToUnsigned,
	}
	for _, checker := range m.checkers {
		for k, v := range checker.Funcs() {
			if _, ok := funcs[k]; ok {
				log.Fatalf("duplicate lint function %s", k)
			} else {
				funcs[k] = v
			}
		}
	}
	return funcs
}

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
	fn := func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
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
	}
	for _, f := range j.Program.Files {
		ast.Inspect(f, fn)
	}
}

func main() {
	unusedChecker := unused.NewChecker(unused.CheckAll)
	unusedChecker.WholeProgram = true
	meta := metaChecker{
		checkers: []lint.Checker{
			simple.NewChecker(),
			staticcheck.NewChecker(),
			unused.NewLintChecker(unusedChecker),
		},
	}
	lintutil.ProcessArgs("metacheck", &meta, os.Args[1:])
}
