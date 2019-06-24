// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var binaryOpName = map[tree.BinaryOperator]string{
	tree.Plus:  "Plus",
	tree.Minus: "Minus",
	tree.Mult:  "Mult",
	tree.Div:   "Div",
}

var comparisonOpName = map[tree.ComparisonOperator]string{
	tree.EQ: "EQ",
	tree.NE: "NE",
	tree.LT: "LT",
	tree.LE: "LE",
	tree.GT: "GT",
	tree.GE: "GE",
}

var binaryOpInfix = map[tree.BinaryOperator]string{
	tree.Plus:  "+",
	tree.Minus: "-",
	tree.Mult:  "*",
	tree.Div:   "/",
}

var binaryOpDecMethod = map[tree.BinaryOperator]string{
	tree.Plus:  "Add",
	tree.Minus: "Sub",
	tree.Mult:  "Mul",
	tree.Div:   "Quo",
}

var comparisonOpInfix = map[tree.ComparisonOperator]string{
	tree.EQ: "==",
	tree.NE: "!=",
	tree.LT: "<",
	tree.LE: "<=",
	tree.GT: ">",
	tree.GE: ">=",
}

type overload struct {
	Name string
	// Only one of CmpOp and BinOp will be set, depending on whether the overload
	// is a binary operator or a comparison operator.
	CmpOp tree.ComparisonOperator
	BinOp tree.BinaryOperator
	// OpStr is the string form of whichever of CmpOp and BinOp are set.
	OpStr   string
	LTyp    types.T
	RTyp    types.T
	LGoType string
	RGoType string
	RetTyp  types.T

	AssignFunc  assignFunc
	CompareFunc compareFunc

	// TODO(solon): These would not be necessary if we changed the zero values of
	// ComparisonOperator and BinaryOperator to be invalid.
	IsCmpOp  bool
	IsBinOp  bool
	IsHashOp bool
}

type assignFunc func(op overload, target, l, r string) string
type compareFunc func(l, r string) string

var binaryOpOverloads []*overload
var comparisonOpOverloads []*overload

// binaryOpToOverloads maps a binary operator to all of the overloads that
// implement it.
var binaryOpToOverloads map[tree.BinaryOperator][]*overload

// comparisonOpToOverloads maps a comparison operator to all of the overloads
// that implement it.
var comparisonOpToOverloads map[tree.ComparisonOperator][]*overload

// hashOverloads is a list of all of the overloads that implement the hash
// operation.
var hashOverloads []*overload

// Assign produces a Go source string that assigns the "target" variable to the
// result of applying the overload to the two inputs, l and r.
//
// For example, an overload that implemented the int64 plus operation, when fed
// the inputs "x", "a", "b", would produce the string "x = a + b".
func (o overload) Assign(target, l, r string) string {
	if o.AssignFunc != nil {
		if ret := o.AssignFunc(o, target, l, r); ret != "" {
			return ret
		}
	}
	// Default assign form assumes an infix operator.
	return fmt.Sprintf("%s = %s %s %s", target, l, o.OpStr, r)
}

// Compare produces a Go source string that assigns the "target" variable to the
// result of comparing the two inputs, l and r.
func (o overload) Compare(target, l, r string) string {
	if o.CompareFunc != nil {
		if ret := o.CompareFunc(l, r); ret != "" {
			return fmt.Sprintf("%s = %s", target, ret)
		}
	}
	// Default compare form assumes an infix operator.
	return fmt.Sprintf(
		"if %s < %s { %s = -1 } else if %s > %s { %s = 1 } else { %s = 0 }",
		l, r, target, l, r, target, target)
}

func (o overload) UnaryAssign(target, v string) string {
	if o.AssignFunc != nil {
		if ret := o.AssignFunc(o, target, v, ""); ret != "" {
			return ret
		}
	}
	// Default assign form assumes a function operator.
	return fmt.Sprintf("%s = %s(%s)", target, o.OpStr, v)
}

func init() {
	registerTypeCustomizers()

	// Build overload definitions for basic types.
	inputTypes := types.AllTypes
	binOps := []tree.BinaryOperator{tree.Plus, tree.Minus, tree.Mult, tree.Div}
	cmpOps := []tree.ComparisonOperator{tree.EQ, tree.NE, tree.LT, tree.LE, tree.GT, tree.GE}
	binaryOpToOverloads = make(map[tree.BinaryOperator][]*overload, len(binaryOpName))
	comparisonOpToOverloads = make(map[tree.ComparisonOperator][]*overload, len(comparisonOpName))
	for _, t := range inputTypes {
		customizer := typeCustomizers[t]
		for _, op := range binOps {
			// Skip types that don't have associated binary ops.
			switch t {
			case types.Bytes, types.Bool:
				continue
			}
			ov := &overload{
				Name:    binaryOpName[op],
				BinOp:   op,
				IsBinOp: true,
				OpStr:   binaryOpInfix[op],
				LTyp:    t,
				RTyp:    t,
				LGoType: t.GoTypeName(),
				RGoType: t.GoTypeName(),
				RetTyp:  t,
			}
			if customizer != nil {
				if b, ok := customizer.(binOpTypeCustomizer); ok {
					ov.AssignFunc = b.getBinOpAssignFunc()
				}
			}
			binaryOpOverloads = append(binaryOpOverloads, ov)
			binaryOpToOverloads[op] = append(binaryOpToOverloads[op], ov)
		}
		for _, op := range cmpOps {
			opStr := comparisonOpInfix[op]
			ov := &overload{
				Name:    comparisonOpName[op],
				CmpOp:   op,
				IsCmpOp: true,
				OpStr:   opStr,
				LTyp:    t,
				RTyp:    t,
				LGoType: t.GoTypeName(),
				RGoType: t.GoTypeName(),
				RetTyp:  types.Bool,
			}
			if customizer != nil {
				if b, ok := customizer.(cmpOpTypeCustomizer); ok {
					ov.AssignFunc = func(op overload, target, l, r string) string {
						c := b.getCmpOpCompareFunc()(l, r)
						if c == "" {
							return ""
						}
						return fmt.Sprintf("%s = %s %s 0", target, c, op.OpStr)
					}
					ov.CompareFunc = b.getCmpOpCompareFunc()
				}
			}
			comparisonOpOverloads = append(comparisonOpOverloads, ov)
			comparisonOpToOverloads[op] = append(comparisonOpToOverloads[op], ov)
		}

		ov := &overload{
			IsHashOp: true,
			LTyp:     t,
			OpStr:    "uint64",
		}
		if customizer != nil {
			if b, ok := customizer.(hashTypeCustomizer); ok {
				ov.AssignFunc = b.getHashAssignFunc()
			}
		}
		hashOverloads = append(hashOverloads, ov)
	}
}

// typeCustomizer is a marker interface for something that implements one or
// more of binOpTypeCustomizer and cmpOpTypeCustomizer.
//
// A type customizer allows custom templating behavior for a particular type
// that doesn't permit the ordinary Go assignment (x = y), comparison
// (==, <, etc) or binary operator (+, -, etc) semantics.
type typeCustomizer interface{}

var typeCustomizers map[types.T]typeCustomizer

// registerTypeCustomizer registers a particular type customizer to a type, for
// usage by templates.
func registerTypeCustomizer(t types.T, customizer typeCustomizer) {
	typeCustomizers[t] = customizer
}

// binOpTypeCustomizer is a type customizer that changes how the templater
// produces binary operator output for a particular type.
type binOpTypeCustomizer interface {
	getBinOpAssignFunc() assignFunc
}

// cmpOpTypeCustomizer is a type customizer that changes how the templater
// produces comparison operator output for a particular type.
type cmpOpTypeCustomizer interface {
	getCmpOpCompareFunc() compareFunc
}

// hashTypeCustomizer is a type customizer that changes how the templater
// produces hash output for a particular type.
type hashTypeCustomizer interface {
	getHashAssignFunc() assignFunc
}

// boolCustomizer is necessary since bools don't support < <= > >= in Go.
type boolCustomizer struct{}

// bytesCustomizer is necessary since []byte doesn't support comparison ops in
// Go - bytes.Compare and so on have to be used.
type bytesCustomizer struct{}

// decimalCustomizer is necessary since apd.Decimal doesn't have infix operator
// support for binary or comparison operators, and also doesn't have normal
// variable-set semantics.
type decimalCustomizer struct{}

// floatCustomizers are used for hash functions.
type floatCustomizer struct{ width int }

// intCustomizers are used for hash functions.
type intCustomizer struct{ width int }

func (boolCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(l, r string) string {
		return fmt.Sprintf("tree.CompareBools(%s, %s)", l, r)
	}
}

func (boolCustomizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf(`
			x := 0
			if %[2]s {
    		x = 1
			}
			%[1]s = %[1]s*31 + uintptr(x)
		`, target, v)
	}
}

func (bytesCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(l, r string) string {
		return fmt.Sprintf("bytes.Compare(%s, %s)", l, r)
	}
}

func (bytesCustomizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf(`
			sh := (*reflect.SliceHeader)(unsafe.Pointer(&%[1]s))
			%[2]s = memhash(unsafe.Pointer(sh.Data), %[2]s, uintptr(len(%[1]s)))

		`, v, target)
	}
}

func (decimalCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(l, r string) string {
		return fmt.Sprintf("tree.CompareDecimals(&%s, &%s)", l, r)
	}
}

func (decimalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		return fmt.Sprintf("if _, err := tree.DecimalCtx.%s(&%s, &%s, &%s); err != nil { panic(err) }",
			binaryOpDecMethod[op.BinOp], target, l, r)
	}
}

func (decimalCustomizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf(`
			d, err := %[2]s.Float64()
			if err != nil {
				panic(fmt.Sprintf("%%v", err))
			}

			%[1]s = f64hash(noescape(unsafe.Pointer(&d)), %[1]s)
		`, target, v)
	}
}

func (c floatCustomizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf("%[1]s = f%[3]dhash(noescape(unsafe.Pointer(&%[2]s)), %[1]s)", target, v, c.width)
	}
}

func (c intCustomizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf("%[1]s = memhash%[3]d(noescape(unsafe.Pointer(&%[2]s)), %[1]s)", target, v, c.width)
	}
}

func registerTypeCustomizers() {
	typeCustomizers = make(map[types.T]typeCustomizer)
	registerTypeCustomizer(types.Bool, boolCustomizer{})
	registerTypeCustomizer(types.Bytes, bytesCustomizer{})
	registerTypeCustomizer(types.Decimal, decimalCustomizer{})
	registerTypeCustomizer(types.Float32, floatCustomizer{width: 32})
	registerTypeCustomizer(types.Float64, floatCustomizer{width: 64})
	registerTypeCustomizer(types.Int8, intCustomizer{width: 8})
	registerTypeCustomizer(types.Int16, intCustomizer{width: 16})
	registerTypeCustomizer(types.Int32, intCustomizer{width: 32})
	registerTypeCustomizer(types.Int64, intCustomizer{width: 64})
}

// Avoid unused warning for functions which are only used in templates.
var _ = overload{}.Assign
var _ = overload{}.Compare
var _ = overload{}.UnaryAssign

// buildDict is a template function that builds a dictionary out of its
// arguments. The argument to this function should be an alternating sequence of
// argument name strings and arguments (argName1, arg1, argName2, arg2, etc).
// This is needed because the template language only allows 1 argument to be
// passed into a defined template.
func buildDict(values ...interface{}) (map[string]interface{}, error) {
	if len(values)%2 != 0 {
		return nil, errors.New("invalid call to buildDict")
	}
	dict := make(map[string]interface{}, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].(string)
		if !ok {
			return nil, errors.New("buildDict keys must be strings")
		}
		dict[key] = values[i+1]
	}
	return dict, nil
}

// intersectOverloads takes in a slice of overloads and returns a new slice of
// overloads the corresponding intersected overloads at each position. The
// intersection is determined to be the maximum common set of LTyp types shared
// by each overloads.
func intersectOverloads(allOverloads ...[]*overload) [][]*overload {
	inputTypes := types.AllTypes
	keepTypes := make(map[types.T]bool, len(inputTypes))

	for _, t := range inputTypes {
		keepTypes[t] = true
		for _, overloads := range allOverloads {
			found := false
			for _, ov := range overloads {
				if ov.LTyp == t {
					found = true
				}
			}

			if !found {
				keepTypes[t] = false
			}
		}
	}

	for i, overloads := range allOverloads {
		newOverloads := make([]*overload, 0, cap(overloads))
		for _, ov := range overloads {
			if keepTypes[ov.LTyp] {
				newOverloads = append(newOverloads, ov)
			}
		}

		allOverloads[i] = newOverloads
	}

	return allOverloads
}

// makeFunctionRegex makes a regexp representing a function with a specified
// number of arguments. For example, a function with 3 arguments looks like
// `(?s)funcName\(\s*(.*?),\s*(.*?),\s*(.*?)\)`.
func makeFunctionRegex(funcName string, numArgs int) *regexp.Regexp {
	argsRegex := ""

	for i := 0; i < numArgs; i++ {
		if argsRegex != "" {
			argsRegex += ","
		}
		argsRegex += `\s*(.*?)`
	}

	return regexp.MustCompile(`(?s)` + funcName + `\(` + argsRegex + `\)`)
}
