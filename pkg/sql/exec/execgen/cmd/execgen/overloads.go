// Copyright 2018 The Cockroach Authors.
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

package main

import (
	"errors"
	"fmt"

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

	AssignFunc assignFunc

	// TODO(solon): These would not be necessary if we changed the zero values of
	// ComparisonOperator and BinaryOperator to be invalid.
	IsCmpOp  bool
	IsBinOp  bool
	IsHashOp bool
}

type assignFunc func(op overload, target, l, r string) string

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
					ov.AssignFunc = b.getCmpOpAssignFunc()
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
	getCmpOpAssignFunc() assignFunc
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

// float32Customizer and float64Customizer are necessary since float32 and
// float64 require additional logic for hashing.
type float32Customizer struct{}
type float64Customizer struct{}

func (boolCustomizer) getCmpOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		switch op.CmpOp {
		case tree.EQ, tree.NE:
			return ""
		}
		return fmt.Sprintf("%s = tree.CompareBools(%s, %s) %s 0",
			target, l, r, op.OpStr)
	}
}

func (boolCustomizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf(`
			x := uint64(0)
			if %[2]s {
    		x = 1
			}
			%[1]s = x
		`, target, v)
	}
}

func (bytesCustomizer) getCmpOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		switch op.CmpOp {
		case tree.EQ:
			return fmt.Sprintf("%s = bytes.Equal(%s, %s)", target, l, r)
		case tree.NE:
			return fmt.Sprintf("%s = !bytes.Equal(%s, %s)", target, l, r)
		}
		return fmt.Sprintf("%s = bytes.Compare(%s, %s) %s 0",
			target, l, r, op.OpStr)
	}
}

func (bytesCustomizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf(`
			_temp := 1
			for b := range %s {
				_temp = _temp*31 + b
			}
			%s = uint64(hash)
		`, v, target)
	}
}

func (decimalCustomizer) getCmpOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		return fmt.Sprintf("%s = tree.CompareDecimals(&%s, &%s) %s 0",
			target, l, r, op.OpStr)
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
			%[1]s = math.Float64bits(d)
		`, target, v)
	}
}

func (float32Customizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf("%s = uint64(math.Float32bits(%s))", target, v)
	}
}

func (float64Customizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf("%s = math.Float64bits(%s)", target, v)
	}
}

func registerTypeCustomizers() {
	typeCustomizers = make(map[types.T]typeCustomizer)
	registerTypeCustomizer(types.Bool, boolCustomizer{})
	registerTypeCustomizer(types.Bytes, bytesCustomizer{})
	registerTypeCustomizer(types.Decimal, decimalCustomizer{})
	registerTypeCustomizer(types.Float32, float32Customizer{})
	registerTypeCustomizer(types.Float64, float64Customizer{})
}

// Avoid unused warning for Assign, which is only used in templates.
var _ = overload{}.Assign
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
