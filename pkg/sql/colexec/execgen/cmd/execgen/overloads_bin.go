// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

var binaryOpDecMethod = map[treebin.BinaryOperatorSymbol]string{
	treebin.Plus:     "Add",
	treebin.Minus:    "Sub",
	treebin.Mult:     "Mul",
	treebin.Div:      "Quo",
	treebin.FloorDiv: "QuoInteger",
	treebin.Mod:      "Rem",
	treebin.Pow:      "Pow",
}

var binaryOpFloatMethod = map[treebin.BinaryOperatorSymbol]string{
	treebin.FloorDiv: "math.Trunc",
	treebin.Mod:      "math.Mod",
	treebin.Pow:      "math.Pow",
}

var binaryOpDecCtx = map[treebin.BinaryOperatorSymbol]string{
	treebin.Plus:     "ExactCtx",
	treebin.Minus:    "ExactCtx",
	treebin.Mult:     "ExactCtx",
	treebin.Div:      "DecimalCtx",
	treebin.FloorDiv: "HighPrecisionCtx",
	treebin.Mod:      "HighPrecisionCtx",
	treebin.Pow:      "DecimalCtx",
}

var compatibleCanonicalTypeFamilies = map[types.Family][]types.Family{
	types.BoolFamily: append([]types.Family{},
		types.BoolFamily,
		typeconv.DatumVecCanonicalTypeFamily,
	),
	types.BytesFamily: {
		types.BytesFamily,
	},
	types.DecimalFamily: append(
		numericCanonicalTypeFamilies,
		types.IntervalFamily,
		typeconv.DatumVecCanonicalTypeFamily,
	),
	types.IntFamily: append(
		numericCanonicalTypeFamilies,
		types.IntervalFamily,
		typeconv.DatumVecCanonicalTypeFamily,
	),
	types.FloatFamily: append(
		numericCanonicalTypeFamilies,
		types.IntervalFamily,
		typeconv.DatumVecCanonicalTypeFamily,
	),
	types.TimestampTZFamily: append([]types.Family{},
		types.TimestampTZFamily,
		types.IntervalFamily,
	),
	types.IntervalFamily: append(
		numericCanonicalTypeFamilies,
		types.TimestampTZFamily,
		types.IntervalFamily,
		typeconv.DatumVecCanonicalTypeFamily,
	),
	types.JsonFamily: append([]types.Family{},
		types.BytesFamily,
		types.IntFamily,
		types.JsonFamily,
		typeconv.DatumVecCanonicalTypeFamily,
	),
	typeconv.DatumVecCanonicalTypeFamily: append(
		[]types.Family{
			typeconv.DatumVecCanonicalTypeFamily,
			types.BoolFamily,
			types.IntervalFamily,
			types.BytesFamily,
		}, numericCanonicalTypeFamilies...,
	),
}

// sameTypeBinaryOpToOverloads maps a binary operator to all of the overloads
// that implement that comparison between two values of the same type (meaning
// they have the same family and width).
var sameTypeBinaryOpToOverloads = make(map[treebin.BinaryOperatorSymbol][]*oneArgOverload, len(execgen.BinaryOpName))

var binOpOutputTypes = make(map[treebin.BinaryOperatorSymbol]map[typePair]*types.T)

func registerBinOpOutputTypes() {
	populateBinOpIntOutputTypeOnIntArgs := func(binOp treebin.BinaryOperatorSymbol) {
		for _, leftIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			for _, rightIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
				binOpOutputTypes[binOp][typePair{types.IntFamily, leftIntWidth, types.IntFamily, rightIntWidth}] = types.Int
			}
		}
	}

	// Bit binary operators.
	for _, binOp := range []treebin.BinaryOperatorSymbol{treebin.Bitand, treebin.Bitor, treebin.Bitxor} {
		binOpOutputTypes[binOp] = make(map[typePair]*types.T)
		populateBinOpIntOutputTypeOnIntArgs(binOp)
		binOpOutputTypes[binOp][typePair{typeconv.DatumVecCanonicalTypeFamily, anyWidth, typeconv.DatumVecCanonicalTypeFamily, anyWidth}] = types.Any
	}

	// Simple arithmetic binary operators.
	for _, binOp := range []treebin.BinaryOperatorSymbol{treebin.Plus, treebin.Minus, treebin.Mult, treebin.Div} {
		binOpOutputTypes[binOp] = make(map[typePair]*types.T)
		binOpOutputTypes[binOp][typePair{types.FloatFamily, anyWidth, types.FloatFamily, anyWidth}] = types.Float
		populateBinOpIntOutputTypeOnIntArgs(binOp)
		binOpOutputTypes[binOp][typePair{types.DecimalFamily, anyWidth, types.DecimalFamily, anyWidth}] = types.Decimal
		binOpOutputTypes[binOp][typePair{types.FloatFamily, anyWidth, types.FloatFamily, anyWidth}] = types.Float
		for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			binOpOutputTypes[binOp][typePair{types.DecimalFamily, anyWidth, types.IntFamily, intWidth}] = types.Decimal
			binOpOutputTypes[binOp][typePair{types.IntFamily, intWidth, types.DecimalFamily, anyWidth}] = types.Decimal
		}
	}
	// There is a special case for division with integers; it should have a
	// decimal result.
	for _, leftIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		for _, rightIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			binOpOutputTypes[treebin.Div][typePair{types.IntFamily, leftIntWidth, types.IntFamily, rightIntWidth}] = types.Decimal
		}
	}
	binOpOutputTypes[treebin.Minus][typePair{types.TimestampTZFamily, anyWidth, types.TimestampTZFamily, anyWidth}] = types.Interval
	binOpOutputTypes[treebin.Plus][typePair{types.IntervalFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.Interval
	binOpOutputTypes[treebin.Minus][typePair{types.IntervalFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.Interval
	for _, numberTypeFamily := range numericCanonicalTypeFamilies {
		for _, numberTypeWidth := range supportedWidthsByCanonicalTypeFamily[numberTypeFamily] {
			binOpOutputTypes[treebin.Mult][typePair{numberTypeFamily, numberTypeWidth, types.IntervalFamily, anyWidth}] = types.Interval
			binOpOutputTypes[treebin.Mult][typePair{types.IntervalFamily, anyWidth, numberTypeFamily, numberTypeWidth}] = types.Interval
		}
	}
	for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		binOpOutputTypes[treebin.Div][typePair{types.IntervalFamily, anyWidth, types.IntFamily, intWidth}] = types.Interval
	}
	binOpOutputTypes[treebin.Div][typePair{types.IntervalFamily, anyWidth, types.FloatFamily, anyWidth}] = types.Interval
	binOpOutputTypes[treebin.Plus][typePair{types.TimestampTZFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.TimestampTZ
	binOpOutputTypes[treebin.Minus][typePair{types.TimestampTZFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.TimestampTZ
	binOpOutputTypes[treebin.Plus][typePair{types.IntervalFamily, anyWidth, types.TimestampTZFamily, anyWidth}] = types.TimestampTZ
	for _, compatibleFamily := range []types.Family{
		types.IntFamily,      // types.Date + types.Time
		types.IntervalFamily, // types.Time + types.Interval
	} {
		for _, width := range supportedWidthsByCanonicalTypeFamily[compatibleFamily] {
			binOpOutputTypes[treebin.Plus][typePair{typeconv.DatumVecCanonicalTypeFamily, anyWidth, compatibleFamily, width}] = types.Any
			binOpOutputTypes[treebin.Plus][typePair{compatibleFamily, width, typeconv.DatumVecCanonicalTypeFamily, anyWidth}] = types.Any
		}
	}
	for _, compatibleFamily := range []types.Family{
		typeconv.DatumVecCanonicalTypeFamily, // types.INet - types.INet
		types.IntFamily,                      // types.Date - types.Time
		types.IntervalFamily,                 // types.Time - types.Interval
		types.BytesFamily,                    // types.Jsonb - types.String
	} {
		for _, width := range supportedWidthsByCanonicalTypeFamily[compatibleFamily] {
			binOpOutputTypes[treebin.Minus][typePair{typeconv.DatumVecCanonicalTypeFamily, anyWidth, compatibleFamily, width}] = types.Any
			binOpOutputTypes[treebin.Minus][typePair{compatibleFamily, width, typeconv.DatumVecCanonicalTypeFamily, anyWidth}] = types.Any
		}
	}

	// Other arithmetic binary operators.
	for _, binOp := range []treebin.BinaryOperatorSymbol{treebin.FloorDiv, treebin.Mod, treebin.Pow} {
		binOpOutputTypes[binOp] = make(map[typePair]*types.T)
		populateBinOpIntOutputTypeOnIntArgs(binOp)
		binOpOutputTypes[binOp][typePair{types.FloatFamily, anyWidth, types.FloatFamily, anyWidth}] = types.Float
		binOpOutputTypes[binOp][typePair{types.DecimalFamily, anyWidth, types.DecimalFamily, anyWidth}] = types.Decimal
		for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			binOpOutputTypes[binOp][typePair{types.DecimalFamily, anyWidth, types.IntFamily, intWidth}] = types.Decimal
			binOpOutputTypes[binOp][typePair{types.IntFamily, intWidth, types.DecimalFamily, anyWidth}] = types.Decimal
		}
	}

	// Other non-arithmetic binary operators.
	binOpOutputTypes[treebin.Concat] = map[typePair]*types.T{
		{types.BytesFamily, anyWidth, types.BytesFamily, anyWidth}:                                       types.Bytes,
		{typeconv.DatumVecCanonicalTypeFamily, anyWidth, typeconv.DatumVecCanonicalTypeFamily, anyWidth}: types.Any,
	}

	for _, binOp := range []treebin.BinaryOperatorSymbol{treebin.LShift, treebin.RShift} {
		binOpOutputTypes[binOp] = make(map[typePair]*types.T)
		populateBinOpIntOutputTypeOnIntArgs(binOp)
		for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			binOpOutputTypes[binOp][typePair{typeconv.DatumVecCanonicalTypeFamily, anyWidth, types.IntFamily, intWidth}] = types.Any
		}
	}

	// JSON operators.

	// ->, ->>, -

	binOpOutputTypes[treebin.JSONFetchVal] = map[typePair]*types.T{
		{types.JsonFamily, anyWidth, types.BytesFamily, anyWidth}: types.Jsonb,
	}
	binOpOutputTypes[treebin.JSONFetchText] = map[typePair]*types.T{
		{types.JsonFamily, anyWidth, types.BytesFamily, anyWidth}: types.String,
	}
	binOpOutputTypes[treebin.JSONFetchValPath] = map[typePair]*types.T{
		{types.JsonFamily, anyWidth, typeconv.DatumVecCanonicalTypeFamily, anyWidth}: types.Jsonb,
	}
	binOpOutputTypes[treebin.JSONFetchTextPath] = map[typePair]*types.T{
		{types.JsonFamily, anyWidth, typeconv.DatumVecCanonicalTypeFamily, anyWidth}: types.String,
	}
	binOpOutputTypes[treebin.Minus][typePair{types.JsonFamily, anyWidth, types.BytesFamily, anyWidth}] = types.Jsonb
	for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		binOpOutputTypes[treebin.JSONFetchVal][typePair{types.JsonFamily, anyWidth, types.IntFamily, intWidth}] = types.Jsonb
		binOpOutputTypes[treebin.Minus][typePair{types.JsonFamily, anyWidth, types.IntFamily, intWidth}] = types.Jsonb
		binOpOutputTypes[treebin.JSONFetchText][typePair{types.JsonFamily, anyWidth, types.IntFamily, intWidth}] = types.String
	}

	// ||
	binOpOutputTypes[treebin.Concat][typePair{types.JsonFamily, anyWidth, types.JsonFamily, anyWidth}] = types.Jsonb
}

func newBinaryOverloadBase(op treebin.BinaryOperatorSymbol) *overloadBase {
	opStr := op.String()
	switch op {
	case treebin.Bitxor:
		// treebin.Bitxor is "#" when stringified, but Go uses "^" for it, so
		// we override the former.
		opStr = "^"
	case treebin.FloorDiv:
		// treebin.FloorDiv is "//" when stringified, but that would be the
		// beginning of a comment in Go and we need to use "/".
		opStr = "/"
	}
	return &overloadBase{
		kind:  binaryOverload,
		Name:  execgen.BinaryOpName[op],
		BinOp: op,
		OpStr: opStr,
	}
}

func populateBinOpOverloads() {
	registerBinOpOutputTypes()

	// Note that we're using all binary operators here although we don't yet
	// support all of them. Such behavior is acceptable as long as we haven't
	// defined the output types (in binOpOutputTypes) for the operators that we
	// don't support because all such operators will be skipped.
	var allBinaryOperators []treebin.BinaryOperatorSymbol
	for binOp := treebin.BinaryOperatorSymbol(0); binOp < treebin.NumBinaryOperatorSymbols; binOp++ {
		allBinaryOperators = append(allBinaryOperators, binOp)
	}

	for _, op := range allBinaryOperators {
		sameTypeBinaryOpToOverloads[op] = populateTwoArgsOverloads(
			newBinaryOverloadBase(op), binOpOutputTypes[op],
			func(lawo *lastArgWidthOverload, customizer typeCustomizer) {
				if b, ok := customizer.(binOpTypeCustomizer); ok {
					lawo.AssignFunc = b.getBinOpAssignFunc()
				}
			},
			typeCustomizers,
		)
	}
}

// binOpTypeCustomizer is a type customizer that changes how the templater
// produces binary operator output for a particular type.
type binOpTypeCustomizer interface {
	getBinOpAssignFunc() assignFunc
}

func (bytesCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		var result string
		if op.overloadBase.BinOp == treebin.Concat {
			caller, idx, err := parseNonIndexableTargetElem(targetElem)
			if err != nil {
				return fmt.Sprintf("colexecerror.InternalError(\"%s\")", err)
			}
			result = fmt.Sprintf(`
			{
				var r = []byte{}
				r = append(r, %s...)
				r = append(r, %s...)
				%s.Set(%s, r)
			}
			`, leftElem, rightElem, caller, idx)
		} else {
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return result
	}
}

func checkRightIsZero(binOp treebin.BinaryOperatorSymbol) bool {
	// TODO(yuzefovich): introduce a new operator that would check whether a
	// vector contains a zero value so that the division didn't have to do the
	// check. This is likely to be more performant.
	return binOp == treebin.Div || binOp == treebin.FloorDiv || binOp == treebin.Mod
}

func (decimalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		binOp := op.overloadBase.BinOp
		args := map[string]interface{}{
			"Ctx":              binaryOpDecCtx[binOp],
			"Op":               binaryOpDecMethod[binOp],
			"CheckRightIsZero": checkRightIsZero(binOp),
			"Target":           targetElem,
			"Left":             leftElem,
			"Right":            rightElem,
		}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				{{if .CheckRightIsZero}}
				if {{.Right}}.IsZero() {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				{{end}}
				_, err := tree.{{.Ctx}}.{{.Op}}(&{{.Target}}, &{{.Left}}, &{{.Right}})
				if err != nil {
					colexecerror.ExpectedError(err)
				}
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
		}
		return buf.String()
	}
}

func (c floatCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		binOp := op.overloadBase.BinOp
		var computeBinOp string
		switch binOp {
		case treebin.FloorDiv:
			computeBinOp = fmt.Sprintf("%s(float64(%s) / float64(%s))", binaryOpFloatMethod[binOp], leftElem, rightElem)
		case treebin.Mod, treebin.Pow:
			computeBinOp = fmt.Sprintf("%s(float64(%s), float64(%s))", binaryOpFloatMethod[binOp], leftElem, rightElem)
		default:
			computeBinOp = fmt.Sprintf("float64(%s) %s float64(%s)", leftElem, binOp, rightElem)
		}
		args := map[string]interface{}{
			"CheckRightIsZero": checkRightIsZero(binOp),
			"Target":           targetElem,
			"Right":            rightElem,
			"ComputeBinOp":     computeBinOp,
		}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				{{if .CheckRightIsZero}}
				if {{.Right}} == 0.0 {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				{{end}}
				{{.Target}} = {{.ComputeBinOp}}
			}
			`))
		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
		}
		return buf.String()
	}
}

func (c intCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		binOp := op.overloadBase.BinOp
		// Regardless of the width of the customizer we upcast to int64 because
		// the result of any binary operation on integers of any width is
		// currently int64.
		args := map[string]string{
			"Op":     op.overloadBase.OpStr,
			"Target": targetElem,
			"Left":   fmt.Sprintf("int64(%s)", leftElem),
			"Right":  fmt.Sprintf("int64(%s)", rightElem),
		}
		buf := strings.Builder{}
		var t *template.Template

		switch binOp {
		case treebin.Bitand, treebin.Bitor, treebin.Bitxor:
			t = template.Must(template.New("").Parse(`
				{{.Target}} = {{.Left}} {{.Op}} {{.Right}}
			`))

		case treebin.Plus, treebin.Minus:
			overflowCmp := "<"
			if binOp == treebin.Minus {
				overflowCmp = ">"
			}
			args["OverflowCmp"] = overflowCmp
			t = template.Must(template.New("").Parse(`
				{
					result := {{.Left}} {{.Op}} {{.Right}}
					if (result < {{.Left}}) != ({{.Right}} {{.OverflowCmp}} 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					{{.Target}} = result
				}
			`))

		case treebin.Mult:
			// If the inputs are small enough, then we don't have to do any further
			// checks. For the sake of legibility, upperBound and lowerBound are both
			// not set to their maximal/minimal values. An even more advanced check
			// (for positive values) might involve adding together the highest bit
			// positions of the inputs, and checking if the sum is less than the
			// integer width.
			var upperBound, lowerBound string
			switch c.width {
			case 16:
				upperBound = "math.MaxInt8"
				lowerBound = "math.MinInt8"
			case 32:
				upperBound = "math.MaxInt16"
				lowerBound = "math.MinInt16"
			case anyWidth:
				upperBound = "math.MaxInt32"
				lowerBound = "math.MinInt32"
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unhandled integer width %d", c.width))
			}

			args["UpperBound"] = upperBound
			args["LowerBound"] = lowerBound
			t = template.Must(template.New("").Parse(`
				{
					_left, _right := {{.Left}}, {{.Right}}
					result := _left * _right
					if _left > {{.UpperBound}} || _left < {{.LowerBound}} || _right > {{.UpperBound}} || _right < {{.LowerBound}} {
						if _left != 0 && _right != 0 {
							sameSign := (_left < 0) == (_right < 0)
							if (result < 0) == sameSign {
								colexecerror.ExpectedError(tree.ErrIntOutOfRange)
							} else if result/_right != _left {
								colexecerror.ExpectedError(tree.ErrIntOutOfRange)
							}
						}
					}
					{{.Target}} = result
				}
			`))

		case treebin.Div:
			// Note that this is the '/' operator, which has a decimal result.
			args["Ctx"] = binaryOpDecCtx[binOp]
			t = template.Must(template.New("").Parse(`
			{
				if {{.Right}} == 0 {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				var leftTmpDec, rightTmpDec apd.Decimal //gcassert:noescape
				leftTmpDec.SetInt64(int64({{.Left}}))
				rightTmpDec.SetInt64(int64({{.Right}}))
				if _, err := tree.{{.Ctx}}.Quo(&{{.Target}}, &leftTmpDec, &rightTmpDec); err != nil {
					colexecerror.ExpectedError(err)
				}
			}
		`))
		case treebin.Pow:
			args["Ctx"] = binaryOpDecCtx[binOp]

			t = template.Must(template.New("").Parse(`
			{
				var leftTmpDec, rightTmpDec apd.Decimal //gcassert:noescape
				leftTmpDec.SetInt64(int64({{.Left}}))
				rightTmpDec.SetInt64(int64({{.Right}}))
				if _, err := tree.{{.Ctx}}.Pow(&leftTmpDec, &leftTmpDec, &rightTmpDec); err != nil {
					colexecerror.ExpectedError(err)
				}
				resultInt, err := leftTmpDec.Int64()
				if err != nil {
					colexecerror.ExpectedError(tree.ErrIntOutOfRange)
				}
				{{.Target}} = resultInt
			}
			`))
		case treebin.FloorDiv, treebin.Mod:
			// Note that these operators have integer result.
			t = template.Must(template.New("").Parse(fmt.Sprintf(`
			{
				if {{.Right}} == 0 {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				{{.Target}} = {{.Left}} %s {{.Right}}
			}
		`, op.overloadBase.OpStr)))

		case treebin.LShift, treebin.RShift:
			t = template.Must(template.New("").Parse(fmt.Sprintf(`
			{
				if {{.Right}} < 0 || {{.Right}} >= 64 {
					telemetry.Inc(sqltelemetry.Large%sArgumentCounter)
					colexecerror.ExpectedError(tree.ErrShiftArgOutOfRange)
				}
				{{.Target}} = {{.Left}} {{.Op}} {{.Right}}
			}
			`, execgen.BinaryOpName[binOp])))

		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.OpStr))
		}

		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
		}
		return buf.String()
	}
}

func (c decimalIntCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		binOp := op.overloadBase.BinOp
		args := map[string]interface{}{
			"Ctx":              binaryOpDecCtx[binOp],
			"Op":               binaryOpDecMethod[binOp],
			"CheckRightIsZero": checkRightIsZero(binOp),
			"Target":           targetElem,
			"Left":             leftElem,
			"Right":            rightElem,
		}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				{{if .CheckRightIsZero}}
				if {{.Right}} == 0 {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				{{end}}
				var tmpDec apd.Decimal //gcassert:noescape
				tmpDec.SetInt64(int64({{.Right}}))
				if _, err := tree.{{.Ctx}}.{{.Op}}(&{{.Target}}, &{{.Left}}, &tmpDec); err != nil {
					colexecerror.ExpectedError(err)
				}
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
		}
		return buf.String()
	}
}

func (c intDecimalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		binOp := op.overloadBase.BinOp
		args := map[string]interface{}{
			"Ctx":              binaryOpDecCtx[binOp],
			"Op":               binaryOpDecMethod[binOp],
			"CheckRightIsZero": checkRightIsZero(binOp),
			"Target":           targetElem,
			"Left":             leftElem,
			"Right":            rightElem,
		}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				{{if .CheckRightIsZero}}
				if {{.Right}}.IsZero() {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				{{end}}
				var tmpDec apd.Decimal //gcassert:noescape
				tmpDec.SetInt64(int64({{.Left}}))
				_, err := tree.{{.Ctx}}.{{.Op}}(&{{.Target}}, &tmpDec, &{{.Right}})
				if err != nil {
					colexecerror.ExpectedError(err)
				}
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
		}
		return buf.String()
	}
}

func (c timestampCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case treebin.Minus:
			return fmt.Sprintf(`
		  nanos := %[2]s.Sub(%[3]s).Nanoseconds()
		  %[1]s = duration.MakeDuration(nanos, 0, 0)
		  `,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (c intervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case treebin.Plus:
			return fmt.Sprintf(`%[1]s = %[2]s.Add(%[3]s)`,
				targetElem, leftElem, rightElem)
		case treebin.Minus:
			return fmt.Sprintf(`%[1]s = %[2]s.Sub(%[3]s)`,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (c jsonCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		vecVariable, idxVariable, err := parseNonIndexableTargetElem(targetElem)
		if err != nil {
			return fmt.Sprintf("colexecerror.InternalError(\"%s\")", err)
		}
		switch op.overloadBase.BinOp {
		case treebin.Concat:
			return fmt.Sprintf(`
_j, _err := %[3]s.Concat(%[4]s)
if _err != nil {
    colexecerror.ExpectedError(_err)
}

%[1]s.Set(%[2]s, _j)`, vecVariable, idxVariable, leftElem, rightElem)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (j jsonBytesCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		vecVariable, idxVariable, err := parseNonIndexableTargetElem(targetElem)
		if err != nil {
			return fmt.Sprintf("colexecerror.InternalError(\"%s\")", err)
		}
		getJSONFetchValKey := func(handleNonNilPath string) string {
			return fmt.Sprintf(`
// Get an unsafe string handle onto the bytes, to avoid a spurious copy. This
// is safe since we know the bytes won't change out from under us during
// FetchValKey.
_j, _err := %[3]s.FetchValKey(*(*string)(unsafe.Pointer(&%[4]s)))
if _err != nil {
    colexecerror.ExpectedError(_err)
}
if _j == nil {
    _outNulls.SetNull(%[2]s)
} else {
    %[1]s
}`,
				handleNonNilPath, idxVariable, leftElem, rightElem)
		}
		switch op.overloadBase.BinOp {
		case treebin.JSONFetchVal:
			return getJSONFetchValKey(fmt.Sprintf("%s.Set(%s, _j)", vecVariable, idxVariable))
		case treebin.JSONFetchText:
			return getJSONFetchValKey(fmt.Sprintf(`
			_text, _err := _j.AsText()
			if _err != nil {
				colexecerror.ExpectedError(_err)
			}
			if _text == nil {
				_outNulls.SetNull(%[2]s)
			} else {
				%[1]s.Set(%[2]s, []byte(*_text))
			}`, vecVariable, idxVariable))
		case treebin.Minus:
			return fmt.Sprintf(`
// Get an unsafe string handle onto the bytes, to avoid a spurious copy. This
// is safe since we know the bytes won't change out from under us during
// RemoveString.
_j, _, _err := %[3]s.RemoveString(*(*string)(unsafe.Pointer(&%[4]s)))
if _err != nil {
    colexecerror.ExpectedError(_err)
}
%[1]s.Set(%[2]s, _j)
`, vecVariable, idxVariable, leftElem, rightElem)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (j jsonIntCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		vecVariable, idxVariable, err := parseNonIndexableTargetElem(targetElem)
		if err != nil {
			return fmt.Sprintf("colexecerror.InternalError(\"%s\")", err)
		}
		getJSONFetchValIdx := func(handleNonNilPath string) string {
			return fmt.Sprintf(`
_j, _err := %[3]s.FetchValIdx(int(%[4]s))
if _err != nil {
    colexecerror.ExpectedError(_err)
}
if _j == nil {
    _outNulls.SetNull(%[2]s)
} else {
    %[1]s
}`,
				handleNonNilPath, idxVariable, leftElem, rightElem)
		}
		switch op.overloadBase.BinOp {
		case treebin.JSONFetchVal:
			return getJSONFetchValIdx(fmt.Sprintf("%s.Set(%s, _j)", vecVariable, idxVariable))
		case treebin.JSONFetchText:
			return getJSONFetchValIdx(fmt.Sprintf(`
			_text, _err := _j.AsText()
			if _err != nil {
				colexecerror.ExpectedError(_err)
			}
			if _text == nil {
				_outNulls.SetNull(%[2]s)
			} else {
				%[1]s.Set(%[2]s, []byte(*_text))
			}`, vecVariable, idxVariable))
		case treebin.Minus:
			return fmt.Sprintf(`
_j, _, _err := %[3]s.RemoveIndex(int(%[4]s))
if _err != nil {
	  colexecerror.ExpectedError(_err)
}
%[1]s.Set(%[2]s, _j)`, vecVariable, idxVariable, leftElem, rightElem)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (j jsonDatumCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		vecVariable, idxVariable, err := parseNonIndexableTargetElem(targetElem)
		if err != nil {
			return fmt.Sprintf("colexecerror.InternalError(\"%s\")", err)
		}
		// getJSONFetchPath is a utility function that generates code for JSON
		// fetch operation. It takes the code snippet that will be invoked when
		// JSON path (stored at "_path") is non-nil.
		getJSONFetchPath := func(handleNonNilPath string) string {
			return fmt.Sprintf(`
_path, _err := tree.GetJSONPath(%[3]s, *tree.MustBeDArray(%[4]s.(tree.Datum)))
if _err != nil {
    colexecerror.ExpectedError(_err)
}
if _path == nil {
    _outNulls.SetNull(%[2]s)
} else {
    %[1]s
}`,
				handleNonNilPath, idxVariable, leftElem, rightElem)
		}
		switch op.overloadBase.BinOp {
		case treebin.JSONFetchValPath:
			return getJSONFetchPath(fmt.Sprintf("%s.Set(%s, _path)", vecVariable, idxVariable))
		case treebin.JSONFetchTextPath:
			return getJSONFetchPath(fmt.Sprintf(`
    _text, _err := _path.AsText()
    if _err != nil {
        colexecerror.ExpectedError(_err)
    }
    if _text == nil {
        _outNulls.SetNull(%[2]s)
    } else {
        %[1]s.Set(%[2]s, []byte(*_text))
    }
`, vecVariable, idxVariable))
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

// timestampRangeCheck should be added at the end of operations that modify and
// return timestamps in order to ensure that the vectorized engine returns the
// same errors as the row engine. The range check expects the timestamp to be
// stored in a local variable named 't_res'.
const timestampRangeCheck = `	
rounded_res := t_res.Round(time.Microsecond)
if rounded_res.After(tree.MaxSupportedTime) || rounded_res.Before(tree.MinSupportedTime) {
		colexecerror.ExpectedError(errors.Newf("timestamp %q exceeds supported timestamp bounds", t_res.Format(time.RFC3339)))
}`

func (c timestampIntervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case treebin.Plus:
			return fmt.Sprintf(`t_res := duration.Add(%[1]s, %[2]s)`,
				leftElem, rightElem) + timestampRangeCheck + fmt.Sprintf("\n%s = t_res", targetElem)
		case treebin.Minus:
			return fmt.Sprintf(`t_res := duration.Add(%[1]s, %[2]s.Mul(-1))`,
				leftElem, rightElem) + timestampRangeCheck + fmt.Sprintf("\n%s = t_res", targetElem)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (c intervalTimestampCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case treebin.Plus:
			return fmt.Sprintf(`t_res := duration.Add(%[2]s, %[1]s)`,
				leftElem, rightElem) + timestampRangeCheck + fmt.Sprintf("\n%s = t_res", targetElem)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return ""
	}
}

func (c intervalIntCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case treebin.Mult:
			return fmt.Sprintf(`%[1]s = %[2]s.Mul(int64(%[3]s))`,
				targetElem, leftElem, rightElem)
		case treebin.Div:
			return fmt.Sprintf(`
				if %[3]s == 0 {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				%[1]s = %[2]s.Div(int64(%[3]s))`,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return ""
	}
}

func (c intIntervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case treebin.Mult:
			return fmt.Sprintf(`%[1]s = %[3]s.Mul(int64(%[2]s))`,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return ""
	}
}

func (c intervalFloatCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case treebin.Mult:
			return fmt.Sprintf(`%[1]s = %[2]s.MulFloat(float64(%[3]s))`,
				targetElem, leftElem, rightElem)
		case treebin.Div:
			return fmt.Sprintf(`
				if %[3]s == 0.0 {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				%[1]s = %[2]s.DivFloat(float64(%[3]s))`,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return ""
	}
}

func (c floatIntervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case treebin.Mult:
			return fmt.Sprintf(`%[1]s = %[3]s.MulFloat(float64(%[2]s))`,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return ""
	}
}

func (c intervalDecimalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case treebin.Mult:
			return fmt.Sprintf(`
		  f, err := %[3]s.Float64()
		  if err != nil {
		    colexecerror.ExpectedError(err)
		  }
		  %[1]s = %[2]s.MulFloat(f)`,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return ""
	}
}

func (c decimalIntervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case treebin.Mult:
			return fmt.Sprintf(`
		  f, err := %[2]s.Float64()
		  if err != nil {
		    colexecerror.ExpectedError(err)
		  }
		  %[1]s = %[3]s.MulFloat(f)`,
				targetElem, leftElem, rightElem)

		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return ""
	}
}

// executeBinOpOnDatums returns a string that performs a binary operation on
// two datum elements. It takes the following arguments:
// - prelude - will be prepended before binary function evaluation and should
// be used to do any setup (like converting non-datum element to its datum
// equivalent)
// - targetElem - same as targetElem parameter in assignFunc signature
// - leftDatumElem and rightDatumElem - the variable names of the left and right
// datum elements that must be convertable to tree.Datum type.
func executeBinOpOnDatums(prelude, targetElem, leftDatumElem, rightDatumElem string) string {
	codeBlock := fmt.Sprintf(`
			%s
			_res, err := _overloadHelper.BinFn(_overloadHelper.EvalCtx, %s.(tree.Datum), %s.(tree.Datum))
			if err != nil {
				colexecerror.ExpectedError(err)
			}`, prelude, leftDatumElem, rightDatumElem,
	)
	if regexp.MustCompile(`.*\[.*]`).MatchString(targetElem) {
		// targetElem is of the form 'vec[i]'.
		vecVariable, idxVariable, err := parseNonIndexableTargetElem(targetElem)
		if err != nil {
			colexecerror.InternalError(err)
		}
		codeBlock += fmt.Sprintf(`
			if _res == tree.DNull {
				_outNulls.SetNull(%s)
			}
			%s.Set(%s, _res)
			`, idxVariable, vecVariable, idxVariable,
		)
	} else {
		// targetElem is assumed to simply be the same type as res.
		codeBlock += fmt.Sprintf(`
			%s = _res
    	`, targetElem,
		)
	}
	return codeBlock
}

func (c datumCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		return executeBinOpOnDatums(
			"" /* prelude */, targetElem, leftElem, rightElem,
		)
	}
}

// convertNativeToDatum returns a string that converts nativeElem to a
// tree.Datum that is stored in local variable named datumElemVarName.
func convertNativeToDatum(
	op treebin.BinaryOperatorSymbol,
	canonicalTypeFamily types.Family,
	nativeElem, datumElemVarName string,
) string {
	var runtimeConversion string
	switch canonicalTypeFamily {
	case types.BoolFamily:
		runtimeConversion = fmt.Sprintf("tree.DBool(%s)", nativeElem)
	case types.IntFamily:
		// TODO(yuzefovich): dates are represented as ints, so this will need
		// to be updated once we allow mixed-type operations on dates.
		runtimeConversion = fmt.Sprintf("tree.DInt(%s)", nativeElem)
	case types.FloatFamily:
		runtimeConversion = fmt.Sprintf("tree.DFloat(%s)", nativeElem)
	case types.DecimalFamily:
		runtimeConversion = fmt.Sprintf("tree.DDecimal{Decimal: %s}", nativeElem)
	case types.IntervalFamily:
		runtimeConversion = fmt.Sprintf("tree.DInterval{Duration: %s}", nativeElem)
	case types.BytesFamily:
		// TODO(yuzefovich): figure out a better way to perform type resolution
		// for types that have the same physical representation.
		switch op {
		case treebin.Minus, treebin.JSONFetchVal:
			// We currently support two operations that take in one datum
			// argument and one argument with Bytes canonical type family (Minus
			// and JSONFetchVal) and both require the value to be of String
			// type, so we perform such conversion.
			runtimeConversion = fmt.Sprintf("tree.DString(%s)", nativeElem)
		default:
			// In order to mistakenly not add support for another binary
			// operator in such mixed representation scenario while forgetting
			// to choose the correct conversion, we'll panic for all other
			// operators (during the code generation).
			colexecerror.InternalError(errors.AssertionFailedf("unexpected binary op %s that requires conversion from Bytes canonical type family", op))
		}
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unexpected canonical type family: %s", canonicalTypeFamily))
	}
	return fmt.Sprintf(`
			_convertedNativeElem := %[1]s
			var %[2]s tree.Datum
			%[2]s = &_convertedNativeElem
			`, runtimeConversion, datumElemVarName)
}

func (c datumNonDatumCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		const rightDatumElem = "_nonDatumArgAsDatum"
		prelude := convertNativeToDatum(
			op.BinOp, op.lastArgTypeOverload.CanonicalTypeFamily, rightElem, rightDatumElem,
		)
		return executeBinOpOnDatums(
			prelude, targetElem, leftElem, rightDatumElem,
		)
	}
}

func (c nonDatumDatumCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		const leftDatumElem = "_nonDatumArgAsDatum"
		prelude := convertNativeToDatum(op.BinOp, c.leftCanonicalTypeFamily, leftElem, leftDatumElem)
		return executeBinOpOnDatums(prelude, targetElem, leftDatumElem, rightElem)
	}
}

// Some target element has form "caller[index]", however, types like Bytes and datumVec
// don't support indexing, we need to translate that into a set operation.This method
// is used to extract caller and index value from targetElem.
func parseNonIndexableTargetElem(targetElem string) (caller string, index string, err error) {
	if !regexp.MustCompile(`.*\[.*]`).MatchString(targetElem) {
		err = fmt.Errorf("couldn't translate indexing on target element: %s", targetElem)
		return
	}
	// Next, we separate the target into two tokens preemptively removing
	// the closing square bracket.
	tokens := strings.Split(targetElem[:len(targetElem)-1], "[")
	if len(tokens) != 2 {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly len(tokens) != 2"))
	}
	caller = tokens[0]
	index = tokens[1]
	return
}
