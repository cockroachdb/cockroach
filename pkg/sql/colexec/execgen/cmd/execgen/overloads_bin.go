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
	"sort"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var binaryOpDecMethod = map[tree.BinaryOperator]string{
	tree.Plus:     "Add",
	tree.Minus:    "Sub",
	tree.Mult:     "Mul",
	tree.Div:      "Quo",
	tree.FloorDiv: "QuoInteger",
	tree.Mod:      "Rem",
	tree.Pow:      "Pow",
}

var binaryOpFloatMethod = map[tree.BinaryOperator]string{
	tree.FloorDiv: "math.Trunc",
	tree.Mod:      "math.Mod",
	tree.Pow:      "math.Pow",
}

var binaryOpDecCtx = map[tree.BinaryOperator]string{
	tree.Plus:     "ExactCtx",
	tree.Minus:    "ExactCtx",
	tree.Mult:     "ExactCtx",
	tree.Div:      "DecimalCtx",
	tree.FloorDiv: "HighPrecisionCtx",
	tree.Mod:      "HighPrecisionCtx",
	tree.Pow:      "DecimalCtx",
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
var sameTypeBinaryOpToOverloads = make(map[tree.BinaryOperator][]*oneArgOverload, len(execgen.BinaryOpName))

var binOpOutputTypes = make(map[tree.BinaryOperator]map[typePair]*types.T)

func registerBinOpOutputTypes() {
	populateBinOpIntOutputTypeOnIntArgs := func(binOp tree.BinaryOperator) {
		for _, leftIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			for _, rightIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
				binOpOutputTypes[binOp][typePair{types.IntFamily, leftIntWidth, types.IntFamily, rightIntWidth}] = types.Int
			}
		}
	}

	// Bit binary operators.
	for _, binOp := range []tree.BinaryOperator{tree.Bitand, tree.Bitor, tree.Bitxor} {
		binOpOutputTypes[binOp] = make(map[typePair]*types.T)
		populateBinOpIntOutputTypeOnIntArgs(binOp)
		binOpOutputTypes[binOp][typePair{typeconv.DatumVecCanonicalTypeFamily, anyWidth, typeconv.DatumVecCanonicalTypeFamily, anyWidth}] = types.Any
	}

	// Simple arithmetic binary operators.
	for _, binOp := range []tree.BinaryOperator{tree.Plus, tree.Minus, tree.Mult, tree.Div} {
		binOpOutputTypes[binOp] = make(map[typePair]*types.T)
		binOpOutputTypes[binOp][typePair{types.FloatFamily, anyWidth, types.FloatFamily, anyWidth}] = types.Float
		populateBinOpIntOutputTypeOnIntArgs(binOp)
		binOpOutputTypes[binOp][typePair{types.DecimalFamily, anyWidth, types.DecimalFamily, anyWidth}] = types.Decimal
		binOpOutputTypes[binOp][typePair{types.FloatFamily, anyWidth, types.FloatFamily, anyWidth}] = types.Float
		for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			binOpOutputTypes[binOp][typePair{types.DecimalFamily, anyWidth, types.IntFamily, intWidth}] = types.Decimal
			binOpOutputTypes[binOp][typePair{types.IntFamily, intWidth, types.DecimalFamily, anyWidth}] = types.Decimal
		}

		for _, compatibleFamily := range compatibleCanonicalTypeFamilies[typeconv.DatumVecCanonicalTypeFamily] {
			for _, width := range supportedWidthsByCanonicalTypeFamily[compatibleFamily] {
				binOpOutputTypes[binOp][typePair{typeconv.DatumVecCanonicalTypeFamily, anyWidth, compatibleFamily, width}] = types.Any
				binOpOutputTypes[binOp][typePair{compatibleFamily, width, typeconv.DatumVecCanonicalTypeFamily, anyWidth}] = types.Any
			}
		}
	}
	// There is a special case for division with integers; it should have a
	// decimal result.
	for _, leftIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		for _, rightIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			binOpOutputTypes[tree.Div][typePair{types.IntFamily, leftIntWidth, types.IntFamily, rightIntWidth}] = types.Decimal
		}
	}
	binOpOutputTypes[tree.Minus][typePair{types.TimestampTZFamily, anyWidth, types.TimestampTZFamily, anyWidth}] = types.Interval
	binOpOutputTypes[tree.Plus][typePair{types.IntervalFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.Interval
	binOpOutputTypes[tree.Minus][typePair{types.IntervalFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.Interval
	for _, numberTypeFamily := range numericCanonicalTypeFamilies {
		for _, numberTypeWidth := range supportedWidthsByCanonicalTypeFamily[numberTypeFamily] {
			binOpOutputTypes[tree.Mult][typePair{numberTypeFamily, numberTypeWidth, types.IntervalFamily, anyWidth}] = types.Interval
			binOpOutputTypes[tree.Mult][typePair{types.IntervalFamily, anyWidth, numberTypeFamily, numberTypeWidth}] = types.Interval
		}
	}
	binOpOutputTypes[tree.Div][typePair{types.IntervalFamily, anyWidth, types.IntFamily, anyWidth}] = types.Interval
	binOpOutputTypes[tree.Div][typePair{types.IntervalFamily, anyWidth, types.FloatFamily, anyWidth}] = types.Interval
	binOpOutputTypes[tree.Plus][typePair{types.TimestampTZFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.TimestampTZ
	binOpOutputTypes[tree.Minus][typePair{types.TimestampTZFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.TimestampTZ
	binOpOutputTypes[tree.Plus][typePair{types.IntervalFamily, anyWidth, types.TimestampTZFamily, anyWidth}] = types.TimestampTZ

	// Other arithmetic binary operators.
	for _, binOp := range []tree.BinaryOperator{tree.FloorDiv, tree.Mod, tree.Pow} {
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
	binOpOutputTypes[tree.Concat] = map[typePair]*types.T{
		{types.BytesFamily, anyWidth, types.BytesFamily, anyWidth}:                                       types.Bytes,
		{typeconv.DatumVecCanonicalTypeFamily, anyWidth, typeconv.DatumVecCanonicalTypeFamily, anyWidth}: types.Any,
	}

	for _, binOp := range []tree.BinaryOperator{tree.LShift, tree.RShift} {
		binOpOutputTypes[binOp] = make(map[typePair]*types.T)
		populateBinOpIntOutputTypeOnIntArgs(binOp)
		for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			binOpOutputTypes[binOp][typePair{typeconv.DatumVecCanonicalTypeFamily, anyWidth, types.IntFamily, intWidth}] = types.Any
		}
	}

	binOpOutputTypes[tree.JSONFetchVal] = map[typePair]*types.T{
		{typeconv.DatumVecCanonicalTypeFamily, anyWidth, types.BytesFamily, anyWidth}: types.Any,
	}
	for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		binOpOutputTypes[tree.JSONFetchVal][typePair{typeconv.DatumVecCanonicalTypeFamily, anyWidth, types.IntFamily, intWidth}] = types.Any
	}
}

func newBinaryOverloadBase(op tree.BinaryOperator) *overloadBase {
	opStr := op.String()
	if op == tree.Bitxor {
		// tree.Bitxor is "#" when stringified, but Go uses "^" for it, so
		// we override the former.
		opStr = "^"
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
	// Also note that we're sorting all operators in order to have the
	// generated code not change when the order of iteration over map
	// tree.BinOps changes.
	var allBinaryOperators []tree.BinaryOperator
	for binOp := range tree.BinOps {
		allBinaryOperators = append(allBinaryOperators, binOp)
	}
	sort.SliceStable(allBinaryOperators, func(i, j int) bool {
		return allBinaryOperators[i] < allBinaryOperators[j]
	})

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
		if op.overloadBase.BinOp == tree.Concat {
			caller, idx, err := parseNonIndexableTargetElem(targetElem)
			if err != nil {
				return fmt.Sprintf("colexecerror.InternalError(\"%s\")", err)
			}
			result = fmt.Sprintf(`
			{
				var r = []byte{}
				r = append(r, %s...)
				r = append(r, %s...)
				%s
			}
			`, leftElem, rightElem, set(types.BytesFamily, caller, idx, "r"))
		} else {
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return result
	}
}

func checkRightIsZero(binOp tree.BinaryOperator) bool {
	// TODO(yuzefovich): introduce a new operator that would check whether a
	// vector contains a zero value so that the division didn't have to do the
	// check. This is likely to be more performant.
	return binOp == tree.Div || binOp == tree.FloorDiv || binOp == tree.Mod
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
		case tree.FloorDiv:
			computeBinOp = fmt.Sprintf("%s(float64(%s) / float64(%s))", binaryOpFloatMethod[binOp], leftElem, rightElem)
		case tree.Mod, tree.Pow:
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
		case tree.Bitand, tree.Bitor, tree.Bitxor:
			t = template.Must(template.New("").Parse(`
				{{.Target}} = {{.Left}} {{.Op}} {{.Right}}
			`))

		case tree.Plus, tree.Minus:
			overflowCmp := "<"
			if binOp == tree.Minus {
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

		case tree.Mult:
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
				colexecerror.InternalError(fmt.Sprintf("unhandled integer width %d", c.width))
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

		case tree.Div:
			// Note that this is the '/' operator, which has a decimal result.
			args["Ctx"] = binaryOpDecCtx[binOp]
			t = template.Must(template.New("").Parse(`
			{
				if {{.Right}} == 0 {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				leftTmpDec, rightTmpDec := &_overloadHelper.tmpDec1, &_overloadHelper.tmpDec2
				leftTmpDec.SetInt64(int64({{.Left}}))
				rightTmpDec.SetInt64(int64({{.Right}}))
				if _, err := tree.{{.Ctx}}.Quo(&{{.Target}}, leftTmpDec, rightTmpDec); err != nil {
					colexecerror.ExpectedError(err)
				}
			}
		`))
		case tree.Pow:
			args["Ctx"] = binaryOpDecCtx[binOp]

			t = template.Must(template.New("").Parse(`
			{
				leftTmpDec, rightTmpDec := &_overloadHelper.tmpDec1, &_overloadHelper.tmpDec2
				leftTmpDec.SetInt64(int64({{.Left}}))
				rightTmpDec.SetInt64(int64({{.Right}}))
				if _, err := tree.{{.Ctx}}.Pow(leftTmpDec, leftTmpDec, rightTmpDec); err != nil {
					colexecerror.ExpectedError(err)
				}
				resultInt, err := leftTmpDec.Int64()
				if err != nil {
					colexecerror.ExpectedError(tree.ErrIntOutOfRange)
				}
				{{.Target}} = resultInt
			}
			`))
		case tree.FloorDiv, tree.Mod:
			// Note that these operators have integer result.
			t = template.Must(template.New("").Parse(fmt.Sprintf(`
			{
				if {{.Right}} == 0 {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				{{.Target}} = {{.Left}} %s {{.Right}}
			}
		`, op.overloadBase.OpStr)))

		case tree.LShift, tree.RShift:
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
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.overloadBase.OpStr))
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
				tmpDec := &_overloadHelper.tmpDec1
				tmpDec.SetInt64(int64({{.Right}}))
				if _, err := tree.{{.Ctx}}.{{.Op}}(&{{.Target}}, &{{.Left}}, tmpDec); err != nil {
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
				tmpDec := &_overloadHelper.tmpDec1
				tmpDec.SetInt64(int64({{.Left}}))
				_, err := tree.{{.Ctx}}.{{.Op}}(&{{.Target}}, tmpDec, &{{.Right}})
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
		case tree.Minus:
			return fmt.Sprintf(`
		  nanos := %[2]s.Sub(%[3]s).Nanoseconds()
		  %[1]s = duration.MakeDuration(nanos, 0, 0)
		  `,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (c intervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case tree.Plus:
			return fmt.Sprintf(`%[1]s = %[2]s.Add(%[3]s)`,
				targetElem, leftElem, rightElem)
		case tree.Minus:
			return fmt.Sprintf(`%[1]s = %[2]s.Sub(%[3]s)`,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (c timestampIntervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case tree.Plus:
			return fmt.Sprintf(`%[1]s = duration.Add(%[2]s, %[3]s)`,
				targetElem, leftElem, rightElem)
		case tree.Minus:
			return fmt.Sprintf(`%[1]s = duration.Add(%[2]s, %[3]s.Mul(-1))`,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (c intervalTimestampCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case tree.Plus:
			return fmt.Sprintf(`%[1]s = duration.Add(%[3]s, %[2]s)`,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return ""
	}
}

func (c intervalIntCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case tree.Mult:
			return fmt.Sprintf(`%[1]s = %[2]s.Mul(int64(%[3]s))`,
				targetElem, leftElem, rightElem)
		case tree.Div:
			return fmt.Sprintf(`
				if %[3]s == 0 {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				%[1]s = %[2]s.Div(int64(%[3]s))`,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return ""
	}
}

func (c intIntervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case tree.Mult:
			return fmt.Sprintf(`%[1]s = %[3]s.Mul(int64(%[2]s))`,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return ""
	}
}

func (c intervalFloatCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case tree.Mult:
			return fmt.Sprintf(`%[1]s = %[2]s.MulFloat(float64(%[3]s))`,
				targetElem, leftElem, rightElem)
		case tree.Div:
			return fmt.Sprintf(`
				if %[3]s == 0.0 {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				%[1]s = %[2]s.DivFloat(float64(%[3]s))`,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return ""
	}
}

func (c floatIntervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case tree.Mult:
			return fmt.Sprintf(`%[1]s = %[3]s.MulFloat(float64(%[2]s))`,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return ""
	}
}

func (c intervalDecimalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case tree.Mult:
			return fmt.Sprintf(`
		  f, err := %[3]s.Float64()
		  if err != nil {
		    colexecerror.InternalError(err)
		  }
		  %[1]s = %[2]s.MulFloat(f)`,
				targetElem, leftElem, rightElem)
		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}
		return ""
	}
}

func (c decimalIntervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		switch op.overloadBase.BinOp {
		case tree.Mult:
			return fmt.Sprintf(`
		  f, err := %[2]s.Float64()
		  if err != nil {
		    colexecerror.InternalError(err)
		  }
		  %[1]s = %[3]s.MulFloat(f)`,
				targetElem, leftElem, rightElem)

		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
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
// - leftColdataExtDatum - the variable name of the left datum element that
// must be of *coldataext.Datum type
// - rightDatumElem - the variable name of the right datum element which could
// be *coldataext.Datum, tree.Datum, or nil
// - leftCol and rightCol - same as corresponding parameters in assignFunc
// signature
// - datumVecOnRightSide - indicates whether we have datum-backed vector on the
// right side (it'll be used only to supply the eval context).
func executeBinOpOnDatums(
	prelude, targetElem, leftColdataExtDatum, rightDatumElem, leftCol, rightCol string,
	datumVecOnRightSide bool,
) string {
	vecVariable, idxVariable, err := parseNonIndexableTargetElem(targetElem)
	if err != nil {
		return fmt.Sprintf("colexecerror.InternalError(\"%s\")", err)
	}
	return fmt.Sprintf(`
			%s
			_res, err := %s.BinFn(_overloadHelper.binFn, %s, %s)
			if err != nil {
				colexecerror.ExpectedError(err)
			}
			%s
		`, prelude, leftColdataExtDatum, getDatumVecVariableName(leftCol, rightCol, datumVecOnRightSide), rightDatumElem,
		set(typeconv.DatumVecCanonicalTypeFamily, vecVariable, idxVariable, "_res"),
	)
}

func (c datumCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		return executeBinOpOnDatums(
			"" /* prelude */, targetElem,
			leftElem+".(*coldataext.Datum)", rightElem,
			leftCol, rightCol, false, /* datumVecOnRightSide */
		)
	}
}

// convertNativeToDatum returns a string that converts nativeElem to a
// tree.Datum that is stored in local variable named datumElemVarName.
func convertNativeToDatum(
	op tree.BinaryOperator, canonicalTypeFamily types.Family, nativeElem, datumElemVarName string,
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
		case tree.JSONFetchVal:
			runtimeConversion = fmt.Sprintf("tree.DString(%s)", nativeElem)
		default:
			runtimeConversion = fmt.Sprintf("tree.DBytes(%s)", nativeElem)
		}
	default:
		colexecerror.InternalError(fmt.Sprintf("unexpected canonical type family: %s", canonicalTypeFamily))
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
			prelude, targetElem,
			leftElem+".(*coldataext.Datum)", rightDatumElem,
			leftCol, rightCol, false, /* datumVecOnRightSide */
		)
	}
}

func (c nonDatumDatumCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		const (
			leftDatumElem       = "_nonDatumArgAsDatum"
			leftColdataExtDatum = "_nonDatumArgAsColdataExtDatum"
		)
		prelude := fmt.Sprintf(`
			%s
			%s := &coldataext.Datum{Datum: %s}
			`,
			convertNativeToDatum(op.BinOp, c.leftCanonicalTypeFamily, leftElem, leftDatumElem),
			leftColdataExtDatum, leftDatumElem,
		)
		return executeBinOpOnDatums(
			prelude, targetElem,
			leftColdataExtDatum, rightElem,
			leftCol, rightCol, true, /* datumVecOnRightSide */
		)
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
		colexecerror.InternalError("unexpectedly len(tokens) != 2")
	}
	caller = tokens[0]
	index = tokens[1]
	return
}
