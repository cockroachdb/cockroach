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
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

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

var binaryOpDecCtx = map[tree.BinaryOperator]string{
	tree.Plus:  "ExactCtx",
	tree.Minus: "ExactCtx",
	tree.Mult:  "ExactCtx",
	tree.Div:   "DecimalCtx",
}

var compatibleCanonicalTypeFamilies = map[types.Family][]types.Family{
	types.BoolFamily:        {types.BoolFamily},
	types.BytesFamily:       {types.BytesFamily},
	types.DecimalFamily:     append(numericCanonicalTypeFamilies, types.IntervalFamily),
	types.IntFamily:         append(numericCanonicalTypeFamilies, types.IntervalFamily),
	types.FloatFamily:       append(numericCanonicalTypeFamilies, types.IntervalFamily),
	types.TimestampTZFamily: timeCanonicalTypeFamilies,
	types.IntervalFamily:    append(numericCanonicalTypeFamilies, timeCanonicalTypeFamilies...),
}

// sameTypeBinaryOpToOverloads maps a binary operator to all of the overloads
// that implement that comparison between two values of the same type (meaning
// they have the same family and width).
var sameTypeBinaryOpToOverloads = make(map[tree.BinaryOperator][]*oneArgOverload, len(execgen.BinaryOpName))

var binOpOutputTypes = make(map[tree.BinaryOperator]map[typePair]*types.T)

func registerBinOpOutputTypes() {
	for binOp := range execgen.BinaryOpName {
		binOpOutputTypes[binOp] = make(map[typePair]*types.T)
		binOpOutputTypes[binOp][typePair{types.FloatFamily, anyWidth, types.FloatFamily, anyWidth}] = types.Float
		for _, leftIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			for _, rightIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
				binOpOutputTypes[binOp][typePair{types.IntFamily, leftIntWidth, types.IntFamily, rightIntWidth}] = types.Int
			}
		}
		binOpOutputTypes[binOp][typePair{types.DecimalFamily, anyWidth, types.DecimalFamily, anyWidth}] = types.Decimal
		binOpOutputTypes[binOp][typePair{types.FloatFamily, anyWidth, types.FloatFamily, anyWidth}] = types.Float
		// Use an output type of the same width when input widths are the same.
		// Note: keep output type of binary operations on integers of different
		// widths in line with planning in colexec/execplan.go.
		for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			var intType *types.T
			switch intWidth {
			case 16:
				intType = types.Int2
			case 32:
				intType = types.Int4
			case anyWidth:
				intType = types.Int
			default:
				colexecerror.InternalError(fmt.Sprintf("unexpected int width: %d", intWidth))
			}
			binOpOutputTypes[binOp][typePair{types.IntFamily, intWidth, types.IntFamily, intWidth}] = intType
		}
		for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			binOpOutputTypes[binOp][typePair{types.DecimalFamily, anyWidth, types.IntFamily, intWidth}] = types.Decimal
			binOpOutputTypes[binOp][typePair{types.IntFamily, intWidth, types.DecimalFamily, anyWidth}] = types.Decimal
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
		binOpOutputTypes[tree.Mult][typePair{numberTypeFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.Interval
		binOpOutputTypes[tree.Mult][typePair{types.IntervalFamily, anyWidth, numberTypeFamily, anyWidth}] = types.Interval
	}
	binOpOutputTypes[tree.Div][typePair{types.IntervalFamily, anyWidth, types.IntFamily, anyWidth}] = types.Interval
	binOpOutputTypes[tree.Div][typePair{types.IntervalFamily, anyWidth, types.FloatFamily, anyWidth}] = types.Interval
	binOpOutputTypes[tree.Plus][typePair{types.TimestampTZFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.TimestampTZ
	binOpOutputTypes[tree.Minus][typePair{types.TimestampTZFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.TimestampTZ
	binOpOutputTypes[tree.Plus][typePair{types.IntervalFamily, anyWidth, types.TimestampTZFamily, anyWidth}] = types.TimestampTZ
}

func populateBinOpOverloads() {
	registerBinOpOutputTypes()
	for _, op := range []tree.BinaryOperator{tree.Plus, tree.Minus, tree.Mult, tree.Div} {
		ob := &overloadBase{
			kind:  binaryOverload,
			Name:  execgen.BinaryOpName[op],
			BinOp: op,
			OpStr: binaryOpInfix[op],
		}
		sameTypeBinaryOpToOverloads[op] = populateTwoArgsOverloads(
			ob, binOpOutputTypes[op],
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

func (decimalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		if op.overloadBase.BinOp == tree.Div {
			return fmt.Sprintf(`
			{
				cond, err := tree.%s.%s(&%s, &%s, &%s)
				if cond.DivisionByZero() {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				if err != nil {
					colexecerror.ExpectedError(err)
				}
			}
			`, binaryOpDecCtx[op.overloadBase.BinOp], binaryOpDecMethod[op.overloadBase.BinOp], targetElem, leftElem, rightElem)
		}
		return fmt.Sprintf("if _, err := tree.%s.%s(&%s, &%s, &%s); err != nil { colexecerror.ExpectedError(err) }",
			binaryOpDecCtx[op.overloadBase.BinOp], binaryOpDecMethod[op.overloadBase.BinOp], targetElem, leftElem, rightElem)
	}
}

func (c floatCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		return fmt.Sprintf("%s = float64(%s) %s float64(%s)", targetElem, leftElem, op.overloadBase.OpStr, rightElem)
	}
}

func (c intCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		args := map[string]string{"Target": targetElem, "Left": leftElem, "Right": rightElem}
		// The int64 customizer handles binOps with integers of different widths (in
		// addition to handling int64-only arithmetic), so we must cast to int64 in
		// this case.
		if c.width == anyWidth {
			args["Left"] = fmt.Sprintf("int64(%s)", leftElem)
			args["Right"] = fmt.Sprintf("int64(%s)", rightElem)
		}
		buf := strings.Builder{}
		var t *template.Template

		switch op.overloadBase.BinOp {

		case tree.Plus:
			t = template.Must(template.New("").Parse(`
				{
					result := {{.Left}} + {{.Right}}
					if (result < {{.Left}}) != ({{.Right}} < 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					{{.Target}} = result
				}
			`))

		case tree.Minus:
			t = template.Must(template.New("").Parse(`
				{
					result := {{.Left}} - {{.Right}}
					if (result < {{.Left}}) != ({{.Right}} > 0) {
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
					result := {{.Left}} * {{.Right}}
					if {{.Left}} > {{.UpperBound}} || {{.Left}} < {{.LowerBound}} || {{.Right}} > {{.UpperBound}} || {{.Right}} < {{.LowerBound}} {
						if {{.Left}} != 0 && {{.Right}} != 0 {
							sameSign := ({{.Left}} < 0) == ({{.Right}} < 0)
							if (result < 0) == sameSign {
								colexecerror.ExpectedError(tree.ErrIntOutOfRange)
							} else if result/{{.Right}} != {{.Left}} {
								colexecerror.ExpectedError(tree.ErrIntOutOfRange)
							}
						}
					}
					{{.Target}} = result
				}
			`))

		case tree.Div:
			// Note that this is the '/' operator, which has a decimal result.
			// TODO(rafi): implement the '//' floor division operator.
			args["Ctx"] = binaryOpDecCtx[op.overloadBase.BinOp]
			t = template.Must(template.New("").Parse(`
			{
				if {{.Right}} == 0 {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				leftTmpDec, rightTmpDec := &decimalScratch.tmpDec1, &decimalScratch.tmpDec2
				leftTmpDec.SetFinite(int64({{.Left}}), 0)
				rightTmpDec.SetFinite(int64({{.Right}}), 0)
				if _, err := tree.{{.Ctx}}.Quo(&{{.Target}}, leftTmpDec, rightTmpDec); err != nil {
					colexecerror.ExpectedError(err)
				}
			}
		`))

		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.overloadBase.BinOp.String()))
		}

		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
		}
		return buf.String()
	}
}

func (c decimalIntCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
		isDivision := op.overloadBase.BinOp == tree.Div
		args := map[string]interface{}{
			"Ctx":        binaryOpDecCtx[op.overloadBase.BinOp],
			"Op":         binaryOpDecMethod[op.overloadBase.BinOp],
			"IsDivision": isDivision,
			"Target":     targetElem, "Left": leftElem, "Right": rightElem,
		}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				{{if .IsDivision}}
				if {{.Right}} == 0 {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				{{end}}
				tmpDec := &decimalScratch.tmpDec1
				tmpDec.SetFinite(int64({{.Right}}), 0)
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
		isDivision := op.overloadBase.BinOp == tree.Div
		args := map[string]interface{}{
			"Ctx":        binaryOpDecCtx[op.overloadBase.BinOp],
			"Op":         binaryOpDecMethod[op.overloadBase.BinOp],
			"IsDivision": isDivision,
			"Target":     targetElem, "Left": leftElem, "Right": rightElem,
		}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				tmpDec := &decimalScratch.tmpDec1
				tmpDec.SetFinite(int64({{.Left}}), 0)
				{{if .IsDivision}}
				cond, err := tree.{{.Ctx}}.{{.Op}}(&{{.Target}}, tmpDec, &{{.Right}})
				if cond.DivisionByZero() {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				{{else}}
				_, err := tree.{{.Ctx}}.{{.Op}}(&{{.Target}}, tmpDec, &{{.Right}})
				{{end}}
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
