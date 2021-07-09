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

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var comparisonOpInfix = map[tree.ComparisonOperatorSymbol]string{
	tree.EQ: "==",
	tree.NE: "!=",
	tree.LT: "<",
	tree.LE: "<=",
	tree.GT: ">",
	tree.GE: ">=",
}

var comparableCanonicalTypeFamilies = map[types.Family][]types.Family{
	types.BoolFamily:                     {types.BoolFamily},
	types.BytesFamily:                    {types.BytesFamily},
	types.DecimalFamily:                  numericCanonicalTypeFamilies,
	types.IntFamily:                      numericCanonicalTypeFamilies,
	types.FloatFamily:                    numericCanonicalTypeFamilies,
	types.TimestampTZFamily:              {types.TimestampTZFamily},
	types.IntervalFamily:                 {types.IntervalFamily},
	types.JsonFamily:                     {types.JsonFamily},
	typeconv.DatumVecCanonicalTypeFamily: {typeconv.DatumVecCanonicalTypeFamily},
}

// sameTypeComparisonOpToOverloads maps a comparison operator to all of the
// overloads that implement that comparison between two values of the same type
// (meaning they have the same family and width).
var sameTypeComparisonOpToOverloads = make(map[tree.ComparisonOperatorSymbol][]*oneArgOverload, len(execgen.ComparisonOpName))

// cmpOpOutputTypes contains a types.Bool entry for each type pair that we
// support.
var cmpOpOutputTypes = make(map[typePair]*types.T)

func registerCmpOpOutputTypes() {
	for _, leftFamily := range supportedCanonicalTypeFamilies {
		for _, leftWidth := range supportedWidthsByCanonicalTypeFamily[leftFamily] {
			for _, rightFamily := range comparableCanonicalTypeFamilies[leftFamily] {
				for _, rightWidth := range supportedWidthsByCanonicalTypeFamily[rightFamily] {
					cmpOpOutputTypes[typePair{leftFamily, leftWidth, rightFamily, rightWidth}] = types.Bool
				}
			}
		}
	}
}

func populateCmpOpOverloads() {
	registerCmpOpOutputTypes()
	for _, op := range []tree.ComparisonOperatorSymbol{tree.EQ, tree.NE, tree.LT, tree.LE, tree.GT, tree.GE} {
		base := &overloadBase{
			kind:  comparisonOverload,
			Name:  execgen.ComparisonOpName[op],
			CmpOp: tree.MakeComparisonOperator(op),
			OpStr: comparisonOpInfix[op],
		}
		sameTypeComparisonOpToOverloads[op] = populateTwoArgsOverloads(
			base,
			cmpOpOutputTypes,
			func(lawo *lastArgWidthOverload, customizer typeCustomizer) {
				if b, ok := customizer.(cmpOpTypeCustomizer); ok {
					lawo.AssignFunc = func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
						cmp := b.getCmpOpCompareFunc()("cmpResult", leftElem, rightElem, leftCol, rightCol)
						if cmp == "" {
							return ""
						}
						args := map[string]string{"Target": targetElem, "Cmp": cmp, "Op": op.overloadBase.OpStr}
						buf := strings.Builder{}
						t := template.Must(template.New("").Parse(`
										{
											var cmpResult int
											{{.Cmp}}
											{{.Target}} = cmpResult {{.Op}} 0
										}
									`))
						if err := t.Execute(&buf, args); err != nil {
							colexecerror.InternalError(err)
						}
						return buf.String()
					}
					lawo.CompareFunc = b.getCmpOpCompareFunc()
				}
			},
			typeCustomizers,
		)
	}
}

// cmpOpTypeCustomizer is a type customizer that changes how the templater
// produces comparison operator output for a particular type.
type cmpOpTypeCustomizer interface {
	getCmpOpCompareFunc() compareFunc
}

func (boolCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(targetElem, leftElem, rightElem, leftCol, rightCol string) string {
		args := map[string]string{"Target": targetElem, "Left": leftElem, "Right": rightElem}
		buf := strings.Builder{}
		// Inline the code from tree.CompareBools
		t := template.Must(template.New("").Parse(`
			if !{{.Left}} && {{.Right}} {
				{{.Target}} = -1
			}	else if {{.Left}} && !{{.Right}} {
				{{.Target}} = 1
			}	else {
				{{.Target}} = 0
			}
		`))

		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
		}
		return buf.String()
	}
}

func (bytesCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(targetElem, leftElem, rightElem, leftCol, rightCol string) string {
		return fmt.Sprintf("%s = bytes.Compare(%s, %s)", targetElem, leftElem, rightElem)
	}
}

func (decimalCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(targetElem, leftElem, rightElem, leftCol, rightCol string) string {
		return fmt.Sprintf("%s = tree.CompareDecimals(&%s, &%s)", targetElem, leftElem, rightElem)
	}
}

func (c floatCustomizer) getCmpOpCompareFunc() compareFunc {
	return getFloatCmpOpCompareFunc(true /* checkLeftNan */, true /* checkRightNan */)
}

func getFloatCmpOpCompareFunc(checkLeftNan, checkRightNan bool) compareFunc {
	return func(targetElem, leftElem, rightElem, leftCol, rightCol string) string {
		args := map[string]interface{}{
			"Target":        targetElem,
			"Left":          leftElem,
			"Right":         rightElem,
			"CheckLeftNan":  checkLeftNan,
			"CheckRightNan": checkRightNan}
		buf := strings.Builder{}
		// In SQL, NaN is treated as less than all other float values. In Go, any
		// comparison with NaN returns false. To allow floats of different sizes to
		// be compared, always upcast to float64. The CheckLeftNan and CheckRightNan
		// flags skip NaN checks when the input is an int (which is necessary to
		// pass linting.)
		t := template.Must(template.New("").Parse(`
			{
				a, b := float64({{.Left}}), float64({{.Right}})
				if a < b {
					{{.Target}} = -1
				} else if a > b {
					{{.Target}} = 1
				}	else if a == b {
					{{.Target}} = 0
				}	else if {{if .CheckLeftNan}} math.IsNaN(a) {{else}} false {{end}} {
					if {{if .CheckRightNan}} math.IsNaN(b) {{else}} false {{end}} {
						{{.Target}} = 0
					} else {
						{{.Target}} = -1
					}
				}	else {
					{{.Target}} = 1
				}
			}
		`))

		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
		}
		return buf.String()
	}
}

func (c intCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(targetElem, leftElem, rightElem, leftCol, rightCol string) string {
		args := map[string]string{"Target": targetElem, "Left": leftElem, "Right": rightElem}
		buf := strings.Builder{}
		// To allow ints of different sizes to be compared, always upcast to int64.
		t := template.Must(template.New("").Parse(`
			{
				a, b := int64({{.Left}}), int64({{.Right}})
				if a < b {
					{{.Target}} = -1
				} else if a > b {
					{{.Target}} = 1
				}	else {
					{{.Target}} = 0
				}
			}
		`))

		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
		}
		return buf.String()
	}
}

func (c decimalFloatCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(targetElem, leftElem, rightElem, leftCol, rightCol string) string {
		args := map[string]string{"Target": targetElem, "Left": leftElem, "Right": rightElem}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				tmpDec := &_overloadHelper.TmpDec1
				if _, err := tmpDec.SetFloat64(float64({{.Right}})); err != nil {
					colexecerror.ExpectedError(err)
				}
				{{.Target}} = tree.CompareDecimals(&{{.Left}}, tmpDec)
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
		}
		return buf.String()
	}
}

func (c decimalIntCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(targetElem, leftElem, rightElem, leftCol, rightCol string) string {
		args := map[string]string{"Target": targetElem, "Left": leftElem, "Right": rightElem}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				tmpDec := &_overloadHelper.TmpDec1
				tmpDec.SetInt64(int64({{.Right}}))
				{{.Target}} = tree.CompareDecimals(&{{.Left}}, tmpDec)
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
		}
		return buf.String()
	}
}

func (c floatDecimalCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(targetElem, leftElem, rightElem, leftCol, rightCol string) string {
		args := map[string]string{"Target": targetElem, "Left": leftElem, "Right": rightElem}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				tmpDec := &_overloadHelper.TmpDec1
				if _, err := tmpDec.SetFloat64(float64({{.Left}})); err != nil {
					colexecerror.ExpectedError(err)
				}
				{{.Target}} = tree.CompareDecimals(tmpDec, &{{.Right}})
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
		}
		return buf.String()
	}
}

func (c intDecimalCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(targetElem, leftElem, rightElem, leftCol, rightCol string) string {
		args := map[string]string{"Target": targetElem, "Left": leftElem, "Right": rightElem}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				tmpDec := &_overloadHelper.TmpDec1
				tmpDec.SetInt64(int64({{.Left}}))
				{{.Target}} = tree.CompareDecimals(tmpDec, &{{.Right}})
			}
		`))

		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
		}
		return buf.String()
	}
}

func (c floatIntCustomizer) getCmpOpCompareFunc() compareFunc {
	// floatCustomizer's comparison function can be reused since float-int
	// comparison works by casting the int.
	return getFloatCmpOpCompareFunc(true /* checkLeftNan */, false /* checkRightNan */)
}

func (c intFloatCustomizer) getCmpOpCompareFunc() compareFunc {
	// floatCustomizer's comparison function can be reused since int-float
	// comparison works by casting the int.
	return getFloatCmpOpCompareFunc(false /* checkLeftNan */, true /* checkRightNan */)
}

func (c timestampCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(targetElem, leftElem, rightElem, leftCol, rightCol string) string {
		args := map[string]string{"Target": targetElem, "Left": leftElem, "Right": rightElem}
		buf := strings.Builder{}
		// Inline the code from tree.compareTimestamps.
		t := template.Must(template.New("").Parse(`
		if {{.Left}}.Before({{.Right}}) {
			{{.Target}} = -1
		} else if {{.Right}}.Before({{.Left}}) {
			{{.Target}} = 1
		} else {
			{{.Target}} = 0
		}`))

		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
		}
		return buf.String()
	}
}

func (c intervalCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(targetElem, leftElem, rightElem, leftCol, rightCol string) string {
		return fmt.Sprintf("%s = %s.Compare(%s)", targetElem, leftElem, rightElem)
	}
}

func (c jsonCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(targetElem, leftElem, rightElem, leftCol, rightCol string) string {
		return fmt.Sprintf(`
var err error
%s, err = %s.Compare(%s)
if err != nil {
  colexecerror.ExpectedError(err)
}
`, targetElem, leftElem, rightElem)
	}
}

// getDatumVecVariableName returns the variable name for a datumVec given
// leftCol and rightCol (either of which could be "_" - meaning there is no
// vector in scope for the corresponding side).
func getDatumVecVariableName(leftCol, rightCol string) string {
	if leftCol == "_" {
		return rightCol
	}
	return leftCol
}

func (c datumCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(targetElem, leftElem, rightElem, leftCol, rightCol string) string {
		datumVecVariableName := getDatumVecVariableName(leftCol, rightCol)
		return fmt.Sprintf(`
			%s = %s.(*coldataext.Datum).CompareDatum(%s, %s)
		`, targetElem, leftElem, datumVecVariableName, rightElem)
	}
}
