// Copyright 2021 The Cockroach Authors.
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
	"io"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

const rangeOffsetHandlerTmpl = "pkg/sql/colexec/colexecwindow/range_offset_handler_tmpl.go"

// Note that TimeFamily and TimestampFamily are added by
// windowTypeFamilyReplacement so that they can be handled in the same switch
// case blocks as TimeTZFamily and TimestampTZFamily respectively.
var rangeOrderColTypeFamilies = []types.Family{
	types.IntFamily, types.DecimalFamily, types.FloatFamily, types.IntervalFamily, types.DateFamily,
	types.TimestampTZFamily, types.TimeTZFamily,
}

const windowTypeFamilyReplacement = `{{.TypeFamily}}{{if eq .TypeFamily "types.TimeTZFamily"}}, types.TimeFamily{{else if eq .TypeFamily "types.TimestampTZFamily"}}, types.TimestampFamily{{end}}`

func rangeOffsetHandlerGenerator(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_OP_STRING", "{{.OpString}}",
		"_OFFSET_BOUND", "{{boundToExecinfrapb .BoundType}}",
		"_ORDER_DIRECTION", "{{.IsOrdColAsc}}",
		"_IS_START", "{{.IsStart}}",
		"_TYPE_FAMILY", windowTypeFamilyReplacement,
		"_TYPE_WIDTH", typeWidthReplacement,
		"_OFFSET_GOTYPE", "{{.OffsetGoType}}",
		"_CMP_GOTYPE", "{{.CmpGoType}}",
		"_TYPE", "{{.VecMethod}}",
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignAddRe := makeFunctionRegex("_VALUE_BY_OFFSET", 3)
	s = assignAddRe.ReplaceAllString(s, makeTemplateFunctionCall("ValueByOffset", 3))

	assignCmpRe := makeFunctionRegex("_ASSIGN_CMP", 4)
	s = assignCmpRe.ReplaceAllString(s, makeTemplateFunctionCall("AssignCmp", 4))

	tmpl, err := template.New("window_framer").Funcs(
		template.FuncMap{
			"buildDict":          buildDict,
			"boundToExecinfrapb": boundToExecinfrapb,
		}).Parse(s)
	if err != nil {
		return err
	}

	var rangeOffsetHandlerTmplInfos []windowFrameOffsetBoundInfo
	for _, bound := range []treewindow.WindowFrameBoundType{treewindow.OffsetPreceding, treewindow.OffsetFollowing} {
		boundInfo := windowFrameOffsetBoundInfo{BoundType: bound}
		for _, isStart := range []bool{true, false} {
			isStartInfo := windowFrameOffsetIsStartInfo{IsStart: isStart}
			for _, isOrdColAsc := range []bool{true, false} {
				ordColDirInfo := windowFrameOrdDirInfo{IsOrdColAsc: isOrdColAsc}
				for _, typeFamily := range rangeOrderColTypeFamilies {
					canonicalTypeFamily := typeconv.TypeFamilyToCanonicalTypeFamily(typeFamily)
					typeFamilyStr := toString(typeFamily)
					typeFamilyInfo := windowFrameOrderTypeFamilyInfo{
						TypeFamily: typeFamilyStr,
					}
					for _, width := range getTypeWidths(typeFamily) {
						opString := "rangeHandler" + bound.Name()
						if isStart {
							opString += "Start"
						} else {
							opString += "End"
						}
						if isOrdColAsc {
							opString += "Asc"
						} else {
							opString += "Desc"
						}
						opString += typeName(typeFamily, width)
						widthOverload := windowFrameOrderWidthOverload{
							Width:           width,
							VecMethod:       toVecMethod(canonicalTypeFamily, width),
							OffsetGoType:    getOffsetGoType(typeFamily, width),
							CmpGoType:       getCmpGoType(typeFamily),
							OpString:        opString,
							IsStart:         isStart,
							IsOrdColAsc:     isOrdColAsc,
							assignFunc:      getAssignFunc(typeFamily),
							valueByOffsetOp: getValueByOffsetOp(bound, isOrdColAsc),
							cmpFunc:         getCmpFunc(typeFamily),
						}
						typeFamilyInfo.WidthOverloads = append(typeFamilyInfo.WidthOverloads, widthOverload)
					}
					ordColDirInfo.TypeFamilies = append(ordColDirInfo.TypeFamilies, typeFamilyInfo)
				}
				isStartInfo.Directions = append(isStartInfo.Directions, ordColDirInfo)
			}
			boundInfo.Bounds = append(boundInfo.Bounds, isStartInfo)
		}
		rangeOffsetHandlerTmplInfos = append(rangeOffsetHandlerTmplInfos, boundInfo)
	}

	return tmpl.Execute(wr, rangeOffsetHandlerTmplInfos)
}

func init() {
	registerGenerator(rangeOffsetHandlerGenerator, "range_offset_handler.eg.go", rangeOffsetHandlerTmpl)
}

type windowFrameOffsetBoundInfo struct {
	BoundType treewindow.WindowFrameBoundType
	Bounds    []windowFrameOffsetIsStartInfo
}

type windowFrameOffsetIsStartInfo struct {
	IsStart    bool
	Directions []windowFrameOrdDirInfo
}

type windowFrameOrdDirInfo struct {
	IsOrdColAsc  bool
	TypeFamilies []windowFrameOrderTypeFamilyInfo
}

type windowFrameOrderTypeFamilyInfo struct {
	TypeFamily     string
	WidthOverloads []windowFrameOrderWidthOverload
}

type windowFrameOrderWidthOverload struct {
	Width        int32
	VecMethod    string
	OffsetGoType string
	CmpGoType    string

	OpString        string
	IsStart         bool
	IsOrdColAsc     bool
	cmpFunc         compareFunc
	assignFunc      assignFunc
	valueByOffsetOp treebin.BinaryOperatorSymbol
}

func (overload windowFrameOrderWidthOverload) ValueByOffset(
	targetElem, leftElem, rightElem string,
) string {
	// Note that we need to create lastArgWidthOverload only in order to tell
	// the resolved overload to use correct binary operator (plus or minus), so
	// all other fields remain unset.
	lawo := &lastArgWidthOverload{lastArgTypeOverload: &lastArgTypeOverload{
		overloadBase: newBinaryOverloadBase(overload.valueByOffsetOp),
	}}
	return overload.assignFunc(lawo, targetElem, leftElem, rightElem, "", "", "")
}

var _ = windowFrameOrderWidthOverload{}.ValueByOffset

func (overload windowFrameOrderWidthOverload) AssignCmp(
	targetElem, leftElem, rightElem, leftCol string,
) string {
	return overload.cmpFunc(targetElem, leftElem, rightElem, leftCol, "")
}

var _ = windowFrameOrderWidthOverload{}.AssignCmp

func getAssignFunc(typeFamily types.Family) assignFunc {
	switch typeFamily {
	case types.TimestampFamily, types.TimestampTZFamily:
		var c timestampIntervalCustomizer
		return c.getBinOpAssignFunc()
	case types.DateFamily:
		// Date needs to be specially handled because it is represented as int64s in
		// the vectorized engine, but the behavior needs to be different than for
		// actual integers. We don't define a type customizer for this case in order
		// to avoid handling non-canonical type families.
		return dateAssignFunc
	case types.IntFamily:
		var c intCustomizer
		return c.getBinOpAssignFunc()
	case types.TimeFamily, types.TimeTZFamily:
		var c datumCustomizer
		return c.getBinOpAssignFunc()
	}
	canonicalTypeFamily := typeconv.TypeFamilyToCanonicalTypeFamily(typeFamily)
	var overload *oneArgOverload
	for _, o := range sameTypeBinaryOpToOverloads[treebin.Plus] {
		if o.CanonicalTypeFamily == canonicalTypeFamily {
			overload = o
			break
		}
	}
	if overload == nil {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly didn't find binary overload for %s", typeFamily))
	}
	if len(overload.WidthOverloads) != 1 {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly binary overload for %s doesn't contain a single overload", typeFamily))
	}
	return overload.WidthOverloads[0].AssignFunc
}

func getValueByOffsetOp(
	bound treewindow.WindowFrameBoundType, isOrdColAsc bool,
) treebin.BinaryOperatorSymbol {
	if bound == treewindow.OffsetFollowing {
		if isOrdColAsc {
			return treebin.Plus
		}
		return treebin.Minus
	}
	if isOrdColAsc {
		return treebin.Minus
	}
	return treebin.Plus
}

func getCmpFunc(typeFamily types.Family) compareFunc {
	switch typeFamily {
	case types.TimestampFamily, types.TimestampTZFamily:
		var c timestampCustomizer
		return c.getCmpOpCompareFunc()
	case types.DateFamily:
		// Dates are converted to timestamps when the plus (or minus) operator is
		// applied. Dates have to be specially handled because they are not a
		// canonical type family (stored as int64s).
		return dateCmpFunc
	case types.IntFamily:
		var c intCustomizer
		return c.getCmpOpCompareFunc()
	case types.TimeFamily, types.TimeTZFamily:
		var c datumCustomizer
		return c.getCmpOpCompareFunc()
	}
	canonicalTypeFamily := typeconv.TypeFamilyToCanonicalTypeFamily(typeFamily)
	var overload *oneArgOverload
	for _, o := range sameTypeComparisonOpToOverloads[treecmp.EQ] {
		if o.CanonicalTypeFamily == canonicalTypeFamily {
			overload = o
			break
		}
	}
	if overload == nil {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly didn't find comparison overload for %s", typeFamily))
	}
	if len(overload.WidthOverloads) != 1 {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly comparison overload for %s doesn't contain a single overload", typeFamily))
	}
	return overload.WidthOverloads[0].CompareFunc
}

func (overload windowFrameOrderWidthOverload) BinOpIsPlus() bool {
	return overload.valueByOffsetOp == treebin.Plus
}

var _ = windowFrameOrderWidthOverload{}.BinOpIsPlus()

func getTypeWidths(family types.Family) []int32 {
	if family == types.IntFamily {
		return []int32{16, 32, anyWidth}
	}
	return []int32{anyWidth}
}

func getOffsetGoType(orderColFamily types.Family, orderColWidth int32) string {
	switch orderColFamily {
	case types.TimestampFamily, types.TimestampTZFamily, types.DateFamily:
		return toPhysicalRepresentation(types.IntervalFamily, anyWidth)
	case types.TimeFamily, types.TimeTZFamily:
		return "tree.Datum"
	}
	return toPhysicalRepresentation(
		typeconv.TypeFamilyToCanonicalTypeFamily(orderColFamily), orderColWidth)
}

func getCmpGoType(typeFamily types.Family) string {
	switch typeFamily {
	case types.DateFamily:
		return toPhysicalRepresentation(types.TimestampTZFamily, anyWidth)
	case types.TimeFamily, types.TimeTZFamily:
		return "tree.Datum"
	}
	return toPhysicalRepresentation(typeconv.TypeFamilyToCanonicalTypeFamily(typeFamily), anyWidth)
}

func typeName(typeFamily types.Family, typeWidth int32) string {
	switch typeFamily {
	case types.DateFamily:
		return "Date"
	}
	return toVecMethod(typeconv.TypeFamilyToCanonicalTypeFamily(typeFamily), typeWidth)
}

// This format string should be used with the left operand of the binary
// operation as the first argument, and the desired result variable name as the
// second argument.
const dateToTimeCastStr = `
		d_casted, err := pgdate.MakeDateFromUnixEpoch(%s)
		if err != nil {
			colexecerror.ExpectedError(err)
		}
		%s, err := d_casted.ToTime()
		if err != nil {
			colexecerror.ExpectedError(err)
		}
`

func dateAssignFunc(
	op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string,
) string {
	// Date rows are stored as int64s representing the number of days since the
	// unix epoch. We have to convert to timestamps before executing the binary
	// operator.
	const castVarName = "t_casted"
	castStr := fmt.Sprintf(dateToTimeCastStr, leftElem, castVarName)
	var o timestampIntervalCustomizer
	return castStr + o.getBinOpAssignFunc()(
		op, targetElem, castVarName, rightElem, targetCol, leftCol, rightCol)
}

func dateCmpFunc(targetElem, leftElem, rightElem, leftCol, rightCol string) string {
	// Date rows are stored as int64s representing the number of days since the
	// unix epoch. We have to convert to timestamps before executing the
	// comparison operator.
	const castVarName = "t_casted"
	castStr := fmt.Sprintf(dateToTimeCastStr, leftElem, castVarName)
	var o timestampCustomizer
	return castStr + o.getCmpOpCompareFunc()(targetElem, castVarName, rightElem, leftCol, rightCol)
}
