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
	"fmt"
	"io"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type rowsToVecTmplInfo struct {
	// TypeFamily contains the type family this struct is handling, with
	// "types." prefix.
	TypeFamily string
	// Widths contains all of the type widths that this struct is handling.
	// Note that the entry with 'anyWidth' width must be last in the slice.
	Widths []rowsToVecWidthTmplInfo
}

type rowsToVecWidthTmplInfo struct {
	canonicalTypeFamily types.Family
	Width               int32
	VecMethod           string
	GoType              string
	// ConversionTmpl is a "format string" for the conversion template. It has
	// the same "signature" as Convert, meaning that it should use %[1]s for
	// a datum variable to be converted to physical representation.
	// NOTE: this template must be a single line, any necessary setup needs to
	// happen in PreludeTmpl.
	ConversionTmpl string
	PreludeTmpl    string
}

// Prelude returns a string that performs necessary setup before a conversion
// of datum can be done.
func (i rowsToVecWidthTmplInfo) Prelude(datum string) string {
	if i.PreludeTmpl == "" {
		return ""
	}
	return fmt.Sprintf(i.PreludeTmpl, datum)
}

// Convert returns a string that performs a conversion of datum to its
// physical equivalent and assigns the result to target.
func (i rowsToVecWidthTmplInfo) Convert(datum string) string {
	return fmt.Sprintf(i.ConversionTmpl, datum)
}

func (i rowsToVecWidthTmplInfo) Sliceable() bool {
	return sliceable(i.canonicalTypeFamily)
}

// Remove unused warnings.
var _ = rowsToVecWidthTmplInfo{}.Prelude
var _ = rowsToVecWidthTmplInfo{}.Convert
var _ = rowsToVecWidthTmplInfo{}.Sliceable

type familyWidthPair struct {
	family types.Family
	width  int32
}

var rowsToVecPreludeTmpls = map[familyWidthPair]string{
	{types.StringFamily, anyWidth}: `// Handle other STRING-related OID types, like oid.T_name.
												 wrapper, ok := %[1]s.(*tree.DOidWrapper)
												 if ok {
												     %[1]s = wrapper.Wrapped
												 }`,
}

// rowsToVecConversionTmpls maps the type families to the corresponding
// "format" strings (see comment above for details).
var rowsToVecConversionTmpls = map[familyWidthPair]string{
	{types.BoolFamily, anyWidth}:                     `bool(*%[1]s.(*tree.DBool))`,
	{types.BytesFamily, anyWidth}:                    `encoding.UnsafeConvertStringToBytes(string(*%[1]s.(*tree.DBytes)))`,
	{types.IntFamily, 16}:                            `int16(*%[1]s.(*tree.DInt))`,
	{types.IntFamily, 32}:                            `int32(*%[1]s.(*tree.DInt))`,
	{types.IntFamily, anyWidth}:                      `int64(*%[1]s.(*tree.DInt))`,
	{types.DateFamily, anyWidth}:                     `%[1]s.(*tree.DDate).UnixEpochDaysWithOrig()`,
	{types.FloatFamily, anyWidth}:                    `float64(*%[1]s.(*tree.DFloat))`,
	{types.StringFamily, anyWidth}:                   `encoding.UnsafeConvertStringToBytes(string(*%[1]s.(*tree.DString)))`,
	{types.DecimalFamily, anyWidth}:                  `%[1]s.(*tree.DDecimal).Decimal`,
	{types.JsonFamily, anyWidth}:                     `%[1]s.(*tree.DJSON).JSON`,
	{types.UuidFamily, anyWidth}:                     `%[1]s.(*tree.DUuid).UUID.GetBytesMut()`,
	{types.TimestampFamily, anyWidth}:                `%[1]s.(*tree.DTimestamp).Time`,
	{types.TimestampTZFamily, anyWidth}:              `%[1]s.(*tree.DTimestampTZ).Time`,
	{types.IntervalFamily, anyWidth}:                 `%[1]s.(*tree.DInterval).Duration`,
	{typeconv.DatumVecCanonicalTypeFamily, anyWidth}: `%[1]s`,
}

const rowsToVecTmpl = "pkg/sql/colexec/rowstovec_tmpl.go"

func genRowsToVec(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_TYPE_FAMILY", "{{.TypeFamily}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"TemplateType", "{{.VecMethod}}",
		"_GOTYPE", "{{.GoType}}",
	)
	s := r.Replace(inputFileContents)

	rowsToVecRe := makeFunctionRegex("_ROWS_TO_COL_VEC", 5)
	s = rowsToVecRe.ReplaceAllString(s, `{{template "rowsToColVec" .}}`)

	preludeRe := makeFunctionRegex("_PRELUDE", 1)
	s = preludeRe.ReplaceAllString(s, makeTemplateFunctionCall("Prelude", 1))
	convertRe := makeFunctionRegex("_CONVERT", 1)
	s = convertRe.ReplaceAllString(s, makeTemplateFunctionCall("Convert", 1))

	tmpl, err := template.New("rowsToVec").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, getRowsToVecTmplInfos())
}

func getRowsToVecTmplInfos() []rowsToVecTmplInfo {
	var tmplInfos []rowsToVecTmplInfo
	for typeFamily := types.Family(0); typeFamily < types.AnyFamily; typeFamily++ {
		canonicalTypeFamily := typeconv.TypeFamilyToCanonicalTypeFamily(typeFamily)
		if canonicalTypeFamily == typeconv.DatumVecCanonicalTypeFamily {
			// Datum-backed type families are handled below.
			continue
		}
		tmplInfo := rowsToVecTmplInfo{TypeFamily: "types." + typeFamily.String()}
		widths := supportedWidthsByCanonicalTypeFamily[canonicalTypeFamily]
		if typeFamily != canonicalTypeFamily {
			// We have a type family that is supported via another's physical
			// representation (e.g. dates are the same as INT8s), so we
			// override the widths to use only the default one.
			widths = []int32{anyWidth}
		}
		for _, width := range widths {
			tmplInfo.Widths = append(tmplInfo.Widths, rowsToVecWidthTmplInfo{
				canonicalTypeFamily: canonicalTypeFamily,
				Width:               width,
				VecMethod:           toVecMethod(canonicalTypeFamily, width),
				GoType:              toPhysicalRepresentation(canonicalTypeFamily, width),
				ConversionTmpl:      rowsToVecConversionTmpls[familyWidthPair{typeFamily, width}],
				PreludeTmpl:         rowsToVecPreludeTmpls[familyWidthPair{typeFamily, width}],
			})
		}
		tmplInfos = append(tmplInfos, tmplInfo)
	}

	// Datum-backed types require special handling.
	tmplInfos = append(tmplInfos, rowsToVecTmplInfo{
		// This special "type family" value will result in matching all type
		// families that haven't been matched explicitly, i.e a code like this
		// will get generated:
		//   switch typ.Family() {
		//     case <all types that have optimized physical representation>
		//       ...
		//     case typeconv.DatumVecCanonicalTypeFamily:
		//     default:
		//       <datum-vec conversion>
		//   }
		// Such structure requires that datum-vec tmpl info is added last.
		TypeFamily: "typeconv.DatumVecCanonicalTypeFamily: default",
		Widths: []rowsToVecWidthTmplInfo{{
			canonicalTypeFamily: typeconv.DatumVecCanonicalTypeFamily,
			Width:               anyWidth,
			VecMethod:           toVecMethod(typeconv.DatumVecCanonicalTypeFamily, anyWidth),
			GoType:              "tree.Datum",
			ConversionTmpl:      rowsToVecConversionTmpls[familyWidthPair{typeconv.DatumVecCanonicalTypeFamily, anyWidth}],
			PreludeTmpl:         rowsToVecPreludeTmpls[familyWidthPair{typeconv.DatumVecCanonicalTypeFamily, anyWidth}],
		}},
	})
	return tmplInfos
}

func init() {
	registerGenerator(genRowsToVec, "rowstovec.eg.go", rowsToVecTmpl)
}
