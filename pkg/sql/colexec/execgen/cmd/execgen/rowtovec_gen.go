// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"io"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type rowToVecTmplInfo struct {
	// TypeFamily contains the type family this struct is handling, with
	// "types." prefix.
	TypeFamily string
	// Widths contains all of the type widths that this struct is handling.
	// Note that the entry with 'anyWidth' width must be last in the slice.
	Widths []rowToVecWidthTmplInfo
}

type rowToVecWidthTmplInfo struct {
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
func (i rowToVecWidthTmplInfo) Prelude(datum string) string {
	if i.PreludeTmpl == "" {
		return ""
	}
	return fmt.Sprintf(i.PreludeTmpl, datum)
}

// Convert returns a string that performs a conversion of datum to its
// physical equivalent and assigns the result to target.
func (i rowToVecWidthTmplInfo) Convert(datum string) string {
	return fmt.Sprintf(i.ConversionTmpl, datum)
}

func (i rowToVecWidthTmplInfo) Sliceable() bool {
	return sliceable(i.canonicalTypeFamily)
}

// Remove unused warnings.
var _ = rowToVecWidthTmplInfo{}.Prelude
var _ = rowToVecWidthTmplInfo{}.Convert
var _ = rowToVecWidthTmplInfo{}.Sliceable

type familyWidthPair struct {
	family types.Family
	width  int32
}

var rowToVecPreludeTmpls = map[familyWidthPair]string{
	{types.StringFamily, anyWidth}: `// Handle other STRING-related OID types, like oid.T_name.
												 wrapper, ok := %[1]s.(*tree.DOidWrapper)
												 if ok {
												     %[1]s = wrapper.Wrapped
												 }`,
}

// rowToVecConversionTmpls maps the type families to the corresponding
// "format" strings (see comment above for details).
var rowToVecConversionTmpls = map[familyWidthPair]string{
	{types.BoolFamily, anyWidth}:                     `bool(*%[1]s.(*tree.DBool))`,
	{types.BytesFamily, anyWidth}:                    `encoding.UnsafeConvertStringToBytes(string(*%[1]s.(*tree.DBytes)))`,
	{types.EncodedKeyFamily, anyWidth}:               `encoding.UnsafeConvertStringToBytes(string(*%[1]s.(*tree.DEncodedKey)))`,
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
	{types.EnumFamily, anyWidth}:                     `%[1]s.(*tree.DEnum).PhysicalRep`,
	{typeconv.DatumVecCanonicalTypeFamily, anyWidth}: `%[1]s`,
}

const rowToVecTmpl = "pkg/sql/colexec/rowtovec_tmpl.go"

func genRowToVec(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_TYPE_FAMILY", "{{.TypeFamily}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"TemplateType", "{{.VecMethod}}",
		"_TYPE", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	preludeRe := makeFunctionRegex("_PRELUDE", 1)
	s = preludeRe.ReplaceAllString(s, makeTemplateFunctionCall("Prelude", 1))
	convertRe := makeFunctionRegex("_CONVERT", 1)
	s = convertRe.ReplaceAllString(s, makeTemplateFunctionCall("Convert", 1))

	tmpl, err := template.New("rowToVec").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, getRowToVecTmplInfos())
}

func getRowToVecTmplInfos() []rowToVecTmplInfo {
	var tmplInfos []rowToVecTmplInfo
	for typeFamily := types.Family(0); typeFamily < types.AnyFamily; typeFamily++ {
		canonicalTypeFamily := typeconv.TypeFamilyToCanonicalTypeFamily(typeFamily)
		if canonicalTypeFamily == typeconv.DatumVecCanonicalTypeFamily {
			// Datum-backed type families are handled below.
			continue
		}
		tmplInfo := rowToVecTmplInfo{TypeFamily: "types." + typeFamily.String()}
		widths := supportedWidthsByCanonicalTypeFamily[canonicalTypeFamily]
		if typeFamily != canonicalTypeFamily {
			// We have a type family that is supported via another's physical
			// representation (e.g. dates are the same as INT8s), so we
			// override the widths to use only the default one.
			widths = []int32{anyWidth}
		}
		for _, width := range widths {
			tmplInfo.Widths = append(tmplInfo.Widths, rowToVecWidthTmplInfo{
				canonicalTypeFamily: canonicalTypeFamily,
				Width:               width,
				VecMethod:           toVecMethod(canonicalTypeFamily, width),
				GoType:              toPhysicalRepresentation(canonicalTypeFamily, width),
				ConversionTmpl:      rowToVecConversionTmpls[familyWidthPair{typeFamily, width}],
				PreludeTmpl:         rowToVecPreludeTmpls[familyWidthPair{typeFamily, width}],
			})
		}
		tmplInfos = append(tmplInfos, tmplInfo)
	}

	// Datum-backed types require special handling.
	tmplInfos = append(tmplInfos, rowToVecTmplInfo{
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
		Widths: []rowToVecWidthTmplInfo{{
			canonicalTypeFamily: typeconv.DatumVecCanonicalTypeFamily,
			Width:               anyWidth,
			VecMethod:           toVecMethod(typeconv.DatumVecCanonicalTypeFamily, anyWidth),
			GoType:              "tree.Datum",
			ConversionTmpl:      rowToVecConversionTmpls[familyWidthPair{typeconv.DatumVecCanonicalTypeFamily, anyWidth}],
			PreludeTmpl:         rowToVecPreludeTmpls[familyWidthPair{typeconv.DatumVecCanonicalTypeFamily, anyWidth}],
		}},
	})
	return tmplInfos
}

func init() {
	registerGenerator(genRowToVec, "rowtovec.eg.go", rowToVecTmpl)
}
