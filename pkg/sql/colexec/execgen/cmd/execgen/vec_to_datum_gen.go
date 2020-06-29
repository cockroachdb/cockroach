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
	"io"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type vecToDatumTmplInfo struct {
	// TypeFamily contains the type family this struct is handling, with
	// "types." prefix.
	TypeFamily string
	// Widths contains all of the type widths that this struct is handling.
	// Note that the entry with 'anyWidth' width must be last in the slice.
	Widths []vecToDatumWidthTmplInfo
}

type vecToDatumWidthTmplInfo struct {
	Width     int32
	VecMethod string
	// ConversionTmpl is a "format string" for the conversion template. It has
	// the same "signature" as AssignConverted, meaning that it should use
	//   %[1]s for targetElem
	//   %[2]s for typedCol
	//   %[3]s for tupleIdx
	//   %[4]s for datumAlloc.
	ConversionTmpl string
}

// AssignConverted returns a string that performs a conversion of the element
// in typedCol at position tupleIdx and assigns the result to targetElem.
// datumAlloc is the name of *sqlbase.DatumAlloc struct that can be used to
// allocate new datums.
func (i vecToDatumWidthTmplInfo) AssignConverted(
	targetElem, typedCol, tupleIdx, datumAlloc string,
) string {
	return fmt.Sprintf(i.ConversionTmpl, targetElem, typedCol, tupleIdx, datumAlloc)
}

// Remove unused warning.
var _ = vecToDatumWidthTmplInfo{}.AssignConverted

// conversionTmpls maps the type families to the corresponding "format" strings
// (see comment above for details).
// Note that the strings are formatted this way so that generated code doesn't
// have empty lines.
var conversionTmpls = map[types.Family]string{
	types.BoolFamily: `%[1]s = tree.MakeDBool(tree.DBool(%[2]s[%[3]s]))`,
	// Note that currently, regardless of the integer's width, we always return
	// INT8, so there is a single conversion template for IntFamily.
	types.IntFamily:   `%[1]s = %[4]s.NewDInt(tree.DInt(%[2]s[%[3]s]))`,
	types.FloatFamily: `%[1]s = %[4]s.NewDFloat(tree.DFloat(%[2]s[%[3]s]))`,
	types.DecimalFamily: `  d := %[4]s.NewDDecimal(tree.DDecimal{Decimal: %[2]s[%[3]s]})
							// Clear the Coeff so that the Set below allocates a new slice for the
							// Coeff.abs field.
							d.Coeff = big.Int{}
							d.Coeff.Set(&%[2]s[%[3]s].Coeff)
							%[1]s = d`,
	types.DateFamily: `%[1]s = %[4]s.NewDDate(tree.DDate{Date: pgdate.MakeCompatibleDateFromDisk(%[2]s[%[3]s])})`,
	types.BytesFamily: `// Note that there is no need for a copy since DBytes uses a string
						// as underlying storage, which will perform the copy for us.
						%[1]s = %[4]s.NewDBytes(tree.DBytes(%[2]s.Get(%[3]s)))`,
	types.OidFamily: `%[1]s = %[4]s.NewDOid(tree.MakeDOid(tree.DInt(%[2]s[%[3]s])))`,
	types.UuidFamily: ` // Note that there is no need for a copy because uuid.FromBytes
						// will perform a copy.
						id, err := uuid.FromBytes(%[2]s.Get(%[3]s))
						if err != nil {
							colexecerror.InternalError(err)
						}
						%[1]s = %[4]s.NewDUuid(tree.DUuid{UUID: id})`,
	types.TimestampFamily:                `%[1]s = %[4]s.NewDTimestamp(tree.DTimestamp{Time: %[2]s[%[3]s]})`,
	types.TimestampTZFamily:              `%[1]s = %[4]s.NewDTimestampTZ(tree.DTimestampTZ{Time: %[2]s[%[3]s]})`,
	types.IntervalFamily:                 `%[1]s = %[4]s.NewDInterval(tree.DInterval{Duration: %[2]s[%[3]s]})`,
	typeconv.DatumVecCanonicalTypeFamily: `%[1]s = %[2]s.Get(%[3]s).(*coldataext.Datum).Datum`,
}

const vecToDatumTmpl = "pkg/sql/colexec/vec_to_datum_tmpl.go"

func genVecToDatum(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_HAS_NULLS", "$.HasNulls",
		"_HAS_SEL", "$.HasSel",
		"_TYPE_FAMILY", "{{.TypeFamily}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_VEC_METHOD", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	setTupleIdx := makeFunctionRegex("_SET_TUPLE_IDX", 4)
	s = setTupleIdx.ReplaceAllString(s, `{{template "setTupleIdx" buildDict "HasSel" $4}}`)
	vecToDatum := makeFunctionRegex("_VEC_TO_DATUM", 7)
	s = vecToDatum.ReplaceAllString(s, `{{template "vecToDatum" buildDict "Global" . "HasNulls" $6 "HasSel" $7}}`)

	assignConvertedRe := makeFunctionRegex("_ASSIGN_CONVERTED", 4)
	s = assignConvertedRe.ReplaceAllString(s, makeTemplateFunctionCall("AssignConverted", 4))

	tmpl, err := template.New("vec_to_datum").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	var tmplInfos []vecToDatumTmplInfo
	// Note that String family is a special case that is handled separately by
	// the template explicitly, so it is omitted from this slice.
	optimizedTypeFamilies := []types.Family{
		types.BoolFamily, types.IntFamily, types.FloatFamily, types.DecimalFamily,
		types.DateFamily, types.BytesFamily, types.OidFamily, types.UuidFamily,
		types.TimestampFamily, types.TimestampTZFamily, types.IntervalFamily,
	}
	for _, typeFamily := range optimizedTypeFamilies {
		canonicalTypeFamily := typeconv.TypeFamilyToCanonicalTypeFamily(typeFamily)
		tmplInfo := vecToDatumTmplInfo{TypeFamily: "types." + typeFamily.String()}
		widths := supportedWidthsByCanonicalTypeFamily[canonicalTypeFamily]
		if typeFamily != canonicalTypeFamily {
			// We have a type family that is supported via another's physical
			// representation (e.g. dates are the same as INT8s), so we
			// override the widths to use only the default one.
			widths = []int32{anyWidth}
		}
		for _, width := range widths {
			tmplInfo.Widths = append(tmplInfo.Widths, vecToDatumWidthTmplInfo{
				Width:          width,
				VecMethod:      toVecMethod(canonicalTypeFamily, width),
				ConversionTmpl: conversionTmpls[typeFamily],
			})
		}
		tmplInfos = append(tmplInfos, tmplInfo)
	}

	// Datum-backed types require special handling.
	tmplInfos = append(tmplInfos, vecToDatumTmplInfo{
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
		Widths: []vecToDatumWidthTmplInfo{{
			Width:          anyWidth,
			VecMethod:      toVecMethod(typeconv.DatumVecCanonicalTypeFamily, anyWidth),
			ConversionTmpl: conversionTmpls[typeconv.DatumVecCanonicalTypeFamily],
		}},
	})

	return tmpl.Execute(wr, tmplInfos)
}

func init() {
	registerGenerator(genVecToDatum, "vec_to_datum.eg.go", vecToDatumTmpl)
}
