// Copyright 2021 The Cockroach Authors.
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

const spanEncoderTmpl = "pkg/sql/colexec/colexecspan/span_encoder_tmpl.go"

func genSpanEncoder(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_OP_STRING", "{{.OpName}}",
		"_IS_ASC", "{{.Asc}}",
		"_CANONICAL_TYPE_FAMILY", "{{.TypeFamily}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignAddRe := makeFunctionRegex("_ASSIGN_SPAN_ENCODING", 2)
	s = assignAddRe.ReplaceAllString(s, makeTemplateFunctionCall("AssignSpanEncoding", 2))

	tmpl, err := template.New("span_encoder").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	var infos []spanEncoderDirectionInfo
	for _, asc := range []bool{true, false} {
		info := spanEncoderDirectionInfo{Asc: asc}
		for _, family := range supportedCanonicalTypeFamilies {
			familyInfo := spanEncoderTypeFamilyInfo{TypeFamily: familyToString(family)}
			for _, width := range supportedWidthsByCanonicalTypeFamily[family] {
				overload := spanEncoderTmplInfo{
					Asc:        asc,
					Sliceable:  sliceable(family),
					Width:      width,
					OpName:     getSpanEncoderOpName(asc, family, width),
					VecMethod:  toVecMethod(family, width),
					TypeFamily: family,
				}
				familyInfo.Overloads = append(familyInfo.Overloads, overload)
			}
			info.TypeFamilies = append(info.TypeFamilies, familyInfo)
		}
		infos = append(infos, info)
	}
	return tmpl.Execute(wr, infos)
}

func init() {
	registerGenerator(genSpanEncoder, "span_encoder.eg.go", spanEncoderTmpl)
}

type spanEncoderDirectionInfo struct {
	Asc          bool
	TypeFamilies []spanEncoderTypeFamilyInfo
}

type spanEncoderTypeFamilyInfo struct {
	TypeFamily string
	Overloads  []spanEncoderTmplInfo
}

type spanEncoderTmplInfo struct {
	Asc        bool
	Sliceable  bool
	Width      int32
	OpName     string
	VecMethod  string
	TypeFamily types.Family
}

func (info spanEncoderTmplInfo) AssignSpanEncoding(appendTo, valToEncode string) string {
	assignEncoding := func(funcName, val string) string {
		ascString := "Ascending"
		if !info.Asc {
			ascString = "Descending"
		}
		return fmt.Sprintf("%[1]s = encoding.Encode%[2]s%[3]s(%[1]s, %[4]s)",
			appendTo, funcName, ascString, val)
	}

	switch info.TypeFamily {
	case types.IntFamily:
		funcName := "Varint"
		if info.Width == 16 || info.Width == 32 {
			// We need to cast the input to an int64.
			valToEncode = "int64(" + valToEncode + ")"
		}
		return assignEncoding(funcName, valToEncode)
	case types.BoolFamily:
		funcName := "Varint"
		prefix := fmt.Sprintf(`
			var x int64
			if %s {
				x = 1
			} else {
				x = 0
			}
    `, valToEncode)
		valToEncode = "x"
		return prefix + assignEncoding(funcName, valToEncode)
	case types.FloatFamily:
		funcName := "Float"
		return assignEncoding(funcName, valToEncode)
	case types.DecimalFamily:
		funcName := "Decimal"
		valToEncode = "&" + valToEncode
		return assignEncoding(funcName, valToEncode)
	case types.BytesFamily:
		funcName := "Bytes"
		return assignEncoding(funcName, valToEncode)
	case types.TimestampTZFamily:
		funcName := "Time"
		return assignEncoding(funcName, valToEncode)
	case types.IntervalFamily:
		funcName := "DurationAscending"
		if !info.Asc {
			funcName = "DurationDescending"
		}
		return fmt.Sprintf(`
			var err error
			%[1]s, err = encoding.Encode%[2]s(%[1]s, %[3]s)
			if err != nil {
				colexecerror.ExpectedError(err)
			}
    `, appendTo, funcName, valToEncode)
	case types.JsonFamily:
		dir := "encoding.Ascending"
		if !info.Asc {
			dir = "encoding.Descending"
		}
		return fmt.Sprintf(`
			var err error
			%[1]s, err = %[2]s.EncodeForwardIndex(%[1]s, %[3]s)
			if err != nil {
				colexecerror.ExpectedError(err)
			}
    `, appendTo, valToEncode, dir)
	case typeconv.DatumVecCanonicalTypeFamily:
		dir := "encoding.Ascending"
		if !info.Asc {
			dir = "encoding.Descending"
		}
		valToEncode += ".(tree.Datum)"
		return fmt.Sprintf(`
			var err error
			%[1]s, err = keyside.Encode(%[1]s, %[2]s, %[3]s)
			if err != nil {
				colexecerror.ExpectedError(err)
			}
    `, appendTo, valToEncode, dir)
	}
	return fmt.Sprintf("unsupported type: %s", info.TypeFamily.Name())
}

var _ = spanEncoderTmplInfo{}.AssignSpanEncoding

func getSpanEncoderOpName(asc bool, family types.Family, width int32) string {
	opName := "spanEncoder" + toVecMethod(family, width)
	if asc {
		opName += "Asc"
	} else {
		opName += "Desc"
	}
	return opName
}
