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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type xorAggTmplInfo struct {
	aggTmplInfoBase
	InputVecMethod string
	RetGoType      string
	RetGoTypeSlice string
	RetVecMethod   string

	xorOverload assignFunc
}

func (x xorAggTmplInfo) AssignXor(
	targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string,
) string {
	// Note that we need to create lastArgWidthOverload only in order to tell
	// the resolved overload to use Bitxor overload in particular, so all other
	// fields remain unset.
	lawo := &lastArgWidthOverload{lastArgTypeOverload: &lastArgTypeOverload{
		overloadBase: newBinaryOverloadBase(treebin.Bitxor),
	}}
	return x.xorOverload(lawo, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol)
}

var _ = xorAggTmplInfo{}.AssignXor

// xorAggTypeTmplInfo is similar to lastArgTypeOverload and provides a way to
// see the type family of the overload. This is the top level of data passed to
// the template.
type xorAggTypeTmplInfo struct {
	TypeFamily     string
	WidthOverloads []xorAggWidthTmplInfo
}

// xorAggWidthTmplInfo is similar to lastArgWidthOverload and provides a way to
// see the width of the type of the overload. This is the middle level of data
// passed to the template.
type xorAggWidthTmplInfo struct {
	Width int32
	// Overload field contains all the necessary information for the template.
	// It should be accessed via {{with .Overload}} template instruction so that
	// the template has all of its info in scope.
	Overload xorAggTmplInfo
}

// getXorOverload returns the resolved overload that can be used to
// compute XOR of values of inputType type.
func getXorOverload(inputTypeFamily types.Family) assignFunc {
	if inputTypeFamily == types.BytesFamily {
		// For bytes, we need to implement custom byte-by-byte XOR.
		return func(
			_ *lastArgWidthOverload,
			targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string,
		) string {
			return fmt.Sprintf(`
			{
				if a.numNonNull == 0 {
					%[2]s = make([]byte, len(%[3]s))
					copy(%[2]s, %[3]s)
				} else {
					if len(%[2]s) != len(%[3]s) {
						colexecerror.ExpectedError(pgerror.Newf(pgcode.InvalidParameterValue,
							"arguments to xor must all be the same length %%d vs %%d", len(%[2]s), len(%[3]s),
						))
					}
					for j := range %[2]s {
						%[1]s[j] = %[2]s[j] ^ %[3]s[j]
					}
				}
			}`, targetElem, leftElem, rightElem)
		}
	}
	// For int types, use the standard Bitxor overload.
	for _, o := range sameTypeBinaryOpToOverloads[treebin.Bitxor] {
		if o.CanonicalTypeFamily == inputTypeFamily {
			for _, wo := range o.WidthOverloads {
				// We'll upcast each int to int64 and use that going forward, so
				// we pick the overload corresponding to anyWidth.
				if wo.Width == anyWidth {
					return wo.AssignFunc
				}
			}
		}
	}
	colexecerror.InternalError(errors.AssertionFailedf("unexpectedly didn't find bitxor binary overload for %s for anyWidth", inputTypeFamily))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

const xorAggTmpl = "pkg/sql/colexec/colexecagg/xor_agg_tmpl.go"

func genXorAgg(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.TypeFamily}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_RET_GOTYPESLICE", `{{.RetGoTypeSlice}}`,
		"_RET_GOTYPE", `{{.RetGoType}}`,
		"_RET_TYPE", "{{.RetVecMethod}}",
		"_TYPE", "{{.InputVecMethod}}",
		"TemplateType", "{{.InputVecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignXorRe := makeFunctionRegex("_ASSIGN_XOR", 6)
	s = assignXorRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.AssignXor", 6))

	accumulateXor := makeFunctionRegex("_ACCUMULATE_XOR", 5)
	s = accumulateXor.ReplaceAllString(s, `{{template "accumulateXor" buildDict "Global" . "HasNulls" $4 "HasSel" $5}}`)

	removeRow := makeFunctionRegex("_REMOVE_ROW", 4)
	s = removeRow.ReplaceAllString(s, `{{template "removeRow" buildDict "Global" . "HasNulls" $4}}`)

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("xor_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	supportedTypeFamilies := []types.Family{types.IntFamily, types.BytesFamily}

	var tmplInfos []xorAggTypeTmplInfo
	for _, inputTypeFamily := range supportedTypeFamilies {
		tmplInfo := xorAggTypeTmplInfo{
			TypeFamily: familyToString(inputTypeFamily),
		}
		for _, inputTypeWidth := range supportedWidthsByCanonicalTypeFamily[inputTypeFamily] {
			tmplInfo.WidthOverloads = append(tmplInfo.WidthOverloads, xorAggWidthTmplInfo{
				Width: inputTypeWidth,
				Overload: xorAggTmplInfo{
					aggTmplInfoBase: aggTmplInfoBase{
						canonicalTypeFamily: typeconv.TypeFamilyToCanonicalTypeFamily(inputTypeFamily),
					},
					InputVecMethod: toVecMethod(inputTypeFamily, inputTypeWidth),
					RetGoType:      toPhysicalRepresentation(inputTypeFamily, anyWidth),
					RetGoTypeSlice: goTypeSliceName(inputTypeFamily, anyWidth),
					RetVecMethod:   toVecMethod(inputTypeFamily, anyWidth),
					xorOverload:    getXorOverload(inputTypeFamily),
				}})
		}
		tmplInfos = append(tmplInfos, tmplInfo)
	}
	return tmpl.Execute(wr, struct {
		Infos []xorAggTypeTmplInfo
	}{
		Infos: tmplInfos,
	})
}

func init() {
	xorAggGenerator := func(inputFileContents string, wr io.Writer) error {
		return genXorAgg(inputFileContents, wr)
	}
	registerAggGenerator(
		xorAggGenerator, "xor_agg.eg.go", /* filenameSuffix */
		xorAggTmpl, "xor" /* aggName */, true, /* genWindowVariant */
	)
}
