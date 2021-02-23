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

	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type booleanAggTmplInfo struct {
	aggTmplInfoBase
	IsAnd bool
}

func (b booleanAggTmplInfo) AssignBoolOp(target, l, r string) string {
	switch b.IsAnd {
	case true:
		return fmt.Sprintf("%s = %s && %s", target, l, r)
	case false:
		return fmt.Sprintf("%s = %s || %s", target, l, r)
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unsupported boolean agg type"))
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (b booleanAggTmplInfo) OpType() string {
	if b.IsAnd {
		return "And"
	}
	return "Or"
}

func (b booleanAggTmplInfo) DefaultVal() string {
	if b.IsAnd {
		return "true"
	}
	return "false"
}

// Avoid unused warnings. These methods are used in the template.
var (
	_ = booleanAggTmplInfo{}.AssignBoolOp
	_ = booleanAggTmplInfo{}.OpType
	_ = booleanAggTmplInfo{}.DefaultVal
)

const boolAggTmpl = "pkg/sql/colexec/colexecagg/bool_and_or_agg_tmpl.go"

func genBooleanAgg(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_OP_TYPE", "{{.OpType}}",
		"_DEFAULT_VAL", "{{.DefaultVal}}",
	)
	s := r.Replace(inputFileContents)

	accumulateBoolean := makeFunctionRegex("_ACCUMULATE_BOOLEAN", 5)
	s = accumulateBoolean.ReplaceAllString(s, `{{template "accumulateBoolean" buildDict "Global" . "HasNulls" $4 "HasSel" $5}}`)

	assignBoolRe := makeFunctionRegex("_ASSIGN_BOOL_OP", 3)
	s = assignBoolRe.ReplaceAllString(s, makeTemplateFunctionCall(`AssignBoolOp`, 3))

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("bool_and_or_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, []booleanAggTmplInfo{
		{
			aggTmplInfoBase: aggTmplInfoBase{canonicalTypeFamily: types.BoolFamily},
			IsAnd:           true,
		},
		{
			aggTmplInfoBase: aggTmplInfoBase{canonicalTypeFamily: types.BoolFamily},
			IsAnd:           false,
		},
	})
}

func init() {
	registerAggGenerator(genBooleanAgg, "bool_and_or_agg.eg.go", boolAggTmpl)
}
