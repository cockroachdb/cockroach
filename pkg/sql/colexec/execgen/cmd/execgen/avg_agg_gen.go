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
	"io/ioutil"
	"regexp"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type avgAggTmplInfo struct {
	Type      coltypes.T
	assignAdd func(string, string, string) string
}

func (a avgAggTmplInfo) AssignAdd(target, l, r string) string {
	return a.assignAdd(target, l, r)
}

func (a avgAggTmplInfo) AssignDivInt64(target, l, r string) string {
	switch a.Type {
	case coltypes.Decimal:
		return fmt.Sprintf(
			`%s.SetInt64(%s)
if _, err := tree.DecimalCtx.Quo(&%s, &%s, &%s); err != nil {
			execerror.VectorizedInternalPanic(err)
		}`,
			target, r, target, l, target,
		)
	case coltypes.Float64:
		return fmt.Sprintf("%s = %s / float64(%s)", target, l, r)
	default:
		execerror.VectorizedInternalPanic("unsupported avg agg type")
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

// Avoid unused warnings. These methods are used in the template.
var (
	_ = avgAggTmplInfo{}.AssignAdd
	_ = avgAggTmplInfo{}.AssignDivInt64
)

func genAvgAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/colexec/avg_agg_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_GOTYPE", "{{.Type.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.Type}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.Type}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.Type}}", -1)

	assignDivRe := regexp.MustCompile(`_ASSIGN_DIV_INT64\((.*),(.*),(.*)\)`)
	s = assignDivRe.ReplaceAllString(s, "{{.AssignDivInt64 $1 $2 $3}}")
	assignAddRe := regexp.MustCompile(`_ASSIGN_ADD\((.*),(.*),(.*)\)`)
	s = assignAddRe.ReplaceAllString(s, "{{.Global.AssignAdd $1 $2 $3}}")

	accumulateAvg := makeFunctionRegex("_ACCUMULATE_AVG", 4)
	s = accumulateAvg.ReplaceAllString(s, `{{template "accumulateAvg" buildDict "Global" . "HasNulls" $4}}`)

	tmpl, err := template.New("avg_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	// TODO(asubiotto): Support more coltypes.
	supportedTypes := []coltypes.T{coltypes.Decimal, coltypes.Float64}
	spm := make(map[coltypes.T]int)
	for i, typ := range supportedTypes {
		spm[typ] = i
	}
	tmplInfos := make([]avgAggTmplInfo, len(supportedTypes))
	for _, o := range sameTypeBinaryOpToOverloads[tree.Plus] {
		i, ok := spm[o.LTyp]
		if !ok {
			continue
		}
		tmplInfos[i].Type = o.LTyp
		tmplInfos[i].assignAdd = o.Assign
	}

	return tmpl.Execute(wr, tmplInfos)
}

func init() {
	registerGenerator(genAvgAgg, "avg_agg.eg.go")
}
