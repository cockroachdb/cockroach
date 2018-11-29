// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"regexp"

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type avgAggTmplInfo struct {
	Type      types.T
	assignAdd func(string, string, string) string
}

func (a avgAggTmplInfo) AssignAdd(target, l, r string) string {
	return a.assignAdd(target, l, r)
}

func (a avgAggTmplInfo) AssignDivInt64(target, l, r string) string {
	switch a.Type {
	case types.Decimal:
		return fmt.Sprintf(
			`%s.SetInt64(%s)
if _, err := tree.DecimalCtx.Quo(&%s, &%s, &%s); err != nil {
			panic(err)
		}`,
			target, r, target, l, target,
		)
	case types.Float32:
		// TODO(asubiotto): Might not want to support this.
		return fmt.Sprintf("%s = %s / float32(%s)", target, l, r)
	case types.Float64:
		return fmt.Sprintf("%s = %s / float64(%s)", target, l, r)
	default:
		panic("unsupported avg agg type")
	}
}

// Avoid unused warnings. These methods are used in the template.
var (
	_ = avgAggTmplInfo{}.AssignAdd
	_ = avgAggTmplInfo{}.AssignDivInt64
)

func genAvgAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/exec/avg_agg_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_GOTYPE", "{{.Type.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "types.{{.Type}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.Type}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.Type}}", -1)

	assignDivRe := regexp.MustCompile(`_ASSIGN_DIV_INT64\((.*),(.*),(.*)\)`)
	s = assignDivRe.ReplaceAllString(s, "{{.AssignDivInt64 $1 $2 $3}}")
	assignAddRe := regexp.MustCompile(`_ASSIGN_ADD\((.*),(.*),(.*)\)`)
	s = assignAddRe.ReplaceAllString(s, "{{.AssignAdd $1 $2 $3}}")

	tmpl, err := template.New("avg_agg").Parse(s)
	if err != nil {
		return err
	}

	// TODO(asubiotto): Support more types.
	supportedTypes := []types.T{types.Decimal, types.Float32, types.Float64}
	spm := make(map[types.T]int)
	for i, typ := range supportedTypes {
		spm[typ] = i
	}
	tmplInfos := make([]avgAggTmplInfo, len(supportedTypes))
	for _, o := range binaryOpToOverloads[tree.Plus] {
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
