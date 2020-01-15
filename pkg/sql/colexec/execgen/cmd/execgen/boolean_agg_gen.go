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

	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
)

type booleanAggTmplInfo struct {
	IsAnd bool
}

func (b booleanAggTmplInfo) AssignBooleanOp(target, l, r string) string {
	switch b.IsAnd {
	case true:
		return fmt.Sprintf("%s = %s && %s", target, l, r)
	case false:
		return fmt.Sprintf("%s = %s || %s", target, l, r)
	default:
		execerror.VectorizedInternalPanic("unsupported boolean agg type")
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (b booleanAggTmplInfo) OpType() string {
	if b.IsAnd {
		return "And"
	} else {
		return "Or"
	}
}

func (b booleanAggTmplInfo) DefaultVal() string {
	if b.IsAnd {
		return "true"
	} else {
		return "false"
	}
}

func genBooleanAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/colexec/boolean_agg_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_OP_TYPE", "{{.OpType}}", -1)
	s = strings.Replace(s, "_DEFAULT_VAL", "{{.DefaultVal}}", -1)

	accumulateBoolean := makeFunctionRegex("_ACCUMULATE_BOOLEAN", 3)
	s = accumulateBoolean.ReplaceAllString(s, `{{template "accumulateBoolean" buildDict "Global" .}}`)

	assignBoolRe := regexp.MustCompile(`_ASSIGN_BOOLEAN_OP\((.*),(.*),(.*)\)`)
	s = assignBoolRe.ReplaceAllString(s, "{{.AssignBooleanOp $1 $2 $3}}")

	tmpl, err := template.New("boolean_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, []booleanAggTmplInfo{{IsAnd: true}, {IsAnd: false}})
}

func init() {
	registerGenerator(genBooleanAgg, "boolean_agg.eg.go")
}
