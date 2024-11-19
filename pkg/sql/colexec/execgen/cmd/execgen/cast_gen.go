// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"io"
	"strings"
	"text/template"
)

const castTmpl = "pkg/sql/colexec/colexecbase/cast_tmpl.go"

const castOpInvocation = `{{template "castOp" buildDict "Global" . "FromInfo" $fromInfo "FromFamily" $fromFamily "ToFamily" $toFamily}}`

func genCastOperators(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_TYPE_FAMILY", "{{.TypeFamily}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_TO_GO_TYPE", "{{.GoType}}",
		"_FROM_TYPE", "{{$fromInfo.VecMethod}}",
		"_TO_TYPE", "{{.VecMethod}}",
		"_NAME", "{{$fromInfo.TypeName}}{{.TypeName}}",
		"_GENERATE_CAST_OP", castOpInvocation,
	)
	s := r.Replace(inputFileContents)

	castRe := makeFunctionRegex("_CAST", 5)
	s = castRe.ReplaceAllString(s, makeTemplateFunctionCall("Cast", 5))

	tmpl, err := template.New("cast").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, getCastFromTmplInfos())
}

func init() {
	registerGenerator(genCastOperators, "cast.eg.go", castTmpl)
}
