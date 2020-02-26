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
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func genHashTable(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/colexec/hashtable_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_PROBE_TYPE", "coltypes.{{$lTyp}}", -1)
	s = strings.Replace(s, "_BUILD_TYPE", "coltypes.{{$rTyp}}", -1)
	s = strings.Replace(s, "_ProbeType", "{{$lTyp}}", -1)
	s = strings.Replace(s, "_BuildType", "{{$rTyp}}", -1)

	assignNeRe := makeFunctionRegex("_ASSIGN_NE", 3)
	s = assignNeRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Assign", 3))

	checkCol := makeFunctionRegex("_CHECK_COL_WITH_NULLS", 7)
	s = checkCol.ReplaceAllString(s, `{{template "checkColWithNulls" buildDict "Global" . "UseSel" $7}}`)

	checkColBody := makeFunctionRegex("_CHECK_COL_BODY", 9)
	s = checkColBody.ReplaceAllString(
		s,
		`{{template "checkColBody" buildDict "Global" .Global "UseSel" .UseSel "ProbeHasNulls" $7 "BuildHasNulls" $8 "AllowNullEquality" $9}}`)

	checkBody := makeFunctionRegex("_CHECK_BODY", 1)
	s = checkBody.ReplaceAllString(s,
		`{{template "checkBody" buildDict "Global" . "IsHashTableInFullMode" $1}}`)

	s = replaceManipulationFuncs(".Global.LTyp", s)

	tmpl, err := template.New("hashtable").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	lTypToRTypToOverload := make(map[coltypes.T]map[coltypes.T]*overload)
	for _, ov := range anyTypeComparisonOpToOverloads[tree.NE] {
		lTyp := ov.LTyp
		rTyp := ov.RTyp
		rTypToOverload := lTypToRTypToOverload[lTyp]
		if rTypToOverload == nil {
			rTypToOverload = make(map[coltypes.T]*overload)
			lTypToRTypToOverload[lTyp] = rTypToOverload
		}
		rTypToOverload[rTyp] = ov
	}
	return tmpl.Execute(wr, lTypToRTypToOverload)
}

func init() {
	registerGenerator(genHashTable, "hashtable.eg.go")
}
