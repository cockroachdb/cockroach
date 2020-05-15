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
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type hashTableMode struct {
	IsDistinct bool

	String string
}

const hashTableTmpl = "pkg/sql/colexec/hashtable_tmpl.go"

func genHashTable(wr io.Writer, htm hashTableMode) error {
	t, err := ioutil.ReadFile(hashTableTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.ReplaceAll(s, "_LEFT_CANONICAL_TYPE_FAMILY", "{{.LeftCanonicalFamilyStr}}")
	s = strings.ReplaceAll(s, "_LEFT_TYPE_WIDTH", typeWidthReplacement)
	s = strings.ReplaceAll(s, "_RIGHT_CANONICAL_TYPE_FAMILY", "{{.RightCanonicalFamilyStr}}")
	s = strings.ReplaceAll(s, "_RIGHT_TYPE_WIDTH", typeWidthReplacement)
	s = strings.ReplaceAll(s, "_ProbeType", "{{.Left.VecMethod}}")
	s = strings.ReplaceAll(s, "_BuildType", "{{.Right.VecMethod}}")
	s = strings.ReplaceAll(s, "_PROBING_AGAINST_ITSELF", ".ProbingAgainstItself")

	s = strings.ReplaceAll(s, "_L_UNSAFEGET", "execgen.UNSAFEGET")
	s = replaceManipulationFuncsAmbiguous(".Global.Left", s)
	s = strings.ReplaceAll(s, "_R_UNSAFEGET", "execgen.UNSAFEGET")
	s = replaceManipulationFuncsAmbiguous(".Global.Right", s)

	assignNeRe := makeFunctionRegex("_ASSIGN_NE", 3)
	s = assignNeRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Right.Assign", 3))

	checkColBody := makeFunctionRegex("_CHECK_COL_BODY", 12)
	s = checkColBody.ReplaceAllString(
		s,
		`{{template "checkColBody" buildDict "Global" .Global "UseProbeSel" .UseProbeSel "ProbeHasNulls" $7 "BuildHasNulls" $8 "AllowNullEquality" $9 "SelectDistinct" $10 "ProbingAgainstItself" $12}}`)

	checkColWithNulls := makeFunctionRegex("_CHECK_COL_WITH_NULLS", 8)
	s = checkColWithNulls.ReplaceAllString(s,
		`{{template "checkColWithNulls" buildDict "Global" . "UseProbeSel" $7 "ProbingAgainstItself" $8}}`)

	checkColForDistinctWithNulls := makeFunctionRegex("_CHECK_COL_FOR_DISTINCT_WITH_NULLS", 6)
	s = checkColForDistinctWithNulls.ReplaceAllString(s,
		`{{template "checkColForDistinctWithNulls" buildDict "Global" . "UseProbeSel" $6}}`)

	checkBody := makeFunctionRegex("_CHECK_BODY", 3)
	s = checkBody.ReplaceAllString(s,
		`{{template "checkBody" buildDict "Global" . "SelectSameTuples" $3}}`)

	updateSelBody := makeFunctionRegex("_UPDATE_SEL_BODY", 4)
	s = updateSelBody.ReplaceAllString(s,
		`{{template "updateSelBody" buildDict "Global" . "UseSel" $4}}`)

	tmpl, err := template.New("hashtable").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	var data *twoArgsResolvedOverloadInfo
	for _, ov := range twoArgsResolvedOverloadsInfo.CmpOps {
		if ov.Name == execgen.ComparisonOpName[tree.NE] {
			data = ov
			break
		}
	}
	if data == nil {
		colexecerror.InternalError("unexpectedly didn't find overload for tree.NE")
	}
	return tmpl.Execute(wr, struct {
		Overloads     interface{}
		HashTableMode interface{}
	}{
		Overloads:     data,
		HashTableMode: htm,
	})
}

func init() {
	hashTableModes := []hashTableMode{
		{
			IsDistinct: false,
			String:     "full",
		},
		{
			IsDistinct: true,
			String:     "distinct",
		},
	}

	hashTableGenerator := func(htm hashTableMode) generator {
		return func(wr io.Writer) error {
			return genHashTable(wr, htm)
		}
	}

	for _, mode := range hashTableModes {
		registerGenerator(hashTableGenerator(mode), fmt.Sprintf("hashtable_%s.eg.go", mode.String), hashTableTmpl)
	}
}
