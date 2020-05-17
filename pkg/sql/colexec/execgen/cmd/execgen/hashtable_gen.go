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

type hashTableInfo struct {
	IsDistinct bool
	IsDeleting bool

	String string
}

const hashTableTmpl = "pkg/sql/colexec/hashtable_tmpl.go"

func genHashTable(wr io.Writer, hti hashTableInfo) error {
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

	s = strings.ReplaceAll(s, "_USE_PROBE_SEL", ".UseProbeSel")
	s = strings.ReplaceAll(s, "_PROBING_AGAINST_ITSELF", "$probingAgainstItself")
	s = strings.ReplaceAll(s, "_DELETING_PROBE_MODE", "$deletingProbeMode")
	s = strings.ReplaceAll(s, "_OVERLOADS", ".Overloads")

	s = strings.ReplaceAll(s, "_L_UNSAFEGET", "execgen.UNSAFEGET")
	s = replaceManipulationFuncsAmbiguous(".Global.Left", s)
	s = strings.ReplaceAll(s, "_R_UNSAFEGET", "execgen.UNSAFEGET")
	s = replaceManipulationFuncsAmbiguous(".Global.Right", s)

	assignNeRe := makeFunctionRegex("_ASSIGN_NE", 6)
	s = assignNeRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Right.Assign", 6))

	checkColBody := makeFunctionRegex("_CHECK_COL_BODY", 7)
	s = checkColBody.ReplaceAllString(s,
		`{{template "checkColBody" buildDict "Global" .Global "ProbeHasNulls" $1 "BuildHasNulls" $2 "AllowNullEquality" $3 "SelectDistinct" $4 "UseProbeSel" $5 "ProbingAgainstItself" $6 "DeletingProbeMode" $7}}`,
	)

	checkColWithNulls := makeFunctionRegex("_CHECK_COL_WITH_NULLS", 3)
	s = checkColWithNulls.ReplaceAllString(s,
		`{{template "checkColWithNulls" buildDict "Global" . "UseProbeSel" $1 "ProbingAgainstItself" $2 "DeletingProbeMode" $3}}`,
	)

	checkColFunctionTemplate := makeFunctionRegex("_CHECK_COL_FUNCTION_TEMPLATE", 2)
	s = checkColFunctionTemplate.ReplaceAllString(s,
		`{{template "checkColFunctionTemplate" buildDict "Global" . "ProbingAgainstItself" $1 "DeletingProbeMode" $2}}`,
	)

	checkColForDistinctWithNulls := makeFunctionRegex("_CHECK_COL_FOR_DISTINCT_WITH_NULLS", 1)
	s = checkColForDistinctWithNulls.ReplaceAllString(s,
		`{{template "checkColForDistinctWithNulls" buildDict "Global" . "UseProbeSel" $1}}`,
	)

	checkBody := makeFunctionRegex("_CHECK_BODY", 2)
	s = checkBody.ReplaceAllString(s,
		`{{template "checkBody" buildDict "Global" . "SelectSameTuples" $1 "DeletingProbeMode" $2}}`,
	)

	updateSelBody := makeFunctionRegex("_UPDATE_SEL_BODY", 1)
	s = updateSelBody.ReplaceAllString(s,
		`{{template "updateSelBody" buildDict "Global" . "UseSel" $1}}`,
	)

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
		HashTableMode: hti,
	})
}

func init() {
	hashTableInfos := []hashTableInfo{
		{
			IsDistinct: false,
			IsDeleting: false,
			String:     "full_default",
		},
		{
			IsDistinct: true,
			IsDeleting: false,
			String:     "distinct",
		},
		{
			IsDistinct: false,
			IsDeleting: true,
			String:     "full_deleting",
		},
	}

	hashTableGenerator := func(htm hashTableInfo) generator {
		return func(wr io.Writer) error {
			return genHashTable(wr, htm)
		}
	}

	for _, mode := range hashTableInfos {
		registerGenerator(hashTableGenerator(mode), fmt.Sprintf("hashtable_%s.eg.go", mode.String), hashTableTmpl)
	}
}
