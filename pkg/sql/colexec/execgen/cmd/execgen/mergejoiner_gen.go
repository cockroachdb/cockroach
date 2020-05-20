// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// selPermutation contains information about which permutation of selection
// vector state the template is materializing.
type selPermutation struct {
	IsLSel bool
	IsRSel bool

	LSelString string
	RSelString string
}

type joinTypeInfo struct {
	IsInner      bool
	IsLeftOuter  bool
	IsRightOuter bool
	IsLeftSemi   bool
	IsLeftAnti   bool
	IsSetOp      bool

	String string
}

const mergeJoinerTmpl = "pkg/sql/colexec/mergejoiner_tmpl.go"

func genMergeJoinOps(wr io.Writer, jti joinTypeInfo) error {
	d, err := ioutil.ReadFile(mergeJoinerTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.ReplaceAll(s, "_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}")
	s = strings.ReplaceAll(s, "_TYPE_WIDTH", typeWidthReplacement)
	s = strings.ReplaceAll(s, "_GOTYPESLICE", "{{.GoTypeSliceName}}")
	s = strings.ReplaceAll(s, "_GOTYPE", "{{.GoType}}")
	s = strings.ReplaceAll(s, "TemplateType", "{{.VecMethod}}")

	s = strings.ReplaceAll(s, "_L_SEL_IND", "{{$sel.LSelString}}")
	s = strings.ReplaceAll(s, "_R_SEL_IND", "{{$sel.RSelString}}")
	s = strings.ReplaceAll(s, "_IS_L_SEL", "{{$sel.IsLSel}}")
	s = strings.ReplaceAll(s, "_IS_R_SEL", "{{$sel.IsRSel}}")
	s = strings.ReplaceAll(s, "_SEL_ARG", "$sel")
	s = strings.ReplaceAll(s, "_JOIN_TYPE_STRING", "{{$.JoinType.String}}")
	s = strings.ReplaceAll(s, "_JOIN_TYPE", "$.JoinType")
	s = strings.ReplaceAll(s, "_L_HAS_NULLS", "$.lHasNulls")
	s = strings.ReplaceAll(s, "_R_HAS_NULLS", "$.rHasNulls")
	s = strings.ReplaceAll(s, "_HAS_NULLS", "$.HasNulls")
	s = strings.ReplaceAll(s, "_HAS_SELECTION", "$.HasSelection")
	s = strings.ReplaceAll(s, "_SEL_PERMUTATION", "$.SelPermutation")

	leftUnmatchedGroupSwitch := makeFunctionRegex("_LEFT_UNMATCHED_GROUP_SWITCH", 1)
	s = leftUnmatchedGroupSwitch.ReplaceAllString(s, `{{template "leftUnmatchedGroupSwitch" buildDict "Global" $ "JoinType" $1}}`)

	rightUnmatchedGroupSwitch := makeFunctionRegex("_RIGHT_UNMATCHED_GROUP_SWITCH", 1)
	s = rightUnmatchedGroupSwitch.ReplaceAllString(s, `{{template "rightUnmatchedGroupSwitch" buildDict "Global" $ "JoinType" $1}}`)

	nullFromLeftSwitch := makeFunctionRegex("_NULL_FROM_LEFT_SWITCH", 1)
	s = nullFromLeftSwitch.ReplaceAllString(s, `{{template "nullFromLeftSwitch" buildDict "Global" $ "JoinType" $1}}`)

	nullFromRightSwitch := makeFunctionRegex("_NULL_FROM_RIGHT_SWITCH", 1)
	s = nullFromRightSwitch.ReplaceAllString(s, `{{template "nullFromRightSwitch" buildDict "Global" $ "JoinType" $1}}`)

	incrementLeftSwitch := makeFunctionRegex("_INCREMENT_LEFT_SWITCH", 3)
	s = incrementLeftSwitch.ReplaceAllString(s, `{{template "incrementLeftSwitch" buildDict "Global" . "JoinType" $1 "SelPermutation" $2 "lHasNulls" $3}}`)

	incrementRightSwitch := makeFunctionRegex("_INCREMENT_RIGHT_SWITCH", 3)
	s = incrementRightSwitch.ReplaceAllString(s, `{{template "incrementRightSwitch" buildDict "Global" . "JoinType" $1 "SelPermutation" $2 "rHasNulls" $3}}`)

	processNotLastGroupInColumnSwitch := makeFunctionRegex("_PROCESS_NOT_LAST_GROUP_IN_COLUMN_SWITCH", 1)
	s = processNotLastGroupInColumnSwitch.ReplaceAllString(s, `{{template "processNotLastGroupInColumnSwitch" buildDict "Global" $ "JoinType" $1}}`)

	probeSwitch := makeFunctionRegex("_PROBE_SWITCH", 4)
	s = probeSwitch.ReplaceAllString(s, `{{template "probeSwitch" buildDict "Global" $ "JoinType" $1 "SelPermutation" $2 "lHasNulls" $3 "rHasNulls" $4}}`)

	sourceFinishedSwitch := makeFunctionRegex("_SOURCE_FINISHED_SWITCH", 1)
	s = sourceFinishedSwitch.ReplaceAllString(s, `{{template "sourceFinishedSwitch" buildDict "Global" $ "JoinType" $1}}`)

	leftSwitch := makeFunctionRegex("_LEFT_SWITCH", 3)
	s = leftSwitch.ReplaceAllString(s, `{{template "leftSwitch" buildDict "Global" $ "JoinType" $1 "HasSelection" $2 "HasNulls" $3}}`)

	rightSwitch := makeFunctionRegex("_RIGHT_SWITCH", 3)
	s = rightSwitch.ReplaceAllString(s, `{{template "rightSwitch" buildDict "Global" $ "JoinType" $1 "HasSelection" $2  "HasNulls" $3}}`)

	assignEqRe := makeFunctionRegex("_ASSIGN_EQ", 6)
	s = assignEqRe.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 6))

	assignLtRe := makeFunctionRegex("_ASSIGN_CMP", 5)
	s = assignLtRe.ReplaceAllString(s, makeTemplateFunctionCall("Compare", 5))

	s = replaceManipulationFuncs(s)

	// Now, generate the op, from the template.
	tmpl, err := template.New("mergejoin_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	// Create each permutation of selection vector state.
	selPermutations := []selPermutation{
		{
			IsLSel:     true,
			IsRSel:     true,
			LSelString: "lSel[curLIdx]",
			RSelString: "rSel[curRIdx]",
		},
		{
			IsLSel:     true,
			IsRSel:     false,
			LSelString: "lSel[curLIdx]",
			RSelString: "curRIdx",
		},
		{
			IsLSel:     false,
			IsRSel:     true,
			LSelString: "curLIdx",
			RSelString: "rSel[curRIdx]",
		},
		{
			IsLSel:     false,
			IsRSel:     false,
			LSelString: "curLIdx",
			RSelString: "curRIdx",
		},
	}

	return tmpl.Execute(wr, struct {
		Overloads       interface{}
		SelPermutations interface{}
		JoinType        interface{}
	}{
		Overloads:       sameTypeComparisonOpToOverloads[tree.EQ],
		SelPermutations: selPermutations,
		JoinType:        jti,
	})
}

func init() {
	joinTypeInfos := []joinTypeInfo{
		{
			IsInner: true,
			String:  "Inner",
		},
		{
			IsLeftOuter: true,
			String:      "LeftOuter",
		},
		{
			IsRightOuter: true,
			String:       "RightOuter",
		},
		{
			IsLeftOuter:  true,
			IsRightOuter: true,
			String:       "FullOuter",
		},
		{
			IsLeftSemi: true,
			String:     "LeftSemi",
		},
		{
			IsLeftAnti: true,
			String:     "LeftAnti",
		},
		{
			IsLeftSemi: true,
			IsSetOp:    true,
			String:     "IntersectAll",
		},
		{
			IsLeftAnti: true,
			IsSetOp:    true,
			String:     "ExceptAll",
		},
	}

	mergeJoinGenerator := func(jti joinTypeInfo) generator {
		return func(wr io.Writer) error {
			return genMergeJoinOps(wr, jti)
		}
	}

	for _, join := range joinTypeInfos {
		registerGenerator(mergeJoinGenerator(join), fmt.Sprintf("mergejoiner_%s.eg.go", strings.ToLower(join.String)), mergeJoinerTmpl)
	}
}
