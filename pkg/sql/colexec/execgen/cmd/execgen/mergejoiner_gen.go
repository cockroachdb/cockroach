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
	IsRightSemi  bool
	IsLeftAnti   bool
	IsRightAnti  bool
	IsSetOp      bool

	String string
}

const mergeJoinerTmpl = "pkg/sql/colexec/colexecjoin/mergejoiner_tmpl.go"

func genMergeJoinOps(inputFileContents string, wr io.Writer, jti joinTypeInfo) error {
	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_GOTYPESLICE", "{{.GoTypeSliceName}}",
		"_GOTYPE", "{{.GoType}}",
		"TemplateType", "{{.VecMethod}}",
		"_L_SEL_IND", "{{$sel.LSelString}}",
		"_R_SEL_IND", "{{$sel.RSelString}}",
		"_IS_L_SEL", "{{$sel.IsLSel}}",
		"_IS_R_SEL", "{{$sel.IsRSel}}",
		"_SEL_ARG", "$sel",
		"_JOIN_TYPE_STRING", "{{$.JoinType.String}}",
		"_JOIN_TYPE", "$.JoinType",
		"_HAS_SELECTION", "$.HasSelection",
		"_SEL_PERMUTATION", "$.SelPermutation",
	)
	s := r.Replace(inputFileContents)

	leftUnmatchedGroupSwitch := makeFunctionRegex("_LEFT_UNMATCHED_GROUP_SWITCH", 1)
	s = leftUnmatchedGroupSwitch.ReplaceAllString(s, `{{template "leftUnmatchedGroupSwitch" buildDict "Global" $ "JoinType" $1}}`)

	rightUnmatchedGroupSwitch := makeFunctionRegex("_RIGHT_UNMATCHED_GROUP_SWITCH", 1)
	s = rightUnmatchedGroupSwitch.ReplaceAllString(s, `{{template "rightUnmatchedGroupSwitch" buildDict "Global" $ "JoinType" $1}}`)

	nullFromLeftSwitch := makeFunctionRegex("_NULL_FROM_LEFT_SWITCH", 1)
	s = nullFromLeftSwitch.ReplaceAllString(s, `{{template "nullFromLeftSwitch" buildDict "Global" $ "JoinType" $1}}`)

	nullFromRightSwitch := makeFunctionRegex("_NULL_FROM_RIGHT_SWITCH", 1)
	s = nullFromRightSwitch.ReplaceAllString(s, `{{template "nullFromRightSwitch" buildDict "Global" $ "JoinType" $1}}`)

	incrementLeftSwitch := makeFunctionRegex("_INCREMENT_LEFT_SWITCH", 2)
	s = incrementLeftSwitch.ReplaceAllString(s, `{{template "incrementLeftSwitch" buildDict "Global" . "JoinType" $1 "SelPermutation" $2}}`)

	incrementRightSwitch := makeFunctionRegex("_INCREMENT_RIGHT_SWITCH", 2)
	s = incrementRightSwitch.ReplaceAllString(s, `{{template "incrementRightSwitch" buildDict "Global" . "JoinType" $1 "SelPermutation" $2}}`)

	processNotLastGroupInColumnSwitch := makeFunctionRegex("_PROCESS_NOT_LAST_GROUP_IN_COLUMN_SWITCH", 1)
	s = processNotLastGroupInColumnSwitch.ReplaceAllString(s, `{{template "processNotLastGroupInColumnSwitch" buildDict "Global" $ "JoinType" $1}}`)

	probeSwitch := makeFunctionRegex("_PROBE_SWITCH", 2)
	s = probeSwitch.ReplaceAllString(s, `{{template "probeSwitch" buildDict "Global" $ "JoinType" $1 "SelPermutation" $2}}`)

	sourceFinishedSwitch := makeFunctionRegex("_SOURCE_FINISHED_SWITCH", 1)
	s = sourceFinishedSwitch.ReplaceAllString(s, `{{template "sourceFinishedSwitch" buildDict "Global" $ "JoinType" $1}}`)

	leftSwitch := makeFunctionRegex("_LEFT_SWITCH", 2)
	s = leftSwitch.ReplaceAllString(s, `{{template "leftSwitch" buildDict "Global" $ "JoinType" $1 "HasSelection" $2}}`)

	rightSwitch := makeFunctionRegex("_RIGHT_SWITCH", 2)
	s = rightSwitch.ReplaceAllString(s, `{{template "rightSwitch" buildDict "Global" $ "JoinType" $1 "HasSelection" $2}}`)

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
			IsRightSemi: true,
			String:      "RightSemi",
		},
		{
			IsLeftAnti: true,
			String:     "LeftAnti",
		},
		{
			IsRightAnti: true,
			String:      "RightAnti",
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
		return func(inputFileContents string, wr io.Writer) error {
			return genMergeJoinOps(inputFileContents, wr, jti)
		}
	}

	for _, join := range joinTypeInfos {
		registerGenerator(mergeJoinGenerator(join), fmt.Sprintf("mergejoiner_%s.eg.go", strings.ToLower(join.String)), mergeJoinerTmpl)
	}
}
