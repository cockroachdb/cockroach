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

	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/errors"
)

// hashTableMode describes the mode in which the hash table can operate.
type hashTableMode int

const (
	// hashTableFullBuildDefaultProbe is the hashTableMode in which the hash
	// table is built fully (i.e. not distinct) and is being probed by
	// default (i.e. not deleting). This mode is used by the hash joiner
	// for non-set operation joins.
	hashTableFullBuildDefaultProbe hashTableMode = iota
	// hashTableDistinctBuildDefaultProbe is the hashTableMode in which the
	// hash table is built distinctly (i.e. duplicates are not added into it)
	// and is being probed by default (i.e. not deleting). This mode is used by
	// the unordered distinct.
	hashTableDistinctBuildDefaultProbe
	// hashTableFullBuildDeletingProbe is the hashTableMode in which the hash
	// table is built fully (i.e. not distinct) and is being probed in deleting
	// mode (i.e. tuples are deleted from the hash table once they are matched
	// with a probe tuple). This mode is used by the hash joiner for set
	// operation joins.
	hashTableFullBuildDeletingProbe
)

func (m hashTableMode) String() string {
	switch m {
	case hashTableFullBuildDefaultProbe:
		return "full_default"
	case hashTableDistinctBuildDefaultProbe:
		return "distinct"
	case hashTableFullBuildDeletingProbe:
		return "full_deleting"
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unexpected hashTableMode"))
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (m hashTableMode) IsDistinctBuild() bool {
	return m == hashTableDistinctBuildDefaultProbe
}

func (m hashTableMode) IsDeletingProbe() bool {
	return m == hashTableFullBuildDeletingProbe
}

// Remove unused warnings.
var _ = hashTableMode.IsDistinctBuild
var _ = hashTableMode.IsDeletingProbe

const hashTableTmpl = "pkg/sql/colexec/colexechash/hashtable_tmpl.go"

func genHashTable(inputFileContents string, wr io.Writer, htm hashTableMode) error {
	r := strings.NewReplacer(
		"_LEFT_CANONICAL_TYPE_FAMILY", "{{.LeftCanonicalFamilyStr}}",
		"_LEFT_TYPE_WIDTH", typeWidthReplacement,
		"_RIGHT_CANONICAL_TYPE_FAMILY", "{{.RightCanonicalFamilyStr}}",
		"_RIGHT_TYPE_WIDTH", typeWidthReplacement,
		"_ProbeType", "{{.Left.VecMethod}}",
		"_BuildType", "{{.Right.VecMethod}}",
		"_GLOBAL", "$global",
		"_SELECT_DISTINCT", "$selectDistinct",
		"_USE_PROBE_SEL", ".UseProbeSel",
		"_PROBING_AGAINST_ITSELF", "$probingAgainstItself",
		"_DELETING_PROBE_MODE", "$deletingProbeMode",
		"_OVERLOADS", ".Overloads",
	)
	s := r.Replace(inputFileContents)

	assignNeRe := makeFunctionRegex("_ASSIGN_NE", 6)
	s = assignNeRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Right.Assign", 6))

	checkColBody := makeFunctionRegex("_CHECK_COL_BODY", 7)
	s = checkColBody.ReplaceAllString(s,
		`{{template "checkColBody" buildDict "Global" $1 "ProbeHasNulls" $2 "BuildHasNulls" $3 "SelectDistinct" $4 "UseProbeSel" $5 "ProbingAgainstItself" $6 "DeletingProbeMode" $7}}`,
	)

	checkColWithNulls := makeFunctionRegex("_CHECK_COL_WITH_NULLS", 4)
	s = checkColWithNulls.ReplaceAllString(s,
		`{{template "checkColWithNulls" buildDict "Global" . "SelectDistinct" $1 "UseProbeSel" $2 "ProbingAgainstItself" $3 "DeletingProbeMode" $4}}`,
	)

	checkColFunctionTemplate := makeFunctionRegex("_CHECK_COL_FUNCTION_TEMPLATE", 3)
	s = checkColFunctionTemplate.ReplaceAllString(s,
		`{{template "checkColFunctionTemplate" buildDict "Global" . "SelectDistinct" $1 "ProbingAgainstItself" $2 "DeletingProbeMode" $3}}`,
	)

	checkBody := makeFunctionRegex("_CHECK_BODY", 3)
	s = checkBody.ReplaceAllString(s,
		`{{template "checkBody" buildDict "Global" . "SelectSameTuples" $1 "DeletingProbeMode" $2 "SelectDistinct" $3}}`,
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
		if ov.Name == execgen.ComparisonOpName[treecmp.NE] {
			data = ov
			break
		}
	}
	if data == nil {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly didn't find overload for treecmp.NE"))
	}
	return tmpl.Execute(wr, struct {
		Overloads     interface{}
		HashTableMode hashTableMode
	}{
		Overloads:     data,
		HashTableMode: htm,
	})
}

func init() {
	hashTableGenerator := func(htm hashTableMode) generator {
		return func(inputFileContents string, wr io.Writer) error {
			return genHashTable(inputFileContents, wr, htm)
		}
	}

	for _, mode := range []hashTableMode{
		hashTableFullBuildDefaultProbe,
		hashTableDistinctBuildDefaultProbe,
		hashTableFullBuildDeletingProbe,
	} {
		registerGenerator(hashTableGenerator(mode), fmt.Sprintf("hashtable_%s.eg.go", mode), hashTableTmpl)
	}
}
