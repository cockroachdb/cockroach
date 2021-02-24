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
	"strings"
	"text/template"
)

type relativeRankTmplInfo struct {
	IsPercentRank bool
	IsCumeDist    bool
	HasPartition  bool
	String        string
}

const relativeRankTmpl = "pkg/sql/colexec/colexecwindow/relative_rank_tmpl.go"

func genRelativeRankOps(inputFileContents string, wr io.Writer) error {
	s := strings.ReplaceAll(inputFileContents, "_RELATIVE_RANK_STRING", "{{.String}}")

	computePartitionsSizesRe := makeFunctionRegex("_COMPUTE_PARTITIONS_SIZES", 1)
	s = computePartitionsSizesRe.ReplaceAllString(s, `{{template "computePartitionsSizes" buildDict "HasSel" $1}}`)
	computePeerGroupsSizesRe := makeFunctionRegex("_COMPUTE_PEER_GROUPS_SIZES", 1)
	s = computePeerGroupsSizesRe.ReplaceAllString(s, `{{template "computePeerGroupsSizes" buildDict "HasSel" $1}}`)

	// Now, generate the op, from the template.
	tmpl, err := template.New("relative_rank_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	relativeRankTmplInfos := []relativeRankTmplInfo{
		{IsPercentRank: true, HasPartition: false, String: "percentRankNoPartition"},
		{IsPercentRank: true, HasPartition: true, String: "percentRankWithPartition"},
		{IsCumeDist: true, HasPartition: false, String: "cumeDistNoPartition"},
		{IsCumeDist: true, HasPartition: true, String: "cumeDistWithPartition"},
	}
	return tmpl.Execute(wr, relativeRankTmplInfos)
}

func init() {
	registerGenerator(genRelativeRankOps, "relative_rank.eg.go", relativeRankTmpl)
}
