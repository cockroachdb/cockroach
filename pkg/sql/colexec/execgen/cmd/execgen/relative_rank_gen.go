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
)

type relativeRankTmplInfo struct {
	IsPercentRank bool
	IsCumeDist    bool
	HasPartition  bool
	String        string
}

func genRelativeRankOps(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/colexec/relative_rank_tmpl.go")
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.Replace(s, "_RELATIVE_RANK_STRING", "{{.String}}", -1)

	computeNumPeersRe := makeFunctionRegex("_COMPUTE_NUM_PEERS", 0)
	s = computeNumPeersRe.ReplaceAllString(s, `{{template "computeNumPeers"}}`)
	computeCumeDistRe := makeFunctionRegex("_COMPUTE_CUME_DIST", 0)
	s = computeCumeDistRe.ReplaceAllString(s, `{{template "computeCumeDist"}}`)

	// Now, generate the op, from the template.
	tmpl, err := template.New("relative_rank_op").Parse(s)
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
	registerGenerator(genRelativeRankOps, "relative_rank.eg.go")
}
