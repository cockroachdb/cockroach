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

type percentRankTmplInfo struct {
	HasPartition bool
	String       string
}

func genPercentRankOps(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/colexec/percent_rank_tmpl.go")
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.Replace(s, "_STRING", "{{.String}}", -1)

	// Now, generate the op, from the template.
	tmpl, err := template.New("percent_rank_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	percentRankTmplInfos := []percentRankTmplInfo{
		{HasPartition: false, String: "NoPartition"},
		{HasPartition: true, String: "WithPartition"},
	}
	return tmpl.Execute(wr, percentRankTmplInfos)
}

func init() {
	registerGenerator(genPercentRankOps, "percent_rank.eg.go")
}
