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
	"regexp"
	"strings"
	"text/template"
)

type rankTmplInfo struct {
	Dense        bool
	HasPartition bool
	String       string
}

// UpdateRank is used to encompass the different logic between DENSE_RANK and
// RANK window functions when updating the internal state of rank operators. It
// is used by the template.
func (r rankTmplInfo) UpdateRank() string {
	if r.Dense {
		return fmt.Sprintf(
			`r.rank++`,
		)
	}
	return fmt.Sprintf(
		`r.rank += r.rankIncrement
r.rankIncrement = 1`,
	)
}

// UpdateRankIncrement is used to encompass the different logic between
// DENSE_RANK and RANK window functions when updating the internal state of
// rank operators. It is used by the template.
func (r rankTmplInfo) UpdateRankIncrement() string {
	if r.Dense {
		return ``
	}
	return fmt.Sprintf(
		`r.rankIncrement++`,
	)
}

// Avoid unused warnings. These methods are used in the template.
var (
	_ = rankTmplInfo{}.UpdateRank()
	_ = rankTmplInfo{}.UpdateRankIncrement()
)

func genRankOps(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/colexec/rank_tmpl.go")
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.Replace(s, "_RANK_STRING", "{{.String}}", -1)

	updateRankRe := regexp.MustCompile(`_UPDATE_RANK_\(\)`)
	s = updateRankRe.ReplaceAllString(s, "{{.UpdateRank}}")
	updateRankIncrementRe := regexp.MustCompile(`_UPDATE_RANK_INCREMENT\(\)`)
	s = updateRankIncrementRe.ReplaceAllString(s, "{{.UpdateRankIncrement}}")

	// Now, generate the op, from the template.
	tmpl, err := template.New("rank_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	rankTmplInfos := []rankTmplInfo{
		{Dense: false, HasPartition: false, String: "rankNoPartition"},
		{Dense: false, HasPartition: true, String: "rankWithPartition"},
		{Dense: true, HasPartition: false, String: "denseRankNoPartition"},
		{Dense: true, HasPartition: true, String: "denseRankWithPartition"},
	}
	return tmpl.Execute(wr, rankTmplInfos)
}

func init() {
	registerGenerator(genRankOps, "rank.eg.go")
}
