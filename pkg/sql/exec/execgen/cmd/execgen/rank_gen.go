// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
}

func (r rankTmplInfo) UpdateRank() string {
	switch r.Dense {
	case true:
		return fmt.Sprintf(
			`r.rank++`,
		)
	case false:
		return fmt.Sprintf(
			`r.rank += r.rankIncrement
r.rankIncrement = 1`,
		)
	default:
		panic("third value of boolean?")
	}
}

func (r rankTmplInfo) UpdateRankIncrement() string {
	switch r.Dense {
	case true:
		return ``
	case false:
		return fmt.Sprintf(
			`r.rankIncrement++`,
		)
	default:
		panic("third value of boolean?")
	}
}

func genRankOps(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/exec/vecbuiltins/rank_tmpl.go")
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.Replace(s, "_DENSE", "{{.Dense}}", -1)
	s = strings.Replace(s, "_PARTITION", "{{.HasPartition}}", -1)

	updateRankRe := regexp.MustCompile(`_UPDATE_RANK\(\)`)
	s = updateRankRe.ReplaceAllString(s, "{{.UpdateRank}}")
	updateRankIncrementRe := regexp.MustCompile(`_UPDATE_RANK_INCREMENT\(\)`)
	s = updateRankIncrementRe.ReplaceAllString(s, "{{.UpdateRankIncrement}}")

	// Now, generate the op, from the template.
	tmpl, err := template.New("rank_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	rankTmplInfos := []rankTmplInfo{
		{Dense: false, HasPartition: false},
		{Dense: false, HasPartition: true},
		{Dense: true, HasPartition: false},
		{Dense: true, HasPartition: true},
	}
	return tmpl.Execute(wr, rankTmplInfos)
}

func init() {
	registerGenerator(genRankOps, "rank.eg.go")
}
