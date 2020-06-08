// Copyright 2018 The Cockroach Authors.
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
	"text/template"
)

const hashJoinerTmpl = "pkg/sql/colexec/hashjoiner_tmpl.go"

func genHashJoiner(inputFileContents string, wr io.Writer) error {
	s := inputFileContents

	tmpl, err := template.New("hashjoiner_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, struct{}{})
}

func init() {
	registerGenerator(genHashJoiner, "hashjoiner.eg.go", hashJoinerTmpl)
}
