// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"io"
	"strings"
	"text/template"
)

type nTileTmplInfo struct {
	HasPartition bool
	String       string
}

const nTileTmpl = "pkg/sql/colexec/colexecwindow/ntile_tmpl.go"

func genNTileOp(inputFileContents string, wr io.Writer) error {
	s := strings.ReplaceAll(inputFileContents, "_NTILE_STRING", "{{.String}}")

	// Now, generate the op, from the template.
	tmpl, err := template.New("ntile_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	nTileTmplInfos := []nTileTmplInfo{
		{HasPartition: false, String: "nTileNoPartition"},
		{HasPartition: true, String: "nTileWithPartition"},
	}
	return tmpl.Execute(wr, nTileTmplInfos)
}

func init() {
	registerGenerator(genNTileOp, "ntile.eg.go", nTileTmpl)
}
