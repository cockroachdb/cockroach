// Copyright 2021 The Cockroach Authors.
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

type nTileTmplInfo struct {
	HasPartition bool
	String       string
}

const nTileTmpl = "pkg/sql/colexec/colexecwindow/ntile_tmpl.go"

func genNTileOp(inputFileContents string, wr io.Writer) error {
	s := strings.ReplaceAll(inputFileContents, "_NTILE_STRING", "{{.String}}")

	seekNTileRe := makeFunctionRegex("_SEEK_NTILE", 1)
	s = seekNTileRe.ReplaceAllString(s,
		`{{template "seekNTile" buildDict "HasPartition" .HasPartition "HasSel" $1}}`)

	computeNTileRe := makeFunctionRegex("_COMPUTE_NTILE", 1)
	s = computeNTileRe.ReplaceAllString(s,
		`{{template "computeNTile" buildDict "HasSel" $1}}`)

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
