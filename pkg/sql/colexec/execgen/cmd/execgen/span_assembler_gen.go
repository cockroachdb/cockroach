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

type spanAssemblerTmplInfo struct {
	WithColFamilies bool
	String          string
}

const spanAssemblerTmpl = "pkg/sql/colexec/colexecspan/span_assembler_tmpl.go"

func genSpanAssemblerOp(inputFileContents string, wr io.Writer) error {
	s := strings.ReplaceAll(inputFileContents, "_OP_STRING", "{{.String}}")

	// Now, generate the op, from the template.
	tmpl, err := template.New("span_assembler_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	spanAssemblerTmplInfos := []spanAssemblerTmplInfo{
		{WithColFamilies: false, String: "spanAssemblerNoColFamily"},
		{WithColFamilies: true, String: "spanAssemblerWithColFamily"},
	}
	return tmpl.Execute(wr, spanAssemblerTmplInfos)
}

func init() {
	registerGenerator(genSpanAssemblerOp, "span_assembler.eg.go", spanAssemblerTmpl)
}
