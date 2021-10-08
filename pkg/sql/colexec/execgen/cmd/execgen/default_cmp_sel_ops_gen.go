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

const defaultCmpSelOpsTmpl = "pkg/sql/colexec/colexecsel/default_cmp_sel_ops_tmpl.go"

func genDefaultCmpSelOps(inputFileContents string, wr io.Writer) error {
	s := strings.ReplaceAll(inputFileContents, "_KIND", "{{.Kind}}")

	tmpl, err := template.New("default_cmp_sel_ops").Parse(s)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, []struct {
		HasConst bool
		Kind     string
	}{
		{HasConst: false, Kind: ""},
		{HasConst: true, Kind: "Const"},
	})
}

func init() {
	registerGenerator(genDefaultCmpSelOps, "default_cmp_sel_ops.eg.go", defaultCmpSelOpsTmpl)
}
