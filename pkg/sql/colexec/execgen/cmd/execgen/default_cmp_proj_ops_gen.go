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

const defaultCmpProjOpsTmpl = "pkg/sql/colexec/colexecproj/default_cmp_proj_ops_tmpl.go"

func genDefaultCmpProjOps(inputFileContents string, wr io.Writer) error {
	s := strings.ReplaceAll(inputFileContents, "_KIND", "{{.Kind}}")

	tmpl, err := template.New("default_cmp_proj_ops").Parse(s)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, []struct {
		// Comparison operators are always normalized so that the constant is
		// on the right side, so we skip generating the code when the constant
		// is on the left.
		IsRightConst bool
		Kind         string
	}{
		{IsRightConst: false, Kind: ""},
		{IsRightConst: true, Kind: "RConst"},
	})
}

func init() {
	registerGenerator(genDefaultCmpProjOps, "default_cmp_proj_ops.eg.go", defaultCmpProjOpsTmpl)
}
