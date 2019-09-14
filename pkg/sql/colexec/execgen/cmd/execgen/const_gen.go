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
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

func genConstOps(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/colexec/const_tmpl.go")
	if err != nil {
		return err
	}

	s := string(d)

	// Replace the template variables.
	s = strings.Replace(s, "_GOTYPE", "{{.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.}}", -1)
	s = replaceManipulationFuncs("", s)

	// Now, generate the op, from the template.
	tmpl, err := template.New("const_op").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, coltypes.AllTypes)
}
func init() {
	registerGenerator(genConstOps, "const.eg.go")
}
