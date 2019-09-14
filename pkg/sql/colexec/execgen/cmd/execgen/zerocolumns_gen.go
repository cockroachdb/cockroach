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
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

func genZeroColumns(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/colexec/zerocolumns_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)
	s = strings.Replace(s, "_GOTYPE", "{{.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.}}", -1)

	tmpl, err := template.New("zerocolumns").Parse(s)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, coltypes.AllTypes)
}

func init() {
	registerGenerator(genZeroColumns, "zerocolumns.eg.go")
}
