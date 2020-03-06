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
)

type rowNumberTmplInfo struct {
	HasPartition bool
	String       string
}

const rowNumberTmpl = "pkg/sql/colexec/row_number_tmpl.go"

func genRowNumberOp(wr io.Writer) error {
	d, err := ioutil.ReadFile(rowNumberTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.Replace(s, "_ROW_NUMBER_STRING", "{{.String}}", -1)

	// Now, generate the op, from the template.
	tmpl, err := template.New("row_number_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	rankTmplInfos := []rowNumberTmplInfo{
		{HasPartition: false, String: "rowNumberNoPartition"},
		{HasPartition: true, String: "rowNumberWithPartition"},
	}
	return tmpl.Execute(wr, rankTmplInfos)
}

func init() {
	registerGenerator(genRowNumberOp, "row_number.eg.go", rowNumberTmpl)
}
