// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"io"
	"io/ioutil"
	"text/template"
)

func genRowNumberOp(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/exec/vecbuiltins/row_number_tmpl.go")
	if err != nil {
		return err
	}

	s := string(d)

	nextRowNumber := makeFunctionRegex("_NEXT_ROW_NUMBER_", 1)
	s = nextRowNumber.ReplaceAllString(s, `{{template "nextRowNumber" buildDict "Global" $ "HasPartition" $1 }}`)

	// Now, generate the op, from the template.
	tmpl, err := template.New("row_number_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, struct{}{})
}

func init() {
	registerGenerator(genRowNumberOp, "row_number.eg.go")
}
