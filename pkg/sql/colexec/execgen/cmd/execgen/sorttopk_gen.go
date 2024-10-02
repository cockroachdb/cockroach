// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"io"
)

const sortTopKTmpl = "pkg/sql/colexec/sorttopk_tmpl.go"

func genSortTopK(inputFileContents string, wr io.Writer) error {
	_, err := fmt.Fprint(wr, inputFileContents)
	return err
}

func init() {
	registerGenerator(genSortTopK, "sorttopk.eg.go", sortTopKTmpl)
}
