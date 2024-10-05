// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"io"
)

const hashJoinerTmpl = "pkg/sql/colexec/colexecjoin/hashjoiner_tmpl.go"

func genHashJoiner(inputFileContents string, wr io.Writer) error {
	_, err := fmt.Fprint(wr, inputFileContents)
	return err
}

func init() {
	registerGenerator(genHashJoiner, "hashjoiner.eg.go", hashJoinerTmpl)
}
