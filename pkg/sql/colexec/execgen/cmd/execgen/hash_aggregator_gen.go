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
	"fmt"
	"io"
)

const hashAggTmpl = "pkg/sql/colexec/hash_aggregator_tmpl.go"

func genHashAggregator(inputFileContents string, wr io.Writer) error {
	_, err := fmt.Fprint(wr, inputFileContents)
	return err
}

func init() {
	registerGenerator(genHashAggregator, "hash_aggregator.eg.go", hashAggTmpl)
}
