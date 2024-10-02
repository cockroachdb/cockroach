// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import "io"

const windowAggregatorTmpl = "pkg/sql/colexec/colexecwindow/window_aggregator_tmpl.go"

func genWindowAggregator(inputFileContents string, outputFile io.Writer) error {
	_, err := outputFile.Write([]byte(inputFileContents))
	return err
}

func init() {
	registerGenerator(genWindowAggregator, "window_aggregator.eg.go", windowAggregatorTmpl)
}
