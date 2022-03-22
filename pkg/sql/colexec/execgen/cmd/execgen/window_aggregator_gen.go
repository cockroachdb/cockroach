// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
