// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"path"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
)

func TestExtractBenchmarkResultsDataDriven(t *testing.T) {
	ddFilePath := path.Join(datapathutils.TestDataPath(t), "benchmark")
	datadriven.RunTest(t, ddFilePath, func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd != "benchmark" {
			d.Fatalf(t, "unknown command %s", d.Cmd)
		}
		result := parser.ExtractBenchmarkResults(d.Input)
		output := fmt.Sprintf("%v", result)
		return output
	})
}
