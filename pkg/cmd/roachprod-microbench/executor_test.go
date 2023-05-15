// Copyright 2022 The Cockroach Authors.
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
	"path"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
)

func TestExtractBenchmarkResultsDataDriven(t *testing.T) {
	ddFilePath := path.Join(datapathutils.TestDataPath(t), "benchmark")
	datadriven.RunTest(t, ddFilePath, func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd != "benchmark" {
			d.Fatalf(t, "unknown command %s", d.Cmd)
		}
		result := extractBenchmarkResults(d.Input)
		output := fmt.Sprintf("%v", result)
		return output
	})
}
