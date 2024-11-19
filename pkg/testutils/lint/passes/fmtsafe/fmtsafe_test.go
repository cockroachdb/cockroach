// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fmtsafe_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/fmtsafe"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"golang.org/x/tools/go/analysis/analysistest"
)

func init() {
	if bazel.BuiltWithBazel() {
		bazel.SetGoEnv()
	}
}

func Test(t *testing.T) {
	skip.UnderStress(t)
	fmtsafe.Tip = ""
	testdata := datapathutils.TestDataPath(t)
	analysistest.TestData = func() string { return testdata }
	results := analysistest.Run(t, testdata, fmtsafe.Analyzer, "a")
	for _, r := range results {
		for _, d := range r.Diagnostics {
			t.Logf("%s: %v: %s", r.Pass.Analyzer.Name, d.Pos, d.Message)
		}
	}
}
