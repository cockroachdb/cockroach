// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package leaktestcall_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/leaktestcall"
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
	testdata := datapathutils.TestDataPath(t)
	analysistest.TestData = func() string { return testdata }
	analysistest.Run(t, testdata, leaktestcall.Analyzer, "a")
}
