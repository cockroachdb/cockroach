// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nilness_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/nilness"
	"golang.org/x/tools/go/analysis/analysistest"
)

func init() {
	if bazel.BuiltWithBazel() {
		bazel.SetGoEnv()
	}
}

func Test(t *testing.T) {
	testdata := datapathutils.TestDataPath(t)
	analysistest.TestData = func() string { return testdata }
	analysistest.Run(t, testdata, nilness.TestAnalyzer, "a")
}
