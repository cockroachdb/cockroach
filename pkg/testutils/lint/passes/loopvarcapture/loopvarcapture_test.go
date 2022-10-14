// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loopvarcapture_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/loopvarcapture"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"golang.org/x/tools/go/analysis/analysistest"
)

var extraGoRoutineFunctions = []loopvarcapture.GoWrapper{
	{
		// test non-pointer receiver
		Func: loopvarcapture.Function{
			Pkg: "example.org/concurrency", Type: "Group", Name: "Go",
		},
		WaitFuncName: "Wait",
	},
	{
		// test a package-level function
		Func: loopvarcapture.Function{
			Pkg: "example.org/concurrency", Name: "Go",
		},
		WaitFuncName: "Wait1",
	},
	{
		// test a function with a return value
		Func: loopvarcapture.Function{
			Pkg: "example.org/concurrency", Name: "GoWithError",
		},
		WaitFuncName: "Await",
	},
}

func init() {
	if bazel.BuiltWithBazel() {
		bazel.SetGoEnv()
	}
}

func TestAnalyzer(t *testing.T) {
	skip.UnderStress(t)

	originalGoRoutineFunctions := loopvarcapture.GoRoutineFunctions
	loopvarcapture.GoRoutineFunctions = append(originalGoRoutineFunctions, extraGoRoutineFunctions...)
	defer func() { loopvarcapture.GoRoutineFunctions = originalGoRoutineFunctions }()

	testdata := testutils.TestDataPath(t)
	analysistest.TestData = func() string { return testdata }
	analysistest.Run(t, testdata, loopvarcapture.Analyzer, "p")
}
