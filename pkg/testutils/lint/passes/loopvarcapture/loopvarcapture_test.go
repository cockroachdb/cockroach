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
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/loopvarcapture"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"golang.org/x/tools/go/analysis/analysistest"
)

var extraGoRoutineFunctions = []loopvarcapture.Function{
	{Pkg: "example.org/concurrency", Type: "Group", Name: "Go"}, // test non-pointer receiver
	{Pkg: "example.org/concurrency", Name: "Go"},                // test a package-level function
	{Pkg: "example.org/concurrency", Name: "GoWithError"},       // test a function with a return value
	{Pkg: "p", Type: "Monitor", Name: "Go"},                     // test an interface method
}

func init() {
	if bazel.BuiltWithBazel() {
		bazel.SetGoEnv()
	}
}

func TestAnalyzer(t *testing.T) {
	skip.UnderStress(t)

	testAnalyzer := *loopvarcapture.Analyzer
	originalGoRoutineFunctions := loopvarcapture.GoRoutineFunctions
	loopvarcapture.GoRoutineFunctions = append(originalGoRoutineFunctions, extraGoRoutineFunctions...)
	defer func() { loopvarcapture.GoRoutineFunctions = originalGoRoutineFunctions }()

	testdata := datapathutils.TestDataPath(t)
	analysistest.TestData = func() string { return testdata }
	results := analysistest.Run(t, testdata, &testAnalyzer, "p")
	// Perform a sanity check--we expect analysis results.
	if len(results) == 0 {
		t.Fatal("analysis results are empty")
	}
}
