// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package xform_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
)

// TestCoster files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestCoster/sort"
//   make test PKG=./pkg/sql/opt/xform TESTS="TestCoster/scan"
//   ...
func TestCoster(t *testing.T) {
	runDataDrivenTest(
		t, "testdata/coster/",
		memo.ExprFmtHideRuleProps|memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars,
	)
}

// TestPhysicalPropsFactory files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestPhysicalPropsFactory/ordering"
//   make test PKG=./pkg/sql/opt/xform TESTS="TestPhysicalPropsFactory/presentation"
//   ...
func TestPhysicalPropsFactory(t *testing.T) {
	runDataDrivenTest(t, "testdata/physprops/", memo.ExprFmtHideAll)
}

// TestRuleProps files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRuleProps/orderings"
//   ...
func TestRuleProps(t *testing.T) {
	datadriven.Walk(t, "testdata/ruleprops", func(t *testing.T, path string) {
		catalog := testcat.New()
		datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
			tester := testutils.NewOptTester(catalog, d.Input)
			tester.Flags.ExprFormat = memo.ExprFmtHideStats | memo.ExprFmtHideCost |
				memo.ExprFmtHideQualifications | memo.ExprFmtHideScalars
			return tester.RunCommand(t, d)
		})
	})
}

// TestRules files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRules/scan"
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRules/select"
//   ...
func TestRules(t *testing.T) {
	runDataDrivenTest(
		t,
		"testdata/rules/",
		memo.ExprFmtHideStats|memo.ExprFmtHideCost|memo.ExprFmtHideRuleProps|
			memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars,
	)
}

// TestExternal contains test cases from external customers and external
// benchmarks (like TPCH), so that changes in their query plans can be monitored
// over time.
//
// TestExternal files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestExternal/tpch"
//   ...
func TestExternal(t *testing.T) {
	runDataDrivenTest(
		t,
		"testdata/external/",
		memo.ExprFmtHideStats|memo.ExprFmtHideCost|memo.ExprFmtHideRuleProps|
			memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars,
	)
}

// runDataDrivenTest runs data-driven testcases of the form
//   <command>
//   <SQL statement>
//   ----
//   <expected results>
//
// See OptTester.Handle for supported commands.
func runDataDrivenTest(t *testing.T, path string, fmtFlags memo.ExprFmtFlags) {
	datadriven.Walk(t, path, func(t *testing.T, path string) {
		catalog := testcat.New()
		datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
			tester := testutils.NewOptTester(catalog, d.Input)
			tester.Flags.ExprFormat = fmtFlags
			return tester.RunCommand(t, d)
		})
	})
}
