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

package xform

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
	"github.com/pmezard/go-difflib/difflib"
)

// PhysProps files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestPhysicalProps/ordering"
//   make test PKG=./pkg/sql/opt/xform TESTS="TestPhysicalProps/presentation"
//   ...
func TestPhysicalProps(t *testing.T) {
	runDataDrivenTest(t, "testdata/physprops/*")
}

// runDataDrivenTest runs data-driven testcases of the form
//   <command>
//   <SQL statement>
//   ----
//   <expected results>
//
// The supported commands are:
//
//  - exec-ddl
//
//    Runs a SQL DDL statement to build the test catalog. Only a small number
//    of DDL statements are supported, and those not fully.
//
//  - build
//
//    Builds an expression tree from a SQL query and outputs it without any
//    optimizations applied to it.
//
//  - opt
//
//    Builds an expression tree from a SQL query, fully optimizes it using the
//    memo, and then outputs the lowest cost tree.
//
//  - optsteps
//
//    Outputs the lowest cost tree for each step in optimization using the
//    standard unified diff format. Used for debugging the optimizer.
//
//  - memo
//
//    Builds an expression tree from a SQL query, fully optimizes it using the
//    memo, and then outputs the memo containing the forest of trees.
//
func runDataDrivenTest(t *testing.T, testdataGlob string) {
	paths, err := filepath.Glob(testdataGlob)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("no testfiles found matching: %s", testdataGlob)
	}

	test := newOptimizerTest()
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			test.run(t, path)
		})
	}
}

type optimizerTest struct {
	t       *testing.T
	d       *datadriven.TestData
	ctx     context.Context
	semaCtx tree.SemaContext
	evalCtx tree.EvalContext
	catalog *testutils.TestCatalog
}

func newOptimizerTest() *optimizerTest {
	return &optimizerTest{
		ctx:     context.Background(),
		semaCtx: tree.MakeSemaContext(false /* privileged */),
		evalCtx: tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings()),
		catalog: testutils.NewTestCatalog(),
	}
}

func (ot *optimizerTest) run(t *testing.T, path string) {
	datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
		ot.t = t
		ot.d = d

		if d.Cmd == "exec-ddl" {
			return testutils.ExecuteTestDDL(t, d.Input, ot.catalog)
		}

		stmt, err := parser.ParseOne(d.Input)
		if err != nil {
			d.Fatalf(t, "%v", err)
		}

		switch d.Cmd {
		case "build":
			// build command disables optimizations.
			return ot.optimizeExpr(stmt, OptimizeNone).String()

		case "opt":
			// opt command enables all optimizations.
			return ot.optimizeExpr(stmt, OptimizeAll).String()

		case "optsteps":
			// optsteps command iteratively shows the output from each
			// optimization step for debugging.
			return ot.outputOptSteps(stmt)

		case "memo":
			ev := ot.optimizeExpr(stmt, OptimizeAll)
			root := ev.Group()
			props := ev.Physical().String()
			return fmt.Sprintf("[%d: \"%s\"]\n%s", root, props, ev.mem.String())

		default:
			d.Fatalf(t, "unsupported command: %s", d.Cmd)
			return ""
		}
	})
}

func (ot *optimizerTest) optimizeExpr(stmt tree.Statement, steps OptimizeSteps) ExprView {
	o := NewOptimizer(&ot.evalCtx, steps)
	b := optbuilder.New(ot.ctx, &ot.semaCtx, &ot.evalCtx, ot.catalog, o.Factory(), stmt)
	root, props, err := b.Build()
	if err != nil {
		ot.d.Fatalf(ot.t, "%v", err)
	}
	return o.Optimize(root, props)
}

// outputOptSteps returns a string that shows each optimization step using the
// standard unified diff format. It is used for debugging the optimizer.
func (ot *optimizerTest) outputOptSteps(stmt tree.Statement) string {
	var buf bytes.Buffer
	var prev, next string
	for i := 0; ; i++ {
		o := NewOptimizer(&ot.evalCtx, OptimizeSteps(i))
		b := optbuilder.New(ot.ctx, &ot.semaCtx, &ot.evalCtx, ot.catalog, o.Factory(), stmt)
		root, props, err := b.Build()
		if err != nil {
			ot.d.Fatalf(ot.t, "%v", err)
		}

		next = o.Optimize(root, props).String()
		if prev == next {
			// No change, so nothing more to optimize.
			// TODO(andyk): this method of detecting changes won't work
			// when we introduce exploration patterns.
			break
		}

		if i == 0 {
			// Output starting tree.
			buf.WriteString(next)
		} else {
			diff := difflib.UnifiedDiff{
				A:        difflib.SplitLines(prev),
				B:        difflib.SplitLines(next),
				FromFile: "",
				ToFile:   o.LastRuleName().String(),
				Context:  100,
			}

			text, _ := difflib.GetUnifiedDiffString(diff)
			buf.WriteString(strings.Trim(text, " \t\r\n") + "\n")
		}

		prev = next
	}

	// Output ending tree.
	buf.WriteString("---\n")
	buf.WriteString("+++\n")
	buf.WriteString(next)

	return buf.String()
}
