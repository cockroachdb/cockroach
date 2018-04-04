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

package testutils

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pmezard/go-difflib/difflib"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
)

// OptTester is a helper for testing the various optimizer components. It
// contains the boiler-plate code for the following useful tasks:
//   - Build an unoptimized opt expression tree
//   - Build an optimized opt expression tree
//   - Format the optimizer memo structure
//   - Create a diff showing the optimizer's work, step-by-step
//   - Build the exec node tree
//   - Execute the exec node tree
//
// The OptTester is used by tests in various sub-packages of the opt package.
type OptTester struct {
	Flags OptTesterFlags

	catalog opt.Catalog
	sql     string
	ctx     context.Context
	semaCtx tree.SemaContext
	evalCtx tree.EvalContext
}

// OptTesterFlags are control knobs for tests. Note that specific testcases can
// override these defaults.
type OptTesterFlags struct {
	// Format controls the output detail of build / opt/ optsteps
	// directives.
	Format memo.ExprFmtFlags

	// AllowUnsupportedExpr if set: when building a scalar, the optbuilder takes
	// any TypedExpr node that it doesn't recognize and wraps that expression in
	// an UnsupportedExpr node. This is temporary; it is used for interfacing with
	// the old planning code.
	AllowUnsupportedExpr bool
}

// NewOptTester constructs a new instance of the OptTester for the given SQL
// statement. Metadata used by the SQL query is accessed via the catalog.
func NewOptTester(catalog opt.Catalog, sql string) *OptTester {
	return &OptTester{
		catalog: catalog,
		sql:     sql,
		ctx:     context.Background(),
		semaCtx: tree.MakeSemaContext(false /* privileged */),
		evalCtx: tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings()),
	}
}

// RunCommand implements commands that are used by most tests:
//
//  - exec-ddl
//
//    Runs a SQL DDL statement to build the test catalog. Only a small number
//    of DDL statements are supported, and those not fully. This is only
//    available when using a TestCatalog.
//
//  - build [flags]
//
//    Builds an expression tree from a SQL query and outputs it without any
//    optimizations applied to it.
//
//  - opt [flags]
//
//    Builds an expression tree from a SQL query, fully optimizes it using the
//    memo, and then outputs the lowest cost tree.
//
//  - optsteps [flags]
//
//    Outputs the lowest cost tree for each step in optimization using the
//    standard unified diff format. Used for debugging the optimizer.
//
//  - memo [flags]
//
//    Builds an expression tree from a SQL query, fully optimizes it using the
//    memo, and then outputs the memo containing the forest of trees.
//
// Supported flags:
//
//  - format: controls the formatting of expressions for build, opt, and
//    optsteps commands. Possible values: show-all, hide-all, or any combination
//    of hide-cost, hide-stats, hide-constraints. Example:
//      build format={hide-cost,hide-stats}
//
//  - allow-unsupported: wrap unsupported expressions in UnsupportedOp.
//
func (e *OptTester) RunCommand(tb testing.TB, d *datadriven.TestData) string {
	// Allow testcases to override the flags.
	for _, a := range d.CmdArgs {
		if err := e.Flags.Set(a); err != nil {
			d.Fatalf(tb, "%s", err)
		}
	}

	switch d.Cmd {
	case "exec-ddl":
		testCatalog, ok := e.catalog.(*TestCatalog)
		if !ok {
			tb.Fatal("exec-ddl can only be used with TestCatalog")
		}
		return ExecuteTestDDL(tb, d.Input, testCatalog)

	case "build":
		ev, err := e.OptBuild()
		if err != nil {
			return fmt.Sprintf("error: %s\n", strings.TrimSpace(err.Error()))
		}
		return ev.FormatString(e.Flags.Format)

	case "opt":
		ev, err := e.Optimize()
		if err != nil {
			d.Fatalf(tb, "%v", err)
		}
		return ev.FormatString(e.Flags.Format)

	case "optsteps":
		result, err := e.OptSteps(testing.Verbose())
		if err != nil {
			d.Fatalf(tb, "%v", err)
		}
		return result

	case "memo":
		result, err := e.Memo()
		if err != nil {
			d.Fatalf(tb, "%v", err)
		}
		return result

	default:
		d.Fatalf(tb, "unsupported command: %s", d.Cmd)
		return ""
	}
}

// Set parses an argument that refers to a flag. See OptTester.Handle for
// supported flags.
func (f *OptTesterFlags) Set(arg datadriven.CmdArg) error {
	switch arg.Key {
	case "format":
		f.Format = 0
		if len(arg.Vals) == 0 {
			return fmt.Errorf("format flag requires value(s)")
		}
		for _, v := range arg.Vals {
			m := map[string]memo.ExprFmtFlags{
				"show-all":         memo.ExprFmtShowAll,
				"hide-all":         memo.ExprFmtHideAll,
				"hide-stats":       memo.ExprFmtHideStats,
				"hide-cost":        memo.ExprFmtHideCost,
				"hide-constraints": memo.ExprFmtHideConstraints,
			}
			if val, ok := m[v]; ok {
				f.Format |= val
			} else {
				return fmt.Errorf("unknown format value %s", v)
			}
		}

	case "allow-unsupported":
		f.AllowUnsupportedExpr = true

	default:
		return fmt.Errorf("unknown argument: %s", arg.Key)
	}
	return nil
}

// OptBuild constructs an opt expression tree for the SQL query, with no
// transformations applied to it. The untouched output of the optbuilder is the
// final expression tree.
func (e *OptTester) OptBuild() (memo.ExprView, error) {
	return e.optimizeExpr(false /* allowOptimizations */)
}

// Optimize constructs an opt expression tree for the SQL query, with all
// transformations applied to it. The result is the memo expression tree with
// the lowest estimated cost.
func (e *OptTester) Optimize() (memo.ExprView, error) {
	return e.optimizeExpr(true /* allowOptimizations */)
}

// Memo returns a string that shows the memo data structure that is constructed
// by the optimizer.
func (e *OptTester) Memo() (string, error) {
	o := xform.NewOptimizer(&e.evalCtx)
	root, required, err := e.buildExpr(o.Factory())
	if err != nil {
		return "", err
	}
	o.Optimize(root, required)
	return fmt.Sprintf("[%d: \"%s\"]\n%s", root, required, o.Memo().String()), nil
}

// OptSteps returns a string that shows each optimization step using the
// standard unified diff format. It is used for debugging the optimizer.
// If verbose is true, each step is also printed on stdout.
func (e *OptTester) OptSteps(verbose bool) (string, error) {
	var buf bytes.Buffer
	var prev, next string
	if verbose {
		fmt.Print("------ optsteps verbose output starts ------\n")
	}
	output := func(format string, args ...interface{}) {
		fmt.Fprintf(&buf, format, args...)
		if verbose {
			fmt.Printf(format, args...)
		}
	}
	indent := func(str string) {
		str = strings.TrimRight(str, " \n\t\r")
		lines := strings.Split(str, "\n")
		for _, line := range lines {
			output("  %s\n", line)
		}
	}
	for i := 0; ; i++ {
		o := xform.NewOptimizer(&e.evalCtx)

		// Override SetOnRuleMatch to stop optimizing after the ith rule matches.
		steps := i
		lastRuleName := opt.InvalidRuleName
		o.SetOnRuleMatch(func(ruleName opt.RuleName) bool {
			if steps == 0 {
				return false
			}
			steps--
			lastRuleName = ruleName
			return true
		})

		root, required, err := e.buildExpr(o.Factory())
		if err != nil {
			return "", err
		}

		next = o.Optimize(root, required).FormatString(e.Flags.Format)
		if steps != 0 {
			// All steps were not used, so must be done.
			break
		}

		if i == 0 {
			output("*** Initial expr:\n")
			// Output starting tree.
			indent(next)
		} else {
			output("\n*** %s applied; ", lastRuleName.String())

			if prev == next {
				// The expression can be unchanged if a part of the memo changed that
				// does not affect the final best expression.
				output("best expr unchanged.\n")
			} else {
				output("best expr changed:\n")
				diff := difflib.UnifiedDiff{
					A:       difflib.SplitLines(prev),
					B:       difflib.SplitLines(next),
					Context: 100,
				}

				text, _ := difflib.GetUnifiedDiffString(diff)
				// Skip the "@@ ... @@" header (first line).
				text = strings.SplitN(text, "\n", 2)[1]
				indent(text)
			}
		}

		prev = next
	}

	// Output ending tree.
	output("\n*** Final best expr:\n")
	indent(next)

	if verbose {
		fmt.Print("------ optsteps verbose output ends ------\n")
	}

	return buf.String(), nil
}

// ExecBuild builds the exec node tree for the SQL query. This can be executed
// by the exec engine.
func (e *OptTester) ExecBuild(eng exec.TestEngine) (exec.Node, error) {
	ev, err := e.Optimize()
	if err != nil {
		return nil, err
	}
	return execbuilder.New(eng.Factory(), ev).Build()
}

// Explain builds the exec node tree for the SQL query and then runs the
// explain command that describes the physical execution plan.
func (e *OptTester) Explain(eng exec.TestEngine) ([]tree.Datums, error) {
	node, err := e.ExecBuild(eng)
	if err != nil {
		return nil, err
	}
	return eng.Explain(node)
}

// Exec builds the exec node tree for the SQL query and then executes it.
func (e *OptTester) Exec(eng exec.TestEngine) (sqlbase.ResultColumns, []tree.Datums, error) {
	node, err := e.ExecBuild(eng)
	if err != nil {
		return nil, nil, err
	}

	columns := eng.Columns(node)

	var datums []tree.Datums
	datums, err = eng.Execute(node)
	if err != nil {
		return nil, nil, err
	}
	return columns, datums, err
}

func (e *OptTester) buildExpr(
	factory *norm.Factory,
) (root memo.GroupID, required *memo.PhysicalProps, _ error) {
	stmt, err := parser.ParseOne(e.sql)
	if err != nil {
		return 0, nil, err
	}

	b := optbuilder.New(e.ctx, &e.semaCtx, &e.evalCtx, e.catalog, factory, stmt)
	b.AllowUnsupportedExpr = e.Flags.AllowUnsupportedExpr
	return b.Build()
}

func (e *OptTester) optimizeExpr(allowOptimizations bool) (memo.ExprView, error) {
	o := xform.NewOptimizer(&e.evalCtx)
	if !allowOptimizations {
		o.DisableOptimizations()
	}
	root, required, err := e.buildExpr(o.Factory())
	if err != nil {
		return memo.ExprView{}, err
	}
	return o.Optimize(root, required), nil
}
