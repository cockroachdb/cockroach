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

	"github.com/pmezard/go-difflib/difflib"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	catalog opt.Catalog
	sql     string
	ctx     context.Context
	semaCtx tree.SemaContext
	evalCtx tree.EvalContext

	// AllowUnsupportedExpr is a control knob: if set, when building a scalar,
	// the optbuilder takes any TypedExpr node that it doesn't recognize and
	// wraps that expression in an UnsupportedExpr node. This is temporary; it
	// is used for interfacing with the old planning code.
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

// OptBuild constructs an opt expression tree for the SQL query, with no
// transformations applied to it. The untouched output of the optbuilder is the
// final expression tree.
func (e *OptTester) OptBuild() (memo.ExprView, error) {
	return e.optimizeExpr(xform.OptimizeNone)
}

// Optimize constructs an opt expression tree for the SQL query, with all
// transformations applied to it. The result is the memo expression tree with
// the lowest estimated cost.
func (e *OptTester) Optimize() (memo.ExprView, error) {
	return e.optimizeExpr(xform.OptimizeAll)
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
func (e *OptTester) OptSteps() (string, error) {
	var buf bytes.Buffer
	var prev, next string
	for i := 0; ; i++ {
		o := xform.NewOptimizer(&e.evalCtx)
		o.MaxSteps = xform.OptimizeSteps(i)
		root, required, err := e.buildExpr(o.Factory())
		if err != nil {
			return "", err
		}

		next = o.Optimize(root, required).String()
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
	factory *xform.Factory,
) (root memo.GroupID, required *memo.PhysicalProps, _ error) {
	stmt, err := parser.ParseOne(e.sql)
	if err != nil {
		return 0, nil, err
	}

	b := optbuilder.New(e.ctx, &e.semaCtx, &e.evalCtx, e.catalog, factory, stmt)
	b.AllowUnsupportedExpr = e.AllowUnsupportedExpr
	return b.Build()
}

func (e *OptTester) optimizeExpr(maxSteps xform.OptimizeSteps) (memo.ExprView, error) {
	o := xform.NewOptimizer(&e.evalCtx)
	o.MaxSteps = maxSteps
	root, required, err := e.buildExpr(o.Factory())
	if err != nil {
		return memo.ExprView{}, err
	}
	return o.Optimize(root, required), nil
}
