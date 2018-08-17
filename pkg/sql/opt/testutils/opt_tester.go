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
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/pmezard/go-difflib/difflib"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

	builder strings.Builder
}

// OptTesterFlags are control knobs for tests. Note that specific testcases can
// override these defaults.
type OptTesterFlags struct {
	// Format controls the output detail of build / opt/ optsteps command
	// directives.
	ExprFormat memo.ExprFmtFlags

	// MemoFormat controls the output detail of memo command directives.
	MemoFormat memo.FmtFlags

	// AllowUnsupportedExpr if set: when building a scalar, the optbuilder takes
	// any TypedExpr node that it doesn't recognize and wraps that expression in
	// an UnsupportedExpr node. This is temporary; it is used for interfacing with
	// the old planning code.
	AllowUnsupportedExpr bool

	// FullyQualifyNames if set: when building a query, the optbuilder fully
	// qualifies all column names before adding them to the metadata. This flag
	// allows us to test that name resolution works correctly, and avoids
	// cluttering test output with schema and catalog names in the general case.
	FullyQualifyNames bool

	// Verbose indicates whether verbose test debugging information will be
	// output to stdout when commands run. Only certain commands support this.
	Verbose bool

	// ExploreTraceRule restricts the ExploreTrace output to only show the effects
	// of a specific rule.
	ExploreTraceRule string
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
//  - norm [flags]
//
//    Builds an expression tree from a SQL query, applies normalization
//    optimizations, and outputs it without any exploration optimizations
//    applied to it.
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
//  - exploretrace [flags]
//
//    Outputs information about exploration rule application. Used for debugging
//    the optimizer.
//
//  - memo [flags]
//
//    Builds an expression tree from a SQL query, fully optimizes it using the
//    memo, and then outputs the memo containing the forest of trees.
//
//  - rulestats [flags]
//
//    Performs the optimization and outputs statistics about applied rules.
//
//
// Supported flags:
//
//  - format: controls the formatting of expressions for build, opt, and
//    optsteps commands. Possible values: show-all, hide-all, or any combination
//    of hide-cost, hide-stats, hide-constraints, hide-scalars, hide-qual.
//    For example:
//      build format=(hide-cost,hide-stats)
//
//  - raw-memo: show the raw memo groups, in the order they were originally
//	  added, including any "orphaned" groups.
//
//  - allow-unsupported: wrap unsupported expressions in UnsupportedOp.
//
//  - fully-qualify-names: fully qualify all column names in the test output.
//
//  - rule: used with exploretrace; the value is the name of a rule. When
//    specified, the exploretrace output is filtered to only show expression
//    changes due to that specific rule.
//
func (ot *OptTester) RunCommand(tb testing.TB, d *datadriven.TestData) string {
	// Allow testcases to override the flags.
	for _, a := range d.CmdArgs {
		if err := ot.Flags.Set(a); err != nil {
			d.Fatalf(tb, "%s", err)
		}
	}

	ot.Flags.Verbose = testing.Verbose()

	switch d.Cmd {
	case "exec-ddl":
		testCatalog, ok := ot.catalog.(*testcat.Catalog)
		if !ok {
			tb.Fatal("exec-ddl can only be used with TestCatalog")
		}
		s, err := testCatalog.ExecuteDDL(d.Input)
		if err != nil {
			tb.Fatal(err)
		}
		return s

	case "build":
		ev, err := ot.OptBuild()
		if err != nil {
			text := strings.TrimSpace(err.Error())
			if pgerr, ok := err.(*pgerror.Error); ok {
				// Output Postgres error code if it's available.
				return fmt.Sprintf("error (%s): %s\n", pgerr.Code, text)
			}
			return fmt.Sprintf("error: %s\n", text)
		}
		fillInLazyProps(ev)
		return ev.FormatString(ot.Flags.ExprFormat)

	case "norm":
		ev, err := ot.OptNorm()
		if err != nil {
			text := strings.TrimSpace(err.Error())
			if pgerr, ok := err.(*pgerror.Error); ok {
				// Output Postgres error code if it's available.
				return fmt.Sprintf("error (%s): %s\n", pgerr.Code, text)
			}
			return fmt.Sprintf("error: %s\n", text)
		}
		fillInLazyProps(ev)
		return ev.FormatString(ot.Flags.ExprFormat)

	case "opt":
		ev, err := ot.Optimize()
		if err != nil {
			d.Fatalf(tb, "%v", err)
		}
		fillInLazyProps(ev)
		return ev.FormatString(ot.Flags.ExprFormat)

	case "rulestats":
		result, err := ot.RuleStats()
		if err != nil {
			d.Fatalf(tb, "%v", err)
		}
		return result

	case "optsteps":
		result, err := ot.OptSteps()
		if err != nil {
			d.Fatalf(tb, "%v", err)
		}
		return result

	case "exploretrace":
		result, err := ot.ExploreTrace()
		if err != nil {
			d.Fatalf(tb, "%v", err)
		}
		return result

	case "memo":
		result, err := ot.Memo()
		if err != nil {
			d.Fatalf(tb, "%v", err)
		}
		return result

	default:
		d.Fatalf(tb, "unsupported command: %s", d.Cmd)
		return ""
	}
}

// Fills in lazily-derived properties (for display).
func fillInLazyProps(ev memo.ExprView) {
	if !ev.IsScalar() {
		// Derive columns that are candidates for pruning.
		norm.DerivePruneCols(ev)

		// Derive columns that are candidates for null rejection.
		norm.DeriveRejectNullCols(ev)

		// Make sure the interesting orderings are calculated.
		xform.DeriveInterestingOrderings(ev)
	}

	for i := 0; i < ev.ChildCount(); i++ {
		fillInLazyProps(ev.Child(i))
	}
}

// Set parses an argument that refers to a flag.
// See OptTester.RunCommand for supported flags.
func (f *OptTesterFlags) Set(arg datadriven.CmdArg) error {
	switch arg.Key {
	case "format":
		f.ExprFormat = 0
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
				"hide-ruleprops":   memo.ExprFmtHideRuleProps,
				"hide-scalars":     memo.ExprFmtHideScalars,
				"hide-qual":        memo.ExprFmtHideQualifications,
			}
			if val, ok := m[v]; ok {
				f.ExprFormat |= val
			} else {
				return fmt.Errorf("unknown format value %s", v)
			}
		}

	case "raw-memo":
		f.MemoFormat = memo.FmtRaw

	case "allow-unsupported":
		f.AllowUnsupportedExpr = true

	case "fully-qualify-names":
		f.FullyQualifyNames = true
		// Hiding qualifications defeats the purpose.
		f.ExprFormat &= ^memo.ExprFmtHideQualifications

	case "rule":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("rule requires one argument")
		}
		f.ExploreTraceRule = arg.Vals[0]

	default:
		return fmt.Errorf("unknown argument: %s", arg.Key)
	}
	return nil
}

// OptBuild constructs an opt expression tree for the SQL query, with no
// transformations applied to it. The untouched output of the optbuilder is the
// final expression tree.
func (ot *OptTester) OptBuild() (memo.ExprView, error) {
	return ot.optimizeExpr(false /* allowNormalizations */, false /* allowExplorations */)
}

// OptNorm constructs an opt expression tree for the SQL query, with all
// normalization transformations applied to it. The normalized output of the
// optbuilder is the final expression tree.
func (ot *OptTester) OptNorm() (memo.ExprView, error) {
	return ot.optimizeExpr(true /* allowNormalizations */, false /* allowExplorations */)
}

// Optimize constructs an opt expression tree for the SQL query, with all
// transformations applied to it. The result is the memo expression tree with
// the lowest estimated cost.
func (ot *OptTester) Optimize() (memo.ExprView, error) {
	return ot.optimizeExpr(true /* allowNormalizations */, true /* allowExplorations */)
}

// Memo returns a string that shows the memo data structure that is constructed
// by the optimizer.
func (ot *OptTester) Memo() (string, error) {
	var o xform.Optimizer
	o.Init(&ot.evalCtx)
	root, required, err := ot.buildExpr(o.Factory())
	if err != nil {
		return "", err
	}
	o.Optimize(root, required)
	return o.Memo().FormatString(ot.Flags.MemoFormat), nil
}

// RuleStats performs the optimization and returns statistics about how many
// rules were applied.
func (ot *OptTester) RuleStats() (string, error) {
	type ruleStats struct {
		rule       opt.RuleName
		numApplied int
		numAdded   int
	}
	stats := make([]ruleStats, opt.NumRuleNames)
	for i := range stats {
		stats[i].rule = opt.RuleName(i)
	}

	var o xform.Optimizer
	o.Init(&ot.evalCtx)

	o.NotifyOnAppliedRule(
		func(ruleName opt.RuleName, group memo.GroupID, expr memo.ExprOrdinal, added int) {
			stats[ruleName].numApplied++
			stats[ruleName].numAdded += added
		},
	)
	root, required, err := ot.buildExpr(o.Factory())
	if err != nil {
		return "", err
	}
	o.Optimize(root, required)

	// Split the rules.
	var norm, explore []ruleStats
	var allNorm, allExplore ruleStats
	for i := range stats {
		if stats[i].numApplied > 0 {
			if stats[i].rule.IsNormalize() {
				allNorm.numApplied += stats[i].numApplied
				norm = append(norm, stats[i])
			} else {
				allExplore.numApplied += stats[i].numApplied
				allExplore.numAdded += stats[i].numAdded
				explore = append(explore, stats[i])
			}
		}
	}
	// Sort with most applied rules first.
	sort.SliceStable(norm, func(i, j int) bool {
		return norm[i].numApplied > norm[j].numApplied
	})
	sort.SliceStable(explore, func(i, j int) bool {
		return explore[i].numApplied > explore[j].numApplied
	})

	// Only show the top 5 rules.
	const topK = 5
	if len(norm) > topK {
		norm = norm[:topK]
	}
	if len(explore) > topK {
		explore = explore[:topK]
	}

	// Ready to report.
	var res strings.Builder
	fmt.Fprintf(&res, "Normalization rules applied %d times.\n", allNorm.numApplied)
	if len(norm) > 0 {
		fmt.Fprintf(&res, "Top normalization rules:\n")
		tw := tabwriter.NewWriter(&res, 1 /* minwidth */, 1 /* tabwidth */, 1 /* padding */, ' ', 0)
		for _, s := range norm {
			fmt.Fprintf(tw, "  %s\tapplied\t%d\ttimes.\n", s.rule, s.numApplied)
		}
		_ = tw.Flush()
	}

	fmt.Fprintf(
		&res, "Exploration rules applied %d times, added %d expressions.\n",
		allExplore.numApplied, allExplore.numAdded,
	)

	if len(explore) > 0 {
		fmt.Fprintf(&res, "Top exploration rules:\n")
		tw := tabwriter.NewWriter(&res, 1 /* minwidth */, 1 /* tabwidth */, 1 /* padding */, ' ', 0)
		for _, s := range explore {
			fmt.Fprintf(
				tw, "  %s\tapplied\t%d\ttimes, added\t%d\texpressions.\n", s.rule, s.numApplied, s.numAdded,
			)
		}
		_ = tw.Flush()
	}
	return res.String(), nil
}

// OptSteps steps through the transformations performed by the optimizer on the
// memo, one-by-one. The output of each step is the lowest cost expression tree
// that also contains the expressions that were changed or added by the
// transformation. The output of each step is diff'd against the output of a
// previous step, using the standard unified diff format.
//
//   CREATE TABLE a (x INT PRIMARY KEY, y INT, UNIQUE INDEX (y))
//
//   SELECT x FROM a WHERE x=1
//
// At the time of this writing, this query triggers 6 rule applications:
//   EnsureSelectFilters     Wrap Select predicate with Filters operator
//   FilterUnusedSelectCols  Do not return unused "y" column from Scan
//   EliminateProject        Remove unneeded Project operator
//   GenerateIndexScans      Explore scanning "y" index to get "x" values
//   ConstrainScan           Explore pushing "x=1" into "x" index Scan
//   ConstrainScan           Explore pushing "x=1" into "y" index Scan
//
// Some steps produce better plans that have a lower execution cost. Other steps
// don't. However, it's useful to see both kinds of steps. The optsteps output
// distinguishes these two cases by using stronger "====" header delimiters when
// a better plan has been found, and weaker "----" header delimiters when not.
// In both cases, the output shows the expressions that were changed or added by
// the rule, even if the total expression tree cost worsened.
//
func (ot *OptTester) OptSteps() (string, error) {
	var prevBest, prev, next string
	ot.builder.Reset()

	os := newOptSteps(ot)
	for {
		err := os.next()
		if err != nil {
			return "", err
		}

		next = os.exprView().FormatString(ot.Flags.ExprFormat)

		// This call comes after setting "next", because we want to output the
		// final expression, even though there were no diffs from the previous
		// iteration.
		if os.done() {
			break
		}

		if prev == "" {
			// Output starting tree.
			ot.optStepsDisplay("", next, os)
			prevBest = next
		} else if next == prev || next == prevBest {
			ot.optStepsDisplay(next, next, os)
		} else if os.isBetter() {
			// New expression is better than the previous expression. Diff
			// it against the previous *best* expression (might not be the
			// previous expression).
			ot.optStepsDisplay(prevBest, next, os)
			prevBest = next
		} else {
			// New expression is not better than the previous expression, but
			// still show the change. Diff it against the previous expression,
			// regardless if it was a "best" expression or not.
			ot.optStepsDisplay(prev, next, os)
		}

		prev = next
	}

	// Output ending tree.
	ot.optStepsDisplay(next, "", os)

	return ot.builder.String(), nil
}

func (ot *OptTester) optStepsDisplay(before string, after string, os *optSteps) {
	// bestHeader is used when the expression is an improvement over the previous
	// expression.
	bestHeader := func(ev memo.ExprView, format string, args ...interface{}) {
		ot.separator("=")
		ot.output(format, args...)
		ot.output("  Cost: %.2f\n", ev.Cost())
		ot.separator("=")
	}

	// altHeader is used when the expression doesn't improve over the previous
	// expression, but it's still desirable to see what changed.
	altHeader := func(format string, args ...interface{}) {
		ot.separator("-")
		ot.output(format, args...)
		ot.separator("-")
	}

	if before == "" {
		if ot.Flags.Verbose {
			fmt.Print("------ optsteps verbose output starts ------\n")
		}
		bestHeader(os.exprView(), "Initial expression\n")
		ot.indent(after)
		return
	}

	if before == after {
		altHeader("%s (no changes)\n", os.lastRuleName())
		return
	}

	if after == "" {
		bestHeader(os.exprView(), "Final best expression\n")
		ot.indent(before)

		if ot.Flags.Verbose {
			fmt.Print("------ optsteps verbose output ends ------\n")
		}
		return
	}

	var diff difflib.UnifiedDiff
	if os.isBetter() {
		// New expression is better than the previous expression. Diff
		// it against the previous *best* expression (might not be the
		// previous expression).
		bestHeader(os.exprView(), "%s\n", os.lastRuleName())
	} else {
		altHeader("%s (higher cost)\n", os.lastRuleName())
	}

	diff = difflib.UnifiedDiff{
		A:       difflib.SplitLines(before),
		B:       difflib.SplitLines(after),
		Context: 100,
	}
	text, _ := difflib.GetUnifiedDiffString(diff)
	// Skip the "@@ ... @@" header (first line).
	text = strings.SplitN(text, "\n", 2)[1]
	ot.indent(text)
}

// ExploreTrace steps through exploration transformations performed by the
// optimizer, one-by-one. The output of each step is the expression on which the
// rule was applied, and the expressions that were generated by the rule.
func (ot *OptTester) ExploreTrace() (string, error) {
	ot.builder.Reset()

	et := newExploreTracer(ot)

	for {
		err := et.Next()
		if err != nil {
			return "", err
		}
		if et.Done() {
			break
		}

		if ot.Flags.ExploreTraceRule != "" && et.LastRuleName().String() != ot.Flags.ExploreTraceRule {
			continue
		}

		if ot.builder.Len() > 0 {
			ot.output("\n")
		}
		ot.separator("=")
		ot.output("%s\n", et.LastRuleName())
		ot.separator("=")
		ot.output("Source expression:\n")
		ot.indent(et.SrcExpr().FormatString(ot.Flags.ExprFormat))
		newExprs := et.NewExprs()
		if len(newExprs) == 0 {
			ot.output("\nNo new expressions.\n")
		}
		for i := range newExprs {
			ot.output("\nNew expression %d of %d:\n", i+1, len(newExprs))
			ot.indent(newExprs[i].FormatString(ot.Flags.ExprFormat))
		}
	}
	return ot.builder.String(), nil
}

func (ot *OptTester) buildExpr(
	factory *norm.Factory,
) (root memo.GroupID, required *props.Physical, _ error) {
	stmt, err := parser.ParseOne(ot.sql)
	if err != nil {
		return 0, nil, err
	}

	b := optbuilder.New(ot.ctx, &ot.semaCtx, &ot.evalCtx, ot.catalog, factory, stmt)
	b.AllowUnsupportedExpr = ot.Flags.AllowUnsupportedExpr
	if ot.Flags.FullyQualifyNames {
		b.FmtFlags = tree.FmtAlwaysQualifyTableNames
	}
	return b.Build()
}

func (ot *OptTester) optimizeExpr(
	allowNormalizations, allowExplorations bool,
) (memo.ExprView, error) {
	var o xform.Optimizer
	o.Init(&ot.evalCtx)
	if !allowExplorations {
		if allowNormalizations {
			o.DisableExplorations()
		} else {
			o.DisableOptimizations()
		}
	}
	root, required, err := ot.buildExpr(o.Factory())
	if err != nil {
		return memo.ExprView{}, err
	}
	return o.Optimize(root, required), nil
}

func (ot *OptTester) output(format string, args ...interface{}) {
	fmt.Fprintf(&ot.builder, format, args...)
	if ot.Flags.Verbose {
		fmt.Printf(format, args...)
	}
}

func (ot *OptTester) separator(sep string) {
	ot.output("%s\n", strings.Repeat(sep, 80))
}

func (ot *OptTester) indent(str string) {
	str = strings.TrimRight(str, " \n\t\r")
	lines := strings.Split(str, "\n")
	for _, line := range lines {
		ot.output("  %s\n", line)
	}
}
