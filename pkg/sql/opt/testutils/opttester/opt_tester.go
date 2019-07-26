// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opttester

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	_ "github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder" // for ExprFmtHideScalars.
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/exprgen"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/pmezard/go-difflib/difflib"
)

const rewriteActualFlag = "rewrite-actual-stats"

var (
	rewriteActualStats = flag.Bool(
		rewriteActualFlag, false,
		"used to update the actual statistics for statistics quality tests. If true, the opttester "+
			"will actually run the test queries to calculate actual statistics for comparison with the "+
			"estimated stats.",
	)
	pgurl = flag.String(
		"pgurl", "postgresql://localhost:26257/?sslmode=disable&user=root",
		"the database url to connect to",
	)
)

// RuleSet efficiently stores an unordered set of RuleNames.
type RuleSet = util.FastIntSet

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
	Flags Flags

	catalog   cat.Catalog
	sql       string
	ctx       context.Context
	semaCtx   tree.SemaContext
	evalCtx   tree.EvalContext
	seenRules RuleSet

	builder strings.Builder
}

// Flags are control knobs for tests. Note that specific testcases can
// override these defaults.
type Flags struct {
	// ExprFormat controls the output detail of build / opt/ optsteps command
	// directives.
	ExprFormat memo.ExprFmtFlags

	// MemoFormat controls the output detail of memo command directives.
	MemoFormat xform.FmtFlags

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

	// DisableRules is a set of rules that are not allowed to run.
	DisableRules RuleSet

	// ExploreTraceRule restricts the ExploreTrace output to only show the effects
	// of a specific rule.
	ExploreTraceRule opt.RuleName

	// ExpectedRules is a set of rules which must be exercised for the test to
	// pass.
	ExpectedRules RuleSet

	// UnexpectedRules is a set of rules which must not be exercised for the test
	// to pass.
	UnexpectedRules RuleSet

	// ColStats is a list of ColSets for which a column statistic is requested.
	ColStats []opt.ColSet

	// PerturbCost indicates how much to randomly perturb the cost. It is used
	// to generate alternative plans for testing. For example, if PerturbCost is
	// 0.5, and the estimated cost of an expression is c, the cost returned by
	// the coster will be in the range [c - 0.5 * c, c + 0.5 * c).
	PerturbCost float64

	// ReorderJoinsLimit is the maximum number of joins in a query which the optimizer
	// should attempt to reorder.
	JoinLimit int

	// Locality specifies the location of the planning node as a set of user-
	// defined key/value pairs, ordered from most inclusive to least inclusive.
	// If there are no tiers, then the node's location is not known. Examples:
	//
	//   [region=eu]
	//   [region=us,dc=east]
	//
	Locality roachpb.Locality

	// Database specifies the current database to use for the query. This field
	// is only used by the save-tables command when rewriteActualFlag=true.
	Database string

	// Table specifies the current table to use for the command. This field
	// is only used by the stats command.
	Table string

	// SaveTablesPrefix specifies the prefix of the table to create or print
	// for each subexpression in the query.
	SaveTablesPrefix string

	// File specifies the name of the file to import. This field is only used by
	// the import command.
	File string
}

// New constructs a new instance of the OptTester for the given SQL statement.
// Metadata used by the SQL query is accessed via the catalog.
func New(catalog cat.Catalog, sql string) *OptTester {
	ot := &OptTester{
		catalog: catalog,
		sql:     sql,
		ctx:     context.Background(),
		semaCtx: tree.MakeSemaContext(),
		evalCtx: tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings()),
	}

	// Set any OptTester-wide session flags here.

	ot.evalCtx.SessionData.ZigzagJoinEnabled = true
	ot.evalCtx.SessionData.OptimizerFKs = true
	ot.evalCtx.SessionData.ReorderJoinsLimit = opt.DefaultJoinOrderLimit

	return ot
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
//  - expr
//
//    Builds an expression directly from an opt-gen-like string; see
//    exprgen.Build.
//
//  - save-tables [flags]
//
//    Fully optimizes the given query and saves the subexpressions as tables
//    in the test catalog with their estimated statistics injected.
//    If rewriteActualFlag=true, also executes the given query against a
//    running database and saves the intermediate results as tables.
//
//  - stats [flags]
//
//    Compares estimated statistics for a relational expression with the actual
//    statistics calculated by calling CREATE STATISTICS on the output of the
//    expression. save-tables must have been called previously to save the
//    target expression as a table. The name of this table must be provided
//    with the table flag.
//
//  - import [flags]
//
//    Imports a file containing exec-ddl commands in order to add tables and/or
//    stats to the catalog. This allows commonly-used schemas such as TPC-C or
//    TPC-H to be used by multiple test files without copying the schemas and
//    stats multiple times. The file name must be provided with the file flag.
//    The path of the file should be relative to testutils/opttester/testdata.
//
// Supported flags:
//
//  - format: controls the formatting of expressions for build, opt, and
//    optsteps commands. Possible values: show-all, hide-all, or any combination
//    of hide-cost, hide-stats, hide-constraints, hide-scalars, hide-qual.
//    For example:
//      build format=(hide-cost,hide-stats)
//
//  - allow-unsupported: wrap unsupported expressions in UnsupportedOp.
//
//  - fully-qualify-names: fully qualify all column names in the test output.
//
//  - expect: fail the test if the rules specified by name do not match.
//
//  - expect-not: fail the test if the rules specified by name match.
//
//  - disable: disables optimizer rules by name. Examples:
//      opt disable=ConstrainScan
//      norm disable=(NegateOr,NegateAnd)
//
//  - rule: used with exploretrace; the value is the name of a rule. When
//    specified, the exploretrace output is filtered to only show expression
//    changes due to that specific rule.
//
//  - colstat: requests the calculation of a column statistic on the top-level
//    expression. The value is a column or a list of columns. The flag can
//    be used multiple times to request different statistics.
//
//  - perturb-cost: used to randomly perturb the estimated cost of each
//    expression in the query tree for the purpose of creating alternate query
//    plans in the optimizer.
//
//  - locality: used to set the locality of the node that plans the query. This
//    can affect costing when there are multiple possible indexes to choose
//    from, each in different localities.
//
//  - database: used to set the current database used by the query. This is
//    used by the save-tables command when rewriteActualFlag=true.
//
//  - table: used to set the current table used by the command. This is used by
//    the stats command.
//
//  - save-tables-prefix: must be used with the save-tables command. If
//    rewriteActualFlag=true, indicates that a table should be created with the
//    given prefix for the output of each subexpression in the query.
//    Otherwise, outputs the name of the table that would be created for each
//    subexpression.
//
//  - file: used to set the name of the file to be imported. This is used by
//    the import command.
//
func (ot *OptTester) RunCommand(tb testing.TB, d *datadriven.TestData) string {
	// Allow testcases to override the flags.
	for _, a := range d.CmdArgs {
		if err := ot.Flags.Set(a); err != nil {
			d.Fatalf(tb, "%+v", err)
		}
	}

	if ot.Flags.JoinLimit != 0 {
		defer func(oldValue int) {
			ot.evalCtx.SessionData.ReorderJoinsLimit = oldValue
		}(ot.evalCtx.SessionData.ReorderJoinsLimit)
		ot.evalCtx.SessionData.ReorderJoinsLimit = ot.Flags.JoinLimit
	}

	ot.Flags.Verbose = testing.Verbose()
	ot.evalCtx.TestingKnobs.OptimizerCostPerturbation = ot.Flags.PerturbCost
	ot.evalCtx.Locality = ot.Flags.Locality
	ot.evalCtx.SessionData.SaveTablesPrefix = ot.Flags.SaveTablesPrefix

	switch d.Cmd {
	case "exec-ddl":
		testCatalog, ok := ot.catalog.(*testcat.Catalog)
		if !ok {
			d.Fatalf(tb, "exec-ddl can only be used with TestCatalog")
		}
		s, err := testCatalog.ExecuteDDL(d.Input)
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		return s

	case "build":
		e, err := ot.OptBuild()
		if err != nil {
			if errors.HasAssertionFailure(err) {
				d.Fatalf(tb, "%+v", err)
			}
			pgerr := pgerror.Flatten(err)
			text := strings.TrimSpace(pgerr.Error())
			if pgerr.Code != pgcode.Uncategorized {
				// Output Postgres error code if it's available.
				return fmt.Sprintf("error (%s): %s\n", pgerr.Code, text)
			}
			return fmt.Sprintf("error: %s\n", text)
		}
		ot.postProcess(tb, d, e)
		return memo.FormatExpr(e, ot.Flags.ExprFormat, ot.catalog)

	case "norm":
		e, err := ot.OptNorm()
		if err != nil {
			if errors.HasAssertionFailure(err) {
				d.Fatalf(tb, "%+v", err)
			}
			pgerr := pgerror.Flatten(err)
			text := strings.TrimSpace(pgerr.Error())
			if pgerr.Code != pgcode.Uncategorized {
				// Output Postgres error code if it's available.
				return fmt.Sprintf("error (%s): %s\n", pgerr.Code, text)
			}
			return fmt.Sprintf("error: %s\n", text)
		}
		ot.postProcess(tb, d, e)
		return memo.FormatExpr(e, ot.Flags.ExprFormat, ot.catalog)

	case "opt":
		e, err := ot.Optimize()
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		ot.postProcess(tb, d, e)
		return memo.FormatExpr(e, ot.Flags.ExprFormat, ot.catalog)

	case "optsteps":
		result, err := ot.OptSteps()
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		return result

	case "exploretrace":
		result, err := ot.ExploreTrace()
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		return result

	case "rulestats":
		result, err := ot.RuleStats()
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		return result

	case "memo":
		result, err := ot.Memo()
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		return result

	case "expr":
		e, err := ot.Expr()
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		ot.postProcess(tb, d, e)
		return memo.FormatExpr(e, ot.Flags.ExprFormat, ot.catalog)

	case "exprnorm":
		e, err := ot.ExprNorm()
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		ot.postProcess(tb, d, e)
		return memo.FormatExpr(e, ot.Flags.ExprFormat, ot.catalog)

	case "save-tables":
		e, err := ot.SaveTables()
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		ot.postProcess(tb, d, e)
		return memo.FormatExpr(e, ot.Flags.ExprFormat, ot.catalog)

	case "stats":
		result, err := ot.Stats(d)
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		return result

	case "import":
		ot.Import(tb)
		return ""

	default:
		d.Fatalf(tb, "unsupported command: %s", d.Cmd)
		return ""
	}
}

func formatRuleSet(r RuleSet) string {
	var buf bytes.Buffer
	comma := false
	for i, ok := r.Next(0); ok; i, ok = r.Next(i + 1) {
		if comma {
			buf.WriteString(", ")
		}
		comma = true
		fmt.Fprintf(&buf, "%v", opt.RuleName(i))
	}
	return buf.String()
}

func (ot *OptTester) postProcess(tb testing.TB, d *datadriven.TestData, e opt.Expr) {
	fillInLazyProps(e)

	if rel, ok := e.(memo.RelExpr); ok {
		for _, cols := range ot.Flags.ColStats {
			memo.RequestColStat(&ot.evalCtx, rel, cols)
		}
	}

	if !ot.Flags.ExpectedRules.SubsetOf(ot.seenRules) {
		unseen := ot.Flags.ExpectedRules.Difference(ot.seenRules)
		d.Fatalf(tb, "expected to see %s, but was not triggered. Did see %s",
			formatRuleSet(unseen), formatRuleSet(ot.seenRules))
	}

	if ot.Flags.UnexpectedRules.Intersects(ot.seenRules) {
		seen := ot.Flags.UnexpectedRules.Intersection(ot.seenRules)
		d.Fatalf(tb, "expected not to see %s, but it was triggered", formatRuleSet(seen))
	}
}

// Fills in lazily-derived properties (for display).
func fillInLazyProps(e opt.Expr) {
	if rel, ok := e.(memo.RelExpr); ok {
		// Derive columns that are candidates for pruning.
		norm.DerivePruneCols(rel)

		// Derive columns that are candidates for null rejection.
		norm.DeriveRejectNullCols(rel)

		// Make sure the interesting orderings are calculated.
		xform.DeriveInterestingOrderings(rel)
	}

	for i, n := 0, e.ChildCount(); i < n; i++ {
		fillInLazyProps(e.Child(i))
	}
}

func ruleNamesToRuleSet(args []string) (RuleSet, error) {
	var result RuleSet
	for _, r := range args {
		rn, err := ruleFromString(r)
		if err != nil {
			return result, err
		}
		result.Add(int(rn))
	}
	return result, nil
}

// Set parses an argument that refers to a flag.
// See OptTester.RunCommand for supported flags.
func (f *Flags) Set(arg datadriven.CmdArg) error {
	switch arg.Key {
	case "format":
		f.ExprFormat = 0
		if len(arg.Vals) == 0 {
			return fmt.Errorf("format flag requires value(s)")
		}
		for _, v := range arg.Vals {
			m := map[string]memo.ExprFmtFlags{
				"show-all":         memo.ExprFmtShowAll,
				"hide-miscprops":   memo.ExprFmtHideMiscProps,
				"hide-constraints": memo.ExprFmtHideConstraints,
				"hide-funcdeps":    memo.ExprFmtHideFuncDeps,
				"hide-ruleprops":   memo.ExprFmtHideRuleProps,
				"hide-stats":       memo.ExprFmtHideStats,
				"hide-cost":        memo.ExprFmtHideCost,
				"hide-qual":        memo.ExprFmtHideQualifications,
				"hide-scalars":     memo.ExprFmtHideScalars,
				"hide-orderings":   memo.ExprFmtHideOrderings,
				"hide-types":       memo.ExprFmtHideTypes,
				"hide-columns":     memo.ExprFmtHideColumns,
				"hide-all":         memo.ExprFmtHideAll,
			}
			if val, ok := m[v]; ok {
				f.ExprFormat |= val
			} else {
				return fmt.Errorf("unknown format value %s", v)
			}
		}

	case "allow-unsupported":
		f.AllowUnsupportedExpr = true

	case "fully-qualify-names":
		f.FullyQualifyNames = true
		// Hiding qualifications defeats the purpose.
		f.ExprFormat &= ^memo.ExprFmtHideQualifications

	case "disable":
		if len(arg.Vals) == 0 {
			return fmt.Errorf("disable requires arguments")
		}
		for _, s := range arg.Vals {
			r, err := ruleFromString(s)
			if err != nil {
				return err
			}
			f.DisableRules.Add(int(r))
		}

	case "join-limit":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("join-limit requires a single argument")
		}
		limit, err := strconv.ParseInt(arg.Vals[0], 10, 64)
		if err != nil {
			return errors.Wrap(err, "join-limit")
		}
		f.JoinLimit = int(limit)

	case "rule":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("rule requires one argument")
		}
		var err error
		f.ExploreTraceRule, err = ruleFromString(arg.Vals[0])
		if err != nil {
			return err
		}

	case "expect":
		var err error
		if f.ExpectedRules, err = ruleNamesToRuleSet(arg.Vals); err != nil {
			return err
		}

	case "expect-not":
		var err error
		if f.UnexpectedRules, err = ruleNamesToRuleSet(arg.Vals); err != nil {
			return err
		}

	case "colstat":
		if len(arg.Vals) == 0 {
			return fmt.Errorf("colstat requires arguments")
		}
		var cols opt.ColSet
		for _, v := range arg.Vals {
			col, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("invalid colstat column %v", v)
			}
			cols.Add(opt.ColumnID(col))
		}
		f.ColStats = append(f.ColStats, cols)

	case "perturb-cost":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("perturb-cost requires one argument")
		}
		var err error
		f.PerturbCost, err = strconv.ParseFloat(arg.Vals[0], 64)
		if err != nil {
			return err
		}

	case "locality":
		// Recombine multiple arguments, separated by commas.
		locality := strings.Join(arg.Vals, ",")
		err := f.Locality.Set(locality)
		if err != nil {
			return err
		}

	case "database":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("database requires one argument")
		}
		f.Database = arg.Vals[0]

	case "table":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("table requires one argument")
		}
		f.Table = arg.Vals[0]

	case "save-tables-prefix":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("save-tables-prefix requires one argument")
		}
		f.SaveTablesPrefix = arg.Vals[0]

	case "file":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("file requires one argument")
		}
		f.File = arg.Vals[0]

	default:
		return fmt.Errorf("unknown argument: %s", arg.Key)
	}
	return nil
}

// OptBuild constructs an opt expression tree for the SQL query, with no
// transformations applied to it. The untouched output of the optbuilder is the
// final expression tree.
func (ot *OptTester) OptBuild() (opt.Expr, error) {
	o := ot.makeOptimizer()
	o.DisableOptimizations()
	return ot.optimizeExpr(o)
}

// OptNorm constructs an opt expression tree for the SQL query, with all
// normalization transformations applied to it. The normalized output of the
// optbuilder is the final expression tree.
func (ot *OptTester) OptNorm() (opt.Expr, error) {
	o := ot.makeOptimizer()
	o.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		if !ruleName.IsNormalize() {
			return false
		}
		if ot.Flags.DisableRules.Contains(int(ruleName)) {
			return false
		}
		ot.seenRules.Add(int(ruleName))
		return true
	})
	return ot.optimizeExpr(o)
}

// Optimize constructs an opt expression tree for the SQL query, with all
// transformations applied to it. The result is the memo expression tree with
// the lowest estimated cost.
func (ot *OptTester) Optimize() (opt.Expr, error) {
	o := ot.makeOptimizer()
	o.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		if ot.Flags.DisableRules.Contains(int(ruleName)) {
			return false
		}
		ot.seenRules.Add(int(ruleName))
		return true
	})
	return ot.optimizeExpr(o)
}

// Memo returns a string that shows the memo data structure that is constructed
// by the optimizer.
func (ot *OptTester) Memo() (string, error) {
	var o xform.Optimizer
	o.Init(&ot.evalCtx)
	if _, err := ot.optimizeExpr(&o); err != nil {
		return "", err
	}
	return o.FormatMemo(ot.Flags.MemoFormat), nil
}

// Expr parses the input directly into an expression; see exprgen.Build.
func (ot *OptTester) Expr() (opt.Expr, error) {
	var f norm.Factory
	f.Init(&ot.evalCtx)
	f.DisableOptimizations()

	return exprgen.Build(ot.catalog, &f, ot.sql)
}

// ExprNorm parses the input directly into an expression and runs
// normalization; see exprgen.Build.
func (ot *OptTester) ExprNorm() (opt.Expr, error) {
	var f norm.Factory
	f.Init(&ot.evalCtx)

	f.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		// exprgen.Build doesn't run optimization, so we don't need to explicitly
		// disallow exploration rules here.

		if ot.Flags.DisableRules.Contains(int(ruleName)) {
			return false
		}
		ot.seenRules.Add(int(ruleName))
		return true
	})

	return exprgen.Build(ot.catalog, &f, ot.sql)
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

	o := ot.makeOptimizer()
	o.NotifyOnAppliedRule(
		func(ruleName opt.RuleName, source, target opt.Expr) {
			stats[ruleName].numApplied++
			if target != nil {
				stats[ruleName].numAdded++
				if rel, ok := target.(memo.RelExpr); ok {
					for {
						rel = rel.NextExpr()
						if rel == nil {
							break
						}
						stats[ruleName].numAdded++
					}
				}
			}
		},
	)
	if _, err := ot.optimizeExpr(o); err != nil {
		return "", err
	}

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
		err := os.Next()
		if err != nil {
			return "", err
		}

		next = memo.FormatExpr(os.Root(), ot.Flags.ExprFormat, ot.catalog)

		// This call comes after setting "next", because we want to output the
		// final expression, even though there were no diffs from the previous
		// iteration.
		if os.Done() {
			break
		}

		if prev == "" {
			// Output starting tree.
			ot.optStepsDisplay("", next, os)
			prevBest = next
		} else if next == prev || next == prevBest {
			ot.optStepsDisplay(next, next, os)
		} else if os.IsBetter() {
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
	bestHeader := func(e opt.Expr, format string, args ...interface{}) {
		ot.separator("=")
		ot.output(format, args...)
		if rel, ok := e.(memo.RelExpr); ok {
			ot.output("  Cost: %.2f\n", rel.Cost())
		} else {
			ot.output("\n")
		}
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
		bestHeader(os.Root(), "Initial expression\n")
		ot.indent(after)
		return
	}

	if before == after {
		altHeader("%s (no changes)\n", os.LastRuleName())
		return
	}

	if after == "" {
		bestHeader(os.Root(), "Final best expression\n")
		ot.indent(before)

		if ot.Flags.Verbose {
			fmt.Print("------ optsteps verbose output ends ------\n")
		}
		return
	}

	var diff difflib.UnifiedDiff
	if os.IsBetter() {
		// New expression is better than the previous expression. Diff
		// it against the previous *best* expression (might not be the
		// previous expression).
		bestHeader(os.Root(), "%s\n", os.LastRuleName())
	} else {
		altHeader("%s (higher cost)\n", os.LastRuleName())
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

		if ot.Flags.ExploreTraceRule != opt.InvalidRuleName &&
			et.LastRuleName() != ot.Flags.ExploreTraceRule {
			continue
		}

		if ot.builder.Len() > 0 {
			ot.output("\n")
		}
		ot.separator("=")
		ot.output("%s\n", et.LastRuleName())
		ot.separator("=")
		ot.output("Source expression:\n")
		ot.indent(memo.FormatExpr(et.SrcExpr(), ot.Flags.ExprFormat, ot.catalog))
		newNodes := et.NewExprs()
		if len(newNodes) == 0 {
			ot.output("\nNo new expressions.\n")
		}
		for i := range newNodes {
			ot.output("\nNew expression %d of %d:\n", i+1, len(newNodes))
			ot.indent(memo.FormatExpr(newNodes[i], ot.Flags.ExprFormat, ot.catalog))
		}
	}
	return ot.builder.String(), nil
}

// Stats compares the estimated statistics of a relational expression with
// actual statistics collected from running CREATE STATISTICS on the output
// of the relational expression. If the -rewrite-actual-stats flag is
// used, the actual stats are recalculated.
func (ot *OptTester) Stats(d *datadriven.TestData) (string, error) {
	catalog, ok := ot.catalog.(*testcat.Catalog)
	if !ok {
		return "", fmt.Errorf("stats can only be used with TestCatalog")
	}

	st := statsTester{}
	return st.testStats(catalog, d, ot.Flags.Table)
}

// Import imports a file containing exec-ddl commands in order to add tables
// and/or stats to the catalog. This allows commonly-used schemas such as
// TPC-C or TPC-H to be used by multiple test files without copying the schemas
// and stats multiple times.
func (ot *OptTester) Import(tb testing.TB) {
	// Find the file to be imported in opttester/testdata.
	_, optTesterFile, _, ok := runtime.Caller(1)
	if !ok {
		tb.Fatalf("unable to find file %s", ot.Flags.File)
	}
	path := filepath.Join(filepath.Dir(optTesterFile), "testdata", ot.Flags.File)
	datadriven.RunTest(tb.(*testing.T), path, func(d *datadriven.TestData) string {
		tester := New(ot.catalog, d.Input)
		return tester.RunCommand(tb.(*testing.T), d)
	})
}

// SaveTables optimizes the given query and saves the subexpressions as tables
// in the test catalog with their estimated statistics injected.
// If rewriteActualStats=true, it also executes the given query against a
// running database and saves the intermediate results as tables.
func (ot *OptTester) SaveTables() (opt.Expr, error) {
	if *rewriteActualStats {
		if err := ot.saveActualTables(); err != nil {
			return nil, err
		}
	}

	expr, err := ot.Optimize()
	if err != nil {
		return nil, err
	}

	// Create a table in the test catalog for each relational expression in the
	// tree.
	nameGen := memo.NewExprNameGenerator(ot.Flags.SaveTablesPrefix)
	var traverse func(e opt.Expr) error
	traverse = func(e opt.Expr) error {
		if r, ok := e.(memo.RelExpr); ok {
			// GenerateName is called in a pre-order traversal of the query tree.
			tabName := nameGen.GenerateName(e.Op())
			_, err := ot.createTableAs(tree.MakeUnqualifiedTableName(tree.Name(tabName)), r)
			if err != nil {
				return err
			}
		}
		for i, n := 0, e.ChildCount(); i < n; i++ {
			if err := traverse(e.Child(i)); err != nil {
				return err
			}
		}
		return nil
	}
	if err := traverse(expr); err != nil {
		return nil, err
	}

	return expr, nil
}

// saveActualTables executes the given query against a running database and
// saves the intermediate results as tables.
func (ot *OptTester) saveActualTables() error {
	db, err := gosql.Open("postgres", *pgurl)
	if err != nil {
		return errors.Wrap(err,
			"can only execute a statement when pointed at a running Cockroach cluster",
		)
	}

	ctx := context.Background()
	c, err := db.Conn(ctx)
	if err != nil {
		return err
	}

	if _, err := c.ExecContext(ctx,
		fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", opt.SaveTablesDatabase),
	); err != nil {
		return err
	}

	if _, err := c.ExecContext(ctx,
		fmt.Sprintf("CREATE DATABASE %s", opt.SaveTablesDatabase),
	); err != nil {
		return err
	}

	if _, err := c.ExecContext(ctx, fmt.Sprintf("USE %s", ot.Flags.Database)); err != nil {
		return err
	}

	if _, err := c.ExecContext(ctx,
		fmt.Sprintf("SET save_tables_prefix = '%s'", ot.Flags.SaveTablesPrefix),
	); err != nil {
		return err
	}

	if _, err := c.ExecContext(ctx, ot.sql); err != nil {
		return err
	}

	return nil
}

// createTableAs creates a table in the test catalog based on the output
// of the given relational expression. It also injects the estimated stats
// for the relational expression into the catalog table. It returns a pointer
// to the new table.
func (ot *OptTester) createTableAs(name tree.TableName, rel memo.RelExpr) (*testcat.Table, error) {
	catalog, ok := ot.catalog.(*testcat.Catalog)
	if !ok {
		return nil, fmt.Errorf("createTableAs can only be used with TestCatalog")
	}

	relProps := rel.Relational()
	outputCols := relProps.OutputCols
	colNameGen := memo.NewColumnNameGenerator(rel)

	// Create each of the columns and their estimated stats for the test catalog
	// table.
	columns := make([]*testcat.Column, outputCols.Len())
	jsonStats := make([]stats.JSONStatistic, outputCols.Len())
	i := 0
	for col, ok := outputCols.Next(0); ok; col, ok = outputCols.Next(col + 1) {
		colMeta := rel.Memo().Metadata().ColumnMeta(opt.ColumnID(col))
		colName := colNameGen.GenerateName(opt.ColumnID(col))

		columns[i] = &testcat.Column{
			Ordinal:  i,
			Name:     colName,
			Type:     colMeta.Type,
			ColType:  *colMeta.Type,
			Nullable: !relProps.NotNullCols.Contains(col),
		}

		// Make sure we have estimated stats for this column.
		colSet := opt.MakeColSet(col)
		memo.RequestColStat(&ot.evalCtx, rel, colSet)
		stat, ok := relProps.Stats.ColStats.Lookup(colSet)
		if !ok {
			return nil, fmt.Errorf("could not find statistic for column %s", colName)
		}
		jsonStats[i] = ot.makeStat(
			[]string{colName},
			uint64(int64(math.Round(relProps.Stats.RowCount))),
			uint64(int64(math.Round(stat.DistinctCount))),
			uint64(int64(math.Round(stat.NullCount))),
		)

		i++
	}

	tab := catalog.CreateTableAs(name, columns)
	if err := ot.injectStats(name, jsonStats); err != nil {
		return nil, err
	}
	return tab, nil
}

// injectStats injects statistics into the given table in the test catalog.
func (ot *OptTester) injectStats(name tree.TableName, jsonStats []stats.JSONStatistic) error {
	catalog, ok := ot.catalog.(*testcat.Catalog)
	if !ok {
		return fmt.Errorf("injectStats can only be used with TestCatalog")
	}

	encoded, err := json.Marshal(jsonStats)
	if err != nil {
		return err
	}
	alterStmt := fmt.Sprintf("ALTER TABLE %s INJECT STATISTICS '%s'", name.String(), encoded)
	stmt, err := parser.ParseOne(alterStmt)
	if err != nil {
		return err
	}
	catalog.AlterTable(stmt.AST.(*tree.AlterTable))
	return nil
}

// makeStat creates a JSONStatistic for the given columns, rowCount,
// distinctCount, and nullCount.
func (ot *OptTester) makeStat(
	columns []string, rowCount, distinctCount, nullCount uint64,
) stats.JSONStatistic {
	return stats.JSONStatistic{
		Name: stats.AutoStatsName,
		CreatedAt: tree.AsStringWithFlags(
			&tree.DTimestamp{Time: timeutil.Now()}, tree.FmtBareStrings,
		),
		Columns:       columns,
		RowCount:      rowCount,
		DistinctCount: distinctCount,
		NullCount:     nullCount,
	}
}

func (ot *OptTester) buildExpr(factory *norm.Factory) error {
	stmt, err := parser.ParseOne(ot.sql)
	if err != nil {
		return err
	}
	if err := ot.semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
		return err
	}
	ot.semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
	b := optbuilder.New(ot.ctx, &ot.semaCtx, &ot.evalCtx, ot.catalog, factory, stmt.AST)
	b.AllowUnsupportedExpr = ot.Flags.AllowUnsupportedExpr
	return b.Build()
}

func (ot *OptTester) makeOptimizer() *xform.Optimizer {
	var o xform.Optimizer
	o.Init(&ot.evalCtx)
	return &o
}

func (ot *OptTester) optimizeExpr(o *xform.Optimizer) (opt.Expr, error) {
	err := ot.buildExpr(o.Factory())
	if err != nil {
		return nil, err
	}
	root, err := o.Optimize()
	if err != nil {
		return nil, err
	}
	if ot.Flags.PerturbCost != 0 {
		o.RecomputeCost()
	}
	return root, nil
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

// ruleFromString returns the rule that matches the given string,
// or InvalidRuleName if there is no such rule.
func ruleFromString(str string) (opt.RuleName, error) {
	for i := opt.RuleName(1); i < opt.NumRuleNames; i++ {
		if i.String() == str {
			return i, nil
		}
	}

	return opt.InvalidRuleName, fmt.Errorf("rule '%s' does not exist", str)
}
