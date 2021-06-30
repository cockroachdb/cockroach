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
	"compress/zlib"
	"context"
	gosql "database/sql"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"net/url"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	_ "github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder" // for ExprFmtHideScalars.
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/exprgen"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
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

	formatFlags = map[string]memo.ExprFmtFlags{
		"miscprops":   memo.ExprFmtHideMiscProps,
		"constraints": memo.ExprFmtHideConstraints,
		"funcdeps":    memo.ExprFmtHideFuncDeps,
		"ruleprops":   memo.ExprFmtHideRuleProps,
		"stats":       memo.ExprFmtHideStats,
		"cost":        memo.ExprFmtHideCost,
		"qual":        memo.ExprFmtHideQualifications,
		"scalars":     memo.ExprFmtHideScalars,
		"physprops":   memo.ExprFmtHidePhysProps,
		"types":       memo.ExprFmtHideTypes,
		"notnull":     memo.ExprFmtHideNotNull,
		"columns":     memo.ExprFmtHideColumns,
		"all":         memo.ExprFmtHideAll,
	}
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

	catalog      cat.Catalog
	sql          string
	ctx          context.Context
	semaCtx      tree.SemaContext
	evalCtx      tree.EvalContext
	appliedRules RuleSet

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

	// ExploreTraceSkipNoop hides the ExploreTrace output for instances of rules
	// that fire but don't add any new expressions to the memo.
	ExploreTraceSkipNoop bool

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

	// JoinLimit is the default value for SessionData.ReorderJoinsLimit.
	JoinLimit int

	// PreferLookupJoinsForFK is the default value for
	// SessionData.PreferLookupJoinsForFKs.
	PreferLookupJoinsForFKs bool

	// Locality specifies the location of the planning node as a set of user-
	// defined key/value pairs, ordered from most inclusive to least inclusive.
	// If there are no tiers, then the node's location is not known. Examples:
	//
	//   [region=eu]
	//   [region=us,dc=east]
	//
	Locality roachpb.Locality

	// Database specifies the current database to use for the query. This field
	// is only used by the stats-quality command when rewriteActualFlag=true.
	Database string

	// Table specifies the current table to use for the command. This field is
	// only used by the inject-stats commands.
	Table string

	// SaveTablesPrefix specifies the prefix of the table to create or print
	// for each subexpression in the query.
	SaveTablesPrefix string

	// IgnoreTables specifies the subset of stats tables which should not be
	// outputted by the stats-quality command.
	IgnoreTables util.FastIntSet

	// File specifies the name of the file to import. This field is only used by
	// the import command.
	File string

	// CascadeLevels limits the depth of recursive cascades for build-cascades.
	CascadeLevels int

	// NoStableFolds controls whether constant folding for normalization includes
	// stable operators.
	NoStableFolds bool

	// IndexVersion controls the version of the index descriptor created in the
	// test catalog. This field is only used by the exec-ddl command for CREATE
	// INDEX statements.
	IndexVersion descpb.IndexDescriptorVersion

	// OptStepsSplitDiff, if true, replaces the unified diff output of the
	// optsteps command with a split diff where the before and after expressions
	// are printed in their entirety. The default value is false.
	OptStepsSplitDiff bool

	// RuleApplicationLimit is used by the check-size command to check whether
	// more than RuleApplicationLimit rules are applied during optimization.
	RuleApplicationLimit int64

	// MemoGroupLimit is used by the check-size command to check whether
	// more than MemoGroupLimit memo groups are constructed during optimization.
	MemoGroupLimit int64

	// QueryArgs are values for placeholders, used for assign-placeholders-*.
	QueryArgs []string
}

// New constructs a new instance of the OptTester for the given SQL statement.
// Metadata used by the SQL query is accessed via the catalog.
func New(catalog cat.Catalog, sql string) *OptTester {
	ctx := context.Background()
	ot := &OptTester{
		Flags:   Flags{JoinLimit: opt.DefaultJoinOrderLimit},
		catalog: catalog,
		sql:     sql,
		ctx:     ctx,
		semaCtx: tree.MakeSemaContext(),
		evalCtx: tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings()),
	}
	// To allow opttester tests to use now(), we hardcode a preset transaction
	// time. May 10, 2017 is a historic day: the release date of CockroachDB 1.0.
	ot.evalCtx.TxnTimestamp = time.Date(2017, 05, 10, 13, 0, 0, 0, time.UTC)

	// Set any OptTester-wide session flags here.

	ot.evalCtx.SessionData.UserProto = security.MakeSQLUsernameFromPreNormalizedString("opttester").EncodeProto()
	ot.evalCtx.SessionData.Database = "defaultdb"
	ot.evalCtx.SessionData.ZigzagJoinEnabled = true
	ot.evalCtx.SessionData.OptimizerUseHistograms = true
	ot.evalCtx.SessionData.OptimizerUseMultiColStats = true
	ot.evalCtx.SessionData.LocalityOptimizedSearch = true
	ot.evalCtx.SessionData.ReorderJoinsLimit = opt.DefaultJoinOrderLimit
	ot.evalCtx.SessionData.InsertFastPath = true

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
//  - assign-placeholders-norm query-args=(...)
//
//    Builds a query that has placeholders (with normalization enabled), then
//    assigns placeholders to the given query arguments. Normalization rules are
//    enabled when assigning placeholders.
//
//  - assign-placeholders-opt query-args=(...)
//
//    Builds a query that has placeholders (with normalization enabled), then
//    assigns placeholders to the given query arguments and fully optimizes it.
//
//  - placeholder-fast-path [flags]
//
//    Builds an expression tree from a SQL query which contains placeholders and
//    attempts to use the placeholder fast path to obtain a fully optimized
//    expression with placeholders.
//
//  - build-cascades [flags]
//
//    Builds a query and then recursively builds cascading queries. Outputs all
//    unoptimized plans.
//
//  - optsteps [flags]
//
//    Outputs the lowest cost tree for each step in optimization using the
//    standard unified diff format. Used for debugging the optimizer.
//
//  - optstepsweb [flags]
//
//    Similar to optsteps, but outputs a URL which displays the results.
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
//  - exprnorm
//
//    Builds an expression directly from an opt-gen-like string (see
//    exprgen.Build), applies normalization optimizations, and outputs the tree
//    without any exploration optimizations applied to it.
//
//  - stats-quality [flags]
//
//    Fully optimizes the given query and saves the subexpressions as tables
//    in the test catalog with their estimated statistics injected.
//    If rewriteActualFlag=true, also executes the given query against a
//    running database and saves the intermediate results as tables.
//    Compares estimated statistics for a relational expression with the actual
//    statistics calculated by calling CREATE STATISTICS on the output of the
//    expression. If rewriteActualFlag=false, stats-quality must have been run
//    previously with rewriteActualFlag=true to save the statistics as tables.
//
//  - reorderjoins [flags]
//
//    Fully optimizes the given query and outputs information from
//    joinOrderBuilder during join reordering. See the ReorderJoins comment in
//    reorder_joins.go for information on the output format.
//
//  - import file=...
//
//    Imports a file containing exec-ddl commands in order to add tables and/or
//    stats to the catalog. This allows commonly-used schemas such as TPC-C or
//    TPC-H to be used by multiple test files without copying the schemas and
//    stats multiple times. The file name must be provided with the file flag.
//    The path of the file should be relative to
//    testutils/opttester/testfixtures.
//
//  - inject-stats file=... table=...
//
//    Injects table statistics from a json file.
//
//  - check-size [rule-limit=...] [group-limit=...]
//
//    Fully optimizes the given query and outputs the number of rules applied
//    and memo groups created. If the rule-limit or group-limit flags are set,
//    check-size will result in a test error if the rule application or memo
//    group count exceeds the corresponding limit.
//
// Supported flags:
//
//  - format: controls the formatting of expressions for build, opt, and
//    optsteps commands. Format flags are of the form
//      (show|hide)-(all|miscprops|constraints|scalars|types|...)
//    See formatFlags for all flags. Multiple flags can be specified; each flag
//    modifies the existing set of the flags.
//
//  - no-stable-folds: disallows constant folding for stable operators; only
//                     used with "norm".
//
//  - fully-qualify-names: fully qualify all column names in the test output.
//
//  - expect: fail the test if the rules specified by name are not "applied".
//    For normalization rules, "applied" means that the rule's pattern matched
//    an expression. For exploration rules, "applied" means that the rule's
//    pattern matched an expression and the rule generated one or more new
//    expressions in the memo.
//
//  - expect-not: fail the test if the rules specified by name are "applied".
//
//  - disable: disables optimizer rules by name. Examples:
//      opt disable=ConstrainScan
//      norm disable=(NegateOr,NegateAnd)
//
//  - rule: used with exploretrace; the value is the name of a rule. When
//    specified, the exploretrace output is filtered to only show expression
//    changes due to that specific rule.
//
//  - skip-no-op: used with exploretrace; hide instances of rules that don't
//    generate any new expressions.
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
//    used by the stats-quality command when rewriteActualFlag=true.
//
//  - table: used to set the current table used by the command. This is used by
//    the inject-stats command.
//
//  - stats-quality-prefix: must be used with the stats-quality command. If
//    rewriteActualFlag=true, indicates that a table should be created with the
//    given prefix for the output of each subexpression in the query. Otherwise,
//    outputs the name of the table that would be created for each
//    subexpression.
//
//  - ignore-tables: specifies the set of stats tables for which stats quality
//    comparisons should not be outputted. Only used with the stats-quality
//    command. Note that tables can always be added to the `ignore-tables` set
//    without necessitating a run with `rewrite-actual-stats=true`, because the
//    now-ignored stats outputs will simply be removed. However, the reverse is
//    not possible. So, the best way to rewrite a stats quality test for which
//    the plan has changed is to first remove the `ignore-tables` flag, then add
//    it back and do a normal rewrite to remove the superfluous tables.
//
//  - file: specifies a file, used for the following commands:
//     - import: the file path is relative to opttester/testfixtures;
//     - inject-stats: the file path is relative to the test file.
//
//  - cascade-levels: used to limit the depth of recursive cascades for
//    build-cascades.
//
//  - index-version: controls the version of the index descriptor created in
//    the test catalog. This is used by the exec-ddl command for CREATE INDEX
//    statements.
//
//  - split-diff: replaces the unified diff output of the optsteps command with
//    a split diff where the before and after expressions are printed in their
//    entirety. This is only used by the optsteps command.
//
//  - rule-limit: used with check-size to set a max limit on the number of rules
//    that can be applied before a testing error is returned.
//
//  - group-limit: used with check-size to set a max limit on the number of
//    groups that can be added to the memo before a testing error is returned.
//
func (ot *OptTester) RunCommand(tb testing.TB, d *datadriven.TestData) string {
	// Allow testcases to override the flags.
	for _, a := range d.CmdArgs {
		if err := ot.Flags.Set(a); err != nil {
			d.Fatalf(tb, "%+v", err)
		}
	}
	ot.Flags.Verbose = datadriven.Verbose()

	ot.semaCtx.Placeholders = tree.PlaceholderInfo{}

	ot.evalCtx.SessionData.ReorderJoinsLimit = ot.Flags.JoinLimit
	ot.evalCtx.SessionData.PreferLookupJoinsForFKs = ot.Flags.PreferLookupJoinsForFKs

	ot.evalCtx.TestingKnobs.OptimizerCostPerturbation = ot.Flags.PerturbCost
	ot.evalCtx.Locality = ot.Flags.Locality
	ot.evalCtx.SessionData.SaveTablesPrefix = ot.Flags.SaveTablesPrefix
	ot.evalCtx.Placeholders = nil

	switch d.Cmd {
	case "exec-ddl":
		testCatalog, ok := ot.catalog.(*testcat.Catalog)
		if !ok {
			d.Fatalf(tb, "exec-ddl can only be used with TestCatalog")
		}
		var s string
		var err error
		if ot.Flags.IndexVersion != 0 {
			s, err = testCatalog.ExecuteDDLWithIndexVersion(d.Input, ot.Flags.IndexVersion)
		} else {
			s, err = testCatalog.ExecuteDDL(d.Input)
		}
		if err != nil {
			d.Fatalf(tb, "%v", err)
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
			if pgcode.MakeCode(pgerr.Code) != pgcode.Uncategorized {
				// Output Postgres error code if it's available.
				return fmt.Sprintf("error (%s): %s\n", pgerr.Code, text)
			}
			return fmt.Sprintf("error: %s\n", text)
		}
		ot.postProcess(tb, d, e)
		return ot.FormatExpr(e)

	case "norm":
		e, err := ot.OptNorm()
		if err != nil {
			if errors.HasAssertionFailure(err) {
				d.Fatalf(tb, "%+v", err)
			}
			pgerr := pgerror.Flatten(err)
			text := strings.TrimSpace(pgerr.Error())
			if pgcode.MakeCode(pgerr.Code) != pgcode.Uncategorized {
				// Output Postgres error code if it's available.
				return fmt.Sprintf("error (%s): %s\n", pgerr.Code, text)
			}
			return fmt.Sprintf("error: %s\n", text)
		}
		ot.postProcess(tb, d, e)
		return ot.FormatExpr(e)

	case "opt":
		e, err := ot.Optimize()
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		ot.postProcess(tb, d, e)
		return ot.FormatExpr(e)

	case "assign-placeholders-norm", "assign-placeholders-opt":
		explore := d.Cmd == "assign-placeholders-opt"
		e, err := ot.AssignPlaceholders(ot.Flags.QueryArgs, explore)
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		ot.postProcess(tb, d, e)
		return ot.FormatExpr(e)

	case "placeholder-fast-path":
		e, ok, err := ot.PlaceholderFastPath()
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		if !ok {
			return "no fast path"
		}
		return ot.FormatExpr(e)

	case "build-cascades":
		o := ot.makeOptimizer()
		o.DisableOptimizations()
		if err := ot.buildExpr(o.Factory()); err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		e := o.Memo().RootExpr()

		var buildCascades func(e opt.Expr, tp treeprinter.Node, level int)
		buildCascades = func(e opt.Expr, tp treeprinter.Node, level int) {
			if ot.Flags.CascadeLevels != 0 && level > ot.Flags.CascadeLevels {
				return
			}
			if opt.IsMutationOp(e) {
				p := e.Private().(*memo.MutationPrivate)

				for _, c := range p.FKCascades {
					// We use the same memo to build the cascade. This makes the entire
					// tree easier to read (e.g. the column IDs won't overlap).
					cascade, err := c.Builder.Build(
						context.Background(),
						&ot.semaCtx,
						&ot.evalCtx,
						ot.catalog,
						o.Factory(),
						c.WithID,
						e.Child(0).(memo.RelExpr).Relational(),
						c.OldValues,
						c.NewValues,
					)
					if err != nil {
						d.Fatalf(tb, "error building cascade: %+v", err)
					}
					n := tp.Child("cascade")
					n.Child(strings.TrimRight(ot.FormatExpr(cascade), "\n"))
					buildCascades(cascade, n, level+1)
				}
			}
			for i := 0; i < e.ChildCount(); i++ {
				buildCascades(e.Child(i), tp, level)
			}
		}
		tp := treeprinter.New()
		root := tp.Child("root")
		root.Child(strings.TrimRight(ot.FormatExpr(e), "\n"))
		buildCascades(e, root, 1)

		return tp.String()

	case "optsteps":
		result, err := ot.OptSteps()
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		return result

	case "optstepsweb":
		result, err := ot.OptStepsWeb()
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
		return ot.FormatExpr(e)

	case "exprnorm":
		e, err := ot.ExprNorm()
		if err != nil {
			return fmt.Sprintf("error: %s\n", err)
		}
		ot.postProcess(tb, d, e)
		return ot.FormatExpr(e)

	case "stats-quality":
		result, err := ot.StatsQuality(tb, d)
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		return result

	case "import":
		ot.Import(tb)
		return ""

	case "inject-stats":
		ot.InjectStats(tb, d)
		return ""

	case "reorderjoins":
		result, err := ot.ReorderJoins()
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		return result

	case "check-size":
		result, err := ot.CheckSize()
		if err != nil {
			d.Fatalf(tb, "%+v", err)
		}
		return result

	default:
		d.Fatalf(tb, "unsupported command: %s", d.Cmd)
		return ""
	}
}

// FormatExpr is a convenience wrapper for memo.FormatExpr.
func (ot *OptTester) FormatExpr(e opt.Expr) string {
	var mem *memo.Memo
	if rel, ok := e.(memo.RelExpr); ok {
		mem = rel.Memo()
	}
	return memo.FormatExpr(e, ot.Flags.ExprFormat, mem, ot.catalog)
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

	if !ot.Flags.ExpectedRules.SubsetOf(ot.appliedRules) {
		unseen := ot.Flags.ExpectedRules.Difference(ot.appliedRules)
		d.Fatalf(tb, "expected to see %s, but was not triggered. Did see %s",
			formatRuleSet(unseen), formatRuleSet(ot.appliedRules))
	}

	if ot.Flags.UnexpectedRules.Intersects(ot.appliedRules) {
		seen := ot.Flags.UnexpectedRules.Intersection(ot.appliedRules)
		d.Fatalf(tb, "expected not to see %s, but it was triggered", formatRuleSet(seen))
	}
}

// Fills in lazily-derived properties (for display).
func fillInLazyProps(e opt.Expr) {
	if rel, ok := e.(memo.RelExpr); ok {
		// These properties are derived from the normalized expression.
		rel = rel.FirstExpr()

		// Derive columns that are candidates for pruning.
		norm.DerivePruneCols(rel)

		// Derive columns that are candidates for null rejection.
		norm.DeriveRejectNullCols(rel)

		// Make sure the interesting orderings are calculated.
		ordering.DeriveInterestingOrderings(rel)
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
		if len(arg.Vals) == 0 {
			return fmt.Errorf("format flag requires value(s)")
		}
		for _, v := range arg.Vals {
			// Format values are of the form (hide|show)-(flag). These flags modify
			// the default flags for the test and multiple flags are applied in order.
			parts := strings.SplitN(v, "-", 2)
			if len(parts) != 2 ||
				(parts[0] != "show" && parts[0] != "hide") ||
				formatFlags[parts[1]] == 0 {
				return fmt.Errorf("unknown format value %s", v)
			}
			if parts[0] == "hide" {
				f.ExprFormat |= formatFlags[parts[1]]
			} else {
				f.ExprFormat &= ^formatFlags[parts[1]]
			}
		}

	case "fully-qualify-names":
		f.FullyQualifyNames = true
		// Hiding qualifications defeats the purpose.
		f.ExprFormat &= ^memo.ExprFmtHideQualifications

	case "no-stable-folds":
		f.NoStableFolds = true

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

	case "prefer-lookup-joins-for-fks":
		f.PreferLookupJoinsForFKs = true

	case "rule":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("rule requires one argument")
		}
		var err error
		f.ExploreTraceRule, err = ruleFromString(arg.Vals[0])
		if err != nil {
			return err
		}

	case "skip-no-op":
		f.ExploreTraceSkipNoop = true

	case "expect":
		ruleset, err := ruleNamesToRuleSet(arg.Vals)
		if err != nil {
			return err
		}
		f.ExpectedRules.UnionWith(ruleset)

	case "expect-not":
		ruleset, err := ruleNamesToRuleSet(arg.Vals)
		if err != nil {
			return err
		}
		f.UnexpectedRules.UnionWith(ruleset)

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

	case "stats-quality-prefix":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("stats-quality-prefix requires one argument")
		}
		f.SaveTablesPrefix = arg.Vals[0]

	case "ignore-tables":
		var tables util.FastIntSet
		addTables := func(val string) error {
			table, err := strconv.Atoi(val)
			if err != nil {
				var start, end int
				bounds := strings.Split(val, "-")
				if len(bounds) != 2 {
					return fmt.Errorf("ignore-tables arguments must be of the form: '1-3,5'")
				}
				if start, err = strconv.Atoi(bounds[0]); err != nil {
					return fmt.Errorf("ignore-tables arguments must be integers")
				}
				if end, err = strconv.Atoi(bounds[1]); err != nil {
					return fmt.Errorf("ignore-tables arguments must be integers")
				}
				tables.AddRange(start, end)
			} else {
				tables.Add(table)
			}
			return nil
		}
		for i := range arg.Vals {
			if err := addTables(arg.Vals[i]); err != nil {
				return err
			}
		}
		f.IgnoreTables = tables

	case "file":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("file requires one argument")
		}
		f.File = arg.Vals[0]

	case "cascade-levels":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("cascade-levels requires a single argument")
		}
		levels, err := strconv.ParseInt(arg.Vals[0], 10, 64)
		if err != nil {
			return errors.Wrap(err, "cascade-levels")
		}
		f.CascadeLevels = int(levels)

	case "index-version":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("index-version requires one argument")
		}
		version, err := strconv.ParseInt(arg.Vals[0], 10, 64)
		if err != nil {
			return err
		}
		f.IndexVersion = descpb.IndexDescriptorVersion(version)

	case "split-diff":
		f.OptStepsSplitDiff = true

	case "rule-limit":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("rule-limit requires one argument")
		}
		limit, err := strconv.ParseInt(arg.Vals[0], 10, 64)
		if err != nil {
			return err
		}
		f.RuleApplicationLimit = limit

	case "group-limit":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("group-limit requires one argument")
		}
		limit, err := strconv.ParseInt(arg.Vals[0], 10, 64)
		if err != nil {
			return err
		}
		f.MemoGroupLimit = limit

	case "query-args":
		f.QueryArgs = arg.Vals

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
		return true
	})
	if !ot.Flags.NoStableFolds {
		o.Factory().FoldingControl().AllowStableFolds()
	}
	return ot.optimizeExpr(o)
}

// Optimize constructs an opt expression tree for the SQL query, with all
// transformations applied to it. The result is the memo expression tree with
// the lowest estimated cost.
func (ot *OptTester) Optimize() (opt.Expr, error) {
	o := ot.makeOptimizer()
	o.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		return !ot.Flags.DisableRules.Contains(int(ruleName))
	})
	o.Factory().FoldingControl().AllowStableFolds()
	return ot.optimizeExpr(o)
}

// AssignPlaceholders builds the given query with placeholders, then assigns the
// placeholders to the given argument values, and optionally runs exploration.
//
// The arguments are parsed as SQL expressions.
func (ot *OptTester) AssignPlaceholders(queryArgs []string, explore bool) (opt.Expr, error) {
	o := ot.makeOptimizer()

	// Build the prepared memo. Note that placeholders don't have values yet, so
	// they won't be replaced.
	err := ot.buildExpr(o.Factory())
	if err != nil {
		return nil, err
	}
	prepMemo := o.DetachMemo()

	// Construct placeholder values.
	if exp := len(ot.semaCtx.Placeholders.Types); len(queryArgs) != exp {
		return nil, errors.Errorf("expected %d arguments, got %d", exp, len(queryArgs))
	}
	ot.semaCtx.Placeholders.Values = make(tree.QueryArguments, len(queryArgs))
	for i, arg := range queryArgs {
		var parg tree.Expr
		parg, err := parser.ParseExpr(fmt.Sprintf("%v", arg))
		if err != nil {
			return nil, err
		}

		id := tree.PlaceholderIdx(i)
		typ, _ := ot.semaCtx.Placeholders.ValueType(id)
		texpr, err := schemaexpr.SanitizeVarFreeExpr(
			context.Background(),
			parg,
			typ,
			"", /* context */
			&ot.semaCtx,
			tree.VolatilityVolatile,
		)
		if err != nil {
			return nil, err
		}

		ot.semaCtx.Placeholders.Values[i] = texpr
	}
	ot.evalCtx.Placeholders = &ot.semaCtx.Placeholders

	// We want expect/expect-not to refer only to rules that run during
	// AssignPlaceholders.
	ot.appliedRules = RuleSet{}
	// Now assign placeholders.
	o = ot.makeOptimizer()
	o.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		if !explore && !ruleName.IsNormalize() {
			return false
		}
		if ot.Flags.DisableRules.Contains(int(ruleName)) {
			return false
		}
		return true
	})

	o.Factory().FoldingControl().AllowStableFolds()
	if err := o.Factory().AssignPlaceholders(prepMemo); err != nil {
		return nil, err
	}
	return o.Optimize()
}

// PlaceholderFastPath tests TryPlaceholderFastPath; it should be used on
// queries with placeholders.
func (ot *OptTester) PlaceholderFastPath() (_ opt.Expr, ok bool, _ error) {
	o := ot.makeOptimizer()
	o.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		return !ot.Flags.DisableRules.Contains(int(ruleName))
	})

	err := ot.buildExpr(o.Factory())
	if err != nil {
		return nil, false, err
	}
	return o.TryPlaceholderFastPath()
}

// Memo returns a string that shows the memo data structure that is constructed
// by the optimizer.
func (ot *OptTester) Memo() (string, error) {
	var o xform.Optimizer
	o.Init(&ot.evalCtx, ot.catalog)
	if _, err := ot.optimizeExpr(&o); err != nil {
		return "", err
	}
	return o.FormatMemo(ot.Flags.MemoFormat), nil
}

// Expr parses the input directly into an expression; see exprgen.Build.
func (ot *OptTester) Expr() (opt.Expr, error) {
	var f norm.Factory
	f.Init(&ot.evalCtx, ot.catalog)
	f.DisableOptimizations()

	return exprgen.Build(ot.catalog, &f, ot.sql)
}

// ExprNorm parses the input directly into an expression and runs
// normalization; see exprgen.Build.
func (ot *OptTester) ExprNorm() (opt.Expr, error) {
	var f norm.Factory
	f.Init(&ot.evalCtx, ot.catalog)

	f.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		// exprgen.Build doesn't run optimization, so we don't need to explicitly
		// disallow exploration rules here.
		return !ot.Flags.DisableRules.Contains(int(ruleName))
	})

	f.NotifyOnAppliedRule(func(ruleName opt.RuleName, source, target opt.Expr) {
		ot.appliedRules.Add(int(ruleName))
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

		next = os.fo.o.FormatExpr(os.Root(), ot.Flags.ExprFormat)

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

// OptStepsWeb is similar to Optsteps but it uses a special web page for
// formatting the output. The result will be an URL which contains the encoded
// data.
func (ot *OptTester) OptStepsWeb() (string, error) {
	normDiffStr, err := ot.optStepsNormDiff()
	if err != nil {
		return "", err
	}

	exploreDiffStr, err := ot.optStepsExploreDiff()
	if err != nil {
		return "", err
	}
	url, err := ot.encodeOptstepsURL(normDiffStr, exploreDiffStr)
	if err != nil {
		return "", err
	}
	return url.String(), nil
}

// optStepsNormDiff produces the normalization steps as a diff where each step
// is a pair of "files" (showing the before and after plans).
func (ot *OptTester) optStepsNormDiff() (string, error) {
	// Store all the normalization steps.
	type step struct {
		Name string
		Expr string
	}
	var normSteps []step
	for os := newOptSteps(ot); !os.Done(); {
		err := os.Next()
		if err != nil {
			return "", err
		}
		expr := os.fo.o.FormatExpr(os.Root(), ot.Flags.ExprFormat)
		name := "Initial"
		if len(normSteps) > 0 {
			rule := os.LastRuleName()
			if rule.IsExplore() {
				// Stop at the first exploration rule.
				break
			}
			name = rule.String()
		}
		normSteps = append(normSteps, step{Name: name, Expr: expr})
	}

	var buf bytes.Buffer
	for i, s := range normSteps {
		before := ""
		if i > 0 {
			before = normSteps[i-1].Expr
		}
		after := s.Expr
		diff := difflib.UnifiedDiff{
			A:        difflib.SplitLines(before),
			FromFile: fmt.Sprintf("a/%s", s.Name),
			B:        difflib.SplitLines(after),
			ToFile:   fmt.Sprintf("b/%s", s.Name),
			Context:  10000,
		}
		diffStr, err := difflib.GetUnifiedDiffString(diff)
		if err != nil {
			return "", err
		}
		diffStr = strings.TrimRight(diffStr, " \r\t\n")
		buf.WriteString(diffStr)
		buf.WriteString("\n")
	}
	return buf.String(), nil
}

// optStepsExploreDiff produces the exploration steps as a diff where each new
// expression is shown as a pair of "files" (showing the before and after
// expression). Note that normalization rules that are applied as part of
// creating the new expression are not shown separately.
func (ot *OptTester) optStepsExploreDiff() (string, error) {
	et := newExploreTracer(ot)

	var buf bytes.Buffer

	for step := 0; ; step++ {
		if step > 2000 {
			ot.output("step limit reached\n")
			break
		}
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
		newNodes := et.NewExprs()
		before := et.fo.o.FormatExpr(et.SrcExpr(), ot.Flags.ExprFormat)

		for i := range newNodes {
			name := et.LastRuleName().String()
			after := memo.FormatExpr(newNodes[i], ot.Flags.ExprFormat, et.fo.o.Memo(), ot.catalog)

			diff := difflib.UnifiedDiff{
				A:        difflib.SplitLines(before),
				FromFile: fmt.Sprintf("a/%s", name),
				B:        difflib.SplitLines(after),
				ToFile:   fmt.Sprintf("b/%s", name),
				Context:  10000,
			}
			diffStr, err := difflib.GetUnifiedDiffString(diff)
			if err != nil {
				return "", err
			}
			diffStr = strings.TrimRight(diffStr, " \r\t\n")
			if diffStr == "" {
				// It's possible that the "new" expression is identical to the original
				// one; ignore it in that case.
				continue
			}
			buf.WriteString(diffStr)
			buf.WriteString("\n")
		}
	}
	return buf.String(), nil
}

func (ot *OptTester) encodeOptstepsURL(normDiff, exploreDiff string) (url.URL, error) {
	output := struct {
		SQL         string
		NormDiff    string
		ExploreDiff string
	}{
		SQL:         ot.sql,
		NormDiff:    normDiff,
		ExploreDiff: exploreDiff,
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(output); err != nil {
		return url.URL{}, err
	}
	var compressed bytes.Buffer

	encoder := base64.NewEncoder(base64.URLEncoding, &compressed)
	compressor := zlib.NewWriter(encoder)
	if _, err := buf.WriteTo(compressor); err != nil {
		return url.URL{}, err
	}
	if err := compressor.Close(); err != nil {
		return url.URL{}, err
	}
	if err := encoder.Close(); err != nil {
		return url.URL{}, err
	}
	url := url.URL{
		Scheme: "https",
		Host:   "raduberinde.github.io",
		Path:   "optsteps.html",
		// We could use Fragment (which avoids the data being sent to the server),
		// but then the link will become invalid when a real fragment link is used.
		RawQuery: compressed.String(),
	}
	return url, nil
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

	if os.IsBetter() {
		// New expression is better than the previous expression. Diff
		// it against the previous *best* expression (might not be the
		// previous expression).
		bestHeader(os.Root(), "%s\n", os.LastRuleName())
	} else {
		altHeader("%s (higher cost)\n", os.LastRuleName())
	}

	if ot.Flags.OptStepsSplitDiff {
		ot.output("<<<<<<< before\n")
		ot.indent(before)
		ot.output("=======\n")
		ot.indent(after)
		ot.output(">>>>>>> after\n")
	} else {
		diff := difflib.UnifiedDiff{
			A:       difflib.SplitLines(before),
			B:       difflib.SplitLines(after),
			Context: 100,
		}
		text, _ := difflib.GetUnifiedDiffString(diff)
		// Skip the "@@ ... @@" header (first line).
		text = strings.SplitN(text, "\n", 2)[1]
		ot.indent(text)
	}
}

// ExploreTrace steps through exploration transformations performed by the
// optimizer, one-by-one. The output of each step is the expression on which the
// rule was applied, and the expressions that were generated by the rule.
func (ot *OptTester) ExploreTrace() (string, error) {
	ot.builder.Reset()

	et := newExploreTracer(ot)

	for step := 0; ; step++ {
		if step > 2000 {
			ot.output("step limit reached\n")
			break
		}
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
		newNodes := et.NewExprs()
		if ot.Flags.ExploreTraceSkipNoop && len(newNodes) == 0 {
			continue
		}

		if ot.builder.Len() > 0 {
			ot.output("\n")
		}
		ot.separator("=")
		ot.output("%s\n", et.LastRuleName())
		ot.separator("=")
		ot.output("Source expression:\n")
		ot.indent(et.fo.o.FormatExpr(et.SrcExpr(), ot.Flags.ExprFormat))
		if len(newNodes) == 0 {
			ot.output("\nNo new expressions.\n")
		}
		for i := range newNodes {
			ot.output("\nNew expression %d of %d:\n", i+1, len(newNodes))
			ot.indent(memo.FormatExpr(newNodes[i], ot.Flags.ExprFormat, et.fo.o.Memo(), ot.catalog))
		}
	}
	return ot.builder.String(), nil
}

// Import imports a file containing exec-ddl commands in order to add tables
// and/or stats to the catalog. This allows commonly-used schemas such as
// TPC-C or TPC-H to be used by multiple test files without copying the schemas
// and stats multiple times.
func (ot *OptTester) Import(tb testing.TB) {
	if ot.Flags.File == "" {
		tb.Fatal("file not specified")
	}
	path := ot.testFixturePath(tb, ot.Flags.File)
	datadriven.RunTest(tb.(*testing.T), path, func(t *testing.T, d *datadriven.TestData) string {
		tester := New(ot.catalog, d.Input)
		return tester.RunCommand(t, d)
	})
}

// testFixturePath returns the path of a fixture inside opttester/testfixtures.
func (ot *OptTester) testFixturePath(tb testing.TB, file string) string {
	if bazel.BuiltWithBazel() {
		runfile, err := bazel.Runfile("pkg/sql/opt/testutils/opttester/testfixtures/" + file)
		if err != nil {
			tb.Fatalf("%s; is your package missing a dependency on \"//pkg/sql/opt/testutils/opttester:testfixtures\"?", err)
		}
		return runfile
	}
	// Get the path to this file from the runtime.
	_, thisFilePath, _, ok := runtime.Caller(0)
	if !ok {
		tb.Fatal("unable to get caller information")
	}
	return filepath.Join(filepath.Dir(thisFilePath), "testfixtures", file)
}

// InjectStats constructs and executes an ALTER TABLE INJECT STATISTICS
// statement using the statistics in a separate json file.
func (ot *OptTester) InjectStats(tb testing.TB, d *datadriven.TestData) {
	if ot.Flags.File == "" {
		tb.Fatal("file not specified")
	}
	if ot.Flags.Table == "" {
		tb.Fatal("table not specified")
	}
	// We get the file path from the Pos string which always of the form
	// "file:linenum".
	testfilePath := strings.SplitN(d.Pos, ":", 1)[0]
	path := filepath.Join(filepath.Dir(testfilePath), ot.Flags.File)
	stats, err := ioutil.ReadFile(path)
	if err != nil {
		tb.Fatalf("error reading %s: %v", path, err)
	}
	stmt := fmt.Sprintf(
		"ALTER TABLE %s INJECT STATISTICS '%s'",
		ot.Flags.Table,
		strings.Replace(string(stats), "'", "''", -1),
	)
	testCatalog, ok := ot.catalog.(*testcat.Catalog)
	if !ok {
		d.Fatalf(tb, "inject-stats can only be used with TestCatalog")
	}
	_, err = testCatalog.ExecuteDDL(stmt)
	if err != nil {
		d.Fatalf(tb, "%v", err)
	}
}

// StatsQuality optimizes the given query and saves the subexpressions as tables
// in the test catalog with their estimated statistics injected.
// If rewriteActualStats=true, it also executes the given query against a
// running database and saves the intermediate results as tables.
func (ot *OptTester) StatsQuality(tb testing.TB, d *datadriven.TestData) (string, error) {
	if *rewriteActualStats {
		if err := ot.saveActualTables(); err != nil {
			return "", err
		}
	}

	expr, err := ot.Optimize()
	if err != nil {
		return "", err
	}

	// Create a table in the test catalog for each relational expression in the
	// tree. Keep track of the name of each table so that stats can be outputted
	// later.
	var names []string
	nameGen := memo.NewExprNameGenerator(ot.Flags.SaveTablesPrefix)
	var traverse func(e opt.Expr) error
	traverse = func(e opt.Expr) error {
		if r, ok := e.(memo.RelExpr); ok {
			// GenerateName is called in a pre-order traversal of the query tree.
			tabName := nameGen.GenerateName(e.Op())
			names = append(names, tabName)
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
		return "", err
	}

	catalog, ok := ot.catalog.(*testcat.Catalog)
	if !ok {
		return "", fmt.Errorf("stats can only be used with TestCatalog")
	}

	buf := bytes.Buffer{}
	ot.postProcess(tb, d, expr)
	buf.WriteString(ot.FormatExpr(expr))

	// Split the previous test output into blocks containing the stats for each
	// expression. The first element will contain the expression tree itself, so
	// remove it from the slice.
	const headingPrefix = "\n----Stats for "
	const headingPostfix = "----\n"
	prevOutputs := strings.Split(d.Expected, headingPrefix)
	if len(prevOutputs) > 1 {
		prevOutputs = prevOutputs[1:]
	} else {
		prevOutputs = nil
	}

	// Output stats for each previously saved table.
	st := statsTester{}
	for i, name := range names {
		if ot.Flags.IgnoreTables.Contains(i + 1) {
			// Skip over any tables in the ignore set.
			continue
		}
		buf.WriteString(fmt.Sprintf("%s%s%s", headingPrefix, name, headingPostfix))
		var err error
		var statsOutput string
		if statsOutput, err = st.testStats(catalog, prevOutputs, name, headingPostfix); err != nil {
			return "", err
		}
		buf.WriteString(statsOutput)
	}

	return buf.String(), nil
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
	columns := make([]cat.Column, outputCols.Len())
	jsonStats := make([]stats.JSONStatistic, outputCols.Len())
	i := 0
	for col, ok := outputCols.Next(0); ok; col, ok = outputCols.Next(col + 1) {
		colMeta := rel.Memo().Metadata().ColumnMeta(col)
		colName := colNameGen.GenerateName(col)

		columns[i].InitNonVirtual(
			i,
			cat.StableID(i+1),
			tree.Name(colName),
			cat.Ordinary,
			colMeta.Type,
			!relProps.NotNullCols.Contains(col),
			cat.Visible,
			nil, /* defaultExpr */
			nil, /* computedExpr */
		)

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
		Name: jobspb.AutoStatsName,
		CreatedAt: tree.AsStringWithFlags(
			&tree.DTimestamp{Time: timeutil.Now()}, tree.FmtBareStrings,
		),
		Columns:       columns,
		RowCount:      rowCount,
		DistinctCount: distinctCount,
		NullCount:     nullCount,
	}
}

// CheckSize optimizes the given query and tracks the number of rule
// applications that take place and the number of groups added to the memo.
// If either of these values exceeds the given limits (if any), an error is
// returned.
func (ot *OptTester) CheckSize() (string, error) {
	o := ot.makeOptimizer()
	var ruleApplications int64
	o.NotifyOnAppliedRule(
		func(ruleName opt.RuleName, source, target opt.Expr) {
			ruleApplications++
		},
	)
	var groups int64
	o.Memo().NotifyOnNewGroup(func(expr opt.Expr) {
		groups++
	})
	if _, err := ot.optimizeExpr(o); err != nil {
		return "", err
	}
	if ot.Flags.RuleApplicationLimit > 0 && ruleApplications > ot.Flags.RuleApplicationLimit {
		return "", fmt.Errorf(
			"rule applications exceeded limit: %d applications", ruleApplications)
	}
	if ot.Flags.MemoGroupLimit > 0 && groups > ot.Flags.MemoGroupLimit {
		return "", fmt.Errorf("memo groups exceeded limit: %d groups", groups)
	}
	return fmt.Sprintf("Rules Applied: %d\nGroups Added: %d\n", ruleApplications, groups), nil
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
	return b.Build()
}

// makeOptimizer initializes a new optimizer and sets up an applied rule
// notifier that updates ot.appliedRules.
func (ot *OptTester) makeOptimizer() *xform.Optimizer {
	var o xform.Optimizer
	o.Init(&ot.evalCtx, ot.catalog)
	o.NotifyOnAppliedRule(func(ruleName opt.RuleName, source, target opt.Expr) {
		// Exploration rules are marked as "applied" if they generate one or
		// more new expressions.
		if target != nil {
			ot.appliedRules.Add(int(ruleName))
		}
	})
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
