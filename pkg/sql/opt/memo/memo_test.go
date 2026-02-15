// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	opttestutils "github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/opttester"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/datadriven"
)

func TestMemo(t *testing.T) {
	flags := memo.ExprFmtHideCost | memo.ExprFmtHideRuleProps | memo.ExprFmtHideQualifications |
		memo.ExprFmtHideStats | memo.ExprFmtHideNotVisibleIndexInfo | memo.ExprFmtHideFastPathChecks
	runDataDrivenTest(t, datapathutils.TestDataPath(t, "memo"), flags)
}

func TestFormat(t *testing.T) {
	runDataDrivenTest(t, datapathutils.TestDataPath(t, "format"), memo.ExprFmtShowAll|memo.ExprFmtHideFastPathChecks)
}

func TestLogicalProps(t *testing.T) {
	flags := memo.ExprFmtHideCost | memo.ExprFmtHideQualifications | memo.ExprFmtHideStats |
		memo.ExprFmtHideNotVisibleIndexInfo | memo.ExprFmtHideFastPathChecks
	runDataDrivenTest(t, datapathutils.TestDataPath(t, "logprops"), flags)
}

func TestStats(t *testing.T) {
	flags := memo.ExprFmtHideCost | memo.ExprFmtHideRuleProps | memo.ExprFmtHideQualifications |
		memo.ExprFmtHideScalars | memo.ExprFmtHideNotVisibleIndexInfo | memo.ExprFmtHideFastPathChecks
	runDataDrivenTest(t, datapathutils.TestDataPath(t, "stats"), flags)
}

func TestStatsQuality(t *testing.T) {
	flags := memo.ExprFmtHideCost | memo.ExprFmtHideRuleProps | memo.ExprFmtHideQualifications |
		memo.ExprFmtHideScalars | memo.ExprFmtHideNotVisibleIndexInfo | memo.ExprFmtHideFastPathChecks
	runDataDrivenTest(t, datapathutils.TestDataPath(t, "stats_quality"), flags)
}

func TestCompositeSensitive(t *testing.T) {
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "composite_sensitive"), func(t *testing.T, d *datadriven.TestData) string {
		semaCtx := tree.MakeSemaContext(nil /* resolver */)
		evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

		var f norm.Factory
		f.Init(context.Background(), &evalCtx, nil /* catalog */)
		md := f.Metadata()

		if d.Cmd != "composite-sensitive" {
			d.Fatalf(t, "unsupported command: %s\n", d.Cmd)
		}
		var sv opttestutils.ScalarVars

		for _, arg := range d.CmdArgs {
			key, vals := arg.Key, arg.Vals
			switch key {
			case "vars":
				err := sv.Init(md, vals)
				if err != nil {
					d.Fatalf(t, "%v", err)
				}

			default:
				d.Fatalf(t, "unknown argument: %s\n", key)
			}
		}

		expr, err := parser.ParseExpr(d.Input)
		if err != nil {
			d.Fatalf(t, "error parsing: %v", err)
		}

		b := optbuilder.NewScalar(context.Background(), &semaCtx, &evalCtx, &f)
		scalar, err := b.Build(expr)
		if err != nil {
			d.Fatalf(t, "error building: %v", err)
		}
		return fmt.Sprintf("%v", memo.CanBeCompositeSensitive(scalar))
	})
}

func TestMemoInit(t *testing.T) {
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))")
	if err != nil {
		t.Fatal(err)
	}

	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM abc WHERE $1=10")

	o.Init(context.Background(), &evalCtx, catalog)
	if !o.Memo().IsEmpty() {
		t.Fatal("memo should be empty")
	}
	if o.Memo().MemoryEstimate() != 0 {
		t.Fatal("memory estimate should be 0")
	}
	if o.Memo().RootExpr() != nil {
		t.Fatal("root expression should be nil")
	}
	if o.Memo().RootProps() != nil {
		t.Fatal("root props should be nil")
	}
}

func TestMemoIsStale(t *testing.T) {
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE xy (x INT PRIMARY KEY, y INT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE VIEW abcview AS SELECT a, b, c FROM abc")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE FUNCTION one() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TYPE typ AS ENUM ('hello', 'hiya')")
	if err != nil {
		t.Fatal(err)
	}

	// Revoke access to the underlying table. The user should retain indirect
	// access via the view.
	catalog.Table(tree.NewTableNameWithSchema("t", catconstants.PublicSchemaName, "abc")).Revoked = true

	// Initialize context with starting values.
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	evalCtx.SessionData().Database = "t"
	evalCtx.SessionData().SearchPath = sessiondata.MakeSearchPath([]string{"public"})
	// MakeTestingEvalContext created a fake planner that can only provide the
	// memory monitor and will encounter a nil-pointer error when other methods
	// are accessed. In this test, GetDatabaseSurvivalGoal method will be called
	// which can handle a case of nil planner but cannot a case when the
	// planner's GetMultiregionConfig is nil, so we nil out the planner.
	evalCtx.Planner = nil
	evalCtx.StreamManagerFactory = nil

	// Use a test query that references each schema object (apart table with
	// restricted access) to help verify that the memo is not invalidated
	// unnecessarily.
	const query = "SELECT a, b+one(), 'hiya'::typ FROM abcview v, xy WHERE a = x AND c='foo'"

	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, query)
	o.Memo().Metadata().AddSchema(catalog.Schema())

	ctx := context.Background()
	stale := func() {
		t.Helper()
		if isStale, err := o.Memo().IsStale(ctx, &evalCtx, catalog); err != nil {
			t.Fatal(err)
		} else if !isStale {
			t.Errorf("memo should be stale")
		}

		// If we did not initialize the Memo's copy of a SessionData setting, the
		// tests as written still pass if the default value is 0. To detect this, we
		// create a new memo with the changed setting and verify it's not stale.
		var o2 xform.Optimizer
		opttestutils.BuildQuery(t, &o2, catalog, &evalCtx, query)

		if isStale, err := o2.Memo().IsStale(ctx, &evalCtx, catalog); err != nil {
			t.Fatal(err)
		} else if isStale {
			t.Errorf("memo should not be stale")
		}
	}

	notStale := func() {
		t.Helper()
		if isStale, err := o.Memo().IsStale(ctx, &evalCtx, catalog); err != nil {
			t.Fatal(err)
		} else if isStale {
			t.Errorf("memo should not be stale")
		}
	}

	notStale()

	// Stale reorder joins limit.
	evalCtx.SessionData().ReorderJoinsLimit = 4
	stale()
	evalCtx.SessionData().ReorderJoinsLimit = 0
	notStale()

	// Stale zig zag join enable.
	evalCtx.SessionData().ZigzagJoinEnabled = true
	stale()
	evalCtx.SessionData().ZigzagJoinEnabled = false
	notStale()

	// Stale optimizer forecast usage enable.
	evalCtx.SessionData().OptimizerUseForecasts = true
	stale()
	evalCtx.SessionData().OptimizerUseForecasts = false
	notStale()

	// Stale optimizer merged partial statistics usage enable.
	evalCtx.SessionData().OptimizerUseMergedPartialStatistics = true
	stale()
	evalCtx.SessionData().OptimizerUseMergedPartialStatistics = false
	notStale()

	// Stale optimizer histogram usage enable.
	evalCtx.SessionData().OptimizerUseHistograms = true
	stale()
	evalCtx.SessionData().OptimizerUseHistograms = false
	notStale()

	// Stale optimizer multi-col stats usage enable.
	evalCtx.SessionData().OptimizerUseMultiColStats = true
	stale()
	evalCtx.SessionData().OptimizerUseMultiColStats = false
	notStale()

	// Stale optimizer not visible indexes usage enable.
	evalCtx.SessionData().OptimizerUseNotVisibleIndexes = true
	stale()
	evalCtx.SessionData().OptimizerUseNotVisibleIndexes = false
	notStale()

	// Stale locality optimized search enable.
	evalCtx.SessionData().LocalityOptimizedSearch = true
	stale()
	evalCtx.SessionData().LocalityOptimizedSearch = false
	notStale()

	// Stale safe updates.
	evalCtx.SessionData().SafeUpdates = true
	stale()
	evalCtx.SessionData().SafeUpdates = false
	notStale()

	// Stale DateStyle.
	evalCtx.SessionData().DataConversionConfig.DateStyle = pgdate.DateStyle{Order: pgdate.Order_YMD}
	stale()
	evalCtx.SessionData().DataConversionConfig.DateStyle = pgdate.DefaultDateStyle()
	notStale()

	// Stale IntervalStyle.
	evalCtx.SessionData().DataConversionConfig.IntervalStyle = duration.IntervalStyle_ISO_8601
	stale()
	evalCtx.SessionData().DataConversionConfig.IntervalStyle = duration.IntervalStyle_POSTGRES
	notStale()

	// Stale prefer lookup joins for FKs.
	evalCtx.SessionData().PreferLookupJoinsForFKs = true
	stale()
	evalCtx.SessionData().PreferLookupJoinsForFKs = false
	notStale()

	// Stale PropagateInputOrdering.
	evalCtx.SessionData().PropagateInputOrdering = true
	stale()
	evalCtx.SessionData().PropagateInputOrdering = false
	notStale()

	// Stale disallow full table scan.
	evalCtx.SessionData().DisallowFullTableScans = true
	stale()
	evalCtx.SessionData().DisallowFullTableScans = false
	notStale()

	// Stale avoid full table scan.
	evalCtx.SessionData().AvoidFullTableScansInMutations = true
	stale()
	evalCtx.SessionData().AvoidFullTableScansInMutations = false
	notStale()

	// Stale large full scan rows.
	evalCtx.SessionData().LargeFullScanRows = 1000
	stale()
	evalCtx.SessionData().LargeFullScanRows = 0
	notStale()

	// Stale txn rows read error.
	evalCtx.SessionData().TxnRowsReadErr = 1000
	stale()
	evalCtx.SessionData().TxnRowsReadErr = 0
	notStale()

	// Stale null ordered last.
	evalCtx.SessionData().NullOrderedLast = true
	stale()
	evalCtx.SessionData().NullOrderedLast = false
	notStale()

	// Stale enable cost scans with default column size.
	evalCtx.SessionData().CostScansWithDefaultColSize = true
	stale()
	evalCtx.SessionData().CostScansWithDefaultColSize = false
	notStale()

	// Stale unconstrained non-covering index scan enabled.
	evalCtx.SessionData().UnconstrainedNonCoveringIndexScanEnabled = true
	stale()
	evalCtx.SessionData().UnconstrainedNonCoveringIndexScanEnabled = false
	notStale()

	// Stale enforce home region.
	evalCtx.SessionData().EnforceHomeRegion = true
	stale()
	evalCtx.SessionData().EnforceHomeRegion = false
	notStale()

	// Stale inequality lookup joins enabled.
	evalCtx.SessionData().VariableInequalityLookupJoinEnabled = true
	stale()
	evalCtx.SessionData().VariableInequalityLookupJoinEnabled = false
	notStale()

	// Stale use limit ordering for streaming group by.
	evalCtx.SessionData().OptimizerUseLimitOrderingForStreamingGroupBy = true
	stale()
	evalCtx.SessionData().OptimizerUseLimitOrderingForStreamingGroupBy = false
	notStale()

	// Stale use improved split disjunction for joins.
	evalCtx.SessionData().OptimizerUseImprovedSplitDisjunctionForJoins = true
	stale()
	evalCtx.SessionData().OptimizerUseImprovedSplitDisjunctionForJoins = false
	notStale()

	// Stale testing_optimizer_random_seed.
	evalCtx.SessionData().TestingOptimizerRandomSeed = 100
	stale()
	evalCtx.SessionData().TestingOptimizerRandomSeed = 0
	notStale()

	// Stale testing_optimizer_cost_perturbation.
	evalCtx.SessionData().TestingOptimizerCostPerturbation = 1
	stale()
	evalCtx.SessionData().TestingOptimizerCostPerturbation = 0
	notStale()

	// Stale testing_optimizer_disable_rule_probability.
	evalCtx.SessionData().TestingOptimizerDisableRuleProbability = 1
	stale()
	evalCtx.SessionData().TestingOptimizerDisableRuleProbability = 0
	notStale()

	// Stale disable_optimizer_rules.
	evalCtx.SessionData().DisableOptimizerRules = []string{"some_rule"}
	stale()
	evalCtx.SessionData().DisableOptimizerRules = nil
	notStale()

	// Stale allow_ordinal_column_references.
	evalCtx.SessionData().AllowOrdinalColumnReferences = true
	stale()
	evalCtx.SessionData().AllowOrdinalColumnReferences = false
	notStale()

	// Stale optimizer_use_improve_disjunction_stats.
	evalCtx.SessionData().OptimizerUseImprovedDisjunctionStats = true
	stale()
	evalCtx.SessionData().OptimizerUseImprovedDisjunctionStats = false
	notStale()

	// Stale optimizer_always_use_histograms.
	evalCtx.SessionData().OptimizerAlwaysUseHistograms = true
	stale()
	evalCtx.SessionData().OptimizerAlwaysUseHistograms = false
	notStale()

	// Stale optimizer_hoist_uncorrelated_equality_subqueries.
	evalCtx.SessionData().OptimizerHoistUncorrelatedEqualitySubqueries = true
	stale()
	evalCtx.SessionData().OptimizerHoistUncorrelatedEqualitySubqueries = false
	notStale()

	// Stale optimizer_use_improved_computed_column_filters_derivation.
	evalCtx.SessionData().OptimizerUseImprovedComputedColumnFiltersDerivation = true
	stale()
	evalCtx.SessionData().OptimizerUseImprovedComputedColumnFiltersDerivation = false
	notStale()

	// Stale optimizer_use_improved_join_elimination.
	evalCtx.SessionData().OptimizerUseImprovedJoinElimination = true
	stale()
	evalCtx.SessionData().OptimizerUseImprovedJoinElimination = false
	notStale()

	// Stale enable_implicit_fk_locking_for_serializable.
	evalCtx.SessionData().ImplicitFKLockingForSerializable = true
	stale()
	evalCtx.SessionData().ImplicitFKLockingForSerializable = false
	notStale()

	// Stale enable_durable_locking_for_serializable.
	evalCtx.SessionData().DurableLockingForSerializable = true
	stale()
	evalCtx.SessionData().DurableLockingForSerializable = false
	notStale()

	// Stale enable_shared_locking_for_serializable.
	evalCtx.SessionData().SharedLockingForSerializable = true
	stale()
	evalCtx.SessionData().SharedLockingForSerializable = false
	notStale()

	// Stale optimizer_use_lock_op_for_serializable.
	evalCtx.SessionData().OptimizerUseLockOpForSerializable = true
	stale()
	evalCtx.SessionData().OptimizerUseLockOpForSerializable = false
	notStale()

	// Stale txn isolation level.
	evalCtx.TxnIsoLevel = isolation.ReadCommitted
	stale()
	evalCtx.TxnIsoLevel = isolation.Serializable
	notStale()

	// Stale optimizer_use_provided_ordering_fix.
	evalCtx.SessionData().OptimizerUseProvidedOrderingFix = true
	stale()
	evalCtx.SessionData().OptimizerUseProvidedOrderingFix = false
	notStale()

	// Stale plpgsql_use_strict_into.
	evalCtx.SessionData().PLpgSQLUseStrictInto = true
	stale()
	evalCtx.SessionData().PLpgSQLUseStrictInto = false
	notStale()

	// Stale optimizer_merge_joins_enabled.
	evalCtx.SessionData().OptimizerMergeJoinsEnabled = true
	stale()
	evalCtx.SessionData().OptimizerMergeJoinsEnabled = false
	notStale()

	// Stale optimizer_use_virtual_computed_column_stats.
	evalCtx.SessionData().OptimizerUseVirtualComputedColumnStats = true
	stale()
	evalCtx.SessionData().OptimizerUseVirtualComputedColumnStats = false
	notStale()

	// Stale optimizer_use_trigram_similarity_optimization.
	evalCtx.SessionData().OptimizerUseTrigramSimilarityOptimization = true
	stale()
	evalCtx.SessionData().OptimizerUseTrigramSimilarityOptimization = false
	notStale()

	// Stale optimizer_use_distinct_on_limit_hint_costing.
	evalCtx.SessionData().OptimizerUseImprovedDistinctOnLimitHintCosting = true
	stale()
	evalCtx.SessionData().OptimizerUseImprovedDistinctOnLimitHintCosting = false
	notStale()

	// Stale optimizer_use_improved_trigram_similarity_selectivity.
	evalCtx.SessionData().OptimizerUseImprovedTrigramSimilaritySelectivity = true
	stale()
	evalCtx.SessionData().OptimizerUseImprovedTrigramSimilaritySelectivity = false
	notStale()

	// Stale pg_trgm.similarity_threshold.
	evalCtx.SessionData().TrigramSimilarityThreshold = 0.5
	stale()
	evalCtx.SessionData().TrigramSimilarityThreshold = 0

	// Stale opt_split_scan_limit.
	evalCtx.SessionData().OptSplitScanLimit = 100
	stale()
	evalCtx.SessionData().OptSplitScanLimit = 0
	notStale()

	// Stale optimizer_use_improved_zigzag_join_costing.
	evalCtx.SessionData().OptimizerUseImprovedZigzagJoinCosting = true
	stale()
	evalCtx.SessionData().OptimizerUseImprovedZigzagJoinCosting = false
	notStale()

	// Stale optimizer_use_improved_multi_column_selectivity_estimate.
	evalCtx.SessionData().OptimizerUseImprovedMultiColumnSelectivityEstimate = true
	stale()
	evalCtx.SessionData().OptimizerUseImprovedMultiColumnSelectivityEstimate = false
	notStale()

	// Stale optimizer_use_max_frequency_selectivity.
	evalCtx.SessionData().OptimizerUseMaxFrequencySelectivity = true
	stale()
	evalCtx.SessionData().OptimizerUseMaxFrequencySelectivity = false
	notStale()

	// Stale optimizer_prove_implication_with_virtual_computed_columns.
	evalCtx.SessionData().OptimizerProveImplicationWithVirtualComputedColumns = true
	stale()
	evalCtx.SessionData().OptimizerProveImplicationWithVirtualComputedColumns = false
	notStale()

	// Stale optimizer_push_offset_into_index_join.
	evalCtx.SessionData().OptimizerPushOffsetIntoIndexJoin = true
	stale()
	evalCtx.SessionData().OptimizerPushOffsetIntoIndexJoin = false
	notStale()

	// Stale optimizer_use_polymorphic_parameter_fix.
	evalCtx.SessionData().OptimizerUsePolymorphicParameterFix = true
	stale()
	evalCtx.SessionData().OptimizerUsePolymorphicParameterFix = false
	notStale()

	// Stale optimizer_push_limit_into_project_filtered_scan.
	evalCtx.SessionData().OptimizerPushLimitIntoProjectFilteredScan = true
	stale()
	evalCtx.SessionData().OptimizerPushLimitIntoProjectFilteredScan = false
	notStale()

	// Stale unsafe_allow_triggers_modifying_cascades.
	evalCtx.SessionData().UnsafeAllowTriggersModifyingCascades = true
	stale()
	evalCtx.SessionData().UnsafeAllowTriggersModifyingCascades = false
	notStale()

	evalCtx.SessionData().LegacyVarcharTyping = true
	stale()
	evalCtx.SessionData().LegacyVarcharTyping = false
	notStale()

	evalCtx.SessionData().OptimizerPreferBoundedCardinality = true
	stale()
	evalCtx.SessionData().OptimizerPreferBoundedCardinality = false
	notStale()

	evalCtx.SessionData().OptimizerMinRowCount = 1.0
	stale()
	evalCtx.SessionData().OptimizerMinRowCount = 0
	notStale()

	evalCtx.SessionData().OptimizerCheckInputMinRowCount = 1.0
	stale()
	evalCtx.SessionData().OptimizerCheckInputMinRowCount = 0
	notStale()

	evalCtx.SessionData().OptimizerPlanLookupJoinsWithReverseScans = true
	stale()
	evalCtx.SessionData().OptimizerPlanLookupJoinsWithReverseScans = false
	notStale()

	evalCtx.SessionData().InsertFastPath = true
	stale()
	evalCtx.SessionData().InsertFastPath = false
	notStale()

	evalCtx.SessionData().Internal = true
	stale()
	evalCtx.SessionData().Internal = false
	notStale()

	evalCtx.SessionData().UsePre_25_2VariadicBuiltins = true
	stale()
	evalCtx.SessionData().UsePre_25_2VariadicBuiltins = false
	notStale()

	evalCtx.SessionData().OptimizerUseExistsFilterHoistRule = true
	stale()
	evalCtx.SessionData().OptimizerUseExistsFilterHoistRule = false
	notStale()

	evalCtx.SessionData().OptimizerDisableCrossRegionCascadeFastPathForRBRTables = true
	stale()
	evalCtx.SessionData().OptimizerDisableCrossRegionCascadeFastPathForRBRTables = false
	notStale()

	evalCtx.SessionData().OptimizerUseImprovedHoistJoinProject = true
	stale()
	evalCtx.SessionData().OptimizerUseImprovedHoistJoinProject = false
	notStale()

	evalCtx.SessionData().OptimizerClampLowHistogramSelectivity = true
	stale()
	evalCtx.SessionData().OptimizerClampLowHistogramSelectivity = false
	notStale()

	evalCtx.SessionData().OptimizerClampInequalitySelectivity = true
	stale()
	evalCtx.SessionData().OptimizerClampInequalitySelectivity = false
	notStale()

	// Stale prevent_update_set_column_drop.
	evalCtx.SessionData().PreventUpdateSetColumnDrop = true
	stale()
	evalCtx.SessionData().PreventUpdateSetColumnDrop = false
	notStale()

	// Stale use_improved_routine_deps_triggers_and_computed_cols.
	evalCtx.SessionData().UseImprovedRoutineDepsTriggersAndComputedCols = true
	stale()
	evalCtx.SessionData().UseImprovedRoutineDepsTriggersAndComputedCols = false
	notStale()

	// Stale optimizer_inline_any_unnest_subquery.
	evalCtx.SessionData().OptimizerInlineAnyUnnestSubquery = true
	stale()
	evalCtx.SessionData().OptimizerInlineAnyUnnestSubquery = false
	notStale()

	// Stale optimizer_use_min_row_count_anti_join_fix.
	evalCtx.SessionData().OptimizerUseMinRowCountAntiJoinFix = true
	stale()
	evalCtx.SessionData().OptimizerUseMinRowCountAntiJoinFix = false
	notStale()

	// User no longer has access to view.
	catalog.View(tree.NewTableNameWithSchema("t", catconstants.PublicSchemaName, "abcview")).Revoked = true
	_, err = o.Memo().IsStale(ctx, &evalCtx, catalog)
	if exp := "user does not have privilege"; !testutils.IsError(err, exp) {
		t.Fatalf("expected %q error, but got %+v", exp, err)
	}
	catalog.View(tree.NewTableNameWithSchema("t", catconstants.PublicSchemaName, "abcview")).Revoked = false
	notStale()

	// User no longer has execution privilege on a UDF.
	catalog.RevokeExecution(catalog.Function("one").Oid)
	_, err = o.Memo().IsStale(ctx, &evalCtx, catalog)
	if exp := "user does not have privilege to execute function"; !testutils.IsError(err, exp) {
		t.Fatalf("expected %q error, but got %+v", exp, err)
	}
	catalog.GrantExecution(catalog.Function("one").Oid)
	notStale()

	// Stale data sources and schema. Create new catalog so that data sources are
	// recreated and can be modified independently.
	catalog = testcat.New()
	_, err = catalog.ExecuteDDL("CREATE TABLE xy (x INT PRIMARY KEY, y INT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE VIEW abcview AS SELECT a, b, c FROM abc")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE FUNCTION one() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TYPE typ AS ENUM ('hello', 'hiya')")
	if err != nil {
		t.Fatal(err)
	}

	// Table ID changes.
	catalog.Table(tree.NewTableNameWithSchema("t", catconstants.PublicSchemaName, "abc")).TabID = 1
	stale()
	catalog.Table(tree.NewTableNameWithSchema("t", catconstants.PublicSchemaName, "abc")).TabID = 54
	notStale()

	// Table version changes.
	catalog.Table(tree.NewTableNameWithSchema("t", catconstants.PublicSchemaName, "abc")).TabVersion = 1
	stale()
	catalog.Table(tree.NewTableNameWithSchema("t", catconstants.PublicSchemaName, "abc")).TabVersion = 0
	notStale()

	// Function version changes.
	catalog.Function("one").Version = 1
	stale()
	catalog.Function("one").Version = 0
	notStale()

	// User changes (without RLS)
	oldUser := evalCtx.SessionData().UserProto
	newUser := username.MakeSQLUsernameFromPreNormalizedString("newuser").EncodeProto()
	evalCtx.SessionData().UserProto = newUser
	notStale()
	evalCtx.SessionData().UserProto = oldUser
	notStale()

	// User changes (with RLS)
	o.Memo().Metadata().SetRLSEnabled(evalCtx.SessionData().User(), true, /* admin */
		1 /* tableID */, false, /* isTableOwnerAndNotForced */
		false /* bypassRLS */)
	notStale()
	evalCtx.SessionData().UserProto = newUser
	stale()
	evalCtx.SessionData().UserProto = oldUser
	notStale()

	// User changes (after RLS was reinitialized)
	o.Memo().Metadata().ClearRLSEnabled()
	evalCtx.SessionData().UserProto = newUser
	notStale()
	evalCtx.SessionData().UserProto = oldUser
	notStale()

	// Stale row_security.
	evalCtx.SessionData().RowSecurity = true
	stale()
	evalCtx.SessionData().RowSecurity = false
	notStale()
}

// TestStatsAvailable tests that the statisticsBuilder correctly identifies
// for each expression whether statistics were available on the base table.
// This test is here (instead of statistics_builder_test.go) to avoid import
// cycles.
func TestStatsAvailable(t *testing.T) {
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	catalog := testcat.New()
	if _, err := catalog.ExecuteDDL(
		"CREATE TABLE t (a INT, b INT)",
	); err != nil {
		t.Fatal(err)
	}

	var o xform.Optimizer

	testNotAvailable := func(expr memo.RelExpr) {
		traverseExpr(expr, func(e memo.RelExpr) {
			if e.Relational().Statistics().Available {
				t.Fatal("stats should not be available")
			}
		})
	}

	// Stats should not be available for any expression.
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM t WHERE a=1")
	testNotAvailable(o.Memo().RootExpr())

	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT sum(a), b FROM t GROUP BY b")
	testNotAvailable(o.Memo().RootExpr())

	opttestutils.BuildQuery(t, &o, catalog, &evalCtx,
		"SELECT * FROM t AS t1, t AS t2 WHERE t1.a = t2.a AND t1.b = 5",
	)
	testNotAvailable(o.Memo().RootExpr())

	if _, err := catalog.ExecuteDDL(
		`ALTER TABLE t INJECT STATISTICS '[
		{
			"columns": ["a"],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"row_count": 1000,
			"distinct_count": 500
		},
		{
			"columns": ["b"],
			"created_at": "2018-01-01 1:30:00.00000+00:00",
			"row_count": 1000,
			"distinct_count": 500
		}
	]'`); err != nil {
		t.Fatal(err)
	}

	testAvailable := func(expr memo.RelExpr) {
		traverseExpr(expr, func(e memo.RelExpr) {
			if !e.Relational().Statistics().Available {
				t.Fatal("stats should be available")
			}
		})
	}

	// Stats should be available for all expressions.
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM t WHERE a=1")
	testAvailable(o.Memo().RootExpr())

	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT sum(a), b FROM t GROUP BY b")
	testAvailable(o.Memo().RootExpr())

	opttestutils.BuildQuery(t, &o, catalog, &evalCtx,
		"SELECT * FROM t AS t1, t AS t2 WHERE t1.a = t2.a AND t1.b = 5",
	)
	testAvailable(o.Memo().RootExpr())
}

// traverseExpr is a helper function to recursively traverse a relational
// expression and apply a function to the root as well as each relational
// child.
func traverseExpr(expr memo.RelExpr, f func(memo.RelExpr)) {
	f(expr)
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		if child, ok := expr.Child(i).(memo.RelExpr); ok {
			traverseExpr(child, f)
		}
	}
}

// runDataDrivenTest runs data-driven testcases of the form
//
//	<command>
//	<SQL statement>
//	----
//	<expected results>
//
// See OptTester.Handle for supported commands.
func runDataDrivenTest(t *testing.T, path string, fmtFlags memo.ExprFmtFlags) {
	datadriven.Walk(t, path, func(t *testing.T, path string) {
		catalog := testcat.New()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			tester := opttester.New(catalog, d.Input)
			tester.Flags.ExprFormat = fmtFlags
			return tester.RunCommand(t, d)
		})
	})
}
