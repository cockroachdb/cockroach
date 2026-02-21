// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package memo exposes logic for `Memo`, the central data structure for `opt`.
package memo

import (
	"bytes"
	"context"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// replaceFunc is the callback function passed to norm.Factory.Replace. It is
// copied here from norm.ReplaceFunc to avoid a circular dependency.
type ReplaceFunc func(e opt.Expr) opt.Expr

// replacer is a wrapper around norm.Factory.Replace, so that we can call it
// without creating a circular dependency.
type replacer func(e opt.Expr, replace ReplaceFunc) opt.Expr

// Memo is a data structure for efficiently storing a forest of query plans.
// Conceptually, the memo is composed of a numbered set of equivalency classes
// called groups where each group contains a set of logically equivalent
// expressions. Two expressions are considered logically equivalent if:
//
//  1. They return the same number and data type of columns. However, order and
//     naming of columns doesn't matter.
//  2. They return the same number of rows, with the same values in each row.
//     However, order of rows doesn't matter.
//
// The different expressions in a single group are called memo expressions
// (memo-ized expressions). The children of a memo expression can themselves be
// part of memo groups. Therefore, the memo forest is composed of every possible
// combination of parent expression with its possible child expressions,
// recursively applied.
//
// Memo expressions can be relational (e.g. join) or scalar (e.g. <). Operators
// are always both logical (specify results) and physical (specify results and
// a particular implementation). This means that even a "raw" unoptimized
// expression tree can be executed (naively). Both relational and scalar
// operators are uniformly represented as nodes in memo expression trees, which
// facilitates tree pattern matching and replacement. However, because scalar
// expression memo groups never have more than one expression, scalar
// expressions can use a simpler representation.
//
// Because memo groups contain logically equivalent expressions, all the memo
// expressions in a group share the same logical properties. However, it's
// possible for two logically equivalent expression to be placed in different
// memo groups. This occurs because determining logical equivalency of two
// relational expressions is too complex to perform 100% correctly. A
// correctness failure (i.e. considering two expressions logically equivalent
// when they are not) results in invalid transformations and invalid plans.
// But placing two logically equivalent expressions in different groups has a
// much gentler failure mode: the memo and transformations are less efficient.
// Expressions within the memo may have different physical properties. For
// example, a memo group might contain both hash join and merge join
// expressions which produce the same set of output rows, but produce them in
// different orders.
//
// Expressions are inserted into the memo by the factory, which ensure that
// expressions have been fully normalized before insertion (see the comment in
// factory.go for more details). A new group is created only when unique
// normalized expressions are created by the factory during construction or
// rewrite of the tree. Uniqueness is determined by "interning" each expression,
// which means that multiple equivalent expressions are mapped to a single
// in-memory instance. This allows interned expressions to be checked for
// equivalence by simple pointer comparison. For example:
//
//	SELECT * FROM a, b WHERE a.x = b.x
//
// After insertion into the memo, the memo would contain these six groups, with
// numbers substituted for pointers to the normalized expression in each group:
//
//	G6: [inner-join [G1 G2 G5]]
//	G5: [eq [G3 G4]]
//	G4: [variable b.x]
//	G3: [variable a.x]
//	G2: [scan b]
//	G1: [scan a]
//
// Each leaf expressions is interned by hashing its operator type and any
// private field values. Expressions higher in the tree can then rely on the
// fact that all children have been interned, and include their pointer values
// in its hash value. Therefore, the memo need only hash the expression's fields
// in order to determine whether the expression already exists in the memo.
// Walking the subtree is not necessary.
//
// The normalizing factory will never add more than one expression to a memo
// group. But the explorer does add denormalized expressions to existing memo
// groups, since oftentimes one of these equivalent, but denormalized
// expressions will have a lower cost than the initial normalized expression
// added by the factory. For example, the join commutativity transformation
// expands the memo like this:
//
//	G6: [inner-join [G1 G2 G5]] [inner-join [G2 G1 G5]]
//	G5: [eq [G3 G4]]
//	G4: [variable b.x]
//	G3: [variable a.x]
//	G2: [scan b]
//	G1: [scan a]
//
// See the comments in explorer.go for more details.
type Memo struct {
	// metadata provides information about the columns and tables used in this
	// particular query.
	metadata opt.Metadata

	// interner interns all expressions in the memo, ensuring that there is at
	// most one instance of each expression in the memo.
	interner interner

	// replacer is a wrapper around norm.Factory.Replace, used by statistics
	// builder to rewrite some expressions when calculating stats.
	replacer replacer

	// logPropsBuilder is inlined in the memo so that it can be reused each time
	// scalar or relational properties need to be built.
	logPropsBuilder logicalPropsBuilder

	// rootExpr is the root expression of the memo expression forest. It is set
	// via a call to SetRoot. After optimization, it is set to be the root of the
	// lowest cost tree in the forest.
	rootExpr RelExpr

	// rootProps are the physical properties required of the root memo expression.
	// It is set via a call to SetRoot.
	rootProps *physical.Required

	// memEstimate is the approximate memory usage of the memo, in bytes.
	memEstimate int64

	// The following are selected fields from SessionData which can affect
	// planning. We need to cross-check these before reusing a cached memo.
	reorderJoinsLimit                          int
	zigzagJoinEnabled                          bool
	useForecasts                               bool
	useMergedPartialStatistics                 bool
	useHistograms                              bool
	useMultiColStats                           bool
	useNotVisibleIndex                         bool
	localityOptimizedSearch                    bool
	safeUpdates                                bool
	preferLookupJoinsForFKs                    bool
	saveTablesPrefix                           string
	dateStyle                                  pgdate.DateStyle
	intervalStyle                              duration.IntervalStyle
	propagateInputOrdering                     bool
	disallowFullTableScans                     bool
	avoidFullTableScansInMutations             bool
	largeFullScanRows                          float64
	txnRowsReadErr                             int64
	nullOrderedLast                            bool
	costScansWithDefaultColSize                bool
	allowUnconstrainedNonCoveringIndexScan     bool
	testingOptimizerRandomSeed                 int64
	testingOptimizerCostPerturbation           float64
	testingOptimizerDisableRuleProbability     float64
	disableOptimizerRules                      []string
	enforceHomeRegion                          bool
	variableInequalityLookupJoinEnabled        bool
	allowOrdinalColumnReferences               bool
	useImprovedDisjunctionStats                bool
	useLimitOrderingForStreamingGroupBy        bool
	useImprovedSplitDisjunctionForJoins        bool
	alwaysUseHistograms                        bool
	hoistUncorrelatedEqualitySubqueries        bool
	useImprovedComputedColumnFiltersDerivation bool
	useImprovedJoinElimination                 bool
	implicitFKLockingForSerializable           bool
	durableLockingForSerializable              bool
	sharedLockingForSerializable               bool
	useLockOpForSerializable                   bool
	useProvidedOrderingFix                     bool
	mergeJoinsEnabled                          bool
	plpgsqlUseStrictInto                       bool
	useVirtualComputedColumnStats              bool
	useTrigramSimilarityOptimization           bool
	useImprovedDistinctOnLimitHintCosting      bool
	useImprovedTrigramSimilaritySelectivity    bool
	trigramSimilarityThreshold                 float64
	splitScanLimit                             int32
	useImprovedZigzagJoinCosting               bool
	useImprovedMultiColumnSelectivityEstimate  bool
	proveImplicationWithVirtualComputedCols    bool
	pushOffsetIntoIndexJoin                    bool
	usePolymorphicParameterFix                 bool
	useConditionalHoistFix                     bool
	pushLimitIntoProjectFilteredScan           bool
	unsafeAllowTriggersModifyingCascades       bool
	legacyVarcharTyping                        bool
	preferBoundedCardinality                   bool
	minRowCount                                float64
	checkInputMinRowCount                      float64
	planLookupJoinsWithReverseScans            bool
	useInsertFastPath                          bool
	internal                                   bool
	usePre_25_2VariadicBuiltins                bool
	useExistsFilterHoistRule                   bool
	disableSlowCascadeFastPathForRBRTables     bool
	useImprovedHoistJoinProject                bool
	rowSecurity                                bool
	clampLowHistogramSelectivity               bool
	clampInequalitySelectivity                 bool
	useMaxFrequencySelectivity                 bool
	usingHintInjection                         bool
	useSwapMutations                           bool
	preventUpdateSetColumnDrop                 bool
	useImprovedRoutineDepsTriggersComputedCols bool
	inlineAnyUnnestSubquery                    bool
	useMinRowCountAntiJoinFix                  bool

	// txnIsoLevel is the isolation level under which the plan was created. This
	// affects the planning of some locking operations, so it must be included in
	// memo staleness calculation.
	txnIsoLevel isolation.Level

	// curRank is the highest currently in-use scalar expression rank.
	curRank opt.ScalarRank

	// curWithID is the highest currently in-use WITH ID.
	curWithID opt.WithID

	// curRoutineResultBufferID is the highest currently in-use routine result
	// buffer ID. See the RoutineResultBufferID comment for more details.
	curRoutineResultBufferID RoutineResultBufferID

	newGroupFn func(opt.Expr)

	// disableCheckExpr disables expression validation performed by CheckExpr,
	// if the crdb_test build tag is set. If the crdb_test build tag is not set,
	// CheckExpr is always a no-op, so disableCheckExpr has no effect. This is
	// set to true for the optsteps test command to prevent CheckExpr from
	// erring with partially normalized expressions.
	disableCheckExpr bool

	// optimizationStats tracks decisions made during optimization, for example,
	// to clamp selectivity estimates to a lower bound.
	optimizationStats OptimizationStats

	// WARNING: if you add more members, add initialization code in Init (if
	// reusing allocated data structures is desired).
}

// Init initializes a new empty memo instance, or resets existing state so it
// can be reused. It must be called before use (or reuse). The memo collects
// information about the context in which it is compiled from the evalContext
// argument. If any of that changes, then the memo must be invalidated (see the
// IsStale method for more details).
func (m *Memo) Init(ctx context.Context, evalCtx *eval.Context) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicitpkg/sql/opt/memo/memo.go.
	*m = Memo{
		//nolint metadata is being reused.
		metadata:                                   m.metadata,
		reorderJoinsLimit:                          int(evalCtx.SessionData().ReorderJoinsLimit),
		zigzagJoinEnabled:                          evalCtx.SessionData().ZigzagJoinEnabled,
		useForecasts:                               evalCtx.SessionData().OptimizerUseForecasts,
		useMergedPartialStatistics:                 evalCtx.SessionData().OptimizerUseMergedPartialStatistics,
		useHistograms:                              evalCtx.SessionData().OptimizerUseHistograms,
		useMultiColStats:                           evalCtx.SessionData().OptimizerUseMultiColStats,
		useNotVisibleIndex:                         evalCtx.SessionData().OptimizerUseNotVisibleIndexes,
		localityOptimizedSearch:                    evalCtx.SessionData().LocalityOptimizedSearch,
		safeUpdates:                                evalCtx.SessionData().SafeUpdates,
		preferLookupJoinsForFKs:                    evalCtx.SessionData().PreferLookupJoinsForFKs,
		saveTablesPrefix:                           evalCtx.SessionData().SaveTablesPrefix,
		dateStyle:                                  evalCtx.SessionData().GetDateStyle(),
		intervalStyle:                              evalCtx.SessionData().GetIntervalStyle(),
		propagateInputOrdering:                     evalCtx.SessionData().PropagateInputOrdering,
		disallowFullTableScans:                     evalCtx.SessionData().DisallowFullTableScans,
		avoidFullTableScansInMutations:             evalCtx.SessionData().AvoidFullTableScansInMutations,
		largeFullScanRows:                          evalCtx.SessionData().LargeFullScanRows,
		txnRowsReadErr:                             evalCtx.SessionData().TxnRowsReadErr,
		nullOrderedLast:                            evalCtx.SessionData().NullOrderedLast,
		costScansWithDefaultColSize:                evalCtx.SessionData().CostScansWithDefaultColSize,
		allowUnconstrainedNonCoveringIndexScan:     evalCtx.SessionData().UnconstrainedNonCoveringIndexScanEnabled,
		testingOptimizerRandomSeed:                 evalCtx.SessionData().TestingOptimizerRandomSeed,
		testingOptimizerCostPerturbation:           evalCtx.SessionData().TestingOptimizerCostPerturbation,
		testingOptimizerDisableRuleProbability:     evalCtx.SessionData().TestingOptimizerDisableRuleProbability,
		disableOptimizerRules:                      evalCtx.SessionData().DisableOptimizerRules,
		enforceHomeRegion:                          evalCtx.SessionData().EnforceHomeRegion,
		variableInequalityLookupJoinEnabled:        evalCtx.SessionData().VariableInequalityLookupJoinEnabled,
		allowOrdinalColumnReferences:               evalCtx.SessionData().AllowOrdinalColumnReferences,
		useImprovedDisjunctionStats:                evalCtx.SessionData().OptimizerUseImprovedDisjunctionStats,
		useLimitOrderingForStreamingGroupBy:        evalCtx.SessionData().OptimizerUseLimitOrderingForStreamingGroupBy,
		useImprovedSplitDisjunctionForJoins:        evalCtx.SessionData().OptimizerUseImprovedSplitDisjunctionForJoins,
		alwaysUseHistograms:                        evalCtx.SessionData().OptimizerAlwaysUseHistograms,
		hoistUncorrelatedEqualitySubqueries:        evalCtx.SessionData().OptimizerHoistUncorrelatedEqualitySubqueries,
		useImprovedComputedColumnFiltersDerivation: evalCtx.SessionData().OptimizerUseImprovedComputedColumnFiltersDerivation,
		useImprovedJoinElimination:                 evalCtx.SessionData().OptimizerUseImprovedJoinElimination,
		implicitFKLockingForSerializable:           evalCtx.SessionData().ImplicitFKLockingForSerializable,
		durableLockingForSerializable:              evalCtx.SessionData().DurableLockingForSerializable,
		sharedLockingForSerializable:               evalCtx.SessionData().SharedLockingForSerializable,
		useLockOpForSerializable:                   evalCtx.SessionData().OptimizerUseLockOpForSerializable,
		useProvidedOrderingFix:                     evalCtx.SessionData().OptimizerUseProvidedOrderingFix,
		mergeJoinsEnabled:                          evalCtx.SessionData().OptimizerMergeJoinsEnabled,
		plpgsqlUseStrictInto:                       evalCtx.SessionData().PLpgSQLUseStrictInto,
		useVirtualComputedColumnStats:              evalCtx.SessionData().OptimizerUseVirtualComputedColumnStats,
		useTrigramSimilarityOptimization:           evalCtx.SessionData().OptimizerUseTrigramSimilarityOptimization,
		useImprovedDistinctOnLimitHintCosting:      evalCtx.SessionData().OptimizerUseImprovedDistinctOnLimitHintCosting,
		useImprovedTrigramSimilaritySelectivity:    evalCtx.SessionData().OptimizerUseImprovedTrigramSimilaritySelectivity,
		trigramSimilarityThreshold:                 evalCtx.SessionData().TrigramSimilarityThreshold,
		splitScanLimit:                             evalCtx.SessionData().OptSplitScanLimit,
		useImprovedZigzagJoinCosting:               evalCtx.SessionData().OptimizerUseImprovedZigzagJoinCosting,
		useImprovedMultiColumnSelectivityEstimate:  evalCtx.SessionData().OptimizerUseImprovedMultiColumnSelectivityEstimate,
		proveImplicationWithVirtualComputedCols:    evalCtx.SessionData().OptimizerProveImplicationWithVirtualComputedColumns,
		pushOffsetIntoIndexJoin:                    evalCtx.SessionData().OptimizerPushOffsetIntoIndexJoin,
		usePolymorphicParameterFix:                 evalCtx.SessionData().OptimizerUsePolymorphicParameterFix,
		useConditionalHoistFix:                     evalCtx.SessionData().OptimizerUseConditionalHoistFix,
		pushLimitIntoProjectFilteredScan:           evalCtx.SessionData().OptimizerPushLimitIntoProjectFilteredScan,
		unsafeAllowTriggersModifyingCascades:       evalCtx.SessionData().UnsafeAllowTriggersModifyingCascades,
		legacyVarcharTyping:                        evalCtx.SessionData().LegacyVarcharTyping,
		preferBoundedCardinality:                   evalCtx.SessionData().OptimizerPreferBoundedCardinality,
		minRowCount:                                evalCtx.SessionData().OptimizerMinRowCount,
		checkInputMinRowCount:                      evalCtx.SessionData().OptimizerCheckInputMinRowCount,
		planLookupJoinsWithReverseScans:            evalCtx.SessionData().OptimizerPlanLookupJoinsWithReverseScans,
		useInsertFastPath:                          evalCtx.SessionData().InsertFastPath,
		internal:                                   evalCtx.SessionData().Internal,
		usePre_25_2VariadicBuiltins:                evalCtx.SessionData().UsePre_25_2VariadicBuiltins,
		useExistsFilterHoistRule:                   evalCtx.SessionData().OptimizerUseExistsFilterHoistRule,
		disableSlowCascadeFastPathForRBRTables:     evalCtx.SessionData().OptimizerDisableCrossRegionCascadeFastPathForRBRTables,
		useImprovedHoistJoinProject:                evalCtx.SessionData().OptimizerUseImprovedHoistJoinProject,
		rowSecurity:                                evalCtx.SessionData().RowSecurity,
		clampLowHistogramSelectivity:               evalCtx.SessionData().OptimizerClampLowHistogramSelectivity,
		clampInequalitySelectivity:                 evalCtx.SessionData().OptimizerClampInequalitySelectivity,
		useMaxFrequencySelectivity:                 evalCtx.SessionData().OptimizerUseMaxFrequencySelectivity,
		usingHintInjection:                         evalCtx.Planner != nil && evalCtx.Planner.UsingHintInjection(),
		useSwapMutations:                           evalCtx.SessionData().UseSwapMutations,
		preventUpdateSetColumnDrop:                 evalCtx.SessionData().PreventUpdateSetColumnDrop,
		useImprovedRoutineDepsTriggersComputedCols: evalCtx.SessionData().UseImprovedRoutineDepsTriggersAndComputedCols,
		inlineAnyUnnestSubquery:                    evalCtx.SessionData().OptimizerInlineAnyUnnestSubquery,
		useMinRowCountAntiJoinFix:                  evalCtx.SessionData().OptimizerUseMinRowCountAntiJoinFix,
		txnIsoLevel:                                evalCtx.TxnIsoLevel,
	}
	m.metadata.Init()
	m.logPropsBuilder.init(ctx, evalCtx, m)
}

func (m *Memo) SetReplacer(replacer replacer) {
	m.replacer = replacer
}

// AllowUnconstrainedNonCoveringIndexScan indicates whether unconstrained
// non-covering index scans are enabled.
func (m *Memo) AllowUnconstrainedNonCoveringIndexScan() bool {
	return m.allowUnconstrainedNonCoveringIndexScan
}

// ResetLogProps resets the logPropsBuilder. It should be used in combination
// with the perturb-cost OptTester flag in order to update the query plan tree
// after optimization is complete with the real computed cost, not the perturbed
// cost.
func (m *Memo) ResetLogProps(ctx context.Context, evalCtx *eval.Context) {
	m.logPropsBuilder.init(ctx, evalCtx, m)
}

// NotifyOnNewGroup sets a callback function which is invoked each time we
// create a new memo group.
func (m *Memo) NotifyOnNewGroup(fn func(opt.Expr)) {
	m.newGroupFn = fn
}

// IsEmpty returns true if there are no expressions in the memo.
func (m *Memo) IsEmpty() bool {
	// Root expression can be nil before optimization and interner is empty after
	// exploration, so check both.
	return m.interner.Count() == 0 && m.rootExpr == nil
}

// MemoryEstimate returns a rough estimate of the memo's memory usage, in bytes.
// It only includes memory usage that is proportional to the size and complexity
// of the query, rather than constant overhead bytes.
func (m *Memo) MemoryEstimate() int64 {
	// Multiply by 2 to take rough account of allocation fragmentation, private
	// data, list overhead, properties, etc.
	return m.memEstimate * 2
}

// Metadata returns the metadata instance associated with the memo.
func (m *Memo) Metadata() *opt.Metadata {
	return &m.metadata
}

// RootExpr returns the root memo expression previously set via a call to
// SetRoot.
func (m *Memo) RootExpr() RelExpr {
	return m.rootExpr
}

// RootProps returns the physical properties required of the root memo group,
// previously set via a call to SetRoot.
func (m *Memo) RootProps() *physical.Required {
	return m.rootProps
}

// SetRoot stores the root memo expression when it is a relational expression,
// and also stores the physical properties required of the root group.
func (m *Memo) SetRoot(e RelExpr, phys *physical.Required) {
	m.rootExpr = e
	if m.rootProps != phys {
		m.rootProps = m.InternPhysicalProps(phys)
	}

	// Once memo is optimized, release reference to the eval context and free up
	// the memory used by the interner.
	if m.IsOptimized() {
		m.logPropsBuilder.clear()
		m.interner = interner{}
	}
}

// HasPlaceholders returns true if the memo contains at least one placeholder
// operator.
func (m *Memo) HasPlaceholders() bool {
	return m.rootExpr.Relational().HasPlaceholder
}

// IsStale returns true if the memo has been invalidated by changes to any of
// its dependencies. Once a memo is known to be stale, it must be ejected from
// any query cache or prepared statement and replaced with a recompiled memo
// that takes into account the changes. IsStale checks the following
// dependencies:
//
//  1. Current database: this can change name resolution.
//  2. Current search path: this can change name resolution.
//  3. Current location: this determines time zone, and can change how time-
//     related types are constructed and compared.
//  4. Data source schema: this determines most aspects of how the query is
//     compiled.
//  5. Data source privileges: current user may no longer have access to one or
//     more data sources.
//
// This function cannot swallow errors and return only a boolean, as it may
// perform KV operations on behalf of the transaction associated with the
// provided catalog, and those errors are required to be propagated.
func (m *Memo) IsStale(
	ctx context.Context, evalCtx *eval.Context, catalog cat.Catalog,
) (bool, error) {
	// Memo is stale if fields from SessionData that can affect planning have
	// changed.
	if m.reorderJoinsLimit != int(evalCtx.SessionData().ReorderJoinsLimit) ||
		m.zigzagJoinEnabled != evalCtx.SessionData().ZigzagJoinEnabled ||
		m.useForecasts != evalCtx.SessionData().OptimizerUseForecasts ||
		m.useMergedPartialStatistics != evalCtx.SessionData().OptimizerUseMergedPartialStatistics ||
		m.useHistograms != evalCtx.SessionData().OptimizerUseHistograms ||
		m.useMultiColStats != evalCtx.SessionData().OptimizerUseMultiColStats ||
		m.useNotVisibleIndex != evalCtx.SessionData().OptimizerUseNotVisibleIndexes ||
		m.localityOptimizedSearch != evalCtx.SessionData().LocalityOptimizedSearch ||
		m.safeUpdates != evalCtx.SessionData().SafeUpdates ||
		m.preferLookupJoinsForFKs != evalCtx.SessionData().PreferLookupJoinsForFKs ||
		m.saveTablesPrefix != evalCtx.SessionData().SaveTablesPrefix ||
		m.dateStyle != evalCtx.SessionData().GetDateStyle() ||
		m.intervalStyle != evalCtx.SessionData().GetIntervalStyle() ||
		m.propagateInputOrdering != evalCtx.SessionData().PropagateInputOrdering ||
		m.disallowFullTableScans != evalCtx.SessionData().DisallowFullTableScans ||
		m.avoidFullTableScansInMutations != evalCtx.SessionData().AvoidFullTableScansInMutations ||
		m.largeFullScanRows != evalCtx.SessionData().LargeFullScanRows ||
		m.txnRowsReadErr != evalCtx.SessionData().TxnRowsReadErr ||
		m.nullOrderedLast != evalCtx.SessionData().NullOrderedLast ||
		m.costScansWithDefaultColSize != evalCtx.SessionData().CostScansWithDefaultColSize ||
		m.allowUnconstrainedNonCoveringIndexScan != evalCtx.SessionData().UnconstrainedNonCoveringIndexScanEnabled ||
		m.testingOptimizerRandomSeed != evalCtx.SessionData().TestingOptimizerRandomSeed ||
		m.testingOptimizerCostPerturbation != evalCtx.SessionData().TestingOptimizerCostPerturbation ||
		m.testingOptimizerDisableRuleProbability != evalCtx.SessionData().TestingOptimizerDisableRuleProbability ||
		!reflect.DeepEqual(m.disableOptimizerRules, evalCtx.SessionData().DisableOptimizerRules) ||
		m.enforceHomeRegion != evalCtx.SessionData().EnforceHomeRegion ||
		m.variableInequalityLookupJoinEnabled != evalCtx.SessionData().VariableInequalityLookupJoinEnabled ||
		m.allowOrdinalColumnReferences != evalCtx.SessionData().AllowOrdinalColumnReferences ||
		m.useImprovedDisjunctionStats != evalCtx.SessionData().OptimizerUseImprovedDisjunctionStats ||
		m.useLimitOrderingForStreamingGroupBy != evalCtx.SessionData().OptimizerUseLimitOrderingForStreamingGroupBy ||
		m.useImprovedSplitDisjunctionForJoins != evalCtx.SessionData().OptimizerUseImprovedSplitDisjunctionForJoins ||
		m.alwaysUseHistograms != evalCtx.SessionData().OptimizerAlwaysUseHistograms ||
		m.hoistUncorrelatedEqualitySubqueries != evalCtx.SessionData().OptimizerHoistUncorrelatedEqualitySubqueries ||
		m.useImprovedComputedColumnFiltersDerivation != evalCtx.SessionData().OptimizerUseImprovedComputedColumnFiltersDerivation ||
		m.useImprovedJoinElimination != evalCtx.SessionData().OptimizerUseImprovedJoinElimination ||
		m.implicitFKLockingForSerializable != evalCtx.SessionData().ImplicitFKLockingForSerializable ||
		m.durableLockingForSerializable != evalCtx.SessionData().DurableLockingForSerializable ||
		m.sharedLockingForSerializable != evalCtx.SessionData().SharedLockingForSerializable ||
		m.useLockOpForSerializable != evalCtx.SessionData().OptimizerUseLockOpForSerializable ||
		m.useProvidedOrderingFix != evalCtx.SessionData().OptimizerUseProvidedOrderingFix ||
		m.mergeJoinsEnabled != evalCtx.SessionData().OptimizerMergeJoinsEnabled ||
		m.plpgsqlUseStrictInto != evalCtx.SessionData().PLpgSQLUseStrictInto ||
		m.useVirtualComputedColumnStats != evalCtx.SessionData().OptimizerUseVirtualComputedColumnStats ||
		m.useTrigramSimilarityOptimization != evalCtx.SessionData().OptimizerUseTrigramSimilarityOptimization ||
		m.useImprovedDistinctOnLimitHintCosting != evalCtx.SessionData().OptimizerUseImprovedDistinctOnLimitHintCosting ||
		m.useImprovedTrigramSimilaritySelectivity != evalCtx.SessionData().OptimizerUseImprovedTrigramSimilaritySelectivity ||
		m.trigramSimilarityThreshold != evalCtx.SessionData().TrigramSimilarityThreshold ||
		m.splitScanLimit != evalCtx.SessionData().OptSplitScanLimit ||
		m.useImprovedZigzagJoinCosting != evalCtx.SessionData().OptimizerUseImprovedZigzagJoinCosting ||
		m.useImprovedMultiColumnSelectivityEstimate != evalCtx.SessionData().OptimizerUseImprovedMultiColumnSelectivityEstimate ||
		m.proveImplicationWithVirtualComputedCols != evalCtx.SessionData().OptimizerProveImplicationWithVirtualComputedColumns ||
		m.pushOffsetIntoIndexJoin != evalCtx.SessionData().OptimizerPushOffsetIntoIndexJoin ||
		m.usePolymorphicParameterFix != evalCtx.SessionData().OptimizerUsePolymorphicParameterFix ||
		m.useConditionalHoistFix != evalCtx.SessionData().OptimizerUseConditionalHoistFix ||
		m.pushLimitIntoProjectFilteredScan != evalCtx.SessionData().OptimizerPushLimitIntoProjectFilteredScan ||
		m.unsafeAllowTriggersModifyingCascades != evalCtx.SessionData().UnsafeAllowTriggersModifyingCascades ||
		m.legacyVarcharTyping != evalCtx.SessionData().LegacyVarcharTyping ||
		m.preferBoundedCardinality != evalCtx.SessionData().OptimizerPreferBoundedCardinality ||
		m.minRowCount != evalCtx.SessionData().OptimizerMinRowCount ||
		m.checkInputMinRowCount != evalCtx.SessionData().OptimizerCheckInputMinRowCount ||
		m.planLookupJoinsWithReverseScans != evalCtx.SessionData().OptimizerPlanLookupJoinsWithReverseScans ||
		m.useInsertFastPath != evalCtx.SessionData().InsertFastPath ||
		m.internal != evalCtx.SessionData().Internal ||
		m.usePre_25_2VariadicBuiltins != evalCtx.SessionData().UsePre_25_2VariadicBuiltins ||
		m.useExistsFilterHoistRule != evalCtx.SessionData().OptimizerUseExistsFilterHoistRule ||
		m.disableSlowCascadeFastPathForRBRTables != evalCtx.SessionData().OptimizerDisableCrossRegionCascadeFastPathForRBRTables ||
		m.useImprovedHoistJoinProject != evalCtx.SessionData().OptimizerUseImprovedHoistJoinProject ||
		m.rowSecurity != evalCtx.SessionData().RowSecurity ||
		m.clampLowHistogramSelectivity != evalCtx.SessionData().OptimizerClampLowHistogramSelectivity ||
		m.clampInequalitySelectivity != evalCtx.SessionData().OptimizerClampInequalitySelectivity ||
		m.useMaxFrequencySelectivity != evalCtx.SessionData().OptimizerUseMaxFrequencySelectivity ||
		m.usingHintInjection != (evalCtx.Planner != nil && evalCtx.Planner.UsingHintInjection()) ||
		m.useSwapMutations != evalCtx.SessionData().UseSwapMutations ||
		m.preventUpdateSetColumnDrop != evalCtx.SessionData().PreventUpdateSetColumnDrop ||
		m.useImprovedRoutineDepsTriggersComputedCols != evalCtx.SessionData().UseImprovedRoutineDepsTriggersAndComputedCols ||
		m.inlineAnyUnnestSubquery != evalCtx.SessionData().OptimizerInlineAnyUnnestSubquery ||
		m.useMinRowCountAntiJoinFix != evalCtx.SessionData().OptimizerUseMinRowCountAntiJoinFix ||
		m.txnIsoLevel != evalCtx.TxnIsoLevel {
		return true, nil
	}

	// Memo is stale if the fingerprint of any object in the memo's metadata has
	// changed, or if the current user no longer has sufficient privilege to
	// access the object.
	if depsUpToDate, err := m.Metadata().CheckDependencies(ctx, evalCtx, catalog); err != nil || !depsUpToDate {
		return true, err
	}
	return false, nil
}

// InternPhysicalProps adds the given physical props to the memo if they haven't
// yet been added. If the same props was added previously, then return a pointer
// to the previously added props. This allows interned physical props to be
// compared for equality using simple pointer comparison.
func (m *Memo) InternPhysicalProps(phys *physical.Required) *physical.Required {
	// Special case physical properties that require nothing of operator.
	if !phys.Defined() {
		return physical.MinRequired
	}
	return m.interner.InternPhysicalProps(phys)
}

// SetBestProps updates the physical properties, provided ordering, and cost of
// a relational expression's memo group (see the relevant methods of RelExpr).
// It is called by the optimizer once it determines the expression in the group
// that is part of the lowest cost tree (for the overall query).
func (m *Memo) SetBestProps(
	e RelExpr, required *physical.Required, provided *physical.Provided, cost Cost,
) {
	if e.RequiredPhysical() != nil {
		if e.RequiredPhysical() != required ||
			!e.ProvidedPhysical().Equals(provided) ||
			e.Cost() != cost {
			panic(errors.AssertionFailedf(
				"cannot overwrite %s / %s (%.9g) with %s / %s (%.9g)",
				e.RequiredPhysical(),
				e.ProvidedPhysical(),
				redact.Safe(e.Cost()),
				required.String(),
				provided.String(), // Call String() so provided doesn't escape.
				cost.C,
			))
		}
		return
	}
	bp := e.bestProps()
	bp.required = required
	bp.provided = provided
	bp.cost = cost
}

// ResetCost updates the cost of a relational expression's memo group. It
// should *only* be called by Optimizer.RecomputeCost() for testing purposes.
func (m *Memo) ResetCost(e RelExpr, cost Cost) {
	e.bestProps().cost = cost
}

// IsOptimized returns true if the memo has been fully optimized.
func (m *Memo) IsOptimized() bool {
	// The memo is optimized once the root expression has its physical properties
	// assigned.
	return m.rootExpr != nil && m.rootExpr.RequiredPhysical() != nil
}

// OptimizationCost returns a rough estimate of the cost of optimization of the
// memo. It is dependent on the number of tables in the metadata, based on the
// reasoning that queries with more tables likely have more joins, which tend
// to be the biggest contributors to optimization overhead.
func (m *Memo) OptimizationCost() Cost {
	// This cpuCostFactor is the same as cpuCostFactor in the coster.
	// TODO(mgartner): Package these constants up in a shared location.
	const cpuCostFactor = 0.01
	return Cost{C: float64(m.Metadata().NumTables()) * 1000 * cpuCostFactor}
}

// NextRank returns a new rank that can be assigned to a scalar expression.
func (m *Memo) NextRank() opt.ScalarRank {
	m.curRank++
	return m.curRank
}

// CopyRankAndIDsFrom copies the next ScalarRank, WithID, and
// RoutineResultBufferID from the other memo.
func (m *Memo) CopyRankAndIDsFrom(other *Memo) {
	m.curRank = other.curRank
	m.curWithID = other.curWithID
	m.curRoutineResultBufferID = other.curRoutineResultBufferID
}

// RequestColStat calculates and returns the column statistic calculated on the
// relational expression.
func (m *Memo) RequestColStat(
	expr RelExpr, cols opt.ColSet,
) (colStat *props.ColumnStatistic, ok bool) {
	// When SetRoot is called, the statistics builder may have been cleared.
	// If this happens, we can't serve the request anymore.
	if m.logPropsBuilder.sb.md != nil {
		return m.logPropsBuilder.sb.colStat(cols, expr), true
	}
	return nil, false
}

// RequestColAvgSize calculates and returns the column's average size statistic.
// The column must exist in the table with ID tabId.
func (m *Memo) RequestColAvgSize(tabID opt.TableID, col opt.ColumnID) uint64 {
	// When SetRoot is called, the statistics builder may have been cleared.
	// If this happens, we can't serve the request anymore.
	if m.logPropsBuilder.sb.md != nil {
		return m.logPropsBuilder.sb.colAvgSize(tabID, col)
	}
	return defaultColSize
}

// RowsProcessed calculates and returns the number of rows processed by the
// relational expression. It is currently only supported for joins.
func (m *Memo) RowsProcessed(expr RelExpr) (_ float64, ok bool) {
	// When SetRoot is called, the statistics builder may have been cleared.
	// If this happens, we can't serve the request anymore.
	if m.logPropsBuilder.sb.md != nil {
		return m.logPropsBuilder.sb.rowsProcessed(expr), true
	}
	return 0, false
}

// NextWithID returns a not-yet-assigned identifier for a WITH expression.
func (m *Memo) NextWithID() opt.WithID {
	m.curWithID++
	return m.curWithID
}

// NextRoutineResultBufferID returns a not-yet-assigned identifier for the
// result buffer of a PL/pgSQL set-returning function.
func (m *Memo) NextRoutineResultBufferID() RoutineResultBufferID {
	m.curRoutineResultBufferID++
	return m.curRoutineResultBufferID
}

// Detach is used when we detach a memo that is to be reused later (either for
// execbuilding or with AssignPlaceholders). New expressions should no longer be
// constructed in this memo.
func (m *Memo) Detach() {
	m.interner = interner{}
	m.replacer = nil

	// It is important to not hold on to the EvalCtx in the logicalPropsBuilder
	// (#57059).
	m.logPropsBuilder = logicalPropsBuilder{}

	// Clear all column statistics from every relational expression in the memo.
	// This is used to free up the potentially large amount of memory used by
	// histograms.
	var clearColStats func(parent opt.Expr)
	clearColStats = func(parent opt.Expr) {
		for i, n := 0, parent.ChildCount(); i < n; i++ {
			child := parent.Child(i)
			clearColStats(child)
		}

		switch t := parent.(type) {
		case RelExpr:
			t.Relational().Statistics().ColStats = props.ColStatsMap{}
		}
	}
	clearColStats(m.RootExpr())
}

// DisableCheckExpr disables expression validation performed by CheckExpr,
// if the crdb_test build tag is set. If the crdb_test build tag is not set,
// CheckExpr is always a no-op, so DisableCheckExpr has no effect.
func (m *Memo) DisableCheckExpr() {
	m.disableCheckExpr = true
}

// String prints the current expression tree stored in the memo. It should only
// be used for testing and debugging.
func (m *Memo) String() string {
	return m.FormatExpr(m.rootExpr)
}

// FormatExpr prints the given expression for testing and debugging.
func (m *Memo) FormatExpr(expr opt.Expr) string {
	if expr == nil {
		return ""
	}
	f := MakeExprFmtCtxBuffer(
		context.Background(),
		&bytes.Buffer{},
		ExprFmtHideQualifications,
		false, /* redactableValues */
		m,
		nil, /* catalog */
	)
	f.FormatExpr(expr)
	return f.Buffer.String()
}

// OptimizationStats surfaces information about choices made during optimization
// of a query for top-level observability (e.g. metrics, EXPLAIN output).
type OptimizationStats struct {
	// ClampedHistogramSelectivity is true if the selectivity estimate based on a
	// histogram was prevented from dropping too low. See also the session var
	// "optimizer_clamp_low_histogram_selectivity".
	ClampedHistogramSelectivity bool
	// ClampedInequalitySelectivity is true if the selectivity estimate for an
	// inequality unbounded on one or both sides was prevented from dropping too
	// low. See also the session var "optimizer_clamp_inequality_selectivity".
	ClampedInequalitySelectivity bool
}

// GetOptimizationStats returns the OptimizationStats collected during a
// previous optimization pass.
func (m *Memo) GetOptimizationStats() *OptimizationStats {
	return &m.optimizationStats
}

// ValuesContainer lets ValuesExpr and LiteralValuesExpr share code.
type ValuesContainer interface {
	RelExpr

	Len() int
	ColList() opt.ColList
}

var _ ValuesContainer = &ValuesExpr{}
var _ ValuesContainer = &LiteralValuesExpr{}

// ColList implements the ValuesContainer interface.
func (v *ValuesExpr) ColList() opt.ColList {
	return v.Cols
}

// Len implements the ValuesContainer interface.
func (v *ValuesExpr) Len() int {
	return len(v.Rows)
}

// ColList implements the ValuesContainer interface.
func (l *LiteralValuesExpr) ColList() opt.ColList {
	return l.Cols
}

// Len implements the ValuesContainer interface.
func (l *LiteralValuesExpr) Len() int {
	return l.Rows.Rows.NumRows()
}

// GetLookupJoinLookupTableDistribution returns the Distribution of a lookup
// table in a lookup join if that distribution can be statically determined.
var GetLookupJoinLookupTableDistribution func(
	lookupJoin *LookupJoinExpr,
	required *physical.Required,
	optimizer interface{},
) (physicalDistribution physical.Distribution)

// CopyGroup helps us create a mock LookupJoinExpr in execbuilder during
// building of LockExpr. Unlike setGroup this does *not* add the LookupJoinExpr
// to the group.
func (e *LookupJoinExpr) CopyGroup(expr RelExpr) {
	e.grp = expr.group()
}
