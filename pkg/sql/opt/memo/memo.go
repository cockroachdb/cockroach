// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Memo is a data structure for efficiently storing a forest of query plans.
// Conceptually, the memo is composed of a numbered set of equivalency classes
// called groups where each group contains a set of logically equivalent
// expressions. Two expressions are considered logically equivalent if:
//
//   1. They return the same number and data type of columns. However, order and
//      naming of columns doesn't matter.
//   2. They return the same number of rows, with the same values in each row.
//      However, order of rows doesn't matter.
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
//   SELECT * FROM a, b WHERE a.x = b.x
//
// After insertion into the memo, the memo would contain these six groups, with
// numbers substituted for pointers to the normalized expression in each group:
//
//   G6: [inner-join [G1 G2 G5]]
//   G5: [eq [G3 G4]]
//   G4: [variable b.x]
//   G3: [variable a.x]
//   G2: [scan b]
//   G1: [scan a]
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
//   G6: [inner-join [G1 G2 G5]] [inner-join [G2 G1 G5]]
//   G5: [eq [G3 G4]]
//   G4: [variable b.x]
//   G3: [variable a.x]
//   G2: [scan b]
//   G1: [scan a]
//
// See the comments in explorer.go for more details.
type Memo struct {
	// metadata provides information about the columns and tables used in this
	// particular query.
	metadata opt.Metadata

	// interner interns all expressions in the memo, ensuring that there is at
	// most one instance of each expression in the memo.
	interner interner

	// logPropsBuilder is inlined in the memo so that it can be reused each time
	// scalar or relational properties need to be built.
	logPropsBuilder logicalPropsBuilder

	// rootExpr is the root expression of the memo expression forest. It is set
	// via a call to SetRoot. After optimization, it is set to be the root of the
	// lowest cost tree in the forest.
	rootExpr opt.Expr

	// rootProps are the physical properties required of the root memo expression.
	// It is set via a call to SetRoot.
	rootProps *physical.Required

	// memEstimate is the approximate memory usage of the memo, in bytes.
	memEstimate int64

	// The following are selected fields from SessionData which can affect
	// planning. We need to cross-check these before reusing a cached memo.
	reorderJoinsLimit       int
	zigzagJoinEnabled       bool
	useHistograms           bool
	useMultiColStats        bool
	localityOptimizedSearch bool
	safeUpdates             bool
	preferLookupJoinsForFKs bool
	saveTablesPrefix        string

	// curID is the highest currently in-use scalar expression ID.
	curID opt.ScalarID

	// curWithID is the highest currently in-use WITH ID.
	curWithID opt.WithID

	newGroupFn func(opt.Expr)

	// disableCheckExpr disables expression validation performed by CheckExpr,
	// if the crdb_test build tag is set. If the crdb_test build tag is not set,
	// CheckExpr is always a no-op, so disableCheckExpr has no effect. This is
	// set to true for the optsteps test command to prevent CheckExpr from
	// erring with partially normalized expressions.
	disableCheckExpr bool

	// WARNING: if you add more members, add initialization code in Init (if
	// reusing allocated data structures is desired).
}

// Init initializes a new empty memo instance, or resets existing state so it
// can be reused. It must be called before use (or reuse). The memo collects
// information about the context in which it is compiled from the evalContext
// argument. If any of that changes, then the memo must be invalidated (see the
// IsStale method for more details).
func (m *Memo) Init(evalCtx *tree.EvalContext) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*m = Memo{
		metadata:                m.metadata,
		reorderJoinsLimit:       evalCtx.SessionData.ReorderJoinsLimit,
		zigzagJoinEnabled:       evalCtx.SessionData.ZigzagJoinEnabled,
		useHistograms:           evalCtx.SessionData.OptimizerUseHistograms,
		useMultiColStats:        evalCtx.SessionData.OptimizerUseMultiColStats,
		localityOptimizedSearch: evalCtx.SessionData.LocalityOptimizedSearch,
		safeUpdates:             evalCtx.SessionData.SafeUpdates,
		preferLookupJoinsForFKs: evalCtx.SessionData.PreferLookupJoinsForFKs,
		saveTablesPrefix:        evalCtx.SessionData.SaveTablesPrefix,
	}
	m.metadata.Init()
	m.logPropsBuilder.init(evalCtx, m)
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
func (m *Memo) RootExpr() opt.Expr {
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

// SetScalarRoot stores the root memo expression when it is a scalar expression.
// Used only for testing.
func (m *Memo) SetScalarRoot(scalar opt.ScalarExpr) {
	m.rootExpr = scalar
}

// HasPlaceholders returns true if the memo contains at least one placeholder
// operator.
func (m *Memo) HasPlaceholders() bool {
	rel, ok := m.rootExpr.(RelExpr)
	if !ok {
		panic(errors.AssertionFailedf("placeholders only supported when memo root is relational"))
	}

	return rel.Relational().HasPlaceholder
}

// IsStale returns true if the memo has been invalidated by changes to any of
// its dependencies. Once a memo is known to be stale, it must be ejected from
// any query cache or prepared statement and replaced with a recompiled memo
// that takes into account the changes. IsStale checks the following
// dependencies:
//
//   1. Current database: this can change name resolution.
//   2. Current search path: this can change name resolution.
//   3. Current location: this determines time zone, and can change how time-
//      related types are constructed and compared.
//   4. Data source schema: this determines most aspects of how the query is
//      compiled.
//   5. Data source privileges: current user may no longer have access to one or
//      more data sources.
//
// This function cannot swallow errors and return only a boolean, as it may
// perform KV operations on behalf of the transaction associated with the
// provided catalog, and those errors are required to be propagated.
func (m *Memo) IsStale(
	ctx context.Context, evalCtx *tree.EvalContext, catalog cat.Catalog,
) (bool, error) {
	// Memo is stale if fields from SessionData that can affect planning have
	// changed.
	if m.reorderJoinsLimit != evalCtx.SessionData.ReorderJoinsLimit ||
		m.zigzagJoinEnabled != evalCtx.SessionData.ZigzagJoinEnabled ||
		m.useHistograms != evalCtx.SessionData.OptimizerUseHistograms ||
		m.useMultiColStats != evalCtx.SessionData.OptimizerUseMultiColStats ||
		m.localityOptimizedSearch != evalCtx.SessionData.LocalityOptimizedSearch ||
		m.safeUpdates != evalCtx.SessionData.SafeUpdates ||
		m.preferLookupJoinsForFKs != evalCtx.SessionData.PreferLookupJoinsForFKs ||
		m.saveTablesPrefix != evalCtx.SessionData.SaveTablesPrefix {
		return true, nil
	}

	// Memo is stale if the fingerprint of any object in the memo's metadata has
	// changed, or if the current user no longer has sufficient privilege to
	// access the object.
	if depsUpToDate, err := m.Metadata().CheckDependencies(ctx, catalog); err != nil {
		return true, err
	} else if !depsUpToDate {
		return true, nil
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
				log.Safe(e.Cost()),
				required.String(),
				provided.String(), // Call String() so provided doesn't escape.
				cost,
			))
		}
		return
	}
	bp := e.bestProps()
	bp.required = required
	bp.provided = *provided
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
	rel, ok := m.rootExpr.(RelExpr)
	return ok && rel.RequiredPhysical() != nil
}

// NextID returns a new unique ScalarID to number expressions with.
func (m *Memo) NextID() opt.ScalarID {
	m.curID++
	return m.curID
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

// Detach is used when we detach a memo that is to be reused later (either for
// execbuilding or with AssignPlaceholders). New expressions should no longer be
// constructed in this memo.
func (m *Memo) Detach() {
	m.interner = interner{}
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
			t.Relational().Stats.ColStats = props.ColStatsMap{}
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
