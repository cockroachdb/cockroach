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

package memo

import (
	"bytes"
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// PhysicalPropsID identifies a set of physical properties that has been
// interned by a memo instance. If two ids are the same, then the physical
// properties are the same.
type PhysicalPropsID uint32

const (
	// MinPhysPropsID is the id of the well-known set of physical properties
	// that requires nothing of an operator. Therefore, every operator is
	// guaranteed to provide this set of properties. This is typically the most
	// commonly used set of physical properties in the memo, since most
	// operators do not require any physical properties from their children.
	MinPhysPropsID PhysicalPropsID = 1
)

// Memo is a data structure for efficiently storing a forest of query plans.
// Conceptually, the memo is composed of a numbered set of equivalency classes
// called groups where each group contains a set of logically equivalent
// expressions. The different expressions in a single group are called memo
// expressions (memo-ized expressions). A memo expression has a list of child
// groups as its children rather than a list of individual expressions. The
// forest is composed of every possible combination of parent expression with
// its children, recursively applied.
//
// Memo expressions can be relational (e.g. join) or scalar (e.g. <). Operators
// are always both logical (specify results) and physical (specify results and
// a particular implementation). This means that even a "raw" unoptimized
// expression tree can be executed (naively). Both relational and scalar
// operators are uniformly represented as nodes in memo expression trees, which
// facilitates tree pattern matching and replacement.
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
// rewrite of the tree. Uniqueness is determined by computing the fingerprint
// for a memo expression, which is simply the expression operator and its list
// of child groups. For example, consider this query:
//
//   SELECT * FROM a, b WHERE a.x = b.x
//
// After insertion into the memo, the memo would contain these six groups:
//
//   6: [inner-join [1 2 5]]
//   5: [eq [3 4]]
//   4: [variable b.x]
//   3: [variable a.x]
//   2: [scan b]
//   1: [scan a]
//
// The fingerprint for the inner-join expression is [inner-join [1 2 5]]. The
// memo maintains a map from expression fingerprint to memo group which allows
// quick determination of whether the normalized form of an expression already
// exists in the memo.
//
// The normalizing factory will never add more than one expression to a memo
// group. But the explorer does add denormalized expressions to existing memo
// groups, since oftentimes one of these equivalent, but denormalized
// expressions will have a lower cost than the initial normalized expression
// added by the factory. For example, the join commutativity transformation
// expands the memo like this:
//
//   6: [inner-join [1 2 5]] [inner-join [2 1 5]]
//   5: [eq [3 4]]
//   4: [variable b.x]
//   3: [variable a.x]
//   2: [scan b]
//   1: [scan a]
//
// TODO(andyk): See the comments in explorer.go for more details.
type Memo struct {
	// metadata provides information about the columns and tables used in this
	// particular query.
	metadata opt.Metadata

	// exprMap maps from expression fingerprint (Expr.fingerprint()) to
	// that expression's group. Multiple different fingerprints can map to the
	// same group, but only one of them is the fingerprint of the group's
	// normalized expression.
	exprMap map[Fingerprint]GroupID

	// groups is the set of all groups in the memo, indexed by group ID. Note
	// the group ID 0 is invalid in order to allow zero initialization of an
	// expression to indicate that it did not originate from the memo.
	groups []group

	// logPropsBuilder is inlined in the memo so that it can be reused each time
	// scalar or relational properties need to be built.
	logPropsBuilder logicalPropsBuilder

	// Some memoExprs have a variable number of children. The Expr stores
	// the list as a ListID struct, which is a slice of an array maintained by
	// listStorage. Note that ListID 0 is invalid in order to indicate an
	// unknown list.
	listStorage listStorage

	// Intern the set of unique privates used by expressions in the memo, since
	// there are so many duplicates.
	privateStorage privateStorage

	// rootGroup is the root group of the memo expression forest. It is set via
	// a call to SetRoot.
	rootGroup GroupID

	// rootProps are the physical properties required of the root memo group. It
	// is set via a call to SetRoot.
	rootProps PhysicalPropsID

	// memEstimate is the approximate memory usage of the memo, in bytes.
	memEstimate int64

	// locName is the location which the memo is compiled against. This determines
	// the timezone, which is used for time-related data type construction and
	// comparisons. If the location changes, then this memo is invalidated.
	locName string

	// dbName is the current database at the time the memo was compiled. If this
	// changes, then the memo is invalidated.
	dbName string

	// searchPath is the current search path at the time the memo was compiled.
	// If this changes, then the memo is invalidated.
	searchPath sessiondata.SearchPath
}

// Init initializes a new empty memo instance, or resets existing state so it
// can be reused. It must be called before use (or reuse). The memo collects
// information about the context in which it is compiled from the evalContext
// argument. If any of that changes, then the memo must be invalidated (see the
// IsStale method for more details).
func (m *Memo) Init(evalCtx *tree.EvalContext) {
	// NB: group 0 is reserved and intentionally nil so that the 0 group index
	// can indicate that we don't know the group for an expression. Similarly,
	// index 0 for private data, index 0 for physical properties, and index 0
	// for lists are all reserved.
	m.metadata.Init()

	// TODO(andyk): Investigate fast map clear when we move to Go 1.11.
	m.exprMap = make(map[Fingerprint]GroupID)

	// Reuse groups slice unless it was under-utilized.
	const minGroupCount = 12
	if m.groups == nil || (len(m.groups) > minGroupCount && len(m.groups) < cap(m.groups)/2) {
		m.groups = make([]group, 1, minGroupCount)
	} else {
		m.groups = m.groups[:1]
	}

	m.listStorage.init()
	m.privateStorage.init()
	m.rootGroup = 0
	m.rootProps = 0
	m.memEstimate = 0
	m.locName = evalCtx.GetLocation().String()
	m.dbName = evalCtx.SessionData.Database
	m.searchPath = evalCtx.SessionData.SearchPath
}

// InitFrom initializes the memo with a deep copy of the provided memo. This
// memo can then be modified independent of the copied memo.
func (m *Memo) InitFrom(from *Memo) {
	if from.groups == nil {
		panic("cannot initialize from an uninitialized memo")
	}

	m.rootGroup = from.rootGroup
	m.rootProps = from.rootProps
	m.memEstimate = from.memEstimate
	m.locName = from.locName
	m.dbName = from.dbName
	m.searchPath = from.searchPath

	// Copy the metadata.
	m.metadata.InitFrom(&from.metadata)

	// Copy the expression map.
	m.exprMap = make(map[Fingerprint]GroupID, len(from.exprMap))
	for k, v := range from.exprMap {
		m.exprMap[k] = v
	}

	// Copy the groups.
	if m.groups == nil {
		m.groups = make([]group, 0, len(from.groups))
	} else {
		m.groups = m.groups[:0]
	}
	for i := range from.groups {
		from := &from.groups[i]
		m.groups = append(m.groups, group{
			id:            from.id,
			logical:       from.logical,
			normExpr:      from.normExpr,
			firstBestExpr: from.firstBestExpr,

			// These slices are never reused, so can share the slice prefix.
			otherExprs:     from.otherExprs[:len(from.otherExprs):len(from.otherExprs)],
			otherBestExprs: from.otherBestExprs[:len(from.otherBestExprs):len(from.otherBestExprs)],
		})
	}

	// Copy all memoized lists.
	m.listStorage.initFrom(&from.listStorage)

	// Copy all private values.
	m.privateStorage.initFrom(&from.privateStorage)
}

// MemoryEstimate returns a rough estimate of the memo's memory usage, in bytes.
// It only includes memory usage that is proportional to the size and complexity
// of the query, rather than constant overhead bytes.
func (m *Memo) MemoryEstimate() int64 {
	return m.memEstimate + m.listStorage.memoryEstimate() + m.privateStorage.memoryEstimate()
}

// Metadata returns the metadata instance associated with the memo.
func (m *Memo) Metadata() *opt.Metadata {
	return &m.metadata
}

// RootGroup returns the root memo group previously set via a call to SetRoot.
func (m *Memo) RootGroup() GroupID {
	return m.rootGroup
}

// RootProps returns the physical properties required of the root memo group,
// previously set via a call to SetRoot.
func (m *Memo) RootProps() PhysicalPropsID {
	return m.rootProps
}

// Root returns an ExprView wrapper around the root of the memo. If the memo has
// not yet been optimized, this will be a view over the normalized expression
// tree. Otherwise, it's a view over the lowest cost expression tree.
func (m *Memo) Root() ExprView {
	if m.isOptimized() {
		root := m.group(m.rootGroup)
		for i, n := 0, root.bestExprCount(); i < n; i++ {
			be := root.bestExpr(bestOrdinal(i))
			if be.required == m.rootProps {
				return MakeExprView(m, BestExprID{group: m.rootGroup, ordinal: bestOrdinal(i)})
			}
		}
		panic("could not find best expression that matches the root properties")
	}
	return MakeNormExprView(m, m.rootGroup)
}

// SetRoot stores the root memo group, as well as the physical properties
// required of the root group.
func (m *Memo) SetRoot(group GroupID, physical PhysicalPropsID) {
	m.rootGroup = group
	m.rootProps = physical
}

// HasPlaceholders returns true if the memo contains at least one placeholder
// operator.
func (m *Memo) HasPlaceholders() bool {
	return m.GroupProperties(m.rootGroup).Relational.HasPlaceholder
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
func (m *Memo) IsStale(ctx context.Context, evalCtx *tree.EvalContext, catalog opt.Catalog) bool {
	// Memo is stale if the current database has changed.
	if m.dbName != evalCtx.SessionData.Database {
		return true
	}

	// Memo is stale if the search path has changed. Assume it's changed if the
	// slice length is different, or if it no longer points to the same underlying
	// array. If two slices are the same length and point to the same underlying
	// array, then they are guaranteed to be identical. Note that GetPathArray
	// already specifies that the slice must not be modified, so its elements will
	// never be modified in-place.
	left := m.searchPath.GetPathArray()
	right := evalCtx.SessionData.SearchPath.GetPathArray()
	if len(left) != len(right) {
		return true
	}
	if len(left) != 0 && &left[0] != &right[0] {
		return true
	}

	// Memo is stale if the location has changed.
	if m.locName != evalCtx.GetLocation().String() {
		return true
	}

	// Memo is stale if the fingerprint of any data source in the memo's metadata
	// has changed, or if the current user no longer has sufficient privilege to
	// access the data source.
	if !m.Metadata().CheckDependencies(ctx, catalog) {
		return true
	}
	return false
}

// isOptimized returns true if the memo has been fully optimized.
func (m *Memo) isOptimized() bool {
	// The memo is optimized once a best expression has been set at the root.
	return m.rootGroup != 0 && m.group(m.rootGroup).firstBestExpr.initialized()
}

// --------------------------------------------------------------------
// Group methods.
// --------------------------------------------------------------------

// GroupProperties returns the logical properties of the given group.
func (m *Memo) GroupProperties(group GroupID) *props.Logical {
	return &m.groups[group].logical
}

// GroupByFingerprint returns the group of the expression that has the given
// fingerprint.
func (m *Memo) GroupByFingerprint(f Fingerprint) GroupID {
	return m.exprMap[f]
}

// AddAltFingerprint adds an additional fingerprint that references an existing
// group. The new fingerprint corresponds to a denormalized expression that is
// an alternate form of the group's normalized expression. Adding it to the
// fingerprint map avoids re-adding the same expression in the future.
func (m *Memo) AddAltFingerprint(alt Fingerprint, group GroupID) {
	existing, ok := m.exprMap[alt]
	if ok {
		if existing != group {
			panic("same fingerprint cannot map to different groups")
		}
	} else {
		m.exprMap[alt] = group
	}
}

// newGroup creates a new group and adds it to the memo.
func (m *Memo) newGroup(norm Expr) *group {
	id := GroupID(len(m.groups))
	m.exprMap[norm.Fingerprint()] = id
	m.groups = append(m.groups, makeMemoGroup(id, norm))
	return &m.groups[len(m.groups)-1]
}

// group returns the memo group for the given ID.
func (m *Memo) group(group GroupID) *group {
	return &m.groups[group]
}

// --------------------------------------------------------------------
// Expression methods.
// --------------------------------------------------------------------

// ExprCount returns the number of expressions in the given memo group. There
// is always at least one expression in the group (the normalized expression).
func (m *Memo) ExprCount(group GroupID) int {
	return m.groups[group].exprCount()
}

// Expr returns the memo expression for the given ID.
func (m *Memo) Expr(eid ExprID) *Expr {
	return m.groups[eid.Group].expr(eid.Expr)
}

// NormExpr returns the normalized expression for the given group. Each group
// has one canonical expression that is always the first expression in the
// group, and which results from running normalization rules on the expression
// until the final normal state has been reached.
func (m *Memo) NormExpr(group GroupID) *Expr {
	return m.groups[group].expr(normExprOrdinal)
}

// NormOp returns the operator type of NormExpr. See that method's comment for
// more details.
func (m *Memo) NormOp(group GroupID) opt.Operator {
	return m.groups[group].expr(normExprOrdinal).op
}

// MemoizeNormExpr enters a normalized expression into the memo. This requires
// the creation of a new memo group with the normalized expression as its first
// expression. If the expression is already part of an existing memo group, then
// MemoizeNormExpr is a no-op, and returns the existing ExprView.
func (m *Memo) MemoizeNormExpr(evalCtx *tree.EvalContext, norm Expr) ExprView {
	existing := m.exprMap[norm.Fingerprint()]
	if existing != 0 {
		return MakeNormExprView(m, existing)
	}

	// Use rough memory usage estimate of size of group * 4 to account for size
	// of group struct + logical props + best exprs + expr map overhead.
	const groupSize = int64(unsafe.Sizeof(group{}))
	m.memEstimate += groupSize * 4

	mgrp := m.newGroup(norm)
	ev := MakeNormExprView(m, mgrp.id)
	mgrp.logical = m.logPropsBuilder.buildProps(evalCtx, ev)

	// RaceEnabled ensures that checks are run on every PR (as part of make
	// testrace) while keeping the check code out of non-test builds.
	if util.RaceEnabled {
		m.CheckExpr(MakeNormExprID(mgrp.id))
	}

	return ev
}

// MemoizeDenormExpr enters a denormalized expression into the given memo
// group. A denormalized expression is logically equivalent to the group's
// normalized expression, but is an alternate form that may have a lower cost.
// The group must already exist, since the normalized version of the expression
// should have triggered its creation earlier.
func (m *Memo) MemoizeDenormExpr(group GroupID, denorm Expr) {
	existing := m.exprMap[denorm.Fingerprint()]
	if existing != 0 {
		// Expression has already been entered into the memo.
		if existing != group {
			panic("denormalized expression's group doesn't match fingerprint group")
		}
		return
	}

	// Use rough memory usage estimate of size of expr * 4 to account for size
	// of expr struct + fingerprint + expr map overhead.
	const exprSize = int64(unsafe.Sizeof(Expr{}))
	m.memEstimate += exprSize * 4

	// Add the denormalized expression to the memo.
	m.group(group).addExpr(denorm)
	m.exprMap[denorm.Fingerprint()] = group

	// RaceEnabled ensures that checks are run on every PR (as part of make
	// testrace) while keeping the check code out of non-test builds.
	if util.RaceEnabled {
		m.CheckExpr(ExprID{
			Group: group,
			Expr:  ExprOrdinal(m.group(group).exprCount() - 1),
		})
	}
}

// --------------------------------------------------------------------
// Best expression methods.
// --------------------------------------------------------------------

// EnsureBestExpr finds the expression in the given group that can provide the
// required properties for the lowest cost and returns its id. If no best
// expression exists yet, then EnsureBestExpr creates a new empty BestExpr and
// returns its id.
func (m *Memo) EnsureBestExpr(group GroupID, required PhysicalPropsID) BestExprID {
	return m.group(group).ensureBestExpr(required)
}

// RatchetBestExpr overwrites the existing best expression with the given id if
// the candidate expression has a lower cost.
func (m *Memo) RatchetBestExpr(best BestExprID, candidate *BestExpr) {
	m.bestExpr(best).ratchetCost(candidate)
}

// BestExprCost returns the estimated cost of the given best expression.
func (m *Memo) BestExprCost(best BestExprID) Cost {
	return m.bestExpr(best).cost
}

// BestExprLogical returns the logical properties of the given best expression.
func (m *Memo) BestExprLogical(best BestExprID) *props.Logical {
	return m.GroupProperties(best.group)
}

// bestExpr returns the best expression with the given id.
// NOTE: The returned best expression is only valid until the next call to
//       EnsureBestExpr, since that may trigger a resize of the bestExprs slice
//       in the group.
func (m *Memo) bestExpr(best BestExprID) *BestExpr {
	return m.groups[best.group].bestExpr(best.ordinal)
}

// --------------------------------------------------------------------
// Interning methods.
// --------------------------------------------------------------------

// InternList adds the given list of group IDs to memo storage and returns an
// ID that can be used for later lookup. If the same list was added previously,
// this method is a no-op and returns the ID of the previous value.
func (m *Memo) InternList(items []GroupID) ListID {
	return m.listStorage.intern(items)
}

// LookupList returns a list of group IDs that was earlier stored in the memo
// by a call to InternList.
func (m *Memo) LookupList(id ListID) []GroupID {
	return m.listStorage.lookup(id)
}

// InternPhysicalProps adds the given props to the memo if that set hasn't yet
// been added, and returns an ID which can later be used to look up the props.
// If the same list was added previously, then this method is a no-op and
// returns the same ID as did the previous call.
func (m *Memo) InternPhysicalProps(physical *props.Physical) PhysicalPropsID {
	// Special case physical properties that require nothing of operator.
	if !physical.Defined() {
		return MinPhysPropsID
	}
	return PhysicalPropsID(m.privateStorage.internPhysProps(physical))
}

// LookupPhysicalProps returns the set of physical props that was earlier
// interned in the memo by a call to InternPhysicalProps.
func (m *Memo) LookupPhysicalProps(id PhysicalPropsID) *props.Physical {
	if id == MinPhysPropsID {
		return &props.MinPhysProps
	}
	return m.privateStorage.lookup(PrivateID(id)).(*props.Physical)
}

// LookupPrivate returns a private value that was earlier interned in the memo
// by a call to InternPrivate.
func (m *Memo) LookupPrivate(id PrivateID) interface{} {
	return m.privateStorage.lookup(id)
}

// --------------------------------------------------------------------
// String representation.
// --------------------------------------------------------------------

// FmtFlags controls how the memo output is formatted.
type FmtFlags int

// HasFlags tests whether the given flags are all set.
func (f FmtFlags) HasFlags(subset FmtFlags) bool {
	return f&subset == subset
}

const (
	// FmtPretty performs a breadth-first topological sort on the memo groups,
	// and shows the root group at the top of the memo.
	FmtPretty FmtFlags = 0

	// FmtRaw shows the raw memo groups, in the order they were originally
	// added, and including any "orphaned" groups.
	FmtRaw FmtFlags = 1 << (iota - 1)
)

// String returns a human-readable string representation of this memo for
// testing and debugging.
func (m *Memo) String() string {
	return m.FormatString(FmtPretty)
}

// FormatString returns a string representation of this memo for testing
// and debugging. The given flags control which properties are shown.
func (m *Memo) FormatString(flags FmtFlags) string {
	return m.format(&memoFmtCtx{buf: &bytes.Buffer{}, flags: flags})
}
