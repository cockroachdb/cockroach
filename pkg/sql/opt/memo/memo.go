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
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
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
	metadata *opt.Metadata

	// exprMap maps from expression fingerprint (Expr.fingerprint()) to
	// that expression's group. Multiple different fingerprints can map to the
	// same group, but only one of them is the fingerprint of the group's
	// normalized expression.
	exprMap map[Fingerprint]GroupID

	// groups is the set of all groups in the memo, indexed by group ID. Note
	// the group ID 0 is invalid in order to allow zero initialization of an
	// expression to indicate that it did not originate from the memo.
	groups []group

	// Intern the set of unique physical properties used by expressions in the
	// memo, since there are so many duplicates.
	physPropsMap map[string]PhysicalPropsID
	physProps    []PhysicalProps

	// Some memoExprs have a variable number of children. The Expr stores
	// the list as a ListID struct, which is a slice of an array maintained by
	// listStorage. Note that ListID 0 is invalid in order to indicate an
	// unknown list.
	listStorage listStorage

	// Intern the set of unique privates used by expressions in the memo, since
	// there are so many duplicates.
	privateStorage privateStorage
}

// New constructs a new empty memo instance.
func New() *Memo {
	// NB: group 0 is reserved and intentionally nil so that the 0 group index
	// can indicate that we don't know the group for an expression. Similarly,
	// index 0 for private data, index 0 for physical properties, and index 0
	// for lists are all reserved.
	m := &Memo{
		metadata:     opt.NewMetadata(),
		exprMap:      make(map[Fingerprint]GroupID),
		groups:       make([]group, 1),
		physPropsMap: make(map[string]PhysicalPropsID),
		physProps:    make([]PhysicalProps, 1, 2),
	}

	// Intern physical properties that require nothing of operator.
	physProps := PhysicalProps{}
	m.physProps = append(m.physProps, physProps)
	m.physPropsMap[physProps.Fingerprint()] = MinPhysPropsID

	m.listStorage.init()
	m.privateStorage.init()
	return m
}

// Metadata returns the metadata instance associated with the memo.
func (m *Memo) Metadata() *opt.Metadata {
	return m.metadata
}

// --------------------------------------------------------------------
// Group methods.
// --------------------------------------------------------------------

// GroupProperties returns the logical properties of the given group.
func (m *Memo) GroupProperties(group GroupID) *LogicalProps {
	return &m.groups[group].logical
}

// GroupByFingerprint returns the group of the expression that has the
// given fingerprint.
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

// MemoizeNormExpr enters a normalized expression into the memo. This requires
// the creation of a new memo group with the normalized expression as its first
// expression.
func (m *Memo) MemoizeNormExpr(evalCtx *tree.EvalContext, norm Expr) GroupID {
	if m.exprMap[norm.Fingerprint()] != 0 {
		panic("normalized expression has been entered into the memo more than once")
	}

	mgrp := m.newGroup(norm)
	ev := MakeNormExprView(m, mgrp.id)
	logPropsFactory := logicalPropsFactory{evalCtx: evalCtx}
	mgrp.logical = logPropsFactory.constructProps(ev)

	m.exprMap[norm.Fingerprint()] = mgrp.id
	return mgrp.id
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
	} else {
		// Add the denormalized expression to the memo.
		m.group(group).addExpr(denorm)
		m.exprMap[denorm.Fingerprint()] = group
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
func (m *Memo) BestExprLogical(best BestExprID) *LogicalProps {
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
func (m *Memo) InternPhysicalProps(props *PhysicalProps) PhysicalPropsID {
	fingerprint := props.Fingerprint()
	id, ok := m.physPropsMap[fingerprint]
	if !ok {
		id = PhysicalPropsID(len(m.physProps))
		m.physProps = append(m.physProps, *props)
		m.physPropsMap[fingerprint] = id
	}
	return id
}

// LookupPhysicalProps returns the set of physical props that was earlier
// interned in the memo by a call to InternPhysicalProps.
func (m *Memo) LookupPhysicalProps(id PhysicalPropsID) *PhysicalProps {
	return &m.physProps[id]
}

// LookupPrivate returns a private value that was earlier interned in the memo
// by a call to InternPrivate.
func (m *Memo) LookupPrivate(id PrivateID) interface{} {
	return m.privateStorage.lookup(id)
}

// --------------------------------------------------------------------
// String representation.
// --------------------------------------------------------------------

func (m *Memo) String() string {
	tp := treeprinter.New()
	root := tp.Child("memo")

	var buf bytes.Buffer
	for i := len(m.groups) - 1; i > 0; i-- {
		mgrp := &m.groups[i]

		buf.Reset()
		for ord := 0; ord < mgrp.exprCount(); ord++ {
			if ord != 0 {
				buf.WriteByte(' ')
			}
			m.formatExpr(&buf, mgrp.expr(ExprOrdinal(ord)))
		}

		child := root.Childf("%d: %s", i, buf.String())
		m.formatBestExprSet(child, mgrp)
	}

	return tp.String()
}

func (m *Memo) formatExpr(buf *bytes.Buffer, e *Expr) {
	fmt.Fprintf(buf, "(%s", e.op)
	for i := 0; i < e.ChildCount(); i++ {
		fmt.Fprintf(buf, " %d", e.ChildGroup(m, i))
	}
	m.formatPrivate(buf, e.Private(m))
	buf.WriteString(")")
}

type bestExprSort struct {
	required    PhysicalPropsID
	fingerprint string
	best        *BestExpr
}

func (m *Memo) formatBestExprSet(tp treeprinter.Node, mgrp *group) {
	// Sort the bestExprs by required properties.
	cnt := mgrp.bestExprCount()
	beSort := make([]bestExprSort, 0, cnt)
	for i := 0; i < cnt; i++ {
		best := mgrp.bestExpr(bestOrdinal(i))
		beSort = append(beSort, bestExprSort{
			required:    best.required,
			fingerprint: m.LookupPhysicalProps(best.required).Fingerprint(),
			best:        best,
		})
	}

	sort.Slice(beSort, func(i, j int) bool {
		return strings.Compare(beSort[i].fingerprint, beSort[j].fingerprint) < 0
	})

	var buf bytes.Buffer
	for _, sort := range beSort {
		buf.Reset()

		// Don't show best expressions for scalar groups because they're not too
		// interesting.
		if !isScalarLookup[sort.best.op] {
			child := tp.Childf("\"%s\" [cost=%.2f]", sort.fingerprint, sort.best.cost)
			m.formatBestExpr(&buf, sort.best)
			child.Childf("best: %s", buf.String())
		}
	}
}

func (m *Memo) formatBestExpr(buf *bytes.Buffer, be *BestExpr) {
	fmt.Fprintf(buf, "(%s", be.op)

	for i := 0; i < be.ChildCount(); i++ {
		bestChild := be.Child(i)
		fmt.Fprintf(buf, " %d", bestChild.group)

		// Print properties required of the child if they are interesting.
		required := m.bestExpr(bestChild).required
		if required != MinPhysPropsID {
			fmt.Fprintf(buf, "=\"%s\"", m.LookupPhysicalProps(required).Fingerprint())
		}
	}

	m.formatPrivate(buf, be.Private(m))
	buf.WriteString(")")
}

func (m *Memo) formatPrivate(buf *bytes.Buffer, private interface{}) {
	if private != nil {
		switch t := private.(type) {
		case nil:

		case *ScanOpDef:
			m.formatScanPrivate(buf, t, false /* short */)

		case opt.ColumnID:
			fmt.Fprintf(buf, " %s", m.metadata.ColumnLabel(t))

		case opt.ColSet, opt.ColList:
			// Don't show anything, because it's mostly redundant.

		default:
			fmt.Fprintf(buf, " %s", private)
		}
	}
}

func (m *Memo) formatScanPrivate(buf *bytes.Buffer, def *ScanOpDef, short bool) {
	// Don't output name of index if it's the primary index.
	tab := m.metadata.Table(def.Table)
	if def.Index == opt.PrimaryIndex {
		fmt.Fprintf(buf, " %s", tab.TabName())
	} else {
		fmt.Fprintf(buf, " %s@%s", tab.TabName(), tab.Index(def.Index).IdxName())
	}

	// Add additional fields when short=false.
	if !short {
		if def.Constraint != nil {
			fmt.Fprintf(buf, ",constrained")
		}
		if def.HardLimit > 0 {
			fmt.Fprintf(buf, ",lim=%d", def.HardLimit)
		}
	}
}
