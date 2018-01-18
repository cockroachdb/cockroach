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

package xform

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
)

// PrivateID identifies custom private data used by a memo expression and
// stored by the memo. Privates have numbers greater than 0; a PrivateID of 0
// indicates an unknown private.
type PrivateID uint32

// ListID identifies a variable-sized list used by a memo expression and stored
// by the memo. The ID consists of an offset into the memo's lists slice, plus
// the number of elements in the list. Valid lists have offsets greater than 0;
// a ListID with offset 0 indicates an undefined list (probable indicator of a
// bug).
type ListID struct {
	offset uint32
	len    uint32
}

// memoLoc describes the location of an expression in the memo, which is a
// tuple of the expression's memo group and its index within that group.
type memoLoc struct {
	group GroupID
	expr  exprID
}

// memo is a data structure for efficiently storing a forest of query plans.
// Conceptually, the memo is composed of a numbered set of equivalency classes
// called groups where each group contains a set of logically equivalent
// expressions. The different expressions in a single group are called memo
// expressions (memo-ized expressions). A memo expression has a list of child
// groups as its children rather than a list of individual expressions. The
// forest is composed of every possible combination of parent expression with
// its children, recursively applied.
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
type memo struct {
	// metadata provides access to database metadata and statistics, as well as
	// information about the columns and tables used in this particular query.
	metadata *Metadata

	// exprMap maps from expression fingerprint (memoExpr.fingerprint()) to
	// that expression's group. Multiple different fingerprints can map to the
	// same group, but only one of them is the fingerprint of the group's
	// normalized expression.
	exprMap map[fingerprint]GroupID

	// groups is the set of all groups in the memo, indexed by group ID. Note
	// the group ID 0 is invalid in order to allow zero initialization of an
	// expression to indicate that it did not originate from the memo.
	groups []memoGroup

	// logPropsFactory is used to derive logical properties for an expression,
	// based on the logical properties of its children.
	logPropsFactory logicalPropsFactory

	// Some memoExprs have a variable number of children. The memoExpr stores
	// the list as a ListID struct, which contains an index into this array,
	// plus the count of children. The children are stored as a slice of this
	// array. Note that ListID 0 is invalid in order to indicate an unknown
	// list.
	lists []GroupID

	// Intern the set of unique privates used by expressions in the memo, since
	// there are so many duplicates. Note that PrivateID 0 is invalid in order
	// to indicate an unknown private.
	privatesMap map[interface{}]PrivateID
	privates    []interface{}
}

func newMemo(catalog optbase.Catalog) *memo {
	// NB: group 0 is reserved and intentionally nil so that the 0 group index
	// can indicate that we don't know the group for an expression. Similarly,
	// index 0 for private data, index 0 for physical properties, and index 0
	// for lists are all reserved.
	m := &memo{
		metadata:    newMetadata(catalog),
		exprMap:     make(map[fingerprint]GroupID),
		groups:      make([]memoGroup, 1),
		lists:       make([]GroupID, 1),
		privatesMap: make(map[interface{}]PrivateID),
		privates:    make([]interface{}, 1),
	}

	m.logPropsFactory.init(m)
	return m
}

// newGroup creates a new group and adds it to the memo.
func (m *memo) newGroup(norm *memoExpr) *memoGroup {
	id := GroupID(len(m.groups))
	exprs := []memoExpr{*norm}
	m.groups = append(m.groups, memoGroup{
		id:    id,
		exprs: exprs,
	})
	return &m.groups[len(m.groups)-1]
}

// memoizeNormExpr enters a normalized expression into the memo. This requires
// the creation of a new memo group with the normalized expression as its first
// expression.
func (m *memo) memoizeNormExpr(norm *memoExpr) GroupID {
	if m.exprMap[norm.fingerprint()] != 0 {
		panic("normalized expression has been entered into the memo more than once")
	}

	mgrp := m.newGroup(norm)
	e := makeExprView(m, mgrp.id, minPhysPropsID)
	mgrp.logical = m.logPropsFactory.constructProps(&e)

	m.exprMap[norm.fingerprint()] = mgrp.id
	return mgrp.id
}

// lookupGroup returns the memo group for the given ID.
func (m *memo) lookupGroup(group GroupID) *memoGroup {
	return &m.groups[group]
}

// lookupGroupByFingerprint returns the group of the expression that has the
// given fingerprint.
func (m *memo) lookupGroupByFingerprint(f fingerprint) GroupID {
	return m.exprMap[f]
}

// lookupExpr returns the expression referenced by the given location.
func (m *memo) lookupExpr(loc memoLoc) *memoExpr {
	return m.groups[loc.group].lookupExpr(loc.expr)
}

// storeList allocates storage for a list of group IDs in the memo and returns
// an ID that can be used for later lookup.
// TODO(andyk): lists should be interned.
func (m *memo) storeList(items []GroupID) ListID {
	id := ListID{offset: uint32(len(m.lists)), len: uint32(len(items))}
	m.lists = append(m.lists, items...)
	return id
}

// lookupList returns a list of group IDs that was earlier stored in the memo
// by a call to storeList.
func (m *memo) lookupList(id ListID) []GroupID {
	return m.lists[id.offset : id.offset+id.len]
}

// internPrivate adds the given private value to the memo and returns an ID
// that can be used for later lookup. If the same value was added previously,
// this method is a no-op and returns the ID of the previous value.
// NOTE: Because the internment uses the private value as a map key, only data
//       types which can be map types can be used here.
func (m *memo) internPrivate(private interface{}) PrivateID {
	id, ok := m.privatesMap[private]
	if !ok {
		id = PrivateID(len(m.privates))
		m.privates = append(m.privates, private)
		m.privatesMap[private] = id
	}
	return id
}

// lookupPrivate returns a private value that was earlier interned in the memo
// by a call to internPrivate.
func (m *memo) lookupPrivate(id PrivateID) interface{} {
	return m.privates[id]
}

func (m *memo) String() string {
	var buf bytes.Buffer
	for i := len(m.groups) - 1; i > 0; i-- {
		mgrp := &m.groups[i]
		fmt.Fprintf(&buf, "%d:", i)
		fmt.Fprintf(&buf, " %s", mgrp.memoGroupString(m))
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}
