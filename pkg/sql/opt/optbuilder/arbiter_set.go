// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// arbiterSet represents a set of arbiters. Unique indexes or constraints can be
// arbiters. This set provides an abstraction on top of both types of arbiters
// so that consumers of the set do not need to be concerned with the underlying
// arbiter type.
type arbiterSet struct {
	mb *mutationBuilder

	// indexes contains the index arbiters in the set, as ordinals into the
	// table's indexes.
	indexes util.FastIntSet

	// uniqueConstraints contains the unique constraint arbiters in the set, as
	// ordinals into the table's unique constraints.
	uniqueConstraints util.FastIntSet
}

// makeArbiterSet returns an initialized arbiterSet.
func makeArbiterSet(mb *mutationBuilder) arbiterSet {
	return arbiterSet{
		mb: mb,
	}
}

// makeSingleIndexArbiterSet returns an initialized arbiterSet with the given
// index as the sole arbiter in the set.
func makeSingleIndexArbiterSet(mb *mutationBuilder, idx cat.IndexOrdinal) arbiterSet {
	a := makeArbiterSet(mb)
	a.AddIndex(idx)
	return a
}

// makeSingleUniqueConstraintArbiterSet returns an initialized arbiterSet with
// the given unique constraint as the sole arbiter in the set.
func makeSingleUniqueConstraintArbiterSet(mb *mutationBuilder, uniq cat.UniqueOrdinal) arbiterSet {
	a := makeArbiterSet(mb)
	a.AddUniqueConstraint(uniq)
	return a
}

// AddIndex adds an index arbiter to the set.
func (a *arbiterSet) AddIndex(idx cat.IndexOrdinal) {
	a.indexes.Add(idx)
}

// AddUniqueConstraint adds a unique constraint arbiter to the set.
func (a *arbiterSet) AddUniqueConstraint(uniq cat.UniqueOrdinal) {
	a.uniqueConstraints.Add(uniq)
}

// Empty returns true if the set is empty.
func (a *arbiterSet) Empty() bool {
	return a.indexes.Empty() && a.uniqueConstraints.Empty()
}

// Len returns the number of the arbiters in the set.
func (a *arbiterSet) Len() int {
	return a.indexes.Len() + a.uniqueConstraints.Len()
}

// IndexOrdinals returns a slice of all the index ordinals in the set.
func (a *arbiterSet) IndexOrdinals() []int {
	return a.indexes.Ordered()
}

// UniqueConstraintOrdinals returns a slice of all the unique constraint
// ordinals in the set.
func (a *arbiterSet) UniqueConstraintOrdinals() []int {
	return a.uniqueConstraints.Ordered()
}

// ContainsUniqueConstraint returns true if the set contains the given unique
// constraint.
func (a *arbiterSet) ContainsUniqueConstraint(uniq cat.UniqueOrdinal) bool {
	return a.uniqueConstraints.Contains(uniq)
}

// ForEach calls a function for every arbiter in the set. The function is called
// with the following arguments:
//
//   - name is the name of the index or unique constraint.
//   - conflictOrds is the set of table column ordinals of the arbiter. For
//     index arbiters, this is the lax key columns. For constraint arbiters,
//     this is all the columns in the constraint.
//   - pred is the partial predicate expression of the arbiter, if the arbiter
//     is a partial index or partial constraint. If the arbiter is not partial,
//     pred is nil.
//   - canaryOrd is the table column ordinal of a not-null column in the
//     constraint's table.
//
func (a *arbiterSet) ForEach(
	f func(name string, conflictOrds util.FastIntSet, pred tree.Expr, canaryOrd int),
) {
	// Call the callback for each index arbiter.
	a.indexes.ForEach(func(i int) {
		index := a.mb.tab.Index(i)

		conflictOrds := getIndexLaxKeyOrdinals(index)
		canaryOrd := findNotNullIndexCol(index)

		var pred tree.Expr
		if _, isPartial := index.Predicate(); isPartial {
			pred = a.mb.parsePartialIndexPredicateExpr(i)
		}

		f(string(index.Name()), conflictOrds, pred, canaryOrd)
	})

	// Call the callback for each unique constraint arbiter.
	a.uniqueConstraints.ForEach(func(i int) {
		uniqueConstraint := a.mb.tab.Unique(i)

		conflictOrds := getUniqueConstraintOrdinals(a.mb.tab, uniqueConstraint)
		canaryOrd := findNotNullIndexCol(a.mb.tab.Index(cat.PrimaryIndex))

		var pred tree.Expr
		if _, isPartial := uniqueConstraint.Predicate(); isPartial {
			pred = a.mb.parseUniqueConstraintPredicateExpr(i)
		}

		f(uniqueConstraint.Name(), conflictOrds, pred, canaryOrd)
	})
}
