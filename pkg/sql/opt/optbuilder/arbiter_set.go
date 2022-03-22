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
	"github.com/cockroachdb/errors"
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

// removeIndex removes an index arbiter from the set.
func (a *arbiterSet) removeIndex(idx cat.IndexOrdinal) {
	a.indexes.Remove(idx)
}

// minArbiterSet represents a set of arbiters. It differs from arbiterSet by
// automatically removing arbiter indexes that are made redundant by arbiter
// unique constraints. It is only useful when an ON CONFLICT statement specifies
// no columns or constraints. For example, consider the table and statement:
//
//   CREATE TABLE t (
//     a INT,
//     b INT,
//     UNIQUE INDEX a_b_key (a, b),
//     UNIQUE WITHOUT INDEX b_key (b)
//   )
//
//   INSERT INTO t VALUES (1, 2) ON CONFLICT DO NOTHING
//
// There is no need to use both a_b_key and b_key as arbiters for the INSERT
// statement because any conflict in a_b_key will also be a conflict in b_key.
// Only b_key is required to be an arbiter.
//
// Special care is taken with partial indexes and unique constraints. An arbiter
// index is only made redundant by a unique constraint if they are both
// non-partial, or their partial predicates are identical. Note that in the
// future, we could probably be smarter about this by using implication (see
// partialidx.Implicator) to remove arbiter indexes that have predicates that do
// not exactly match a unique constraint predicate.
//
// Note that minArbiterSet does not currently remove arbiter indexes that are
// made redundant by other arbiter indexes, nor does it remove arbiter unique
// constraints made redundant by other arbiter unique constraints. These cases
// would only occur if a user created redundant unique indexes or unique
// constraints, and the extra arbiters can be easily removed by removing the
// redundant indexes and constraints from the table. The minArbiterSet is
// designed to remove arbiter indexes that are made redundant by UNIQUE WITHOUT
// INDEX constraints that are synthesized for partitioned and hash-sharded
// indexes, which the user has no control over.
type minArbiterSet struct {
	as arbiterSet

	// addUniqueConstraintCalled is set to true when addUniqueConstraint is
	// called. When true, AddIndex will panic to avoid undefined behavior.
	addUniqueConstraintCalled bool

	// indexConflictOrdsCache caches the conflict column sets of arbiter indexes
	// in the set.
	indexConflictOrdsCache map[cat.IndexOrdinal]util.FastIntSet
}

// makeMinArbiterSet returns an initialized arbiterSet.
func makeMinArbiterSet(mb *mutationBuilder) minArbiterSet {
	return minArbiterSet{
		as: makeArbiterSet(mb),
	}
}

// AddIndex adds an index arbiter to the set. Panics if called after
// AddUniqueConstraint has been called.
func (m *minArbiterSet) AddIndex(idx cat.IndexOrdinal) {
	if m.addUniqueConstraintCalled {
		panic(errors.AssertionFailedf("cannot call AddIndex after AddUniqueConstraint"))
	}
	m.as.AddIndex(idx)
}

// AddUniqueConstraint adds a unique constraint arbiter to the set. If the
// unique constraint makes an index redundant, the index is removed from the
// set.
func (m *minArbiterSet) AddUniqueConstraint(uniq cat.UniqueOrdinal) {
	m.addUniqueConstraintCalled = true
	m.as.AddUniqueConstraint(uniq)

	uniqueConstraint := m.as.mb.tab.Unique(uniq)
	if idx, ok := m.findRedundantIndex(uniqueConstraint); ok {
		m.as.removeIndex(idx)
		delete(m.indexConflictOrdsCache, idx)
	}
}

// ArbiterSet converts the minArbiterSet to an arbiterSet.
func (m *minArbiterSet) ArbiterSet() arbiterSet {
	return m.as
}

// findRedundantIndex returns the first arbiter index which is made redundant by
// the unique constraint. An arbiter index is redundant if both of the following
// hold:
//
//   1. Its conflict columns are a super set of the given conflict columns.
//   2. The index and unique constraint are both non-partial, or have the same
//      partial predicate.
//
func (m *minArbiterSet) findRedundantIndex(
	uniq cat.UniqueConstraint,
) (_ cat.IndexOrdinal, ok bool) {
	m.initCache()
	// If there is only one arbiter index, check if it is made redundant by
	// the unique constraint.
	conflictOrds := getUniqueConstraintOrdinals(m.as.mb.tab, uniq)
	pred, _ := uniq.Predicate()
	// Find the first arbiter index that is made redundant by the unique
	// constraint.
	for i, indexConflictOrds := range m.indexConflictOrdsCache {
		indexPred, _ := m.as.mb.tab.Index(i).Predicate()
		if pred == indexPred && conflictOrds.SubsetOf(indexConflictOrds) {
			return i, true
		}
	}
	return -1, false
}

// initCache initializes the index conflict columns cache.
func (m *minArbiterSet) initCache() {
	// Do nothing if the cache has already been initialized.
	if m.indexConflictOrdsCache != nil {
		return
	}
	// Cache each index's conflict columns.
	m.indexConflictOrdsCache = make(map[cat.IndexOrdinal]util.FastIntSet, m.as.indexes.Len())
	m.as.indexes.ForEach(func(i int) {
		index := m.as.mb.tab.Index(i)
		m.indexConflictOrdsCache[i] = getIndexLaxKeyOrdinals(index)
	})
}
