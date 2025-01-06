// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package props

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/stretchr/testify/require"
)

func TestEquivGroups_Rand(t *testing.T) {
	const maxCol = 512
	const maxCols = 256
	const numIterations = 8
	const chanceUseExisting = 0.5

	// Testing oracle
	testOracle := make(map[opt.ColumnID]opt.ColSet)
	addToOracle := func(left, right opt.ColumnID) {
		leftSet, rightSet := testOracle[left], testOracle[right]
		if leftSet.Contains(right) || rightSet.Contains(left) {
			return
		}
		leftSet.Add(right)
		rightSet.Add(left)
		leftSet.UnionWith(rightSet)
		rightSet.UnionWith(leftSet)
		closure := func(left, right opt.ColSet) {
			for leftCol, leftOk := left.Next(0); leftOk; leftCol, leftOk = left.Next(leftCol + 1) {
				addTo := testOracle[leftCol]
				addTo.UnionWith(right)
				testOracle[leftCol] = addTo
			}
		}
		closure(leftSet, rightSet)
		closure(rightSet, leftSet)
		testOracle[left] = leftSet
		testOracle[right] = rightSet
	}
	areColsEquivOracle := func(left, right opt.ColumnID) bool {
		return testOracle[left].Contains(right)
	}

	// Utility funcs
	getNthCol := func(set opt.ColSet, n int) (col opt.ColumnID) {
		// Assume n <= set.Len()
		var i int
		var ok bool
		for col, ok = set.Next(0); i < n && ok; col, ok = set.Next(col + 1) {
			i++
		}
		return col
	}
	getRandValueForCol := func(colsUsed opt.ColSet, useExisting bool) opt.ColumnID {
		if useExisting {
			return getNthCol(colsUsed, rand.Intn(colsUsed.Len()))
		}
		return opt.ColumnID(rand.Intn(maxCol) + 1)
	}
	makeError := func(expected bool) string {
		if expected {
			return "expected columns to be equivalent, but weren't"
		}
		return "expected columns not to be equivalent, but were"
	}

	var equivGroups EquivGroups
	for numCols := 2; numCols <= maxCols; numCols = numCols << 1 {
		for i := 0; i < numIterations; i++ {
			var colsUsed opt.ColSet
			var fds FuncDepSet
			t.Run(fmt.Sprintf("cols=%d/iteration=%d", numCols, i), func(t *testing.T) {
				for c := 0; c < numCols; c++ {
					useExistingLeft := rand.Float64() < chanceUseExisting
					useExistingRight := rand.Float64() < chanceUseExisting
					if colsUsed.Len() <= 1 {
						useExistingLeft, useExistingRight = false, false
					}
					leftCol := getRandValueForCol(colsUsed, useExistingLeft)
					rightCol := getRandValueForCol(colsUsed, useExistingRight)
					for leftCol == rightCol {
						rightCol = getRandValueForCol(colsUsed, useExistingRight)
					}
					addToOracle(leftCol, rightCol)
					equivGroups.AddNoCopy(opt.MakeColSet(leftCol, rightCol))
					fds.AddEquivalency(leftCol, rightCol)
					colsUsed.Add(leftCol)
					colsUsed.Add(rightCol)
				}
				var fromFDEquivSet EquivGroups
				fromFDEquivSet.AddFromFDs(&fds)
				for leftCol, leftOk := colsUsed.Next(0); leftOk; leftCol, leftOk = colsUsed.Next(leftCol + 1) {
					for rightCol, rightOk := colsUsed.Next(0); rightOk; rightCol, rightOk = colsUsed.Next(rightCol + 1) {
						if leftCol == rightCol {
							continue
						}
						expected := areColsEquivOracle(leftCol, rightCol)
						actual := equivGroups.AreColsEquiv(leftCol, rightCol)
						require.Equal(t, expected, actual, makeError(expected))
						actualFromFDs := fromFDEquivSet.AreColsEquiv(leftCol, rightCol)
						require.Equal(t, expected, actualFromFDs, makeError(expected))
					}
				}
			})
			testOracle = make(map[opt.ColumnID]opt.ColSet)
			equivGroups.Reset()
		}
	}
}

func TestEquivSet(t *testing.T) {
	c := func(cols ...opt.ColumnID) opt.ColSet {
		return opt.MakeColSet(cols...)
	}
	var emptyEquiv EquivGroups
	equiv1 := testingMakeEquiv(c(1, 2))
	equiv2 := testingMakeEquiv(c(1, 2), c(3, 4))
	equiv3 := testingMakeEquiv(c(1, 2), c(3, 4), c(5, 6))
	equiv4 := testingMakeEquiv(c(1, 2, 3, 4))

	t.Run("Empty", func(t *testing.T) {
		require.True(t, emptyEquiv.Empty())
		require.False(t, equiv1.Empty())
		require.False(t, equiv2.Empty())
		require.False(t, equiv3.Empty())
		require.False(t, equiv4.Empty())

		var eq EquivGroups
		require.True(t, eq.Empty())
		eq.AddNoCopy(c(1, 2, 3))
		require.False(t, eq.Empty())
		eq.Reset()
		require.True(t, eq.Empty())
	})

	t.Run("GroupCount", func(t *testing.T) {
		require.Zero(t, emptyEquiv.GroupCount())
		require.Equal(t, 1, equiv1.GroupCount())
		require.Equal(t, 2, equiv2.GroupCount())
		require.Equal(t, 3, equiv3.GroupCount())
		require.Equal(t, 1, equiv4.GroupCount())
	})

	t.Run("Group", func(t *testing.T) {
		require.Panics(t, func() { emptyEquiv.Group(0) })
		require.Panics(t, func() { equiv1.Group(1) })
		require.Panics(t, func() { equiv2.Group(2) })
		require.Panics(t, func() { equiv3.Group(3) })
		require.Panics(t, func() { equiv4.Group(1) })

		require.True(t, equiv1.Group(0).Equals(c(1, 2)))
		require.True(t, equiv2.Group(0).Equals(c(1, 2)))
		require.True(t, equiv3.Group(0).Equals(c(1, 2)))
		require.True(t, equiv4.Group(0).Equals(c(1, 2, 3, 4)))
		require.True(t, equiv2.Group(1).Equals(c(3, 4)))
		require.True(t, equiv3.Group(1).Equals(c(3, 4)))
		require.True(t, equiv3.Group(2).Equals(c(5, 6)))
	})

	t.Run("GroupForCol", func(t *testing.T) {
		require.True(t, emptyEquiv.GroupForCol(1).Empty())
		require.True(t, equiv1.GroupForCol(1).Equals(c(1, 2)))
		require.True(t, equiv2.GroupForCol(1).Equals(c(1, 2)))
		require.True(t, equiv3.GroupForCol(1).Equals(c(1, 2)))
		require.True(t, equiv4.GroupForCol(1).Equals(c(1, 2, 3, 4)))

		require.True(t, emptyEquiv.GroupForCol(3).Empty())
		require.True(t, equiv1.GroupForCol(3).Empty())
		require.True(t, equiv2.GroupForCol(3).Equals(c(3, 4)))
		require.True(t, equiv3.GroupForCol(3).Equals(c(3, 4)))
		require.True(t, equiv4.GroupForCol(3).Equals(c(1, 2, 3, 4)))

		require.True(t, emptyEquiv.GroupForCol(5).Empty())
		require.True(t, equiv1.GroupForCol(5).Empty())
		require.True(t, equiv2.GroupForCol(5).Empty())
		require.True(t, equiv3.GroupForCol(5).Equals(c(5, 6)))
		require.True(t, equiv4.GroupForCol(5).Empty())

		require.True(t, emptyEquiv.GroupForCol(7).Empty())
		require.True(t, equiv1.GroupForCol(7).Empty())
		require.True(t, equiv2.GroupForCol(7).Empty())
		require.True(t, equiv3.GroupForCol(7).Empty())
		require.True(t, equiv4.GroupForCol(7).Empty())
	})

	t.Run("ContainsCol", func(t *testing.T) {
		require.False(t, emptyEquiv.ContainsCol(1))
		require.True(t, equiv1.ContainsCol(1))
		require.True(t, equiv2.ContainsCol(1))
		require.True(t, equiv3.ContainsCol(1))
		require.True(t, equiv4.ContainsCol(1))

		require.False(t, emptyEquiv.ContainsCol(3))
		require.False(t, equiv1.ContainsCol(3))
		require.True(t, equiv2.ContainsCol(3))
		require.True(t, equiv3.ContainsCol(3))
		require.True(t, equiv4.ContainsCol(3))

		require.False(t, emptyEquiv.ContainsCol(5))
		require.False(t, equiv1.ContainsCol(5))
		require.False(t, equiv2.ContainsCol(5))
		require.True(t, equiv3.ContainsCol(5))
		require.False(t, equiv4.ContainsCol(5))

		require.False(t, emptyEquiv.ContainsCol(7))
		require.False(t, equiv1.ContainsCol(7))
		require.False(t, equiv2.ContainsCol(7))
		require.False(t, equiv3.ContainsCol(7))
		require.False(t, equiv4.ContainsCol(7))
	})

	t.Run("AreColsEquiv", func(t *testing.T) {
		require.True(t, emptyEquiv.AreColsEquiv(1, 1))
		require.True(t, equiv1.AreColsEquiv(1, 1))
		require.True(t, equiv2.AreColsEquiv(1, 1))
		require.True(t, equiv3.AreColsEquiv(1, 1))
		require.True(t, equiv4.AreColsEquiv(1, 1))

		require.False(t, emptyEquiv.AreColsEquiv(1, 2))
		require.True(t, equiv1.AreColsEquiv(1, 2))
		require.True(t, equiv2.AreColsEquiv(1, 2))
		require.True(t, equiv3.AreColsEquiv(1, 2))
		require.True(t, equiv4.AreColsEquiv(1, 2))

		require.False(t, emptyEquiv.AreColsEquiv(3, 4))
		require.False(t, equiv1.AreColsEquiv(3, 4))
		require.True(t, equiv2.AreColsEquiv(3, 4))
		require.True(t, equiv3.AreColsEquiv(3, 4))
		require.True(t, equiv4.AreColsEquiv(3, 4))

		require.False(t, emptyEquiv.AreColsEquiv(5, 6))
		require.False(t, equiv1.AreColsEquiv(5, 6))
		require.False(t, equiv2.AreColsEquiv(5, 6))
		require.True(t, equiv3.AreColsEquiv(5, 6))
		require.False(t, equiv4.AreColsEquiv(5, 6))

		require.False(t, emptyEquiv.AreColsEquiv(1, 3))
		require.False(t, equiv1.AreColsEquiv(1, 3))
		require.False(t, equiv2.AreColsEquiv(1, 3))
		require.False(t, equiv3.AreColsEquiv(1, 3))
		require.True(t, equiv4.AreColsEquiv(1, 3))

		require.False(t, emptyEquiv.AreColsEquiv(7, 8))
		require.False(t, equiv1.AreColsEquiv(7, 8))
		require.False(t, equiv2.AreColsEquiv(7, 8))
		require.False(t, equiv3.AreColsEquiv(7, 8))
		require.False(t, equiv4.AreColsEquiv(7, 8))
	})

	t.Run("AreAllColsEquiv", func(t *testing.T) {
		require.True(t, emptyEquiv.AreAllColsEquiv(c(1)))
		require.True(t, equiv1.AreAllColsEquiv(c(1)))
		require.True(t, equiv2.AreAllColsEquiv(c(1)))
		require.True(t, equiv3.AreAllColsEquiv(c(1)))
		require.True(t, equiv4.AreAllColsEquiv(c(1)))

		require.False(t, emptyEquiv.AreAllColsEquiv(c(1, 2)))
		require.True(t, equiv1.AreAllColsEquiv(c(1, 2)))
		require.True(t, equiv2.AreAllColsEquiv(c(1, 2)))
		require.True(t, equiv3.AreAllColsEquiv(c(1, 2)))
		require.True(t, equiv4.AreAllColsEquiv(c(1, 2)))

		require.False(t, emptyEquiv.AreAllColsEquiv(c(3, 4)))
		require.False(t, equiv1.AreAllColsEquiv(c(3, 4)))
		require.True(t, equiv2.AreAllColsEquiv(c(3, 4)))
		require.True(t, equiv3.AreAllColsEquiv(c(3, 4)))
		require.True(t, equiv4.AreAllColsEquiv(c(3, 4)))

		require.False(t, emptyEquiv.AreAllColsEquiv(c(5, 6)))
		require.False(t, equiv1.AreAllColsEquiv(c(5, 6)))
		require.False(t, equiv2.AreAllColsEquiv(c(5, 6)))
		require.True(t, equiv3.AreAllColsEquiv(c(5, 6)))
		require.False(t, equiv4.AreAllColsEquiv(c(5, 6)))

		require.False(t, emptyEquiv.AreAllColsEquiv(c(1, 2, 3)))
		require.False(t, equiv1.AreAllColsEquiv(c(1, 2, 3)))
		require.False(t, equiv2.AreAllColsEquiv(c(1, 2, 3)))
		require.False(t, equiv3.AreAllColsEquiv(c(1, 2, 3)))
		require.True(t, equiv4.AreAllColsEquiv(c(1, 2, 3)))

		require.False(t, emptyEquiv.AreAllColsEquiv(c(7, 8)))
		require.False(t, equiv1.AreAllColsEquiv(c(7, 8)))
		require.False(t, equiv2.AreAllColsEquiv(c(7, 8)))
		require.False(t, equiv3.AreAllColsEquiv(c(7, 8)))
		require.False(t, equiv4.AreAllColsEquiv(c(7, 8)))
	})

	t.Run("AreAllColsEquiv2", func(t *testing.T) {
		require.True(t, emptyEquiv.AreAllColsEquiv2(c(1), c()))
		require.True(t, equiv1.AreAllColsEquiv2(c(1), c()))
		require.True(t, equiv2.AreAllColsEquiv2(c(), c(1)))
		require.True(t, equiv3.AreAllColsEquiv2(c(), c(1)))
		require.True(t, equiv4.AreAllColsEquiv2(c(1), c(1)))

		require.False(t, emptyEquiv.AreAllColsEquiv2(c(1), c(2)))
		require.True(t, equiv1.AreAllColsEquiv2(c(2), c(2)))
		require.True(t, equiv2.AreAllColsEquiv2(c(1), c(2)))
		require.True(t, equiv3.AreAllColsEquiv2(c(1), c(2)))
		require.True(t, equiv4.AreAllColsEquiv2(c(1), c(2)))

		require.False(t, emptyEquiv.AreAllColsEquiv2(c(3), c(4)))
		require.False(t, equiv1.AreAllColsEquiv2(c(3), c(4)))
		require.True(t, equiv2.AreAllColsEquiv2(c(3), c(4)))
		require.True(t, equiv3.AreAllColsEquiv2(c(3), c(4)))
		require.True(t, equiv4.AreAllColsEquiv2(c(3), c(4)))

		require.False(t, emptyEquiv.AreAllColsEquiv2(c(5), c(6)))
		require.False(t, equiv1.AreAllColsEquiv2(c(5), c(6)))
		require.False(t, equiv2.AreAllColsEquiv2(c(5), c(6)))
		require.True(t, equiv3.AreAllColsEquiv2(c(5), c(6)))
		require.False(t, equiv4.AreAllColsEquiv2(c(5), c(6)))

		require.False(t, emptyEquiv.AreAllColsEquiv2(c(1, 2), c(3)))
		require.False(t, equiv1.AreAllColsEquiv2(c(1, 2), c(3)))
		require.False(t, equiv2.AreAllColsEquiv2(c(1), c(2, 3)))
		require.False(t, equiv3.AreAllColsEquiv2(c(1), c(2, 3)))
		require.True(t, equiv4.AreAllColsEquiv2(c(1), c(2, 3)))
		require.True(t, equiv4.AreAllColsEquiv2(c(1, 2), c(2, 3)))
		require.True(t, equiv4.AreAllColsEquiv2(c(1, 2, 3), c(1, 2, 3)))

		require.False(t, emptyEquiv.AreAllColsEquiv2(c(7), c(8)))
		require.False(t, equiv1.AreAllColsEquiv2(c(7), c(8)))
		require.False(t, equiv2.AreAllColsEquiv2(c(7), c(8)))
		require.False(t, equiv3.AreAllColsEquiv2(c(7), c(8)))
		require.False(t, equiv4.AreAllColsEquiv2(c(7), c(8)))
		require.True(t, equiv4.AreAllColsEquiv2(c(7), c(7)))
	})

	t.Run("ComputeEquivClosureNoCopy", func(t *testing.T) {
		require.True(t, emptyEquiv.ComputeEquivClosureNoCopy(c()).Empty())
		require.True(t, emptyEquiv.ComputeEquivClosureNoCopy(c(1, 2)).Equals(c(1, 2)))
		require.True(t, equiv1.ComputeEquivClosureNoCopy(c(1)).Equals(c(1, 2)))
		require.True(t, equiv1.ComputeEquivClosureNoCopy(c(5)).Equals(c(5)))
		require.True(t, equiv1.ComputeEquivClosureNoCopy(c(1, 5)).Equals(c(1, 2, 5)))
		require.True(t, equiv3.ComputeEquivClosureNoCopy(c(1, 5)).Equals(c(1, 2, 5, 6)))
		require.True(t, equiv3.ComputeEquivClosureNoCopy(c(1, 3, 5)).Equals(c(1, 2, 3, 4, 5, 6)))
		require.True(t, equiv4.ComputeEquivClosureNoCopy(c(1)).Equals(c(1, 2, 3, 4)))
	})

	t.Run("AddNoCopy", func(t *testing.T) {
		var eq EquivGroups
		equiv := eq.AddNoCopy(c(1, 2))
		testingCheckEquivGroupsEqual(t, eq, testingMakeEquiv(c(1, 2)))
		require.True(t, equiv.Equals(c(1, 2)))

		equiv = eq.AddNoCopy(c(3, 4))
		testingCheckEquivGroupsEqual(t, eq, testingMakeEquiv(c(1, 2), c(3, 4)))
		require.True(t, equiv.Equals(c(3, 4)))

		equiv = eq.AddNoCopy(c(5, 6))
		testingCheckEquivGroupsEqual(t, eq, testingMakeEquiv(c(1, 2), c(3, 4), c(5, 6)))
		require.True(t, equiv.Equals(c(5, 6)))

		// Combine the three equiv groups into one.
		equiv = eq.AddNoCopy(c(1, 3, 5))
		testingCheckEquivGroupsEqual(t, eq, testingMakeEquiv(c(1, 2, 3, 4, 5, 6)))
		require.True(t, equiv.Equals(c(1, 2, 3, 4, 5, 6)))

		// Add a trivial single-column equiv group.
		equiv = eq.AddNoCopy(c(10))
		testingCheckEquivGroupsEqual(t, eq, testingMakeEquiv(c(1, 2, 3, 4, 5, 6)))
		require.True(t, equiv.Empty())
	})

	t.Run("AddFromFDs", func(t *testing.T) {
		var fds FuncDepSet
		fds.AddEquivalency(1, 2)
		fds.AddSynthesizedCol(c(2), 3)
		fds.AddConstants(c(4))
		fds.AddEquivalency(5, 6)

		var eq EquivGroups
		eq.AddFromFDs(&fds)
		testingCheckEquivGroupsEqual(t, eq, testingMakeEquiv(c(1, 2), c(5, 6)))

		// Add to a non-empty set.
		eq = testingMakeEquiv(c(2, 5))
		eq.AddFromFDs(&fds)
		testingCheckEquivGroupsEqual(t, eq, testingMakeEquiv(c(1, 2, 5, 6)))
	})

	t.Run("TranslateColsStrict", func(t *testing.T) {
		l := func(cols ...opt.ColumnID) opt.ColList { return cols }

		emptyClone := testingCloneEquiv(emptyEquiv)
		emptyClone.TranslateColsStrict(l(1, 2), l(3, 4))
		require.True(t, emptyClone.Empty())

		equiv1Clone := testingCloneEquiv(equiv1)
		equiv1Clone.TranslateColsStrict(l(1, 2, 3), l(4, 5, 6))
		testingCheckEquivGroupsEqual(t, equiv1Clone, testingMakeEquiv(c(4, 5)))

		equiv2Clone := testingCloneEquiv(equiv2)
		equiv2Clone.TranslateColsStrict(l(1, 2, 3, 4), l(5, 6, 7, 8))
		testingCheckEquivGroupsEqual(t, equiv2Clone, testingMakeEquiv(c(5, 6), c(7, 8)))

		// Case where an input column is mapped to multiple output columns.
		equiv2Clone = testingCloneEquiv(equiv2)
		equiv2Clone.TranslateColsStrict(l(1, 1, 2, 3, 4), l(5, 6, 7, 8, 9))
		testingCheckEquivGroupsEqual(t, equiv2Clone, testingMakeEquiv(c(5, 6, 7), c(8, 9)))

		// Case where multiple input columns are mapped to the same output column.
		// The input columns are already equivalent, and their equiv group is
		// removed because it is left with single column.
		equiv2Clone = testingCloneEquiv(equiv2)
		equiv2Clone.TranslateColsStrict(l(1, 2, 3, 4), l(5, 5, 6, 7))
		testingCheckEquivGroupsEqual(t, equiv2Clone, testingMakeEquiv(c(6, 7)))

		// Case where multiple input columns are mapped to the same output column.
		// The input columns were part of different equiv groups, which are combined
		// due to the implicit equality.
		equiv2Clone = testingCloneEquiv(equiv2)
		equiv2Clone.TranslateColsStrict(l(1, 2, 3, 4), l(5, 6, 6, 7))
		testingCheckEquivGroupsEqual(t, equiv2Clone, testingMakeEquiv(c(5, 6, 7)))

		// Case where multiple input columns are mapped to the same output column.
		// One input column was part of an equiv group, and one wasn't.
		equiv2Clone = testingCloneEquiv(equiv2)
		equiv2Clone.TranslateColsStrict(l(1, 2, 3, 4, 5), l(6, 7, 8, 9, 6))
		testingCheckEquivGroupsEqual(t, equiv2Clone, testingMakeEquiv(c(6, 7), c(8, 9)))
	})

	t.Run("ProjectCols", func(t *testing.T) {
		emptyClone := testingCloneEquiv(emptyEquiv)
		emptyClone.ProjectCols(c(1, 2))
		require.True(t, emptyClone.Empty())

		equiv1Clone := testingCloneEquiv(equiv1)
		equiv1Clone.ProjectCols(c(1, 2, 3))
		testingCheckEquivGroupsEqual(t, equiv1Clone, testingMakeEquiv(c(1, 2)))

		// Since the (1, 2) equiv group is projected to (1), it is removed.
		equiv1Clone = testingCloneEquiv(equiv1)
		equiv1Clone.ProjectCols(c(1))
		require.True(t, equiv1Clone.Empty())

		equiv2Clone := testingCloneEquiv(equiv2)
		equiv2Clone.ProjectCols(c(1, 2, 3, 4))
		testingCheckEquivGroupsEqual(t, equiv2Clone, testingMakeEquiv(c(1, 2), c(3, 4)))

		// The (3, 4) group should be removed.
		equiv2Clone = testingCloneEquiv(equiv2)
		equiv2Clone.ProjectCols(c(1, 2, 3))
		testingCheckEquivGroupsEqual(t, equiv2Clone, testingMakeEquiv(c(1, 2)))

		// Both equiv groups should be removed.
		equiv2Clone = testingCloneEquiv(equiv2)
		equiv2Clone.ProjectCols(c(1, 5, 6))
		require.True(t, equiv2Clone.Empty())

		// Column 4 is removed from the group, but the group remains.
		equiv4Clone := testingCloneEquiv(equiv4)
		equiv4Clone.ProjectCols(c(1, 2, 3))
		testingCheckEquivGroupsEqual(t, equiv4Clone, testingMakeEquiv(c(1, 2, 3)))
	})

	t.Run("PartitionBy", func(t *testing.T) {
		emptyClone := testingCloneEquiv(emptyEquiv)
		emptyClone.PartitionBy(c(1, 2))
		require.True(t, emptyClone.Empty())

		// Partition that doesn't affect the equiv set.
		equiv2Clone := testingCloneEquiv(equiv2)
		equiv2Clone.PartitionBy(c(1, 2, 3, 4))
		testingCheckEquivGroupsEqual(t, equiv2Clone, testingMakeEquiv(c(1, 2), c(3, 4)))

		// Partition that splits the equiv set. Since the equiv groups only have two
		// columns to start with, the result is empty.
		equiv2Clone = testingCloneEquiv(equiv2)
		equiv2Clone.PartitionBy(c(1, 3))
		require.True(t, equiv2Clone.Empty())

		// Split an equiv group in half.
		equiv4Clone := testingCloneEquiv(equiv4)
		equiv4Clone.PartitionBy(c(1, 2))
		testingCheckEquivGroupsEqual(t, equiv4Clone, testingMakeEquiv(c(1, 2), c(3, 4)))

		// Example from the PartitionBy comment.
		equiv5 := testingMakeEquiv(c(1, 2, 3), c(4, 5, 6, 7, 8), c(9, 10, 11, 12))
		equiv5.PartitionBy(c(1, 5, 6))
		expected := testingMakeEquiv(c(1), c(2, 3), c(4, 7, 8), c(5, 6), c(9, 10, 11, 12))
		testingCheckEquivGroupsEqual(t, equiv5, expected)
	})

	t.Run("CopyFrom", func(t *testing.T) {
		emptyClone := testingCloneEquiv(emptyEquiv)
		emptyClone.CopyFrom(&equiv1)
		testingCheckEquivGroupsEqual(t, emptyClone, equiv1)

		equiv1Clone := testingCloneEquiv(equiv1)
		equiv1Clone.CopyFrom(&equiv2)
		testingCheckEquivGroupsEqual(t, equiv1Clone, equiv2)

		equiv2Clone := testingCloneEquiv(equiv2)
		equiv2Clone.CopyFrom(&equiv3)
		testingCheckEquivGroupsEqual(t, equiv2Clone, equiv3)

		equiv3Clone := testingCloneEquiv(equiv3)
		equiv3Clone.CopyFrom(&equiv4)
		testingCheckEquivGroupsEqual(t, equiv3Clone, equiv4)

		equiv4Clone := testingCloneEquiv(equiv4)
		equiv4Clone.CopyFrom(&emptyEquiv)
		require.True(t, equiv4Clone.Empty())
	})

	t.Run("AppendFromDisjoint", func(t *testing.T) {
		equiv5 := testingMakeEquiv(c(3, 4))
		equiv6 := testingMakeEquiv(c(5, 6))
		equiv7 := testingMakeEquiv(c(3, 4), c(5, 6))

		emptyEquivClone := testingCloneEquiv(emptyEquiv)
		emptyEquivClone.AppendFromDisjoint(&equiv1)
		testingCheckEquivGroupsEqual(t, emptyEquivClone, equiv1)

		equiv1Clone := testingCloneEquiv(equiv1)
		equiv1Clone.AppendFromDisjoint(&equiv5)
		testingCheckEquivGroupsEqual(t, equiv1Clone, equiv2)
		equiv1Clone.AppendFromDisjoint(&equiv6)
		testingCheckEquivGroupsEqual(t, equiv1Clone, equiv3)

		equiv1Clone = testingCloneEquiv(equiv1)
		equiv1Clone.AppendFromDisjoint(&equiv7)
		testingCheckEquivGroupsEqual(t, equiv1Clone, equiv3)

		if buildutil.CrdbTestBuild {
			// Panic on test builds when adding a group that is not disjoint.
			equiv1Clone = testingCloneEquiv(equiv1)
			require.Panics(t, func() { equiv1Clone.AppendFromDisjoint(&equiv2) })
		}
	})
}

func testingMakeEquiv(groups ...opt.ColSet) EquivGroups {
	var eq EquivGroups
	for _, group := range groups {
		eq.AddNoCopy(group)
	}
	return eq
}

func testingCloneEquiv(equiv EquivGroups) EquivGroups {
	var clone EquivGroups
	clone.CopyFrom(&equiv)
	return clone
}

func testingCheckEquivGroupsEqual(t *testing.T, l, r EquivGroups) {
	require.Equal(t, l.GroupCount(), r.GroupCount())
	leftGroups := make([]opt.ColSet, l.GroupCount())
	rightGroups := make([]opt.ColSet, r.GroupCount())
	copy(leftGroups, l.groups)
	copy(rightGroups, r.groups)
	sortGroups := func(groups []opt.ColSet) {
		sort.Slice(groups, func(i, j int) bool {
			left, _ := groups[i].Next(0)
			right, _ := groups[j].Next(0)
			return left < right
		})
	}
	sortGroups(leftGroups)
	sortGroups(rightGroups)
	for i := 0; i < len(leftGroups); i++ {
		require.True(t, leftGroups[i].Equals(rightGroups[i]))
	}
}
