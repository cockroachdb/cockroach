// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package props

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/stretchr/testify/require"
)

func TestEquivSet_Rand(t *testing.T) {
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

	equivSet := NewEquivSet()
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
					equivSet.Add(opt.MakeColSet(leftCol, rightCol))
					fds.AddEquivalency(leftCol, rightCol)
					colsUsed.Add(leftCol)
					colsUsed.Add(rightCol)
				}
				var fromFDEquivSet EquivSet
				fromFDEquivSet.AddFromFDs(&fds)
				for leftCol, leftOk := colsUsed.Next(0); leftOk; leftCol, leftOk = colsUsed.Next(leftCol + 1) {
					for rightCol, rightOk := colsUsed.Next(0); rightOk; rightCol, rightOk = colsUsed.Next(rightCol + 1) {
						if leftCol == rightCol {
							continue
						}
						expected := areColsEquivOracle(leftCol, rightCol)
						actual := equivSet.AreColsEquiv(leftCol, rightCol)
						require.Equal(t, expected, actual, makeError(expected))
						actualFromFDs := fromFDEquivSet.AreColsEquiv(leftCol, rightCol)
						require.Equal(t, expected, actualFromFDs, makeError(expected))
					}
				}
			})
			testOracle = make(map[opt.ColumnID]opt.ColSet)
			equivSet.Reset()
		}
	}
}
