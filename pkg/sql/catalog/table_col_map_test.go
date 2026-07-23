// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catalog_test

import (
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestTableColMap(t *testing.T) {
	var m catalog.TableColMap
	var oracle util.FastIntMap
	rng, _ := randutil.NewTestRand()

	var columnIDs []descpb.ColumnID
	for i := 0; i < 5; i++ {
		columnIDs = append(columnIDs, descpb.ColumnID(i))
	}
	for _, systemColumnDesc := range colinfo.AllSystemColumnDescs {
		columnIDs = append(columnIDs, systemColumnDesc.ID)
	}
	rng.Shuffle(len(columnIDs), func(i, j int) {
		columnIDs[i], columnIDs[j] = columnIDs[j], columnIDs[i]
	})

	// Use each column ID with 50% probability.
	for i, columnID := range columnIDs {
		if rng.Float64() < 0.5 {
			m.Set(columnID, i)
			oracle.Set(int(columnID), i)
		}
	}

	// First, check the length.
	require.Equal(t, oracle.Len(), m.Len())

	// Check that Get and GetDefault return the same results.
	for _, columnID := range columnIDs {
		actual, actualOk := m.Get(columnID)
		expected, expectedOk := oracle.Get(int(columnID))
		require.Equal(t, expectedOk, actualOk)
		if actualOk {
			require.Equal(t, expected, actual)
		}
		actual = m.GetDefault(columnID)
		expected = oracle.GetDefault(int(columnID))
		require.Equal(t, expected, actual)
	}

	// Verify ForEach. We don't bother storing the column IDs here since sorting
	// them below would be mildly annoying.
	var actualValues, expectedValues []int
	m.ForEach(func(_ descpb.ColumnID, returnIndex int) {
		actualValues = append(actualValues, returnIndex)
	})
	oracle.ForEach(func(_ int, val int) {
		expectedValues = append(expectedValues, val)
	})
	// Since the order of iteration is not defined, we have to sort all slices.
	sort.Ints(actualValues)
	sort.Ints(expectedValues)
	require.Equal(t, expectedValues, actualValues)

	// Check that stringification matches too.
	require.Equal(t, oracle.String(), m.String())
}
