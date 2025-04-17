// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package commontest

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats/scalar"
)

// CheckPartitionMetadata tests the correctness of the given metadata's fields.
func CheckPartitionMetadata(
	t *testing.T,
	metadata cspann.PartitionMetadata,
	level cspann.Level,
	centroid vector.T,
	state cspann.PartitionStateDetails,
) {
	require.Equal(t, level, metadata.Level)
	require.Equal(t, []float32(centroid), testutils.RoundFloats(metadata.Centroid, 2))
	state.Timestamp = metadata.StateDetails.Timestamp
	require.Equal(t, state, metadata.StateDetails)
}

// CheckPartitionCount tests the size of a partition.
func CheckPartitionCount(
	ctx context.Context,
	t *testing.T,
	store cspann.Store,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	expectedCount int,
) {
	count, err := store.EstimatePartitionCount(ctx, treeKey, partitionKey)
	require.NoError(t, err)
	require.Equal(t, expectedCount, count)
}

// RunTransaction wraps the store.RunTransaction method.
func RunTransaction(
	ctx context.Context, t *testing.T, store cspann.Store, fn func(txn cspann.Txn),
) {
	require.NoError(t, store.RunTransaction(ctx, func(txn cspann.Txn) error {
		fn(txn)
		return nil
	}))
}

// RoundResults rounds all float fields in the given set of results, using the
// requested precision.
func RoundResults(results cspann.SearchResults, prec int) cspann.SearchResults {
	for i := range results {
		result := &results[i]
		result.QuerySquaredDistance = float32(scalar.Round(float64(result.QuerySquaredDistance), prec))
		result.ErrorBound = float32(scalar.Round(float64(result.ErrorBound), prec))
		result.CentroidDistance = float32(scalar.Round(float64(result.CentroidDistance), prec))
		result.Vector = testutils.RoundFloats(result.Vector, prec)
	}
	return results
}

// ValidatePartitionsEqual validates that the two partitions match.
func ValidatePartitionsEqual(t *testing.T, l, r *cspann.Partition) {
	q1, q2 := l.QuantizedSet(), r.QuantizedSet()
	require.Equal(t, l.Level(), r.Level(), "levels do not match")
	require.Equal(t, l.ChildKeys(), r.ChildKeys(), "childKeys do not match")
	require.Equal(t, l.ValueBytes(), r.ValueBytes(), "valueBytes do not match")
	require.Equal(t, q1.GetCentroid(), q2.GetCentroid(), "centroids do not match")
	require.Equal(t, q1.GetCount(), q2.GetCount(), "counts do not match")
	require.Equal(t, q1.GetCentroidDistances(), q2.GetCentroidDistances(), "distances do not match")
	if eq, ok := q1.(equaler); ok {
		require.True(t, eq.Equal(q2))
	}
}
