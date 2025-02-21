// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestIndexStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Empty stats.
	stats := IndexStats{}
	require.Equal(t, strings.TrimSpace(`
1 levels, 0 partitions, 0.00 vectors/partition.
CV stats:
`), strings.TrimSpace(stats.String()))

	stats = IndexStats{
		NumPartitions:       100,
		VectorsPerPartition: 32.59,
		CVStats: []CVStats{
			{Mean: 10.12, Variance: 2.13},
			{Mean: 18.42, Variance: 3.87},
		},
	}
	require.Equal(t, strings.TrimSpace(`
3 levels, 100 partitions, 32.59 vectors/partition.
CV stats:
  level 2 - mean: 10.1200, stdev: 1.4595
  level 3 - mean: 18.4200, stdev: 1.9672
`), strings.TrimSpace(stats.String()))

	// Clone method.
	cloned := stats.Clone()
	stats.NumPartitions = 50
	stats.VectorsPerPartition = 16
	stats.CVStats[0].Mean = 100
	stats.CVStats[1].Variance = 100

	// Ensure that original and clone can be updated independently.
	require.Equal(t, strings.TrimSpace(`
3 levels, 50 partitions, 16.00 vectors/partition.
CV stats:
  level 2 - mean: 100.0000, stdev: 1.4595
  level 3 - mean: 18.4200, stdev: 10.0000
`), strings.TrimSpace(stats.String()))

	require.Equal(t, strings.TrimSpace(`
3 levels, 100 partitions, 32.59 vectors/partition.
CV stats:
  level 2 - mean: 10.1200, stdev: 1.4595
  level 3 - mean: 18.4200, stdev: 1.9672
`), strings.TrimSpace(cloned.String()))
}

func TestChildKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	childKey1 := ChildKey{PartitionKey: 10}
	childKey2 := ChildKey{PartitionKey: 20}
	childKey3 := ChildKey{KeyBytes: []byte{1, 2, 3}}
	childKey4 := ChildKey{KeyBytes: []byte{1, 10, 3}}
	childKey5 := ChildKey{PartitionKey: 10, KeyBytes: []byte{1, 10, 3}}

	// Equal method.
	require.True(t, childKey1.Equal(childKey1))
	require.False(t, childKey1.Equal(childKey2))
	require.True(t, childKey3.Equal(childKey3))
	require.False(t, childKey3.Equal(childKey4))
	require.False(t, childKey1.Equal(childKey5))
	require.False(t, childKey4.Equal(childKey5))

	// Compare method.
	require.Equal(t, 0, childKey1.Compare(childKey1))
	require.Equal(t, -1, childKey1.Compare(childKey2))
	require.Equal(t, 1, childKey2.Compare(childKey1))

	require.Equal(t, 0, childKey3.Compare(childKey3))
	require.Equal(t, -1, childKey3.Compare(childKey4))
	require.Equal(t, 1, childKey4.Compare(childKey3))
}
