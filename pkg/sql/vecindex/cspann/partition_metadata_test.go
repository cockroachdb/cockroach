// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

func TestMetadataEquals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	meta1 := &PartitionMetadata{}
	meta2 := &PartitionMetadata{}
	require.True(t, meta1.Equal(meta2))

	meta1.Level = 2
	require.False(t, meta1.Equal(meta2))

	meta2.Level = 2
	require.True(t, meta1.Equal(meta2))

	meta2.StateDetails.State = ReadyState
	require.False(t, meta1.Equal(meta2))

	meta1.StateDetails.State = ReadyState
	require.True(t, meta1.Equal(meta2))

	centroid := vector.T{1, 2, 3}
	meta1.Centroid = centroid
	require.False(t, meta1.Equal(meta2))

	meta2.Centroid = centroid
	require.True(t, meta1.Equal(meta2))

	meta2.Centroid = vector.T{4, 5, 6}
	require.False(t, meta1.Equal(meta2))

	meta2.Centroid = vector.T{1, 2, 3}
	require.True(t, meta1.Equal(meta2))
}

func checkPartitionMetadata(
	t *testing.T, metadata *PartitionMetadata, level Level, centroid vector.T,
) {
	require.Equal(t, level, metadata.Level)
	require.Equal(t, []float32(centroid), testutils.RoundFloats(metadata.Centroid, 2))
}
