// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClustersCompatible(t *testing.T) {
	t.Run("spec does not match", func(t *testing.T) {
		s1 := ClusterSpec{NodeCount: 4}
		s2 := ClusterSpec{NodeCount: 5}
		require.False(t, ClustersCompatible(s1, s2, GCE))
	})
	t.Run("spec has different lifetime", func(t *testing.T) {
		s1 := ClusterSpec{NodeCount: 5, Lifetime: 100}
		s2 := ClusterSpec{NodeCount: 5, Lifetime: 200}
		require.True(t, ClustersCompatible(s1, s2, GCE))
	})
	t.Run("spec has different GCE spec with cloud as GCE", func(t *testing.T) {
		s1 := ClusterSpec{NodeCount: 5}
		s2 := ClusterSpec{NodeCount: 5}
		s1.GCE.VolumeType = "mock_volume1"
		s2.GCE.VolumeType = "mock_volume2"
		require.False(t, ClustersCompatible(s1, s2, GCE))
	})
	t.Run("spec has different GCE spec with cloud as AWS", func(t *testing.T) {
		s1 := ClusterSpec{NodeCount: 5}
		s2 := ClusterSpec{NodeCount: 5}
		s1.GCE.VolumeType = "mock_volume1"
		s2.GCE.VolumeType = "mock_volume2"
		require.True(t, ClustersCompatible(s1, s2, AWS))
	})
}
