// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asim_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	testCases := []struct {
		clusterInfo       asim.ClusterInfo
		expectedNodeCount int
	}{
		{
			clusterInfo:       asim.SingleRegionConfig,
			expectedNodeCount: 15,
		},
		{
			clusterInfo:       asim.MultiRegionConfig,
			expectedNodeCount: 36,
		},
		{
			clusterInfo:       asim.ComplexConfig,
			expectedNodeCount: 28,
		},
	}

	for _, tc := range testCases {
		state := asim.LoadConfig(tc.clusterInfo)
		require.Equal(t, tc.expectedNodeCount, len(state.Nodes))
	}
}
