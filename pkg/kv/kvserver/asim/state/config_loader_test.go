// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestLoadClusterInfo(t *testing.T) {
	testCases := []struct {
		desc               string
		clusterInfo        ClusterInfo
		expectedNodeCount  int
		expectedStoreCount int
	}{
		{
			desc:               "single region config",
			clusterInfo:        SingleRegionConfig,
			expectedNodeCount:  15,
			expectedStoreCount: 15,
		},
		{
			desc:               "multi region config",
			clusterInfo:        MultiRegionConfig,
			expectedNodeCount:  36,
			expectedStoreCount: 36,
		},
		{
			desc:               "complex config",
			clusterInfo:        ComplexConfig,
			expectedNodeCount:  28,
			expectedStoreCount: 28,
		},
		{
			desc:               "single region multi-store config",
			clusterInfo:        SingleRegionMultiStoreConfig,
			expectedNodeCount:  3,
			expectedStoreCount: 15,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			state := LoadClusterInfo(tc.clusterInfo, config.DefaultSimulationSettings())
			require.Equal(t, tc.expectedNodeCount, len(state.Nodes()))
			require.Equal(t, tc.expectedStoreCount, len(state.Stores()))
		})
	}
}

func TestLoadRangesInfo(t *testing.T) {
	testCases := []struct {
		desc               string
		rangesInfo         RangesInfo
		expectPanic        bool
		expectedRangeCount int
	}{
		{
			desc:               "single range config",
			rangesInfo:         SingleRangeConfig,
			expectedRangeCount: 1,
		},
		{
			desc:               "multi range config",
			rangesInfo:         MultiRangeConfig,
			expectedRangeCount: 3,
		},
		{
			desc:        "panic duplicate start keys",
			rangesInfo:  append(MultiRangeConfig, SingleRangeConfig...),
			expectPanic: true,
		},
		{
			desc: "panic leaseholder doesn't exist",
			rangesInfo: []RangeInfo{
				{
					Descriptor: roachpb.RangeDescriptor{
						StartKey: MinKey.ToRKey(),
						InternalReplicas: []roachpb.ReplicaDescriptor{
							{
								StoreID: 1,
								Type:    roachpb.VOTER_FULL,
							},
							{
								StoreID: 2,
								Type:    roachpb.VOTER_FULL,
							},
							{
								StoreID: 3,
								Type:    roachpb.VOTER_FULL,
							},
						},
					},
					Config:      &defaultSpanConfig,
					Leaseholder: 10,
				},
			},
			expectPanic: true,
		},
		{
			desc: "panic replica store doesn't exist",
			rangesInfo: []RangeInfo{
				{
					Descriptor: roachpb.RangeDescriptor{
						StartKey: MinKey.ToRKey(),
						InternalReplicas: []roachpb.ReplicaDescriptor{
							{
								StoreID: 1,
							},
							{
								StoreID: 2,
							},
							{
								StoreID: 4,
							},
						},
					},
					Config:      &defaultSpanConfig,
					Leaseholder: 2,
				},
			},
			expectPanic: true,
		},
		{
			desc: "panic duplicate replica stores",
			rangesInfo: []RangeInfo{
				{
					Descriptor: roachpb.RangeDescriptor{
						StartKey: MinKey.ToRKey(),
						InternalReplicas: []roachpb.ReplicaDescriptor{
							{
								StoreID: 1,
							},
							{
								StoreID: 2,
							},
							{
								StoreID: 2,
							},
						},
					},
					Config:      &defaultSpanConfig,
					Leaseholder: 1,
				},
			},
			expectPanic: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			settings := config.DefaultSimulationSettings()
			state := NewState(settings)
			_, ok := state.AddStore(state.AddNode().NodeID())
			require.True(t, ok)
			_, ok = state.AddStore(state.AddNode().NodeID())
			require.True(t, ok)
			_, ok = state.AddStore(state.AddNode().NodeID())
			require.True(t, ok)

			if tc.expectPanic {
				require.Panics(t, func() { LoadRangeInfo(state, tc.rangesInfo...) })
			} else {
				LoadRangeInfo(state, tc.rangesInfo...)
				require.Equal(t, tc.expectedRangeCount, len(state.Ranges()))
			}
		})
	}
}
