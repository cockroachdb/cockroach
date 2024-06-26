// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestMetrics_GetReplicatedBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	m := metrics{}

	sqlLocality := roachpb.Locality{Tiers: []roachpb.Tier{
		{Key: "region", Value: "us-east1"},
		{Key: "zone", Value: "az1"},
	}}
	m.Init(sqlLocality)

	require.Equal(t, sqlLocality, m.localLocality)
	require.Empty(t, m.mu.cachedPathMetrics)
	require.Empty(t, m.mu.pathMetrics)

	crossZonePath := tenantcostmodel.LocalityNetworkPath{
		FromNodeID: roachpb.NodeID(1),
		FromLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east1"},
			{Key: "zone", Value: "1"},
		}},
		ToNodeID: roachpb.NodeID(2),
		ToLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east1"},
			{Key: "zone", Value: "2"},
		}},
	}
	crossRegionPath1 := tenantcostmodel.LocalityNetworkPath{
		FromNodeID: roachpb.NodeID(3),
		FromLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-central1"},
			{Key: "zone", Value: "1"},
		}},
		ToNodeID: roachpb.NodeID(2),
		ToLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east1"},
			{Key: "zone", Value: "2"},
		}},
	}
	crossRegionPath2 := tenantcostmodel.LocalityNetworkPath{
		FromNodeID: roachpb.NodeID(4),
		FromLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-central1"},
			{Key: "zone", Value: "3"},
		}},
		ToNodeID: roachpb.NodeID(5),
		ToLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east1"},
			{Key: "zone", Value: "4"},
		}},
	}

	// Create a new cross-zone metric.
	crossZoneMetric := m.GetReplicatedBytes(crossZonePath)
	require.EqualValues(t, 0, crossZoneMetric.Value())
	crossZoneMetric.Inc(10)
	require.Len(t, m.mu.cachedPathMetrics, 1)
	require.Len(t, m.mu.pathMetrics, 1)

	// Create a new cross-region metric.
	crossRegionMetric := m.GetReplicatedBytes(crossRegionPath1)
	require.EqualValues(t, 0, crossRegionMetric.Value())
	crossRegionMetric.Inc(5)
	require.Len(t, m.mu.cachedPathMetrics, 2)
	require.Len(t, m.mu.pathMetrics, 2)

	// Use a different network path. Metric should be the same after
	// normalization.
	crossRegionMetric = m.GetReplicatedBytes(crossRegionPath2)
	require.EqualValues(t, 5, crossRegionMetric.Value())
	crossRegionMetric.Inc(2)
	require.Len(t, m.mu.cachedPathMetrics, 3)
	require.Len(t, m.mu.pathMetrics, 2)

	// Fetch existing metrics.
	crossZoneMetric = m.GetReplicatedBytes(crossZonePath)
	require.EqualValues(t, 10, crossZoneMetric.Value())
	crossRegionMetric = m.GetReplicatedBytes(crossRegionPath1)
	require.EqualValues(t, 7, crossRegionMetric.Value())
	crossRegionMetric = m.GetReplicatedBytes(crossRegionPath2)
	require.EqualValues(t, 7, crossRegionMetric.Value())
	require.Len(t, m.mu.cachedPathMetrics, 3)
	require.Len(t, m.mu.pathMetrics, 2)

	// Localities were updated, but cached metrics exist.
	crossRegionPath3 := tenantcostmodel.LocalityNetworkPath{
		FromNodeID: roachpb.NodeID(4),
		FromLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "europe-west1"},
		}},
		ToNodeID: roachpb.NodeID(5),
		ToLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east1"},
		}},
	}
	crossRegionMetric = m.GetReplicatedBytes(crossRegionPath3)
	require.EqualValues(t, 7, crossRegionMetric.Value())
	require.Len(t, m.mu.cachedPathMetrics, 3)
	require.Len(t, m.mu.pathMetrics, 2)
}

func TestMakeLocalityLabelValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name           string
		baseKeys       []string
		from           roachpb.Locality
		to             roachpb.Locality
		expectedLabels []string
	}{
		{
			name: "no keys",
			from: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
			}},
			to: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
			}},
			expectedLabels: []string{},
		},
		{
			name:     "no matching keys",
			baseKeys: []string{"foo", "bar"},
			from: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
			}},
			to: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
			}},
			expectedLabels: []string{"", "", "", ""},
		},
		{
			name:     "same locality",
			baseKeys: []string{"region", "zone"},
			from: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
				{Key: "zone", Value: "az1"},
			}},
			to: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
				{Key: "zone", Value: "az1"},
			}},
			expectedLabels: []string{"us-east1", "az1", "us-east1", "az1"},
		},
		{
			name:     "same locality with gaps",
			baseKeys: []string{"region", "zone", "rack", "dns"},
			from: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
				{Key: "zone", Value: "az1"},
			}},
			to: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
				{Key: "zone", Value: "az1"},
			}},
			expectedLabels: []string{"us-east1", "az1", "", "", "us-east1", "az1", "", ""},
		},
		{
			name:     "different region",
			baseKeys: []string{"region", "zone", "rack"},
			from: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
				{Key: "zone", Value: "az1"},
				{Key: "rack", Value: "1"},
			}},
			to: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-central1"},
				{Key: "zone", Value: "az2"},
				{Key: "rack", Value: "2"},
			}},
			expectedLabels: []string{"us-east1", "", "", "us-central1", "", ""},
		},
		{
			name:     "different zone",
			baseKeys: []string{"region", "zone", "rack"},
			from: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-central1"},
				{Key: "zone", Value: "az1"},
				{Key: "rack", Value: "1"},
			}},
			to: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-central1"},
				{Key: "zone", Value: "az2"},
				{Key: "rack", Value: "2"},
			}},
			expectedLabels: []string{"us-central1", "az1", "", "us-central1", "az2", ""},
		},
		{
			name:     "missing zone in from",
			baseKeys: []string{"region", "zone"},
			from: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-central1"},
			}},
			to: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-central1"},
				{Key: "zone", Value: "az2"},
			}},
			expectedLabels: []string{"us-central1", "", "us-central1", ""},
		},
		{
			name:     "missing zone in to",
			baseKeys: []string{"region", "zone"},
			from: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-central1"},
				{Key: "zone", Value: "az1"},
			}},
			to: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-central1"},
			}},
			expectedLabels: []string{"us-central1", "", "us-central1", ""},
		},
		{
			name:     "empty zone in to",
			baseKeys: []string{"region", "zone", "rack"},
			from: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-central1"},
				{Key: "zone", Value: "az1"},
				{Key: "rack", Value: "1"},
				{Key: "dns", Value: "a"},
			}},
			to: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-central1"},
				{Key: "zone", Value: ""},
				{Key: "rack", Value: "1"},
				{Key: "dns", Value: "b"},
			}},
			expectedLabels: []string{"us-central1", "az1", "", "us-central1", "", ""},
		},
		{
			name:     "different localities",
			baseKeys: []string{"region", "zone"},
			from: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-central1"},
				{Key: "zone", Value: "az1"},
			}},
			to: roachpb.Locality{Tiers: []roachpb.Tier{
				{Key: "cloud", Value: "cloud1"},
				{Key: "region", Value: "us-central1"},
			}},
			expectedLabels: []string{"", "", "", ""},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			baseLocality := roachpb.Locality{}
			for _, k := range tc.baseKeys {
				baseLocality.Tiers = append(baseLocality.Tiers, roachpb.Tier{Key: k})
			}
			labels := makeLocalityLabelValues(baseLocality, tc.from, tc.to)
			require.Equal(t, tc.expectedLabels, labels)
		})
	}
}
