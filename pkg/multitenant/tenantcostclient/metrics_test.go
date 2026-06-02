// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostclient

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/stretchr/testify/require"
)

func TestMetrics_EstimatedReplicationBytesForPath(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sqlLocality := roachpb.Locality{Tiers: []roachpb.Tier{
		{Key: "region", Value: "us-east1"},
		{Key: "zone", Value: "az1"},
	}}
	m := metrics{}
	m.Init(sqlLocality)

	require.Equal(t, sqlLocality, m.baseLocality)
	require.Empty(t, m.mu.pathMetrics)

	assertCachedPathMetricsSize := func(size int) {
		count := 0
		m.cachedPathMetrics.Range(func(_ networkPath, _ *networkPathMetrics) bool {
			count++
			return true
		})
		require.Equal(t, size, count)
	}

	assertCachedPathMetricsSize(0)

	type networkPath struct {
		fromNodeID   roachpb.NodeID
		fromLocality roachpb.Locality
		toNodeID     roachpb.NodeID
		toLocality   roachpb.Locality
	}

	get := func(np networkPath) *aggmetric.Counter {
		return m.EstimatedReplicationBytesForPath(
			np.fromNodeID, np.fromLocality, np.toNodeID, np.toLocality,
		)
	}

	crossZonePath := networkPath{
		fromNodeID: roachpb.NodeID(1),
		fromLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east1"},
			{Key: "zone", Value: "1"},
		}},
		toNodeID: roachpb.NodeID(2),
		toLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east1"},
			{Key: "zone", Value: "2"},
		}},
	}
	crossRegionPath1 := networkPath{
		fromNodeID: roachpb.NodeID(3),
		fromLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-central1"},
			{Key: "zone", Value: "1"},
		}},
		toNodeID: roachpb.NodeID(2),
		toLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east1"},
			{Key: "zone", Value: "2"},
		}},
	}
	crossRegionPath2 := networkPath{
		fromNodeID: roachpb.NodeID(4),
		fromLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-central1"},
			{Key: "zone", Value: "3"},
		}},
		toNodeID: roachpb.NodeID(5),
		toLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east1"},
			{Key: "zone", Value: "4"},
		}},
	}

	// Create a new cross-zone metric.
	crossZoneMetric := get(crossZonePath)
	require.EqualValues(t, 0, crossZoneMetric.Value())
	crossZoneMetric.Inc(10)
	require.Len(t, m.mu.pathMetrics, 1)
	assertCachedPathMetricsSize(1)

	// Create a new cross-region metric.
	crossRegionMetric := get(crossRegionPath1)
	require.EqualValues(t, 0, crossRegionMetric.Value())
	crossRegionMetric.Inc(5)
	require.Len(t, m.mu.pathMetrics, 2)
	assertCachedPathMetricsSize(2)

	// Use a different network path. Metric should be the same after
	// normalization.
	crossRegionMetric = get(crossRegionPath2)
	require.EqualValues(t, 5, crossRegionMetric.Value())
	crossRegionMetric.Inc(2)
	require.Len(t, m.mu.pathMetrics, 2)
	assertCachedPathMetricsSize(3)

	// Fetch existing metrics.
	crossZoneMetric = get(crossZonePath)
	require.EqualValues(t, 10, crossZoneMetric.Value())
	crossRegionMetric = get(crossRegionPath1)
	require.EqualValues(t, 7, crossRegionMetric.Value())
	crossRegionMetric = get(crossRegionPath2)
	require.EqualValues(t, 7, crossRegionMetric.Value())
	require.Len(t, m.mu.pathMetrics, 2)
	assertCachedPathMetricsSize(3)

	// Localities were updated, but cached metrics exist.
	crossRegionPath3 := networkPath{
		fromNodeID: roachpb.NodeID(4),
		fromLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "europe-west1"},
		}},
		toNodeID: roachpb.NodeID(5),
		toLocality: roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east1"},
		}},
	}
	crossRegionMetric = get(crossRegionPath3)
	require.EqualValues(t, 7, crossRegionMetric.Value())
	require.Len(t, m.mu.pathMetrics, 2)
	assertCachedPathMetricsSize(3)
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
