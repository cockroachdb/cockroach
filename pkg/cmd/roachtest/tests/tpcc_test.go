// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestTPCCSupportedWarehouses(t *testing.T) {
	const expectPanic = -1
	tests := []struct {
		cloud        spec.Cloud
		spec         spec.ClusterSpec
		buildVersion *version.Version
		expected     int
	}{
		{spec.Local, spec.MakeClusterSpec(4, spec.CPU(16)), version.MustParse(`v2.1.0`), 15},

		{spec.GCE, spec.MakeClusterSpec(4, spec.CPU(16)), version.MustParse(`v2.1.0`), 1300},
		{spec.GCE, spec.MakeClusterSpec(4, spec.CPU(16)), version.MustParse(`v19.1.0-rc.1`), 1250},
		{spec.GCE, spec.MakeClusterSpec(4, spec.CPU(16)), version.MustParse(`v19.1.0`), 1250},

		{spec.AWS, spec.MakeClusterSpec(4, spec.CPU(16)), version.MustParse(`v19.1.0-rc.1`), 2100},
		{spec.AWS, spec.MakeClusterSpec(4, spec.CPU(16)), version.MustParse(`v19.1.0`), 2100},

		{spec.GCE, spec.MakeClusterSpec(5, spec.CPU(160)), version.MustParse(`v2.1.0`), expectPanic},
		{spec.GCE, spec.MakeClusterSpec(4, spec.CPU(16)), version.MustParse(`v1.0.0`), expectPanic},
	}
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			if test.expected == expectPanic {
				require.Panics(t, func() {
					w := maxSupportedTPCCWarehouses(*test.buildVersion, test.cloud, test.spec)
					t.Errorf("%s %s got unexpected result %d", test.cloud, &test.spec, w)
				})
			} else {
				require.Equal(t, test.expected, maxSupportedTPCCWarehouses(*test.buildVersion, test.cloud, test.spec))
			}
		})
	}
}

func TestGetMaxWarehousesAboveEfficiency(t *testing.T) {
	// Initialize test data structures
	histograms := &roachtestutil.HistogramMetric{Summaries: make([]*roachtestutil.HistogramSummaryMetric, 0)}
	wareHouseMetrics := make(map[string][]float64)

	// Base metrics from tpccbench/nodes=3/cpu=4/enc=true
	const (
		baseCount   = int64(125)  // Median value
		baseElapsed = int64(1000) // Average elapsed time per tick
		startWH     = 635         // Starting warehouse count
		endWH       = 750         // Ending warehouse count
		whIncrement = 10          // Warehouse increment step
	)

	maxWarehouse := 0

	// Generate test data for different warehouse counts
	for wh := startWH; wh <= endWH; wh += whIncrement {
		// Add random variation to count (-10 to +30)
		currCount := baseCount + int64(rand.Intn(41)-10)

		// Create histogram summary
		histograms.Summaries = append(histograms.Summaries, &roachtestutil.HistogramSummaryMetric{
			Name:         "newOrder",
			TotalCount:   currCount,
			TotalElapsed: baseElapsed,
			Labels:       []*roachtestutil.Label{{Name: warehouseLabel, Value: fmt.Sprintf("%d", wh)}},
		})

		// Calculate efficiency metrics
		tpmc := (float64(currCount) * 60000.0) / float64(baseElapsed)
		efficiency := (tpmc * 100) / (tpcc.DeckWarehouseFactor * float64(wh))

		// Store metrics for validation
		wareHouseMetrics[fmt.Sprintf("%d", wh)] = []float64{tpmc, efficiency}

		// Track maximum warehouse count meeting efficiency threshold
		if efficiency > tpcc.PassingEfficiency {
			maxWarehouse = wh
		}
	}

	// Get aggregated metrics and validate results
	aggregatedMetrics, err := getMaxWarehousesAboveEfficiency("tpccbench", histograms)
	require.NoError(t, err)

	t.Run("verify efficiency metrics", func(t *testing.T) {
		for _, metric := range aggregatedMetrics {
			// Check metrics with 'efficiency' suffix
			if strings.HasSuffix(metric.Name, "efficiency") {
				whLabel := metric.AdditionalLabels[0].Value
				expectedEfficiency := wareHouseMetrics[whLabel][1]
				require.Equal(t, expectedEfficiency, float64(metric.Value))
			}
		}
	})

	t.Run("verify tpmc metrics", func(t *testing.T) {
		for _, metric := range aggregatedMetrics {
			if strings.HasSuffix(metric.Name, "tpmc") {
				whLabel := metric.AdditionalLabels[0].Value
				expectedTpmc := wareHouseMetrics[whLabel][0]
				require.Equal(t, expectedTpmc, float64(metric.Value))
			}
		}
	})

	t.Run("verify max warehouse", func(t *testing.T) {
		require.Equal(t, float64(maxWarehouse), float64(aggregatedMetrics[len(aggregatedMetrics)-1].Value) /* The last metric emitted is the max warehouse number */)
	})
}
