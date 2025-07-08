// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package history

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/montanaflynn/stats"
)

// History contains recorded information that summarizes a simulation run.
// Currently it only contains the store metrics of the run.
// TODO(kvoli): Add a range log like structure to the history.
type History struct {
	// Recorded contains per-store metrics snapshots at each tick. The outer slice
	// grows over time at each tick, while each inner slice has one StoreMetrics per
	// store. E.g. Recorded[0] is the first tick, Recorded[0][0] is the first store's
	// metrics at the first tick.
	Recorded [][]metrics.StoreMetrics
	S        state.State
}

// Listen implements the metrics.StoreMetricListener interface.
func (h *History) Listen(ctx context.Context, sms []metrics.StoreMetrics) {
	h.Recorded = append(h.Recorded, sms)
}

func (h *History) ShowRecordedValueAt(idx int, stat string) string {
	var buf strings.Builder

	storeMetricsAtTick := h.Recorded[idx]
	values := make([]float64, 0, len(storeMetricsAtTick))

	_, _ = fmt.Fprintf(&buf, "[")

	// Extract values for each store. Note that h.Recorded[idx] is already sorted
	// by store ID when appending to h.Recorded.
	for i, sm := range storeMetricsAtTick {
		if i > 0 {
			_, _ = fmt.Fprintf(&buf, ", ")
		}
		value := sm.GetMetricValue(stat)
		if stat == "disk_fraction_used" {
			_, _ = fmt.Fprintf(&buf, "s%v=%.2f", sm.StoreID, value)
		} else {
			_, _ = fmt.Fprintf(&buf, "s%v=%.0f", sm.StoreID, value)
		}
		values = append(values, value)
	}
	_, _ = fmt.Fprintf(&buf, "]")
	stddev, _ := stats.StandardDeviation(values)
	mean, _ := stats.Mean(values)
	sum, _ := stats.Sum(values)
	_, _ = fmt.Fprintf(&buf, " (stddev=%.2f, mean=%.2f, sum=%.0f)", stddev, mean, sum)
	return buf.String()
}
