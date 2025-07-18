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

// PerStoreValuesAt returns, for tick `idx` and metric `stat`, the per-store
// measurements at that tick, in the History's store order.
func (h *History) PerStoreValuesAt(idx int, stat string) []float64 {
	storeMetricsAtTick := h.Recorded[idx]
	values := make([]float64, 0, len(storeMetricsAtTick))

	// Extract values for each store. Note that h.Recorded[idx] is already sorted
	// by store ID when appending to h.Recorded.
	for _, sm := range storeMetricsAtTick {
		value := sm.GetMetricValue(stat)
		values = append(values, value)
	}
	return values
}

// ShowRecordedValueAt returns a string representation of the recorded values.
// The returned boolean is false if (and only if) the recorded values were all
// zero.
func (h *History) ShowRecordedValueAt(idx int, stat string) (string, bool) {
	var buf strings.Builder

	values := h.PerStoreValuesAt(idx, stat)

	_, _ = fmt.Fprintf(&buf, "[")

	// Extract values for each store. Note that h.Recorded[idx] is already sorted
	// by store ID when appending to h.Recorded.
	for i, v := range values {
		if i > 0 {
			_, _ = fmt.Fprintf(&buf, ", ")
		}
		storeID := h.Recorded[idx][i].StoreID

		if stat == "disk_fraction_used" {
			_, _ = fmt.Fprintf(&buf, "s%v=%.2f", storeID, v)
		} else {
			_, _ = fmt.Fprintf(&buf, "s%v=%.0f", storeID, v)
		}
	}
	_, _ = fmt.Fprintf(&buf, "]")
	stddev, _ := stats.StandardDeviation(values)
	mean, _ := stats.Mean(values)
	sum, _ := stats.Sum(values)
	_, _ = fmt.Fprintf(&buf, " (stddev=%.2f, mean=%.2f, sum=%.0f)", stddev, mean, sum)
	// If the stddev is zero, all values are the same. If additionally the mean
	// is zero, all values were zero.
	nonzero := stddev > 0 || mean != 0
	return buf.String(), nonzero
}
