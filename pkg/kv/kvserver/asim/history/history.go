// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package history

import (
	"context"
	"fmt"
	"math"
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

// Volatility measures the thrashing of a store for a given stat over time. It
// is a tuple of the frequency of thrashing and the magnitude of thrashing.
// Frequency is the number of times the derivative sign changes. Magnitude is
// the sum of the absolute differences between the values when the sign of the
// derivative changes divided by mean value over all ticks.
type volatility struct {
	frequency int
	// TODO(wenyihu6): Magnitude is not very accurate since it just capctures the
	// absolute difference between the values when the sign of the derivative
	// changes. But it is possible that the magnitude is small at that time but
	// change aggressively soonly after.
	magnitude float64
}

// ThrashingForStat computes the thrashing for the given stat. The returned
// values are the maximum volatility, the store ID of the store with the maximum
// volatility, and the volatility for each store. We measure thrashing as the
// sum of the absolute differences between the values when the sign of the
// derivative changes.
func (h *History) ThrashingForStat(
	stat string,
) (maxVolatilityIdx int, storeVolatility []volatility) {
	if len(h.Recorded) == 0 {
		return 0, nil
	}
	numOfTicks := len(h.Recorded)
	numOfStores := len(h.Recorded[0])
	storeVolatility = make([]volatility, numOfStores)
	for j := 0; j < numOfStores; j++ {
		values := make([]float64, 0, numOfTicks)
		prevDerivative := 0.0
		numOfThrashing := 0
		totalThrashing := 0.0
		for i := 0; i < numOfTicks; i++ {
			values = append(values, h.Recorded[i][j].GetMetricValue(stat))
			if i > 0 {
				derivative := values[i] - values[i-1]
				if (derivative < 0) != (prevDerivative < 0) {
					numOfThrashing++
					totalThrashing += math.Abs(derivative - prevDerivative)
				}
				prevDerivative = derivative
			}
		}
		mean, _ := stats.Mean(values)
		if numOfThrashing > storeVolatility[maxVolatilityIdx].frequency {
			maxVolatilityIdx = j
		}
		storeVolatility[j] = volatility{
			frequency: numOfThrashing,
			magnitude: totalThrashing / mean,
		}
	}
	return maxVolatilityIdx, storeVolatility
}

// Thrashing returns a string representation of the thrashing for the given
// stat. If detail is true, the string includes the volatility for each store.
func (h *History) Thrashing(stat string) string {
	var buf strings.Builder
	// Compute volatility of the stat. For every store, compute the standard
	// deviation of the values. Then compute the mean of the standard deviations.
	// Then compute the standard deviation of the means.
	maxVolatilityIdx, storeVolatility := h.ThrashingForStat(stat)
	_, _ = fmt.Fprintf(&buf, "[max(s%d)=(freq=%d, mag=%.2f)",
		maxVolatilityIdx+1, storeVolatility[maxVolatilityIdx].frequency, storeVolatility[maxVolatilityIdx].magnitude)
	_, _ = fmt.Fprintf(&buf, " (")
	for i, v := range storeVolatility {
		if i > 0 {
			_, _ = fmt.Fprintf(&buf, ", ")
		}
		_, _ = fmt.Fprintf(&buf, "s%v=(freq=%d, mag=%.2f)", i+1, v.frequency, v.magnitude)
	}
	_, _ = fmt.Fprintf(&buf, ")")
	_, _ = fmt.Fprintf(&buf, "]")
	return buf.String()
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
