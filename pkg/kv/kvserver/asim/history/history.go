// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package history

import (
	"context"
	"fmt"
	"github.com/montanaflynn/stats"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

// History contains recorded information that summarizes a simulation run.
// Currently it only contains the store metrics of the run.
// TODO(kvoli): Add a range log like structure to the history.
type History struct {
	Recorded [][]metrics.StoreMetrics
	S        state.State
}

// Listen implements the metrics.StoreMetricListener interface.
func (h *History) Listen(ctx context.Context, sms []metrics.StoreMetrics) {
	h.Recorded = append(h.Recorded, sms)
}

func (h *History) ShowRecordedValueAt(idx int, stat string) string {
	var buf strings.Builder
	type storeIDWithStat struct {
		StoreID int64
		Value   float64
	}
	tick := h.Recorded[idx]
	orderedStoreIDs := make([]storeIDWithStat, 0, len(tick))
	values := make([]float64, 0, len(tick))
	for _, sm := range tick {
		var value float64
		switch stat {
		case "qps":
			value = float64(sm.QPS)
		case "cpu":
			value = float64(sm.CPU)
		case "write_bytes_per_second":
			value = float64(sm.WriteBytesPerSecond)
		case "write":
			value = float64(sm.WriteKeys)
		case "write_b":
			value = float64(sm.WriteBytes)
		case "read":
			value = float64(sm.ReadKeys)
		case "read_b":
			value = float64(sm.ReadBytes)
		case "replicas":
			value = float64(sm.Replicas)
		case "leases":
			value = float64(sm.Leases)
		case "lease_moves":
			value = float64(sm.LeaseTransfers)
		case "replica_moves":
			value = float64(sm.Rebalances)
		case "replica_b_rcvd":
			value = float64(sm.RebalanceRcvdBytes)
		case "replica_b_sent":
			value = float64(sm.RebalanceSentBytes)
		case "range_splits":
			value = float64(sm.RangeSplits)
		case "disk_fraction_used":
			value = sm.DiskFractionUsed
		}
		orderedStoreIDs = append(orderedStoreIDs, storeIDWithStat{
			StoreID: sm.StoreID,
			Value:   value,
		})
		values = append(values, value)
	}
	sort.Slice(orderedStoreIDs, func(i, j int) bool {
		return orderedStoreIDs[i].StoreID < orderedStoreIDs[j].StoreID
	})
	fmt.Fprintf(&buf, "[")
	for i, store := range orderedStoreIDs {
		if i > 0 {
			fmt.Fprintf(&buf, " ")
		}
		if stat == "disk_fraction_used" {
			fmt.Fprintf(&buf, "s%v=%.2f", store.StoreID, store.Value)
		} else {
			fmt.Fprintf(&buf, "s%v=%.0f", store.StoreID, store.Value)
		}
	}
	fmt.Fprintf(&buf, "]")
	stddev, _ := stats.StandardDeviation(values)
	fmt.Fprintf(&buf, " (stddev=%.2f)", stddev)
	return buf.String()
}
