// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metrics

import "context"

// TimeSeriesMetricListener implements the metrics Listen interface and is used
// for collecting timeseries data of a run.
type TimeSeriesMetricListener struct {
	collected *map[string][][]float64
}

// NewTimeSeriesMetricListener returns a new new TimeSeriesMetricListener.
func NewTimeSeriesMetricListener(
	collected *map[string][][]float64, stores int,
) *TimeSeriesMetricListener {
	tml := &TimeSeriesMetricListener{
		collected: collected,
	}
	(*tml.collected)["qps"] = make([][]float64, stores)
	(*tml.collected)["write"] = make([][]float64, stores)
	(*tml.collected)["write_b"] = make([][]float64, stores)
	(*tml.collected)["read"] = make([][]float64, stores)
	(*tml.collected)["read_b"] = make([][]float64, stores)
	(*tml.collected)["replicas"] = make([][]float64, stores)
	(*tml.collected)["leases"] = make([][]float64, stores)
	(*tml.collected)["lease_moves"] = make([][]float64, stores)
	(*tml.collected)["replica_moves"] = make([][]float64, stores)
	(*tml.collected)["replica_b_rcvd"] = make([][]float64, stores)
	(*tml.collected)["replica_b_sent"] = make([][]float64, stores)
	(*tml.collected)["range_splits"] = make([][]float64, stores)
	return tml
}

// Listen implements the metrics tracker Listen interface.
func (tml *TimeSeriesMetricListener) Listen(ctx context.Context, sms []StoreMetrics) {
	for i, sm := range sms {
		(*tml.collected)["qps"][i] = append((*tml.collected)["qps"][i], float64(sm.QPS))
		(*tml.collected)["write"][i] = append((*tml.collected)["write"][i], float64(sm.WriteKeys))
		(*tml.collected)["write_b"][i] = append((*tml.collected)["write_b"][i], float64(sm.WriteBytes))
		(*tml.collected)["read"][i] = append((*tml.collected)["read"][i], float64(sm.ReadKeys))
		(*tml.collected)["read_b"][i] = append((*tml.collected)["read_b"][i], float64(sm.ReadBytes))
		(*tml.collected)["replicas"][i] = append((*tml.collected)["replicas"][i], float64(sm.Replicas))
		(*tml.collected)["leases"][i] = append((*tml.collected)["leases"][i], float64(sm.Leases))
		(*tml.collected)["lease_moves"][i] = append((*tml.collected)["lease_moves"][i], float64(sm.LeaseTransfers))
		(*tml.collected)["replica_moves"][i] = append((*tml.collected)["replica_moves"][i], float64(sm.Rebalances))
		(*tml.collected)["replica_b_rcvd"][i] = append((*tml.collected)["replica_b_rcvd"][i], float64(sm.RebalanceRcvdBytes))
		(*tml.collected)["replica_b_sent"][i] = append((*tml.collected)["replica_b_sent"][i], float64(sm.RebalanceSentBytes))
		(*tml.collected)["range_splits"][i] = append((*tml.collected)["range_splits"][i], float64(sm.RangeSplits))
	}
}
