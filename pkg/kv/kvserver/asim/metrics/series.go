// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics

// MakeTS translates a series of k store metrics, into a series of metrics with
// k store values per tick. Originally each row is a tick and the columns
// within a row are a store's metrics for the tick. The return value is a map
// from metric name to a series of store values.
func MakeTS(metrics [][]StoreMetrics) map[string][][]float64 {
	ret := map[string][][]float64{}

	if len(metrics) == 0 || len(metrics[0]) == 0 {
		return ret
	}

	stores := len(metrics[0])
	// TODO(kvoli): Either begin resuing prometheus metric definitions with a
	// custom scraper or provide definitions for each metric available. These
	// are partially duplicated with the cluster tracker.
	ret["qps"] = make([][]float64, stores)
	ret["write"] = make([][]float64, stores)
	ret["write_b"] = make([][]float64, stores)
	ret["read"] = make([][]float64, stores)
	ret["read_b"] = make([][]float64, stores)
	ret["replicas"] = make([][]float64, stores)
	ret["leases"] = make([][]float64, stores)
	ret["lease_moves"] = make([][]float64, stores)
	ret["replica_moves"] = make([][]float64, stores)
	ret["replica_b_rcvd"] = make([][]float64, stores)
	ret["replica_b_sent"] = make([][]float64, stores)
	ret["range_splits"] = make([][]float64, stores)
	ret["disk_fraction_used"] = make([][]float64, stores)

	for _, sms := range metrics {
		for i, sm := range sms {
			ret["qps"][i] = append(ret["qps"][i], float64(sm.QPS))
			ret["write"][i] = append(ret["write"][i], float64(sm.WriteKeys))
			ret["write_b"][i] = append(ret["write_b"][i], float64(sm.WriteBytes))
			ret["read"][i] = append(ret["read"][i], float64(sm.ReadKeys))
			ret["read_b"][i] = append(ret["read_b"][i], float64(sm.ReadBytes))
			ret["replicas"][i] = append(ret["replicas"][i], float64(sm.Replicas))
			ret["leases"][i] = append(ret["leases"][i], float64(sm.Leases))
			ret["lease_moves"][i] = append(ret["lease_moves"][i], float64(sm.LeaseTransfers))
			ret["replica_moves"][i] = append(ret["replica_moves"][i], float64(sm.Rebalances))
			ret["replica_b_rcvd"][i] = append(ret["replica_b_rcvd"][i], float64(sm.RebalanceRcvdBytes))
			ret["replica_b_sent"][i] = append(ret["replica_b_sent"][i], float64(sm.RebalanceSentBytes))
			ret["range_splits"][i] = append(ret["range_splits"][i], float64(sm.RangeSplits))
			ret["disk_fraction_used"][i] = append(ret["disk_fraction_used"][i], sm.DiskFractionUsed)
		}
	}
	return ret
}

// Transpose will a copy of the given matrix, transposed. e.g.
// s1 [t1,t2]    t1 [s1,s2,s3]
// s2 [t1,t2] -> t2 [s1,s2,s3]
// s3 [t1,t2]
func Transpose(mat [][]float64) [][]float64 {
	n := len(mat)
	var ret [][]float64
	if n < 1 {
		return ret
	}
	m := len(mat[0])
	if m < 1 {
		return ret
	}

	ret = make([][]float64, m)
	for j := 0; j < m; j++ {
		ret[j] = make([]float64, n)
		for i := 0; i < n; i++ {
			ret[j][i] = mat[i][j]
		}
	}
	return ret
}
