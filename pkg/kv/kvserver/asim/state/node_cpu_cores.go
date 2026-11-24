// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

type NodeCPUCores []float64

// ToRateCapacityNanos converts NodeCPUCores to node capacities in nanos
// (NodeCPURateCapacities).
func (nc NodeCPUCores) ToRateCapacityNanos() NodeCPURateCapacities {
	res := make(NodeCPURateCapacities, len(nc))
	const nanosPerSecond = 1e9
	for i, cores := range nc {
		res[i] = uint64(cores * nanosPerSecond)
	}
	return res
}
