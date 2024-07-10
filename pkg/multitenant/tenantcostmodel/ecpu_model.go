// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcostmodel

import "sort"

// EstimatedCPU is a prediction of how much CPU a particular operation will
// consume. It includes measured SQL CPU usage as well as an estimate of KV CPU
// usage, based on the number and size of read and write requests sent from the
// SQL layer to the KV layer.
type EstimatedCPU float64

// EstimatedCPUModel contains all the conversion factors needed to predict KV-
// layer CPU consumption from the read and write requests flowing between the
// SQL and KV layers.
//
// Some of the factors are represented by lookup tables that give different
// results depending on the magnitude of the input metric. For example, the
// larger the write batch, the more efficient the KV layer becomes, due to
// implicit batching effects. To derive CPU consumption from a lookup table, we
// perform a binary search on the metric in the first slice and then use linear
// interpolation to find a corresponding value in the second slice.
type EstimatedCPUModel struct {
	// ReadBatchCost is the amount of KV CPU needed to process 1 read batch
	// containing 1 read request with a 1-byte payload.
	ReadBatchCost EstimatedCPU
	// ReadRequestCost is the amount of KV CPU needed to process each additional
	// read request in a batch, beyond the first.
	ReadRequestCost EstimatedCPU
	// ReadBytesCost is a lookup table that maps from the total payload size of
	// a read batch, to the amount of KV CPU needed to process those bytes. As
	// the payload size increases, each KV CPU can process more bytes.
	ReadBytesCost struct {
		PayloadSize []float64
		CPUPerByte  []EstimatedCPU
	}
	// WriteBatchCost is a lookup table that maps from write QPS per node, to
	// the amount of KV CPU used to process 1 write batch containing 1 write
	// request with a 1-byte payload. As the write QPS rises, each KV CPU can
	// process more write batches.
	WriteBatchCost struct {
		RatePerNode []float64
		CPUPerBatch []EstimatedCPU
	}
	// WriteRequestCost is a lookup table that maps from the number of requests
	// in a write batch, to the amount of KV CPU used to process each additional
	// write request in a batch, beyond the first. As the batch size increases,
	// each KV CPU can process more write requests.
	WriteRequestCost struct {
		BatchSize     []float64
		CPUPerRequest []EstimatedCPU
	}
	// WriteBytesCost is a lookup table that maps from the total payload size of
	// a write batch, to the amount of KV CPU needed to process those bytes. As
	// the payload size increases, each KV CPU can process more bytes.
	WriteBytesCost struct {
		PayloadSize []float64
		CPUPerByte  []EstimatedCPU
	}
	// BackgroundCPU is the amount of fixed CPU overhead for a cluster, due to
	// the cost of heartbeats, background GC, storage compaction, etc. This
	// overhead is smoothly amortized across N vCPUs of usage by the cluster.
	// For example, say fixed overhead of 1/2 vCPU is amortized across the first
	// 5 vCPUs of usage. Therefore, estimated usage of 1 vCPU would be increased
	// to 1.1, usage of 4 vCPUs would be increased to 4.4, and usage of 8 vCPUs
	// would be increased to 8.5.
	BackgroundCPU struct {
		Amount       EstimatedCPU
		Amortization float64
	}
}

// DefaultEstimatedCPUModel is the default model that is used if the
// tenant_cost_model.estimated_cpu cluster setting is not specified.
var DefaultEstimatedCPUModel = EstimatedCPUModel{
	ReadBatchCost:   1.0 / 3500,
	ReadRequestCost: 1.0 / 45000,
	ReadBytesCost: struct {
		PayloadSize []float64
		CPUPerByte  []EstimatedCPU
	}{
		PayloadSize: []float64{256, 1024, 4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024},
		CPUPerByte: []EstimatedCPU{
			1.0 / 1.5 / 1024 / 1024,
			1.0 / 5.5 / 1024 / 1024,
			1.0 / 12 / 1024 / 1024,
			1.0 / 34 / 1024 / 1024,
			1.0 / 64 / 1024 / 1024,
			1.0 / 89 / 1024 / 1024,
		},
	},
	WriteBatchCost: struct {
		RatePerNode []float64
		CPUPerBatch []EstimatedCPU
	}{
		RatePerNode: []float64{100, 200, 400, 800, 1600, 3200, 6400, 12800},
		CPUPerBatch: []EstimatedCPU{
			1.0 / 660, 1.0 / 850, 1.0 / 1090, 1.0 / 1400, 1.0 / 1790, 1.0 / 2290, 1.0 / 2930, 1.0 / 3150,
		},
	},
	WriteRequestCost: struct {
		BatchSize     []float64
		CPUPerRequest []EstimatedCPU
	}{
		BatchSize: []float64{3, 6, 12, 25, 50, 100, 200},
		CPUPerRequest: []EstimatedCPU{
			1.0 / 2500, 1.0 / 5250, 1.0 / 9050, 1.0 / 11900, 1.0 / 15400, 1.0 / 17400, 1.0 / 19000},
	},
	WriteBytesCost: struct {
		PayloadSize []float64
		CPUPerByte  []EstimatedCPU
	}{
		PayloadSize: []float64{256, 1024, 4 * 1024, 16 * 1024, 64 * 1024},
		CPUPerByte: []EstimatedCPU{
			1.0 / 3.75 / 1024 / 1024,
			1.0 / 8 / 1024 / 1024,
			1.0 / 11 / 1024 / 1024,
			1.0 / 14 / 1024 / 1024,
			1.0 / 18.5 / 1024 / 1024,
		},
	},
	BackgroundCPU: struct {
		Amount       EstimatedCPU
		Amortization float64
	}{
		Amount:       0.65,
		Amortization: 6,
	},
}

// RequestCost returns the cost, in estimated KV vCPUs, of the given request. If
// it is a write, that includes the per-batch, per-request, and per-byte costs,
// multiplied by the number of replicas. If it is a read, then the cost is zero,
// since reads can only be costed by examining the ResponseInfo. ratePerNode
// is the average write batch QPS in the tenant cluster, on a per-node basis.
// Because tenant clusters do not have physical nodes, these are logical nodes
// derived from the number of provisioned vCPUs and regions in the virtual
// cluster.
func (m *EstimatedCPUModel) RequestCost(bri RequestInfo, ratePerNode float64) EstimatedCPU {
	if !bri.IsWrite() {
		return 0
	}

	// Add cost for the batch.
	ecpu := m.lookupCost(m.WriteBatchCost.RatePerNode, m.WriteBatchCost.CPUPerBatch, ratePerNode)

	// Add cost for additional requests in the batch, beyond the first.
	if bri.writeCount > 1 {
		ecpuPerRequest := m.lookupCost(
			m.WriteRequestCost.BatchSize, m.WriteRequestCost.CPUPerRequest, float64(bri.writeCount))
		ecpu += ecpuPerRequest * EstimatedCPU(bri.writeCount-1)
	}

	// Add cost for bytes in the requests.
	ecpuPerByte := m.lookupCost(
		m.WriteBytesCost.PayloadSize, m.WriteBytesCost.CPUPerByte, float64(bri.writeBytes))
	ecpu += ecpuPerByte * EstimatedCPU(bri.writeBytes)

	// Multiply by the number of replicas.
	return ecpu * EstimatedCPU(bri.writeReplicas)
}

// ResponseCost returns the cost, in estimated KV vCPUs, of the given response.
// If it is a read, that includes the per-batch, per-request, and per-byte
// costs. If it is a write, then the cost is zero, since writes can only be
// costed by examining the RequestInfo.
func (m *EstimatedCPUModel) ResponseCost(bri ResponseInfo) EstimatedCPU {
	if !bri.IsRead() {
		return 0
	}

	// Add cost for the batch.
	ecpu := m.ReadBatchCost

	// Add cost for additional requests in the batch, beyond the first.
	if bri.readCount > 1 {
		ecpu += m.ReadRequestCost * EstimatedCPU(bri.readCount-1)
	}

	// Add cost for bytes in the requests.
	ecpuPerByte := m.lookupCost(
		m.ReadBytesCost.PayloadSize, m.ReadBytesCost.CPUPerByte, float64(bri.readBytes))
	ecpu += ecpuPerByte * EstimatedCPU(bri.readBytes)

	return ecpu
}

func (m *EstimatedCPUModel) lookupCost(
	keys []float64, values []EstimatedCPU, key float64,
) EstimatedCPU {
	if key <= keys[0] {
		return values[0]
	} else if key >= keys[len(keys)-1] {
		return values[len(keys)-1]
	}

	index := sort.SearchFloat64s(keys, key)
	interpolate := (key - keys[index-1]) / (keys[index] - keys[index-1])
	return values[index-1] + EstimatedCPU(interpolate)*(values[index]-values[index-1])
}
