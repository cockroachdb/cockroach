// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostmodel

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
)

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
	// ReadRequestCost is a lookup table that maps from the number of requests
	// in a read batch, to the amount of KV CPU used to process each additional
	// read request in a batch, beyond the first. As the batch size increases,
	// each KV CPU can process more read requests.
	ReadRequestCost struct {
		BatchSize     []float64
		CPUPerRequest []EstimatedCPU
	}
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
	// write request in a batch, beyond the second. The second request in simple
	// write batches is often an EndTxn request, which has near-zero cost in the
	// fast path. As the batch size increases, each KV CPU can process more write
	// requests.
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
	ReadBatchCost: 1.0 / 3500,
	ReadRequestCost: struct {
		BatchSize     []float64
		CPUPerRequest []EstimatedCPU
	}{
		BatchSize: []float64{8, 16, 32, 64},
		CPUPerRequest: []EstimatedCPU{
			1.0 / 16500, 1.0 / 26700, 1.0 / 35000, 1.0 / 40400,
		},
	},
	ReadBytesCost: struct {
		PayloadSize []float64
		CPUPerByte  []EstimatedCPU
	}{
		PayloadSize: []float64{256, 1024, 4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024},
		CPUPerByte: []EstimatedCPU{
			1.0 / 4 / 1024 / 1024,
			1.0 / 6 / 1024 / 1024,
			1.0 / 9.5 / 1024 / 1024,
			1.0 / 32 / 1024 / 1024,
			1.0 / 62 / 1024 / 1024,
			1.0 / 106 / 1024 / 1024,
		},
	},
	WriteBatchCost: struct {
		RatePerNode []float64
		CPUPerBatch []EstimatedCPU
	}{
		RatePerNode: []float64{100, 200, 400, 800, 1600, 3200, 6400, 12800},
		CPUPerBatch: []EstimatedCPU{
			1.0 / 700, 1.0 / 900, 1.0 / 1100, 1.0 / 1300, 1.0 / 1700, 1.0 / 2200, 1.0 / 2700, 1.0 / 3400,
		},
	},
	WriteRequestCost: struct {
		BatchSize     []float64
		CPUPerRequest []EstimatedCPU
	}{
		BatchSize: []float64{2, 3, 6, 11, 22, 43, 84},
		CPUPerRequest: []EstimatedCPU{
			1.0 / 1100, 1.0 / 2700, 1.0 / 6400, 1.0 / 10200, 1.0 / 14600, 1.0 / 18500, 1.0 / 19600,
		},
	},
	WriteBytesCost: struct {
		PayloadSize []float64
		CPUPerByte  []EstimatedCPU
	}{
		PayloadSize: []float64{256, 1024, 4 * 1024, 16 * 1024, 64 * 1024},
		CPUPerByte: []EstimatedCPU{
			1.0 / 9 / 1024 / 1024,
			1.0 / 10 / 1024 / 1024,
			1.0 / 12.5 / 1024 / 1024,
			1.0 / 15 / 1024 / 1024,
			1.0 / 19 / 1024 / 1024,
		},
	},
	BackgroundCPU: struct {
		Amount       EstimatedCPU
		Amortization float64
	}{
		Amount:       0.65,
		Amortization: 2,
	},
}

// MakeBatchInfo returns the count and size of KV read and write operations that
// are part of the given batch.
func (c *EstimatedCPUModel) MakeBatchInfo(
	request *kvpb.BatchRequest, response *kvpb.BatchResponse,
) (info BatchInfo) {
	for i := range request.Requests {
		// Request count is guaranteed to equal response count.
		req := request.Requests[i].GetInner()
		resp := response.Responses[i].GetInner()

		if kvpb.IsReadOnly(req) {
			info.ReadCount++
			info.ReadBytes += resp.Header().NumBytes
		} else {
			info.WriteCount++
			if swr, isSizedWrite := req.(kvpb.SizedWriteRequest); isSizedWrite {
				info.WriteBytes += swr.WriteBytes()
			}
		}
	}
	return info
}

// BatchCost returns the cost of the given batch in estimated KV vCPUs. This
// represents the amount of CPU that the KV layer is expected to consume in
// order to process the read or write requests contained in the batch, including
// the cost of replicating any writes.
//
// ratePerNode is the average write batch QPS in the tenant cluster, on a
// per-node basis. Because tenant clusters do not have physical nodes, these are
// logical nodes derived from the number of provisioned vCPUs and regions in the
// virtual cluster.
func (m *EstimatedCPUModel) BatchCost(
	bi BatchInfo, ratePerNode float64, replicas int64,
) EstimatedCPU {
	var readCPU, writeCPU EstimatedCPU

	if bi.ReadCount > 0 {
		// Add cost for the batch.
		readCPU = m.ReadBatchCost

		// Add cost for additional requests in the batch, beyond the first.
		if bi.ReadCount > 1 {
			ecpuPerRequest := m.lookupCost(
				m.ReadRequestCost.BatchSize, m.ReadRequestCost.CPUPerRequest, float64(bi.ReadCount))
			readCPU += ecpuPerRequest * EstimatedCPU(bi.ReadCount-1)
		}

		// Add cost for bytes in the requests.
		ecpuPerByte := m.lookupCost(
			m.ReadBytesCost.PayloadSize, m.ReadBytesCost.CPUPerByte, float64(bi.ReadBytes))
		readCPU += ecpuPerByte * EstimatedCPU(bi.ReadBytes)
	}

	if bi.WriteCount > 0 {
		// Add cost for the batch.
		writeCPU = m.lookupCost(m.WriteBatchCost.RatePerNode, m.WriteBatchCost.CPUPerBatch, ratePerNode)

		// Add cost for additional requests in the batch, beyond the second (see
		// EstimatedCPUModel.WriteRequestCost comment for furthe).
		if bi.WriteCount > 2 {
			ecpuPerRequest := m.lookupCost(
				m.WriteRequestCost.BatchSize, m.WriteRequestCost.CPUPerRequest, float64(bi.WriteCount))
			writeCPU += ecpuPerRequest * EstimatedCPU(bi.WriteCount-2)
		}

		// Add cost for bytes in the requests.
		ecpuPerByte := m.lookupCost(
			m.WriteBytesCost.PayloadSize, m.WriteBytesCost.CPUPerByte, float64(bi.WriteBytes))
		writeCPU += ecpuPerByte * EstimatedCPU(bi.WriteBytes)

		// Multiply by the number of replicas.
		writeCPU *= EstimatedCPU(replicas)
	}

	return readCPU + writeCPU
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
