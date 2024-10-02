// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostmodel

import "github.com/cockroachdb/cockroach/pkg/kv/kvpb"

// RU stands for "Request Unit(s)"; the tenant cost model maps tenant activity
// into this abstract unit.
//
// To get an idea of the magnitude of an RU, the cost model was designed so that
// a small point read of < 64 bytes costs 1 RU.
type RU float64

// NetworkCost records how expensive network traffic is between two regions.
// bytes_transmitted * NetworkCost is the RU cost of the network traffic.
type NetworkCost float64

// RequestUnitModel contains the cost model parameters for computing request
// units (RUs). The values are controlled by cluster settings.
//
// The aim of the model is to provide a reasonable estimate of the resource
// utilization of a tenant, excluding storage. A tenant uses resources on the
// SQL pods as well as on the KV cluster; we model the latter based on the KV
// operations performed by the tenant.
//
// The cost model takes into account the following activities:
//
//   - KV "read" and "write" batches. KV batches that read or write data have a
//     base cost, a per-request cost, and a per-byte cost. Specifically, the
//     cost of a read batch is:
//     RUs = KVReadBatch +
//     <count of reads in batch> * KVReadRequest +
//     <size of reads in bytes> * KVReadByte
//     The cost of a write batch is:
//     RUs = KVWriteBatch +
//     <count of writes in batch> * KVWriteRequest +
//     <size of writes in bytes> * KVWriteByte
//
//   - CPU usage on the tenant's SQL pods.
//
//   - Writes to external storage services such as S3.
//
//   - Count of bytes returned from SQL to the client (network egress).
type RequestUnitModel struct {
	// KVReadBatch is the baseline cost of a batch of KV reads.
	KVReadBatch RU

	// KVReadRequest is the baseline cost of a KV read.
	KVReadRequest RU

	// KVReadByte is the per-byte cost of a KV read.
	KVReadByte RU

	// KVWriteBatch is the baseline cost of a batch of KV writes.
	KVWriteBatch RU

	// KVWriteRequest is the baseline cost of a KV write.
	KVWriteRequest RU

	// KVWriteByte is the per-byte cost of a KV write.
	KVWriteByte RU

	// PodCPUSecond is the cost of using a CPU second on the SQL pod.
	PodCPUSecond RU

	// PGWireEgressByte is the cost of transferring one byte from a SQL pod to the
	// client.
	PGWireEgressByte RU

	// ExternalIOEgressByte is the cost of transferring one byte from a SQL pod
	// to external services.
	ExternalIOEgressByte RU

	// ExternalIOIngressByte is the cost of transferring one byte from an external
	// service into the SQL pod.
	ExternalIOIngressByte RU

	// NetworkCostTable is a table describing the network cost between regions.
	NetworkCostTable NetworkCostTable
}

// KVReadCost calculates the cost of a KV read operation.
func (c *RequestUnitModel) KVReadCost(count, bytes int64) RU {
	return c.KVReadBatch + RU(count)*c.KVReadRequest + RU(bytes)*c.KVReadByte
}

// KVWriteCost calculates the cost of a KV write operation.
func (c *RequestUnitModel) KVWriteCost(count, bytes int64) RU {
	return c.KVWriteBatch + RU(count)*c.KVWriteRequest + RU(bytes)*c.KVWriteByte
}

// PodCPUCost calculates the cost of CPU seconds consumed in the SQL pod.
func (c *RequestUnitModel) PodCPUCost(seconds float64) RU {
	return RU(seconds) * c.PodCPUSecond
}

// PGWireEgressCost calculates the cost of bytes leaving the SQL pod to external
// services.
func (c *RequestUnitModel) PGWireEgressCost(bytes int64) RU {
	return RU(bytes) * c.PGWireEgressByte
}

// ExternalIOEgressCost calculates the cost of an external write operation.
func (c *RequestUnitModel) ExternalIOEgressCost(bytes int64) RU {
	return RU(bytes) * c.ExternalIOEgressByte
}

// ExternalIOIngressCost calculates the cost of an external read operation.
func (c *RequestUnitModel) ExternalIOIngressCost(bytes int64) RU {
	return RU(bytes) * c.ExternalIOIngressByte
}

// NetworkCost retrieves the RU per byte cost for inter-region transfers from
// the source to the destination region. The order of this relationship matters
// because some clouds have asymetric networking charging.
func (c *RequestUnitModel) NetworkCost(path NetworkPath) NetworkCost {
	return c.NetworkCostTable.Matrix[path]
}

// MakeBatchInfo returns the count and size of KV read and write operations that
// are part of the given batch. Only a subset of operations are counted - those
// that represent user-initiated requests.
func (c *RequestUnitModel) MakeBatchInfo(
	request *kvpb.BatchRequest, response *kvpb.BatchResponse,
) (info BatchInfo) {
	var writeCount, writeBytes int64
	for i := range request.Requests {
		req := request.Requests[i].GetInner()

		// Only count non-admin requests in the batch that write user data. Other
		// requests are considered part of the "base" cost of a batch.
		switch req.(type) {
		case *kvpb.PutRequest, *kvpb.ConditionalPutRequest, *kvpb.IncrementRequest,
			*kvpb.DeleteRequest, *kvpb.DeleteRangeRequest, *kvpb.ClearRangeRequest,
			*kvpb.RevertRangeRequest, *kvpb.InitPutRequest, *kvpb.AddSSTableRequest:
			writeCount++
			if swr, isSizedWrite := req.(kvpb.SizedWriteRequest); isSizedWrite {
				writeBytes += swr.WriteBytes()
			}
		}
	}

	var readCount, readBytes int64
	for i := range response.Responses {
		resp := response.Responses[i].GetInner()

		// Only count requests in the batch that read user data. Other requests
		// are considered part of the "base" cost of a batch.
		switch resp.(type) {
		case *kvpb.GetResponse, *kvpb.ScanResponse, *kvpb.ReverseScanResponse,
			*kvpb.ExportResponse:
			readCount++
			readBytes += resp.Header().NumBytes
		}
	}

	return BatchInfo{
		ReadCount:  readCount,
		ReadBytes:  readBytes,
		WriteCount: writeCount,
		WriteBytes: writeBytes,
	}
}

// BatchCost returns the cost of the given batch in request units, including the
// cost of KV CPU and the cost of any cross-region networking.
func (c *RequestUnitModel) BatchCost(
	bi BatchInfo, networkCost NetworkCost, replicas int64,
) (kv RU, network RU) {
	// NOTE: the RU model assumes that a batch is either a write batch or a read
	// batch and cannot be both at once. If there is at least one write request
	// in the batch, it is treated as a write batch.
	if bi.WriteCount > 0 {
		// writeCost measures how expensive each replica is for crdb to write. This
		// covers things like the cost of serving the RPC, raft consensus, and pebble
		// write amplification.
		kv = c.KVWriteBatch
		kv += RU(bi.WriteCount) * c.KVWriteRequest
		kv += RU(bi.WriteBytes) * c.KVWriteByte
		kv *= RU(replicas)

		// Network cost is intended to cover the cost of cross region networking. The
		// network cost per byte is calculated by the caller and already includes the
		// overhead of each replica.
		network += RU(bi.WriteBytes) * RU(networkCost)
	} else if bi.ReadCount > 0 {
		kv = c.KVReadBatch
		kv += RU(bi.ReadCount) * c.KVReadRequest
		kv += RU(bi.ReadBytes) * c.KVReadByte
		network += RU(bi.ReadBytes) * RU(networkCost)
	}

	return kv, network
}
