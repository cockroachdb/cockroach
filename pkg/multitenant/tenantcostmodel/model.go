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

// Config contains the cost model parameters. The values are controlled by
// cluster settings.
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
type Config struct {
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
func (c *Config) KVReadCost(count, bytes int64) RU {
	return c.KVReadBatch + RU(count)*c.KVReadRequest + RU(bytes)*c.KVReadByte
}

// KVWriteCost calculates the cost of a KV write operation.
func (c *Config) KVWriteCost(count, bytes int64) RU {
	return c.KVWriteBatch + RU(count)*c.KVWriteRequest + RU(bytes)*c.KVWriteByte
}

// PodCPUCost calculates the cost of CPU seconds consumed in the SQL pod.
func (c *Config) PodCPUCost(seconds float64) RU {
	return RU(seconds) * c.PodCPUSecond
}

// PGWireEgressCost calculates the cost of bytes leaving the SQL pod to external
// services.
func (c *Config) PGWireEgressCost(bytes int64) RU {
	return RU(bytes) * c.PGWireEgressByte
}

// ExternalIOEgressCost calculates the cost of an external write operation.
func (c *Config) ExternalIOEgressCost(bytes int64) RU {
	return RU(bytes) * c.ExternalIOEgressByte
}

// ExternalIOIngressCost calculates the cost of an external read operation.
func (c *Config) ExternalIOIngressCost(bytes int64) RU {
	return RU(bytes) * c.ExternalIOIngressByte
}

// NetworkCost retrieves the RU per byte cost for inter-region transfers from
// the source to the destination region. The order of this relationship matters
// because some clouds have asymetric networking charging.
func (c *Config) NetworkCost(path NetworkPath) NetworkCost {
	return c.NetworkCostTable.Matrix[path]
}

// RequestCost returns the cost, in RUs, of the given request. If it is a
// write, that includes the per-batch, per-request, and per-byte costs,
// multiplied by the number of replicas plus the cost of the cross region
// networking. If it is a read, then the cost is zero, since reads can only be
// costed by examining the ResponseInfo.
func (c *Config) RequestCost(bri RequestInfo) (kv RU, network RU) {
	if !bri.IsWrite() {
		return 0, 0
	}
	// writeCost measures how expensive each replica is for crdb to write. This
	// covers things like the cost of serving the RPC, raft consensus, and pebble
	// write amplification.
	writeCost := c.KVWriteBatch
	writeCost += RU(bri.writeCount) * c.KVWriteRequest
	writeCost += RU(bri.writeBytes) * c.KVWriteByte

	// networkCost is intended to cover the cost of cross region networking. The
	// network cost per byte is calculated by distsender and already includes the
	// overhead of each replica.
	networkCost := RU(bri.networkCost) * RU(bri.writeBytes)

	return writeCost * RU(bri.writeReplicas), networkCost
}

// ResponseCost returns the cost, in RUs, of the given response. If it is a
// read, that includes the per-batch, per-request, and per-byte costs, and the
// cross region networking cost. If it is a write, then the cost is zero, since
// writes can only be costed by examining the RequestInfo.
func (c *Config) ResponseCost(bri ResponseInfo) (kv RU, network RU) {
	if !bri.IsRead() {
		return 0, 0
	}
	kv = c.KVReadBatch
	kv += RU(bri.readCount) * c.KVReadRequest
	kv += RU(bri.readBytes) * c.KVReadByte
	network = RU(bri.readBytes) * RU(bri.networkCost)
	return kv, network
}

// RequestInfo captures the BatchRequest information that is used (together
// with the cost model) to determine the portion of the cost that can be
// calculated up-front. Specifically: how many writes were batched together,
// their total size, the number of target replicas (if the request is a write
// batch), and the cross region network cost.
type RequestInfo struct {
	// writeReplicas is the number of range replicas to which this write was sent
	// (i.e. the replication factor). This is 0 if it is a read-only batch.
	writeReplicas int64
	// writeCount is the number of writes that were batched together. This is 0
	// if it is a read-only batch.
	writeCount int64
	// writeBytes is the total size of all batched writes in the request, in
	// bytes, or 0 if it is a read-only batch.
	writeBytes int64
	// networkCost is the per byte cost of the cross region network cost. This is
	// calculated ahead of time by distsender and accounts for the network cost
	// of each replica written.
	networkCost NetworkCost
}

// MakeRequestInfo extracts the relevant information from a BatchRequest.
func MakeRequestInfo(ba *kvpb.BatchRequest, replicas int, networkCost NetworkCost) RequestInfo {
	// The cost of read-only batches is captured by MakeResponseInfo.
	if !ba.IsWrite() {
		return RequestInfo{}
	}

	var writeCount, writeBytes int64
	for i := range ba.Requests {
		req := ba.Requests[i].GetInner()

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
	return RequestInfo{
		writeReplicas: int64(replicas),
		writeCount:    writeCount,
		writeBytes:    writeBytes,
		networkCost:   networkCost,
	}
}

// IsWrite is true if this was a write batch rather than a read-only batch.
func (bri RequestInfo) IsWrite() bool {
	return bri.writeCount != 0
}

// WriteReplicas is the number of range replicas to which the write was sent.
// This is 0 if it is a read-only batch.
func (bri RequestInfo) WriteReplicas() int64 {
	return bri.writeReplicas
}

// WriteCount is the number of writes that were batched together. This is 0 if
// it is a read-only batch.
func (bri RequestInfo) WriteCount() int64 {
	return bri.writeCount
}

// WriteBytes is the total size of all batched writes in the request, in bytes,
// or 0 if it is a read-only batch.
func (bri RequestInfo) WriteBytes() int64 {
	return bri.writeBytes
}

// TestingRequestInfo creates a RequestInfo for testing purposes.
func TestingRequestInfo(
	writeReplicas, writeCount, writeBytes int64, networkCost NetworkCost,
) RequestInfo {
	return RequestInfo{
		writeReplicas: writeReplicas,
		writeCount:    writeCount,
		writeBytes:    writeBytes,
		networkCost:   networkCost,
	}
}

// ResponseInfo captures the BatchResponse information that is used (together
// with the cost model) to determine the portion of the cost that can only be
// calculated after-the-fact. Specifically: how many reads were batched
// together their total size (if the request is a read-only batch), and the
// network cost for the read.
type ResponseInfo struct {
	// isRead is true if this batch contained only read requests, or false if it
	// was a write batch.
	isRead bool
	// readCount is the number of reads that were batched together. This is 0 if
	// it is a write batch.
	readCount int64
	// readBytes is the total size of all batched reads in the response, in
	// bytee, or 0 if it is a write batch.
	readBytes int64
	// networkCost is RU/byte cost for this read request.
	networkCost NetworkCost
}

// MakeResponseInfo extracts the relevant information from a BatchResponse.
func MakeResponseInfo(
	br *kvpb.BatchResponse, isReadOnly bool, networkCost NetworkCost,
) ResponseInfo {
	// The cost of non read-only batches is captured by MakeRequestInfo.
	if !isReadOnly {
		return ResponseInfo{}
	}

	var readCount, readBytes int64
	for i := range br.Responses {
		resp := br.Responses[i].GetInner()

		// Only count requests in the batch that read user data. Other requests
		// are considered part of the "base" cost of a batch.
		switch resp.(type) {
		case *kvpb.GetResponse, *kvpb.ScanResponse, *kvpb.ReverseScanResponse,
			*kvpb.ExportResponse:
			readCount++
			readBytes += resp.Header().NumBytes
		}
	}
	return ResponseInfo{
		isRead:      true,
		readCount:   readCount,
		readBytes:   readBytes,
		networkCost: networkCost,
	}
}

// IsRead is true if this was a read-only batch rather than a write batch.
func (bri ResponseInfo) IsRead() bool {
	return bri.isRead
}

// ReadCount is the number of reads that were batched together. This is 0 if it
// is a write batch.
func (bri ResponseInfo) ReadCount() int64 {
	return bri.readCount
}

// ReadBytes is the total size of all batched reads in the response, in bytes,
// or 0 if it is a write batch.
func (bri ResponseInfo) ReadBytes() int64 {
	return bri.readBytes
}

// TestingResponseInfo creates a ResponseInfo for testing purposes.
func TestingResponseInfo(
	isRead bool, readCount, readBytes int64, networkCost NetworkCost,
) ResponseInfo {
	return ResponseInfo{
		isRead:      isRead,
		readCount:   readCount,
		readBytes:   readBytes,
		networkCost: networkCost,
	}
}
