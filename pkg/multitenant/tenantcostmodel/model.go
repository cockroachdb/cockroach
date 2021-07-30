// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcostmodel

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// RU stands for "Request Unit(s)"; the tenant cost model maps tenant activity
// into this abstract unit.
//
// To get an idea of the magnitude of an RU, the cost model was designed so that
// using one CPU for a second costs 1000 RUs.
type RU float64

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
//  - KV "read" and "write" operations. KV operations that read or write data
//    have a base cost and a per-byte cost. Specifically, the cost of a read is:
//      RUs = ReadRequest + <size of read in bytes> * ReadByte
//    The cost of a write is:
//      RUs = WriteRequst + <size of write in bytes> * WriteByte
//
//  - CPU usage on the tenant's SQL pods.
//
type Config struct {
	// KVReadRequest is the baseline cost of a KV read.
	KVReadRequest RU

	// KVReadByte is the per-byte cost of a KV read.
	KVReadByte RU

	// KVWriteRequest is the baseline cost of a KV write.
	KVWriteRequest RU

	// KVWriteByte is the per-byte cost of a KV write.
	KVWriteByte RU

	// PodCPUSecond is the cost of using a CPU second on the SQL pod.
	PodCPUSecond RU
}

// KVReadCost calculates the cost of a KV read operation.
func (c *Config) KVReadCost(bytes int64) RU {
	return c.KVReadRequest + RU(bytes)*c.KVReadByte
}

// KVWriteCost calculates the cost of a KV write operation.
func (c *Config) KVWriteCost(bytes int64) RU {
	return c.KVWriteRequest + RU(bytes)*c.KVWriteByte
}

// RequestCost returns the portion of the cost that can be calculated upfront:
// the per-request cost (for both reads and writes) and the per-byte write cost.
func (c *Config) RequestCost(bri RequestInfo) RU {
	if isWrite, writeBytes := bri.IsWrite(); isWrite {
		return c.KVWriteCost(writeBytes)
	}
	return c.KVReadRequest
}

// ResponseCost returns the portion of the cost that can only be calculated
// after-the-fact: the per-byte read cost.
func (c *Config) ResponseCost(bri ResponseInfo) RU {
	return RU(bri.ReadBytes()) * c.KVReadByte
}

// RequestInfo captures the request information that is used (together with
// the cost model) to determine the portion of the cost that can be calculated
// upfront. Specifically: whether it is a read or a write and the write size (if
// it's a write).
type RequestInfo struct {
	// writeBytes is the write size if the request is a write, or -1 if it is a read.
	writeBytes int64
}

// MakeRequestInfo extracts the relevant information from a BatchRequest.
func MakeRequestInfo(ba *roachpb.BatchRequest) RequestInfo {
	if !ba.IsWrite() {
		return RequestInfo{writeBytes: -1}
	}

	var writeBytes int64
	for i := range ba.Requests {
		if swr, isSizedWrite := ba.Requests[i].GetInner().(roachpb.SizedWriteRequest); isSizedWrite {
			writeBytes += swr.WriteBytes()
		}
	}
	return RequestInfo{writeBytes: writeBytes}
}

// IsWrite returns whether the request is a write, and if so the write size in
// bytes.
func (bri RequestInfo) IsWrite() (isWrite bool, writeBytes int64) {
	if bri.writeBytes == -1 {
		return false, 0
	}
	return true, bri.writeBytes
}

// TestingRequestInfo creates a RequestInfo for testing purposes.
func TestingRequestInfo(isWrite bool, writeBytes int64) RequestInfo {
	if !isWrite {
		return RequestInfo{writeBytes: -1}
	}
	return RequestInfo{writeBytes: writeBytes}
}

// ResponseInfo captures the BatchResponse information that is used (together
// with the cost model) to determine the portion of the cost that can only be
// calculated after-the-fact. Specifically: the read size (if the request is a
// read).
type ResponseInfo struct {
	readBytes int64
}

// MakeResponseInfo extracts the relevant information from a BatchResponse.
func MakeResponseInfo(br *roachpb.BatchResponse) ResponseInfo {
	var readBytes int64
	for _, ru := range br.Responses {
		readBytes += ru.GetInner().Header().NumBytes
	}
	return ResponseInfo{readBytes: readBytes}
}

// ReadBytes returns the bytes read, or 0 if the request was a write.
func (bri ResponseInfo) ReadBytes() int64 {
	return bri.readBytes
}

// TestingResponseInfo creates a ResponseInfo for testing purposes.
func TestingResponseInfo(readBytes int64) ResponseInfo {
	return ResponseInfo{readBytes: readBytes}
}
