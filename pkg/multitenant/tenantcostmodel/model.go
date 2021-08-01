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
