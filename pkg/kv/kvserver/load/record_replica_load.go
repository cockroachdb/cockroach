// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// RecordBatchRequests records the value for number of batch requests at the
// current time against the gateway nodeID.
func (rl *ReplicaLoad) RecordBatchRequests(val float64, nodeID roachpb.NodeID) {
	rl.record(Queries, val, nodeID)
}

// RecordRequests records the value given for requests.
func (rl *ReplicaLoad) RecordRequests(val float64) {
	rl.record(Requests, val, 0 /* nodeID */)
}

// RecordWriteKeys records the value given for write keys.
func (rl *ReplicaLoad) RecordWriteKeys(val float64) {
	rl.record(WriteKeys, val, 0 /* nodeID */)
}

// RecordReadKeys records the value given for read keys.
func (rl *ReplicaLoad) RecordReadKeys(val float64) {
	rl.record(ReadKeys, val, 0 /* nodeID */)
}

// RecordWriteBytes records the value given for write bytes.
func (rl *ReplicaLoad) RecordWriteBytes(val float64) {
	rl.record(WriteBytes, val, 0 /* nodeID */)
}

// RecordReadBytes records the value given for read bytes.
func (rl *ReplicaLoad) RecordReadBytes(val float64) {
	rl.record(ReadBytes, val, 0 /* nodeID */)
}

// RecordRaftCPUNanos records the value given for raft cpu nanos.
func (rl *ReplicaLoad) RecordRaftCPUNanos(val float64) {
	rl.record(RaftCPUNanos, val, 0 /* nodeID */)
}

// RecordReqCPUNanos records the value given for request cpu nanos.
func (rl *ReplicaLoad) RecordReqCPUNanos(val float64) {
	rl.record(ReqCPUNanos, val, 0 /* nodeID */)
}
