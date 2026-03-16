// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workloadid

// This package defines CRDB-wide workload identifiers that we can
// define and use to attribute observability data.
//
// We will prefer using uint64 workload identifiers because those are
// cheaper to move around the system and can help us avoid allocation
// in hot paths.
//
// String-based identifiers are useful when we want to display the
// identifier to a human, or to label go profiles and execution traces,
// which only accept string values for tags.
//
// Currently, this definition is quite simple consisting only of a
// preset list of static identifiers below that can be used numerically
// or using static strings. Statement Fingerprint IDs are the only
// example here of a dynamic ID that can have high cardinality.
//
// It is expected that future work will expand the structure and
// expressivity of the workloadID. No ID stability is guaranteed.
// While we currently do persist statement fingerprint IDs in the SQL
// Activity tables, we advise users of these values to AVOID PERSISTING
// WORKLOAD IDs until they are stable.

const ProfileTag = "workload.id"

type WorkloadID uint64

// The IDs and Names below are currently not persisted anywhere and
// are subject to change.

const (
	WORKLOAD_ID_UNKNOWN WorkloadID = iota
	WORKLOAD_ID_LDR
	WORKLOAD_ID_RAFT
	WORKLOAD_ID_STORELIVENESS
	WORKLOAD_ID_RPC_HEARTBEAT
)

// Name returns the human-readable name for a system task WorkloadID.
// For IDs that don't correspond to a known system task, it returns
// "UNKNOWN".
func (id WorkloadID) Name() string {
	switch id {
	case WORKLOAD_ID_LDR:
		return WORKLOAD_NAME_LDR
	case WORKLOAD_ID_RAFT:
		return WORKLOAD_NAME_RAFT
	case WORKLOAD_ID_STORELIVENESS:
		return WORKLOAD_NAME_STORELIVENESS
	case WORKLOAD_ID_RPC_HEARTBEAT:
		return WORKLOAD_NAME_RPC_HEARTBEAT
	default:
		return WORKLOAD_NAME_UNKNOWN
	}
}

const (
	WORKLOAD_NAME_UNKNOWN       = "UNKNOWN"
	WORKLOAD_NAME_LDR           = "LDR"
	WORKLOAD_NAME_RAFT          = "RAFT"
	WORKLOAD_NAME_STORELIVENESS = "STORELIVENESS"
	WORKLOAD_NAME_RPC_HEARTBEAT = "RPC_HEARTBEAT"
)

// WorkloadType distinguishes the kind of workload that a WorkloadID
// represents. The sampler uses this to choose the right encoding
// (hex for statement fingerprints, decimal for job IDs, name for
// system tasks).
type WorkloadType uint8

const (
	// WorkloadTypeUnknown is the zero value. Backward compatible:
	// existing callers that don't set a type get hex encoding.
	WorkloadTypeUnknown WorkloadType = iota
	// WorkloadTypeStatement identifies a SQL statement fingerprint ID,
	// hex-encoded by the sampler.
	WorkloadTypeStatement
	// WorkloadTypeJob identifies a job ID, decimal-encoded by the
	// sampler.
	WorkloadTypeJob
	// WorkloadTypeSystem identifies a system task, encoded using
	// WorkloadID.Name() by the sampler.
	WorkloadTypeSystem
)

// ToUint32 converts a WorkloadType to uint32 for use in protobuf
// fields.
func (t WorkloadType) ToUint32() uint32 {
	return uint32(t)
}

// String returns the human-readable name for a WorkloadType.
func (t WorkloadType) String() string {
	switch t {
	case WorkloadTypeStatement:
		return "STATEMENT"
	case WorkloadTypeJob:
		return "JOB"
	case WorkloadTypeSystem:
		return "SYSTEM"
	default:
		return "UNKNOWN"
	}
}
