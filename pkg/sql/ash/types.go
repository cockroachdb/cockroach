// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// WorkEventType categorizes the type of work event observed during ASH sampling.
type WorkEventType int

const (
	WORK_CPU WorkEventType = iota
	WORK_ADMISSION
	WORK_IO
	WORK_NETWORK
	WORK_LOCK
	WORK_OTHER
	WORK_UNKNOWN
)

// SystemWorkloadID represents a well-known system workload identifier for ASH sampling.
// These are used when work cannot be attributed to a specific SQL statement fingerprint.
type SystemWorkloadID uint64

const (
	// SystemWorkloadIDTxnHeartbeat identifies transaction heartbeat work.
	SystemWorkloadIDTxnHeartbeat SystemWorkloadID = iota + 1
	// SystemWorkloadIDTxnRollback identifies async transaction rollback work.
	SystemWorkloadIDTxnRollback
)

// systemWorkloadNames maps system workload IDs to their display names.
var systemWorkloadNames = map[SystemWorkloadID]string{
	SystemWorkloadIDTxnHeartbeat: "TXN_HEARTBEAT",
	SystemWorkloadIDTxnRollback:  "TXN_ROLLBACK",
}

// LookupSystemWorkloadName returns the name for a known system workload ID.
// Returns the name and true if found, or empty string and false if the ID
// is not a known system workload ID (e.g., it's a statement fingerprint ID).
func LookupSystemWorkloadName(id uint64) (string, bool) {
	name, ok := systemWorkloadNames[SystemWorkloadID(id)]
	return name, ok
}

// String returns the string representation of a WorkEventType.
func (w WorkEventType) String() string {
	switch w {
	case WORK_CPU:
		return "CPU"
	case WORK_ADMISSION:
		return "ADMISSION"
	case WORK_IO:
		return "IO"
	case WORK_NETWORK:
		return "NETWORK"
	case WORK_LOCK:
		return "LOCK"
	case WORK_OTHER:
		return "OTHER"
	default:
		return "UNKNOWN"
	}
}

// ASHSample represents a single Active Session History sample.
type ASHSample struct {
	// SampleTime is when this sample was taken.
	SampleTime time.Time
	// NodeID is the node where this sample was captured.
	NodeID roachpb.NodeID
	// WorkloadID identifies the workload (e.g., statement fingerprint).
	WorkloadID string
	// WorkEventType categorizes the work by resource type (e.g., "CPU", "NETWORK", "LOCK").
	WorkEventType WorkEventType
	// WorkEvent is a more specific identifier for the work (e.g., "dist_sender_batch").
	WorkEvent string
	// GoroutineID is the ID of the goroutine that was sampled.
	GoroutineID int64
	// AppName is the application name of the SQL session that executed the query.
	// Empty for work that doesn't come from SQL sessions.
	AppName string
	// GatewayNodeID is the node ID of the gateway node that initiated the query.
	// Zero for work that doesn't come from SQL sessions.
	GatewayNodeID roachpb.NodeID
}
