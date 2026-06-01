// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/obs/workloadid"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// WorkEventType categorizes the type of work event observed during ASH sampling.
type WorkEventType int

const (
	WorkUnknown   WorkEventType = iota // 0
	WorkCPU                            // 1
	WorkAdmission                      // 2
	WorkIO                             // 3
	WorkNetwork                        // 4
	WorkLock                           // 5
	WorkOther                          // 6
)

// String returns the string representation of a WorkEventType.
func (w WorkEventType) String() string {
	switch w {
	case WorkCPU:
		return "CPU"
	case WorkAdmission:
		return "ADMISSION"
	case WorkIO:
		return "IO"
	case WorkNetwork:
		return "NETWORK"
	case WorkLock:
		return "LOCK"
	case WorkOther:
		return "OTHER"
	default:
		return "UNKNOWN"
	}
}

// WorkloadInfo groups the fields that identify the workload producing
// a work state.
type WorkloadInfo struct {
	// WorkloadID identifies the workload (e.g., statement fingerprint ID).
	WorkloadID uint64
	// EnrichmentID is a unique identifier for the enrichment data
	// associated with this workload execution. The ASH sampler uses
	// this to look up attributes like app_name, user, database,
	// session_id, txn_id, and plan_hash from the enrichment cache.
	EnrichmentID uint64
	// GatewayNodeID is the node that initiated the workload.
	GatewayNodeID roachpb.NodeID
	// WorkloadType distinguishes the kind of workload that WorkloadID
	// represents, controlling how the sampler encodes the ID.
	WorkloadType workloadid.WorkloadType
}

// ASHSample represents a single Active Session History sample.
type ASHSample struct {
	// SampleTime is when this sample was taken.
	SampleTime time.Time
	// NodeID is the node where this sample was captured.
	NodeID roachpb.NodeID
	// TenantID identifies which tenant this sample belongs to. Since the
	// sampler is a process-wide singleton, samples from all tenants are
	// mixed in a single ring buffer. The tenant ID enables crdb_internal
	// virtual tables and the ListLocalActiveSessionHistory RPC to filter
	// samples to the requesting tenant.
	TenantID roachpb.TenantID
	// WorkloadID identifies the workload (e.g., statement fingerprint).
	WorkloadID string
	// WorkloadType distinguishes the kind of workload (e.g., "STATEMENT",
	// "JOB", "SYSTEM", "UNKNOWN").
	WorkloadType string
	// AppName is the application name string.
	AppName string
	// User is the SQL user that initiated the workload.
	User string
	// Database is the current database context.
	Database string
	// SessionID is the cluster-unique session identifier.
	SessionID string
	// TxnID is the transaction UUID.
	TxnID string
	// PlanHash is the hash of the query plan gist.
	PlanHash uint64
	// WorkEventType categorizes the work by resource type (e.g., "CPU",
	// "NETWORK", "LOCK").
	WorkEventType WorkEventType
	// WorkEvent is a more specific identifier for the work (e.g.,
	// "DistSenderLocal").
	WorkEvent string
	// GoroutineID is the ID of the goroutine that was sampled.
	GoroutineID int64
}
