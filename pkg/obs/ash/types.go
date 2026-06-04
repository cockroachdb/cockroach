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
	// AppNameID is the hash of the application name. Set when the
	// workload is from SQL execution.
	// Deprecated in favor of EnrichmentID once 26.3 is finalized;
	// retained for mixed-version compatibility.
	AppNameID uint64
	// GatewayNodeID is the node that initiated the workload.
	GatewayNodeID roachpb.NodeID
	// WorkloadType distinguishes the kind of workload that WorkloadID
	// represents, controlling how the sampler encodes the ID.
	WorkloadType workloadid.WorkloadType
	// EnrichmentID is the 64-bit short hash (clusterunique.ID.ShortID)
	// of the execution whose attributes were cached on the gateway
	// node. Zero means "unset" — the value is a hash and the
	// probability of a real ShortID hashing to zero is 2^-64. The
	// sampler stamps it onto each sample so downstream enrichment can
	// resolve attributes via the local cache or the
	// GetASHEnrichmentData RPC.
	EnrichmentID uint64
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
	// AppName is the application name string. Set when the workload is from
	// SQL execution.
	AppName string
	// WorkEventType categorizes the work by resource type (e.g., "CPU",
	// "NETWORK", "LOCK").
	WorkEventType WorkEventType
	// WorkEvent is a more specific identifier for the work (e.g.,
	// "DistSenderLocal").
	WorkEvent string
	// GoroutineID is the ID of the goroutine that was sampled.
	GoroutineID int64
}
