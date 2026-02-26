// Copyright 2026 The Cockroach Authors.
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
	// WorkEventType categorizes the work by resource type (e.g., "CPU",
	// "NETWORK", "LOCK").
	WorkEventType WorkEventType
	// WorkEvent is a more specific identifier for the work (e.g.,
	// "DistSenderLocal").
	WorkEvent string
	// GoroutineID is the ID of the goroutine that was sampled.
	GoroutineID int64
}
