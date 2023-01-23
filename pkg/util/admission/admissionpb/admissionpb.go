// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admissionpb

import (
	"math"

	"github.com/cockroachdb/redact"
)

// WorkPriority represents the priority of work. In an WorkQueue, it is only
// used for ordering within a tenant. High priority work can starve lower
// priority work.
type WorkPriority int8

const (
	// LowPri is low priority work.
	LowPri WorkPriority = math.MinInt8
	// TTLLowPri is low priority work from TTL internal submissions.
	TTLLowPri WorkPriority = -100
	// UserLowPri is low priority work from user submissions (SQL).
	UserLowPri WorkPriority = -50
	// BulkNormalPri is bulk priority work from bulk jobs, which could be run due
	// to user submissions or be automatic.
	BulkNormalPri WorkPriority = -30
	// NormalPri is normal priority work.
	NormalPri WorkPriority = 0
	// UserHighPri is high priority work from user submissions (SQL).
	UserHighPri WorkPriority = 50
	// LockingPri is for transactions that are acquiring locks.
	LockingPri WorkPriority = 100
	// HighPri is high priority work.
	HighPri WorkPriority = math.MaxInt8
	// OneAboveHighPri is one priority level above the highest priority.
	OneAboveHighPri int = int(HighPri) + 1
)

func (w WorkPriority) String() string {
	return redact.StringWithoutMarkers(w)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (w WorkPriority) SafeFormat(p redact.SafePrinter, verb rune) {
	if s, ok := WorkPriorityDict[w]; ok {
		p.Print(s)
		return
	}
	p.Printf("custom-pri=%d", w)
}

// WorkPriorityDict is a mapping of the priorities to a short string name. The
// name is used as the suffix on exported work queue metrics.
var WorkPriorityDict = map[WorkPriority]string{
	LowPri:        "low-pri",
	TTLLowPri:     "ttl-low-pri",
	UserLowPri:    "user-low-pri",
	BulkNormalPri: "bulk-normal-pri",
	NormalPri:     "normal-pri",
	UserHighPri:   "user-high-pri",
	LockingPri:    "locking-pri",
	HighPri:       "high-pri",
}

// WorkClass represents the class of work, which is defined entirely by its
// WorkPriority. Namely, everything less than NormalPri is defined to be
// "Elastic", while everything above and including NormalPri is considered
// "Regular.
type WorkClass int8

const (
	// RegularWorkClass is for work corresponding to workloads that are
	// throughput and latency sensitive.
	RegularWorkClass WorkClass = iota
	// ElasticWorkClass is for work corresponding to workloads that can handle
	// reduced throughput, possibly by taking longer to finish a workload. It is
	// not latency sensitive.
	ElasticWorkClass
	// NumWorkClasses is the number of work classes.
	NumWorkClasses
)

// WorkClassFromPri translates a WorkPriority to its given WorkClass.
func WorkClassFromPri(pri WorkPriority) WorkClass {
	class := RegularWorkClass
	if pri < NormalPri {
		class = ElasticWorkClass
	}
	return class
}

func (w WorkClass) String() string {
	return redact.StringWithoutMarkers(w)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (w WorkClass) SafeFormat(p redact.SafePrinter, verb rune) {
	switch w {
	case RegularWorkClass:
		p.Printf("regular")
	case ElasticWorkClass:
		p.Print("elastic")
	default:
		p.Print("<unknown-class>")
	}
}

// Prevent the linter from emitting unused warnings.
var _ = LowPri
var _ = TTLLowPri
var _ = UserLowPri
var _ = NormalPri
var _ = UserHighPri
var _ = LockingPri
var _ = HighPri
