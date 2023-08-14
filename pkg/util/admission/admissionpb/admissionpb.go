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

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// WorkPriority represents the priority of work. In an WorkQueue, it is only
// used for ordering within a tenant. High priority work can starve lower
// priority work.
type WorkPriority int8

// When adding to this list, remember to update the initialization logic of
// workPriorityToLockPriMap.
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
	p.Printf("custom-pri=%d", int8(w))
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

// workPriorityToLockPriMap maps WorkPriority to another WorkPriority for when
// the txn has already acquired a lock. Since WorkPriority can be negative,
// and this map is an array, the index into the array is priToArrayIndex(p)
// where p is a WorkPriority.
//
// The priority mapping is not simply p+1 since the enum values are used in
// exported metrics, and we don't want to increase the number of such metrics.
var workPriorityToLockPriMap [math.MaxInt8 - math.MinInt8 + 1]WorkPriority

// TestingReverseWorkPriorityDict is the reverse-lookup dictionary for
// WorkPriorityDict, for use in tests.
var TestingReverseWorkPriorityDict map[string]WorkPriority

func priToArrayIndex(pri WorkPriority) int {
	return int(pri) - math.MinInt8
}

func init() {
	TestingReverseWorkPriorityDict = make(map[string]WorkPriority)
	for k, v := range WorkPriorityDict {
		TestingReverseWorkPriorityDict[v] = k
	}

	for i := 0; i < priToArrayIndex(TTLLowPri); i++ {
		workPriorityToLockPriMap[i] = TTLLowPri
	}
	for i := priToArrayIndex(TTLLowPri); i < priToArrayIndex(UserLowPri); i++ {
		workPriorityToLockPriMap[i] = UserLowPri
	}
	for i := priToArrayIndex(UserLowPri); i < priToArrayIndex(BulkNormalPri); i++ {
		workPriorityToLockPriMap[i] = BulkNormalPri
	}
	for i := priToArrayIndex(BulkNormalPri); i < priToArrayIndex(NormalPri); i++ {
		workPriorityToLockPriMap[i] = NormalPri
	}
	for i := priToArrayIndex(NormalPri); i < priToArrayIndex(UserHighPri); i++ {
		workPriorityToLockPriMap[i] = UserHighPri
	}
	for i := priToArrayIndex(UserHighPri); i < priToArrayIndex(LockingPri); i++ {
		workPriorityToLockPriMap[i] = LockingPri
	}
	for i := priToArrayIndex(LockingPri); i <= priToArrayIndex(HighPri); i++ {
		workPriorityToLockPriMap[i] = HighPri
	}
	for i := range workPriorityToLockPriMap {
		if priToArrayIndex(workPriorityToLockPriMap[i]) < i {
			panic(errors.AssertionFailedf("workPriorityToLockPriMap at index %d has value %d",
				i, workPriorityToLockPriMap[i]))
		}
		if priToArrayIndex(workPriorityToLockPriMap[i]) == i && i != priToArrayIndex(math.MaxInt8) {
			panic(errors.AssertionFailedf(
				"workPriorityToLockPriMap at index %d has no change for locking", i))
		}
	}
}

// AdjustedPriorityWhenHoldingLocks takes the original priority of a
// transaction and updates it under the knowledge that the transaction is
// holding locks.
//
// This broader context of locking is technically not in scope of the
// admission package, but we define this function here as the WorkPriority
// enum values are defined here.
func AdjustedPriorityWhenHoldingLocks(pri WorkPriority) WorkPriority {
	return workPriorityToLockPriMap[priToArrayIndex(pri)]
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
