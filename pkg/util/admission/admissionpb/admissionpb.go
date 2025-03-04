// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admissionpb

import (
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/proto"
	"go.opentelemetry.io/otel/attribute"
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
	// BulkLowPri is low priority work from internal bulk submissions.
	BulkLowPri WorkPriority = -100
	// UserLowPri is low priority work from user submissions (SQL).
	UserLowPri WorkPriority = -50
	// BulkNormalPri is bulk priority work from bulk jobs, which could be run due
	// to user submissions or be automatic.
	BulkNormalPri WorkPriority = -30
	// NormalPri is normal priority work.
	NormalPri WorkPriority = 0
	// LockingNormalPri is used for user normal priority transactions that are
	// acquiring locks.
	LockingNormalPri WorkPriority = 10
	// UserHighPri is high priority work from user submissions (SQL).
	UserHighPri WorkPriority = 50
	// LockingUserHighPri is for user high priority transactions that are
	// acquiring locks.
	LockingUserHighPri WorkPriority = 100
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
		p.SafeString(redact.SafeString(s))
		return
	}
	p.Printf("custom-pri=%d", int8(w))
}

// WorkPriorityDict is a mapping of the priorities to a short string name. The
// name is used as the suffix on exported work queue metrics.
var WorkPriorityDict = map[WorkPriority]string{
	LowPri:           "low-pri",
	BulkLowPri:       "bulk-low-pri",
	UserLowPri:       "user-low-pri",
	BulkNormalPri:    "bulk-normal-pri",
	NormalPri:        "normal-pri",
	LockingNormalPri: "locking-normal-pri",
	UserHighPri:      "user-high-pri",
	// This ought to be called "locking-user-high-pri", but we retain the old
	// name for continuity with metrics in older versions.
	LockingUserHighPri: "locking-pri",
	HighPri:            "high-pri",
}

// workPriorityToLockPriMap maps WorkPriority to another WorkPriority for when
// the txn has already acquired a lock. Since WorkPriority can be negative,
// and this map is an array, the index into the array is priToArrayIndex(p)
// where p is a WorkPriority.
//
// The priority mapping is not simply p+1 since the enum values are used in
// exported metrics, and we don't want to increase the number of such metrics.
var workPriorityToLockPriMap [math.MaxInt8 - math.MinInt8 + 1]WorkPriority

func priToArrayIndex(pri WorkPriority) int {
	return int(pri) - math.MinInt8
}

// TestingReverseWorkPriorityDict is the reverse-lookup dictionary for
// WorkPriorityDict, for use in tests.
var TestingReverseWorkPriorityDict map[string]WorkPriority

func init() {
	TestingReverseWorkPriorityDict = make(map[string]WorkPriority)
	for k, v := range WorkPriorityDict {
		TestingReverseWorkPriorityDict[v] = k
	}

	orderedPris := []WorkPriority{
		LowPri,
		BulkLowPri,
		UserLowPri,
		BulkNormalPri,
		NormalPri,
		LockingNormalPri,
		UserHighPri,
		LockingUserHighPri,
		HighPri,
	}
	j := 0
	for i := range workPriorityToLockPriMap {
		pri := WorkPriority(i) + math.MinInt8
		if pri == orderedPris[j] && i != len(workPriorityToLockPriMap)-1 {
			// Move to the next higher priority.
			j++
		}
		workPriorityToLockPriMap[i] = orderedPris[j]
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
//
// For example, UserLowPri should map to BulkNormalPri (see the hack below),
// NormalPri maps to LockingNormalPri, and UserHighPri maps to
// LockingUserHighPri. Say users are running at these different priorities in
// different parts of the key space, say key-low, key-normal, key-high, then
// even after the mapping, a txn holding locks (or resolving intents) in
// key-low will have lower priority (BulkNormalPri) than the non-adjusted
// priority in key-normal (NormalPri). The same holds true for txn holding
// locks in key-normal, since LockingNormalPri is lower priority than
// UserHighPri.
//
// Adjusting the priority can also be beneficial when all txns have the same
// QoS requirements, but there is lock contention. In tpcc with 3000
// warehouses, it halved the number of lock waiters, and increased the
// transaction throughput by 10+%. In that experiment 40% of the BatchRequests
// evaluated by KV had been assigned a higher priority due to locking.
func AdjustedPriorityWhenHoldingLocks(pri WorkPriority) WorkPriority {
	// TODO(sumeer): this is a temporary hack since index backfill and TTL can
	// be running on tables that have user-facing work. We want these background
	// transactions (when holding locks) to run at the priority of user-facing
	// work + 1, but we don't know what that value is. This could be solved by
	// providing the user-facing work priority in a SpanConfig, but for now we
	// just assume that all user-facing work is running at NormalPri. See the
	// examples in intentresolver/admission.go.
	if pri < NormalPri {
		pri = NormalPri
	}
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

// StoreWorkType represents the type of work,
type StoreWorkType int8

const (
	// RegularStoreWorkType is for type of store-specific work that corresponds to
	// RegularWorkClass.
	RegularStoreWorkType StoreWorkType = iota
	// SnapshotIngestStoreWorkType is for snapshot work type. It is classified as
	// ElasticWorkClass, but is prioritized higher than other work of that class.
	SnapshotIngestStoreWorkType = 1
	// ElasticStoreWorkType is for store-specific work that corresponds to
	// ElasticWorkClass, excluding SnapshotIngestStoreWorkType.
	ElasticStoreWorkType = 2
	// NumStoreWorkTypes is the number of store work types.
	NumStoreWorkTypes = 3
)

// WorkClassFromStoreWorkType translates StoreWorkType to a WorkClass
func WorkClassFromStoreWorkType(workType StoreWorkType) WorkClass {
	var class WorkClass
	switch workType {
	case RegularStoreWorkType:
		class = RegularWorkClass
	case ElasticStoreWorkType:
		class = ElasticWorkClass
	case SnapshotIngestStoreWorkType:
		class = ElasticWorkClass
	}
	return class
}

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
		p.SafeString("regular")
	case ElasticWorkClass:
		p.SafeString("elastic")
	default:
		p.SafeString("<unknown-class>")
	}
}

var _ tracing.AggregatorEvent = &AdmissionWorkQueueStats{}

// Identity implements the tracing.AggregatorEvent interface.
func (s *AdmissionWorkQueueStats) Identity() tracing.AggregatorEvent {
	return &AdmissionWorkQueueStats{WorkPriority: int32(HighPri)}
}

// Combine implements the tracing.AggregatorEvent interface.
func (s *AdmissionWorkQueueStats) Combine(other tracing.AggregatorEvent) {
	otherStats, ok := other.(*AdmissionWorkQueueStats)
	if !ok {
		panic(errors.Newf("`other` is not of type AdmissionWorkQueueStats: %T", other))
	}
	s.WaitDurationNanos += otherStats.WaitDurationNanos
	s.DeadlineExceededCount += otherStats.DeadlineExceededCount
	s.WorkPriority = min(s.WorkPriority, otherStats.WorkPriority)

	if s.QueueKind == "" {
		s.QueueKind = otherStats.QueueKind
	} else if s.QueueKind != otherStats.QueueKind {
		s.QueueKind = "multiple-queues"
		// TODO(dt): consider adding a map of queue kinds to counts, e.g.:
		/*
			if s.Agg == nil {
				s.Agg = make(map[string]*AdmissionWorkQueueStats)
				s.Agg[otherStats.QueueKind] = otherStats
			} else if perQueue, ok := s.Agg[otherStats.QueueKind]; !ok {
				s.Agg[otherStats.QueueKind] = otherStats
			} else {
				perQueue.Combine(otherStats)
			}
		*/
	}
}

// ProtoName implements the tracing.AggregatorEvent interface.
func (s *AdmissionWorkQueueStats) ProtoName() string {
	return proto.MessageName(s)
}

func (s *AdmissionWorkQueueStats) ToText() []byte {
	return []byte(s.String())
}

func (s *AdmissionWorkQueueStats) String() string {
	return fmt.Sprintf("queue (%s/%s) wait: %s",
		redact.SafeString(s.QueueKind),
		WorkPriority(s.WorkPriority),
		humanizeutil.Duration(s.WaitDurationNanos),
	)

	// TODO(dt): consider supporting a map over multiple queues when aggregating
	// into a single stat, e.g. treat the above as a fast-path if the map is nil
	// but otherwise do something like this:
	/*
		var b strings.Builder
		fmt.Fprintf(&b, "queue (%s/%s) wait: %s (",
			redact.SafeString(s.QueueKind),
			WorkPriority(s.WorkPriority),
			humanizeutil.Duration(s.WaitDurationNanos),
		)

		for _, v := range s.Agg {
			fmt.Fprintf(&b, "%s[%s]: %s;",
				redact.SafeString(v.QueueKind),
				WorkPriority(s.WorkPriority),
				humanizeutil.Duration(v.WaitDurationNanos),
			)
		}
		b.WriteString(")")
		return b.String()
	*/
}

// Render implements the AggregatorEvent interface.
func (s *AdmissionWorkQueueStats) Render() []attribute.KeyValue {
	return []attribute.KeyValue{
		{Key: "queue_wait", Value: attribute.StringValue(string(humanizeutil.Duration(s.WaitDurationNanos)))},
		{Key: "queue_kind", Value: attribute.StringValue(s.QueueKind)},
		{Key: "queue_priority", Value: attribute.StringValue(WorkPriority(s.WorkPriority).String())},
	}
}

// Prevent the linter from emitting unused warnings.
var _ = LowPri
var _ = BulkLowPri
var _ = UserLowPri
var _ = NormalPri
var _ = UserHighPri
var _ = LockingUserHighPri
var _ = HighPri
