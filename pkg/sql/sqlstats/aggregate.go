// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlstats

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/eventagg"
	"github.com/cockroachdb/redact"
)

// MergeableStmtStat represents statistics for a given statement fingerprint ID.
// It implements the Mergeable[*MergeableStmtStat] interface.
//
// It provides examples of struct tags that could be used to leverage code-generation
// to reduce the amount of work required to create a new type that implements the
// Mergeable interface.
//
// We can easily imagine merge functions like sum, avg, min, max, etc.
// Within the struct definition, we also note some more interesting & challenging
// examples which require more thinking.
type MergeableStmtStat struct {
	// StmtFingerprintID is the fingerprint ID for the statement statistics.
	// NB: The `merge_key:"include"` annotation marks this field to become
	// part of a composite key (in this example, it is the sole member of the key),
	// which will be used during code generation for the Key() function's
	// implementation.
	//
	// Fields making use of this annotation must implement the Stringer interface.
	StmtFingerprintID appstatspb.StmtFingerprintID `merge_key:"include"`
	// Statement is the redactable statement string.
	Statement redact.RedactableString
	// ExecCount is the count of total executions for the statement.
	// NB: The `merge_fn:"sum"` annotation marks the aggregation function to use
	// for this field, which will be used during code generation for the Merge()
	// function's implementation.
	ExecCount int `merge_fn:"sum"`

	// TODO(abarganier): The below fields represent interesting aggregation types to consider
	// with respect to code generation.
	//
	// SQLInstanceIDs    []int32 `merge_fn:"set_merge"`
	// ExecTime          uint64  `merge_fn:"histogram_record"`
	//
	// Can we find a way to use code generation to turn the 'set_merge' annotation into a
	// map[int32]struct{} field, where the generated code merges the SQLInstanceIDs of the
	// incoming event into the aggregate set of SQLInstanceIDs?
	//
	// Can we find a way to use code generation to turn the 'histogram_record' annotation
	// into a metric.Histogram field, where the generated code records the singular ExecTime
	// into the aggregate ExecTime histogram?
	//
	// We could also simply build the event with these types from the start, where you'd merge
	// the two histograms. However, in the case of the singular incoming event, having to
	// construct a histogram for a single value feels heavy.
}

var _ eventagg.Mergeable[*MergeableStmtStat] = (*MergeableStmtStat)(nil)

// Merge implements the Mergeable interface.
// TODO(abarganier): We imagine this function as being generated code, using
// struct tags (more comments about this in the MergeableStmtStat struct definition).
func (m *MergeableStmtStat) Merge(other *MergeableStmtStat) {
	m.ExecCount += other.ExecCount
}

// Key implements the Mergeable interface.
// TODO(abarganier): We imagine this function as being generated code, using struct
// tags to determine which fields should be included in the composite key. (more
// comments about this in the MergeableStmtStat struct definition)
func (m *MergeableStmtStat) Key() string {
	return fmt.Sprintf("%s", m.StmtFingerprintID)
}

// NewStmtStatsAggregator leverages the generic MapReduceAggregator to instantiate
// and return a new aggregator for MergeableStmtStat's..
//
// It is an example of a system that'd use the eventagg library to easily define aggregation
// rules/criteria, and how to process those results.
func NewStmtStatsAggregator() *eventagg.MapReduceAggregator[*MergeableStmtStat] {
	return eventagg.NewMapReduceAggregator[*MergeableStmtStat](
		"stmt-stats",
		eventagg.NewLogWriteConsumer[*MergeableStmtStat](), // We'd like to log all the aggregated results, as-is.
		eventagg.NewTopKFlushConsumer[*MergeableStmtStat]( // Also, compute topK by exec count for the aggregation window.
			"top-100-exec-count",
			100, /* k */
			func(e *MergeableStmtStat) int {
				return e.ExecCount
			}, /* priorityFn */
			func(pq heap.Interface[*MergeableStmtStat]) {
				for pq.Len() > 0 {
					// e := pq.Pop()
					// Do something with e, either log it, or process it further.
				}
			}, /* consumeFn - ideally, this would be another plugin */
		))
}
