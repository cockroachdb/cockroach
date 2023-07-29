// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package decommissioning

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
)

// RangeCheckResult is the result of evaluating the allocator action
// and target for a single range that has an extant replica on a node targeted
// for decommission.
type RangeCheckResult struct {
	Desc         roachpb.RangeDescriptor
	Action       string
	TracingSpans tracingpb.Recording
	Err          error
}

// PreCheckResult is the result of checking the readiness
// of a node or set of nodes to be decommissioned.
type PreCheckResult struct {
	RangesChecked  int
	ReplicasByNode map[roachpb.NodeID][]roachpb.ReplicaIdent
	ActionCounts   map[string]int
	RangesNotReady []RangeCheckResult
}
