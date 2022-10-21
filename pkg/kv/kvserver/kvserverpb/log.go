// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserverpb

import "github.com/cockroachdb/cockroach/pkg/clusterversion"

func init() {
	// In the real world, when the field below was deprecated, we'd have
	// introduced something like clusterversion.NoReferenceToUsingAppliedStateKey
	// and would refer to that instead, but this is only a demonstration.
	clusterversion.Refer(clusterversion.Start22_2, ReplicaState{}.DeprecatedUsingAppliedStateKey)
}

// RangeLogEventReason specifies the reason why a range-log event happened.
type RangeLogEventReason string

// The set of possible reasons for range events to happen.
const (
	ReasonUnknown              RangeLogEventReason = ""
	ReasonRangeUnderReplicated RangeLogEventReason = "range under-replicated"
	ReasonRangeOverReplicated  RangeLogEventReason = "range over-replicated"
	ReasonStoreDead            RangeLogEventReason = "store dead"
	ReasonStoreDecommissioning RangeLogEventReason = "store decommissioning"
	ReasonRebalance            RangeLogEventReason = "rebalance"
	ReasonAdminRequest         RangeLogEventReason = "admin request"
	ReasonAbandonedLearner     RangeLogEventReason = "abandoned learner replica"
	ReasonUnsafeRecovery       RangeLogEventReason = "unsafe loss of quorum recovery"
)
