// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserverpb

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
