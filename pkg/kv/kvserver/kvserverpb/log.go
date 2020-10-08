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
)
