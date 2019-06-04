// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package storagepb

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
)
