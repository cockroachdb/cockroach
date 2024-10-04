// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package uncertainty

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

// Interval represents a transaction's uncertainty interval.
//
// The GlobalLimit is the transaction's initial upper bound on the interval,
// which is computed by its coordinator when the transaction is initiated as the
// current local HLC time plus the maximum clock offset between any two nodes in
// the system. Assuming this maximum clock skew bound holds, this represents an
// upper bound on the HLC time of any node in the system at the instant that the
// transaction began.
//
// The LocalLimit is an optional tighter bound established through HLC clock
// observations on individual nodes in the system. These clock observations
// ("observed timestamps") can only be used to lower the local limit of an
// uncertainty interval when a transaction evaluates on the node from which the
// observation was taken.
//
// The local limit can reduce the uncertainty interval applied to most values on
// a range. This can lead to values that would otherwise be considered uncertain
// by the original global limit to be considered "certainly concurrent", and
// thus not causally related, with the transaction due to observed timestamps.
// However, the local limit does not operate on a value's version timestamp. It
// instead applies to a value's local timestamp, which is a recording of the
// local HLC clock on the leaseholder that originally wrote the value.
//
// Uncertainty intervals also apply to non-transactional requests that require
// strong consistency (single-key linearizability). These requests defer their
// timestamp allocation to the leaseholder of their (single) range. They then
// establish an uncertainty interval and perform any uncertainty restarts on the
// server.
//
// NOTE: LocalLimit can be empty if no observed timestamp has been captured on
// the local node. However, if it is set, it must be <= GlobalLimit.
type Interval struct {
	GlobalLimit hlc.Timestamp
	LocalLimit  hlc.ClockTimestamp
}

// IsUncertain determines whether a value with the provided version and local
// timestamps is uncertain to a reader with a ReadTimestamp below the value's
// version timestamp and with the specified uncertainty interval.
func (in Interval) IsUncertain(valueTs hlc.Timestamp, localTs hlc.ClockTimestamp) bool {
	if valueTs.IsEmpty() {
		panic("unexpected empty value timestamp")
	}
	if localTs.IsEmpty() {
		panic("unexpected empty local timestamp")
	}
	if !in.LocalLimit.IsEmpty() && in.LocalLimit.Less(localTs) {
		// The reader has an observed timestamp that precedes the local timestamp of
		// this value. There is no uncertainty as the reader transaction must have
		// started before the writer transaction completed, so they are concurrent.
		return false
	}
	return valueTs.LessEq(in.GlobalLimit)
}
