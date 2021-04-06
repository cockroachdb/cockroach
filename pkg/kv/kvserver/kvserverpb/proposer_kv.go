// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserverpb

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

var maxMaxLeaseFooterSize = (&MaxLeaseFooter{
	MaxLeaseIndex: math.MaxUint64,
}).Size()

var maxClosedTimestampFooterSize = (&ClosedTimestampFooter{
	ClosedTimestamp: hlc.Timestamp{
		WallTime:  math.MaxInt64,
		Logical:   math.MaxInt32,
		Synthetic: true,
	},
}).Size()

// MaxMaxLeaseFooterSize returns the maximum possible size of an encoded
// MaxLeaseFooter proto.
func MaxMaxLeaseFooterSize() int {
	return maxMaxLeaseFooterSize
}

// MaxClosedTimestampFooterSize returns the maximmum possible size of an encoded
// ClosedTimestampFooter.
func MaxClosedTimestampFooterSize() int {
	return maxClosedTimestampFooterSize
}

// IsZero returns whether all fields are set to their zero value.
func (r ReplicatedEvalResult) IsZero() bool {
	return r == ReplicatedEvalResult{}
}

// IsLeaseRequest returns whether the command corresponds to a lease request.
func (r ReplicatedEvalResult) IsLeaseRequest() bool {
	return (r.Flags & int64(ReplicatedEvalResultFlags_LeaseRequest)) != 0
}

// SetLeaseRequest sets the respective flag on the command, indicating that this
// command corresponds to a lease request.
func (r *ReplicatedEvalResult) SetLeaseRequest() {
	r.Flags |= int64(ReplicatedEvalResultFlags_LeaseRequest)
}

// IsNonMVCC indicates that this command does not writes MVCC keys and it does
// no result in intents. Requests with this flag set can evaluate below the
// range's closed timestamp. This is the inverse of ba.IsIntentWrite(). Note
// that an EndTxn by itself sets this flag - it doesn't write any *new* intents
// and so it can evaluate below the closed timestamp. IsNonMVCC is the inverse
// of IsIntentWrite.
func (r ReplicatedEvalResult) IsNonMVCC() bool {
	return (r.Flags & int64(ReplicatedEvalResultFlags_NonMVCC)) != 0
}

// IsIntentWrite returns whether the command corresponds to a request with the
// isIntentWrite flag. This is the inverse of IsNonMVCC.
// Note that all requests proposed by 20.2 nodes return true.
func (r ReplicatedEvalResult) IsIntentWrite() bool {
	return !r.IsNonMVCC()
}

// SetNonMVCC sets the respective flag on the command, indicating that this
// command corresponds to a non-MVCC request, or more generally a request that
// doesn't write new intents.
func (r *ReplicatedEvalResult) SetNonMVCC() {
	r.Flags |= int64(ReplicatedEvalResultFlags_NonMVCC)
}

// ClearNonMVCC clears the NonMVCC flag.
func (r *ReplicatedEvalResult) ClearNonMVCC() {
	r.Flags &= ^(int64(ReplicatedEvalResultFlags_NonMVCC))
}
