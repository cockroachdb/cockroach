// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserverbase

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// MergeQueueEnabled is a setting that controls whether the merge queue is
// enabled.
var MergeQueueEnabled = settings.RegisterBoolSetting(
	"kv.range_merge.queue_enabled",
	"whether the automatic merge queue is enabled",
	true,
)

// TxnCleanupThreshold is the threshold after which a transaction is
// considered abandoned and fit for removal, as measured by the
// maximum of its last heartbeat and timestamp. Abort spans for the
// transaction are cleaned up at the same time.
//
// TODO(tschottdorf): need to enforce at all times that this is much
// larger than the heartbeat interval used by the coordinator.
const TxnCleanupThreshold = time.Hour

// CmdIDKey is a Raft command id. This will be logged unredacted - keep it random.
type CmdIDKey string

// SafeFormat implements redact.SafeFormatter.
func (s CmdIDKey) SafeFormat(sp redact.SafePrinter, verb rune) {
	sp.Printf("%q", redact.SafeString(s))
}

func (s CmdIDKey) String() string {
	return redact.StringWithoutMarkers(s)
}

var _ redact.SafeFormatter = CmdIDKey("")

// FilterArgs groups the arguments to a ReplicaCommandFilter.
type FilterArgs struct {
	Ctx   context.Context
	CmdID CmdIDKey
	Index int
	Sid   roachpb.StoreID
	Req   roachpb.Request
	Hdr   roachpb.Header
	Err   error // only used for TestingPostEvalFilter
}

// ProposalFilterArgs groups the arguments to ReplicaProposalFilter.
type ProposalFilterArgs struct {
	Ctx   context.Context
	Cmd   kvserverpb.RaftCommand
	CmdID CmdIDKey
	Req   roachpb.BatchRequest
}

// ApplyFilterArgs groups the arguments to a ReplicaApplyFilter.
type ApplyFilterArgs struct {
	kvserverpb.ReplicatedEvalResult
	CmdID   CmdIDKey
	RangeID roachpb.RangeID
	StoreID roachpb.StoreID
	Req     *roachpb.BatchRequest // only set on the leaseholder
}

// InRaftCmd returns true if the filter is running in the context of a Raft
// command (it could be running outside of one, for example for a read).
func (f *FilterArgs) InRaftCmd() bool {
	return f.CmdID != ""
}

// ReplicaRequestFilter can be used in testing to influence the error returned
// from a request before it is evaluated. Return nil to continue with regular
// processing or non-nil to terminate processing with the returned error.
type ReplicaRequestFilter func(context.Context, roachpb.BatchRequest) *roachpb.Error

// ReplicaConcurrencyRetryFilter can be used to examine a concurrency retry
// error before it is handled and its batch is re-evaluated.
type ReplicaConcurrencyRetryFilter func(context.Context, roachpb.BatchRequest, *roachpb.Error)

// ReplicaCommandFilter may be used in tests through the StoreTestingKnobs to
// intercept the handling of commands and artificially generate errors. Return
// nil to continue with regular processing or non-nil to terminate processing
// with the returned error.
type ReplicaCommandFilter func(args FilterArgs) *roachpb.Error

// ReplicaProposalFilter can be used in testing to influence the error returned
// from proposals after a request is evaluated but before it is proposed.
type ReplicaProposalFilter func(args ProposalFilterArgs) *roachpb.Error

// A ReplicaApplyFilter can be used in testing to influence the error returned
// from proposals after they apply. The returned int is treated as a
// storage.proposalReevaluationReason and will only take an effect when it is
// nonzero and the existing reason is zero. Similarly, the error is only applied
// if there's no error so far.
type ReplicaApplyFilter func(args ApplyFilterArgs) (int, *roachpb.Error)

// ReplicaResponseFilter is used in unittests to modify the outbound
// response returned to a waiting client after a replica command has
// been processed. This filter is invoked only by the command proposer.
type ReplicaResponseFilter func(context.Context, roachpb.BatchRequest, *roachpb.BatchResponse) *roachpb.Error

// ReplicaRangefeedFilter is used in unit tests to modify the request, inject
// responses, or return errors from rangefeeds.
type ReplicaRangefeedFilter func(
	args *roachpb.RangeFeedRequest, stream roachpb.Internal_RangeFeedServer,
) *roachpb.Error

// ContainsKey returns whether this range contains the specified key.
func ContainsKey(desc *roachpb.RangeDescriptor, key roachpb.Key) bool {
	if bytes.HasPrefix(key, keys.LocalRangeIDPrefix) {
		return bytes.HasPrefix(key, keys.MakeRangeIDPrefix(desc.RangeID))
	}
	keyAddr, err := keys.Addr(key)
	if err != nil {
		return false
	}
	return desc.ContainsKey(keyAddr)
}

// ContainsKeyRange returns whether this range contains the specified key range
// from start to end.
func ContainsKeyRange(desc *roachpb.RangeDescriptor, start, end roachpb.Key) bool {
	startKeyAddr, err := keys.Addr(start)
	if err != nil {
		return false
	}
	endKeyAddr, err := keys.Addr(end)
	if err != nil {
		return false
	}
	return desc.ContainsKeyRange(startKeyAddr, endKeyAddr)
}

// IntersectSpan takes an span and a descriptor. It then splits the span
// into up to three pieces: A first piece which is contained in the Range,
// and a slice of up to two further spans which are outside of the key
// range. An span for which [Key, EndKey) is empty does not result in any
// spans; thus intersectIntent only applies to span ranges.
//
// A range-local span range is never split: It's returned as either
// belonging to or outside of the descriptor's key range, and passing an
// span which begins range-local but ends non-local results in a panic.
//
// TODO(tschottdorf): move to proto, make more gen-purpose - kv.truncate does
// some similar things.
func IntersectSpan(
	span roachpb.Span, desc *roachpb.RangeDescriptor,
) (middle *roachpb.Span, outside []roachpb.Span) {
	start, end := desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey()
	if len(span.EndKey) == 0 {
		outside = append(outside, span)
		return
	}
	if bytes.Compare(span.Key, keys.LocalRangeMax) < 0 {
		if bytes.Compare(span.EndKey, keys.LocalRangeMax) >= 0 {
			panic(fmt.Sprintf("a local intent range may not have a non-local portion: %s", span))
		}
		if ContainsKeyRange(desc, span.Key, span.EndKey) {
			return &span, nil
		}
		return nil, append(outside, span)
	}
	// From now on, we're dealing with plain old key ranges - no more local
	// addressing.
	if bytes.Compare(span.Key, start) < 0 {
		// Span spans a part to the left of [start, end).
		iCopy := span
		if bytes.Compare(start, span.EndKey) < 0 {
			iCopy.EndKey = start
		}
		span.Key = iCopy.EndKey
		outside = append(outside, iCopy)
	}
	if bytes.Compare(span.Key, span.EndKey) < 0 && bytes.Compare(end, span.EndKey) < 0 {
		// Span spans a part to the right of [start, end).
		iCopy := span
		if bytes.Compare(iCopy.Key, end) < 0 {
			iCopy.Key = end
		}
		span.EndKey = iCopy.Key
		outside = append(outside, iCopy)
	}
	if bytes.Compare(span.Key, span.EndKey) < 0 && bytes.Compare(span.Key, start) >= 0 && bytes.Compare(end, span.EndKey) >= 0 {
		middle = &span
	}
	return
}

// SplitByLoadMergeDelay wraps "kv.range_split.by_load_merge_delay".
var SplitByLoadMergeDelay = settings.RegisterDurationSetting(
	"kv.range_split.by_load_merge_delay",
	"the delay that range splits created due to load will wait before considering being merged away",
	5*time.Minute,
	func(v time.Duration) error {
		const minDelay = 5 * time.Second
		if v < minDelay {
			return errors.Errorf("cannot be set to a value below %s", minDelay)
		}
		return nil
	},
)
