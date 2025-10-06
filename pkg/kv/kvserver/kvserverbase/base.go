// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserverbase

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/redact"
)

// LeaseQueueEnabled is a setting that controls whether the lease queue
// is enabled.
var LeaseQueueEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.lease_queue.enabled",
	"whether the lease queue is enabled",
	true,
)

// MergeQueueEnabled is a setting that controls whether the merge queue is
// enabled.
var MergeQueueEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.range_merge.queue_enabled",
	"whether the automatic merge queue is enabled",
	true,
	settings.WithName("kv.range_merge.queue.enabled"),
)

// ReplicateQueueEnabled is a setting that controls whether the replicate queue
// is enabled.
var ReplicateQueueEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.replicate_queue.enabled",
	"whether the replicate queue is enabled",
	true,
)

// ReplicaGCQueueEnabled is a setting that controls whether the replica GC queue
// is enabled.
var ReplicaGCQueueEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.replica_gc_queue.enabled",
	"whether the replica gc queue is enabled",
	true,
)

// RaftLogQueueEnabled is a setting that controls whether the raft log queue is
// enabled.
var RaftLogQueueEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.raft_log_queue.enabled",
	"whether the raft log queue is enabled",
	true,
)

// RaftSnapshotQueueEnabled is a setting that controls whether the raft snapshot
// queue is enabled.
var RaftSnapshotQueueEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.raft_snapshot_queue.enabled",
	"whether the raft snapshot queue is enabled",
	true,
)

// ConsistencyQueueEnabled is a setting that controls whether the consistency
// queue is enabled.
var ConsistencyQueueEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.consistency_queue.enabled",
	"whether the consistency queue is enabled",
	true,
)

// TimeSeriesMaintenanceQueueEnabled is a setting that controls whether the
// timeseries maintenance queue is enabled.
var TimeSeriesMaintenanceQueueEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.timeseries_maintenance_queue.enabled",
	"whether the timeseries maintenance queue is enabled",
	true,
)

// SplitQueueEnabled is a setting that controls whether the split queue is
// enabled.
var SplitQueueEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.split_queue.enabled",
	"whether the split queue is enabled",
	true,
)

// MVCCGCQueueEnabled is a setting that controls whether the MVCC GC queue is
// enabled.
var MVCCGCQueueEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.mvcc_gc_queue.enabled",
	"whether the MVCC GC queue is enabled",
	true,
)

// RangeFeedRefreshInterval is injected from kvserver to avoid import cycles
// when accessed from kvcoord.
var RangeFeedRefreshInterval *settings.DurationSetting

// CmdIDKey is a Raft command id. This will be logged unredacted - keep it random.
type CmdIDKey string

// SafeFormat implements redact.SafeFormatter.
func (s CmdIDKey) SafeFormat(sp redact.SafePrinter, verb rune) {
	sp.Printf("%x", redact.SafeString(s))
}

func (s CmdIDKey) String() string {
	return redact.StringWithoutMarkers(s)
}

var _ redact.SafeFormatter = CmdIDKey("")

// FilterArgs groups the arguments to a ReplicaCommandFilter.
type FilterArgs struct {
	Ctx     context.Context
	CmdID   CmdIDKey
	Index   int
	Sid     roachpb.StoreID
	Req     kvpb.Request
	Hdr     kvpb.Header
	Version roachpb.Version
	Err     error // only used for TestingPostEvalFilter
}

// ProposalFilterArgs groups the arguments to ReplicaProposalFilter.
type ProposalFilterArgs struct {
	Ctx        context.Context
	RangeID    roachpb.RangeID
	StoreID    roachpb.StoreID
	ReplicaID  roachpb.ReplicaID
	Cmd        *kvserverpb.RaftCommand
	QuotaAlloc *quotapool.IntAlloc
	CmdID      CmdIDKey
	SeedID     CmdIDKey
	Req        *kvpb.BatchRequest
}

// ApplyFilterArgs groups the arguments to a ReplicaApplyFilter.
type ApplyFilterArgs struct {
	kvserverpb.ReplicatedEvalResult
	CmdID       CmdIDKey
	Cmd         kvserverpb.RaftCommand
	Entry       raftpb.Entry
	RangeID     roachpb.RangeID
	StoreID     roachpb.StoreID
	ReplicaID   roachpb.ReplicaID
	Ephemeral   bool
	Req         *kvpb.BatchRequest // only set on the leaseholder
	ForcedError *kvpb.Error
}

// InRaftCmd returns true if the filter is running in the context of a Raft
// command (it could be running outside of one, for example for a read).
func (f *FilterArgs) InRaftCmd() bool {
	return f.CmdID != ""
}

// ReplicaRequestFilter can be used in testing to influence the error returned
// from a request before it is evaluated. Return nil to continue with regular
// processing or non-nil to terminate processing with the returned error.
type ReplicaRequestFilter func(context.Context, *kvpb.BatchRequest) *kvpb.Error

// ReplicaConcurrencyRetryFilter can be used to examine a concurrency retry
// error before it is handled and its batch is re-evaluated.
type ReplicaConcurrencyRetryFilter func(context.Context, *kvpb.BatchRequest, *kvpb.Error)

// ReplicaCommandFilter may be used in tests through the StoreTestingKnobs to
// intercept the handling of commands and artificially generate errors. Return
// nil to continue with regular processing or non-nil to terminate processing
// with the returned error.
type ReplicaCommandFilter func(args FilterArgs) *kvpb.Error

// ReplicaProposalFilter can be used in testing to influence the error returned
// from proposals after a request is evaluated but before it is proposed.
type ReplicaProposalFilter func(args ProposalFilterArgs) *kvpb.Error

// A ReplicaApplyFilter is a testing hook into raft command application.
// See StoreTestingKnobs.
type ReplicaApplyFilter func(args ApplyFilterArgs) (int, *kvpb.Error)

// ReplicaResponseFilter is used in unittests to modify the outbound
// response returned to a waiting client after a replica command has
// been processed. This filter is invoked only by the command proposer.
type ReplicaResponseFilter func(context.Context, *kvpb.BatchRequest, *kvpb.BatchResponse) *kvpb.Error

// ReplicaRangefeedFilter is used in unit tests to modify the request, inject
// responses, or return errors from rangefeeds.
type ReplicaRangefeedFilter func(
	args *kvpb.RangeFeedRequest, stream kvpb.RangeFeedEventSink,
) *kvpb.Error

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
// spans; thus IntersectSpan only applies to span ranges and point keys will
// cause the function to panic.
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
		panic("unsupported point key")
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
	settings.SystemVisible, // used by TRUNCATE in SQL
	"kv.range_split.by_load_merge_delay",
	"the delay that range splits created due to load will wait before considering being merged away",
	5*time.Minute,
	settings.DurationWithMinimum(5*time.Second),
)

const (
	// MaxCommandSizeDefault is the default for the kv.raft.command.max_size
	// cluster setting.
	MaxCommandSizeDefault = 64 << 20 // 64 MB

	// MaxCommandSizeFloor is the minimum allowed value for the
	// kv.raft.command.max_size cluster setting.
	MaxCommandSizeFloor = 4 << 20 // 4 MB
)

// MaxCommandSize wraps "kv.raft.command.max_size".
var MaxCommandSize = settings.RegisterByteSizeSetting(
	settings.SystemVisible, // used by SQL/bulk to determine mutation batch sizes
	"kv.raft.command.max_size",
	"maximum size of a raft command",
	MaxCommandSizeDefault,
	settings.ByteSizeWithMinimum(MaxCommandSizeFloor),
)
