// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keys

import "github.com/cockroachdb/cockroach/pkg/roachpb"

var (
	// EverythingSpan is a span that covers everything.
	EverythingSpan = roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax}

	// Meta1Span holds all first level addressing records.
	Meta1Span = roachpb.Span{Key: roachpb.KeyMin, EndKey: Meta2Prefix}

	// Meta2MaxSpan begins at key Meta2KeyMax with the last entry in the second
	// level addressing keyspace. The rest of the span is always empty. We cannot
	// split at this starting key or between it and MetaMax.
	// - The first condition is explained and enforced in libroach/mvcc.cc.
	// - The second condition is necessary because a split between Meta2KeyMax
	//   and MetaMax would conflict with the use of Meta1KeyMax to hold the
	//   descriptor for the range that spans the meta2/userspace boundary (see
	//   case 3a in rangeAddressing). If splits were allowed in this span then
	//   the descriptor for the ranges ending in this span would be stored AFTER
	//   Meta1KeyMax, which would allow meta1 to get out of order.
	Meta2MaxSpan = roachpb.Span{Key: Meta2KeyMax, EndKey: MetaMax}

	// NodeLivenessSpan holds the liveness records for nodes in the cluster.
	NodeLivenessSpan = roachpb.Span{Key: NodeLivenessPrefix, EndKey: NodeLivenessKeyMax}

	// TimeseriesSpan holds all the timeseries data in the cluster.
	TimeseriesSpan = roachpb.Span{Key: TimeseriesPrefix, EndKey: TimeseriesKeyMax}

	// SystemSpanConfigSpan is part of the system keyspace that is used to carve
	// out spans for system span configurations. No data is stored in these spans,
	// instead, special meaning is assigned to them when stored in
	// `system.span_configurations`.
	SystemSpanConfigSpan = roachpb.Span{Key: SystemSpanConfigPrefix, EndKey: SystemSpanConfigKeyMax}

	// SystemDescriptorTableSpan is the span for the system.descriptor table.
	SystemDescriptorTableSpan = roachpb.Span{Key: SystemSQLCodec.TablePrefix(DescriptorTableID), EndKey: SystemSQLCodec.TablePrefix(DescriptorTableID + 1)}
	// SystemZonesTableSpan is the span for the system.zones table.
	SystemZonesTableSpan = roachpb.Span{Key: SystemSQLCodec.TablePrefix(ZonesTableID), EndKey: SystemSQLCodec.TablePrefix(ZonesTableID + 1)}

	// NoSplitSpans describes the ranges that should never be split.
	// Meta1Span: needed to find other ranges.
	// Meta2MaxSpan: between meta and system ranges.
	// NodeLivenessSpan: liveness information on nodes in the cluster.
	NoSplitSpans = []roachpb.Span{Meta1Span, Meta2MaxSpan, NodeLivenessSpan}
)

// MakeRangeSpans returns all key spans for the given range, in sorted order.
func MakeRangeSpans(d *roachpb.RangeDescriptor) []roachpb.Span {
	return makeRangeSpans(d, false /* replicatedOnly */)
}

// MakeReplicatedRangeSpans returns all key spans for the given range that are
// fully Raft replicated.
//
// NOTE: The logic for Raft snapshots relies on this function returning the
// ranges in the following sorted order:
//
// 1. Replicated range-id local key span.
// 2. Range-local key span.
// 3. Lock-table key spans.
// 4. User key span.
func MakeReplicatedRangeSpans(d *roachpb.RangeDescriptor) []roachpb.Span {
	return makeRangeSpans(d, true /* replicatedOnly */)
}

func makeRangeSpans(d *roachpb.RangeDescriptor, replicatedOnly bool) []roachpb.Span {
	rangeLockTable := makeLockTableRangeSpans(d)
	if len(rangeLockTable) != 2 {
		panic("unexpected number of lock table key spans")
	}
	return []roachpb.Span{
		MakeRangeIDLocalRangeSpan(d.RangeID, replicatedOnly),
		makeRangeLocalRangeSpan(d),
		rangeLockTable[0],
		rangeLockTable[1],
		MakeUserRangeSpan(d),
	}
}

// MakeReplicatedRangeSpansExceptLockTable returns all key spans that are fully
// Raft replicated for the given range, except for the lock table spans. These
// are returned in the following sorted order:
//
// 1. Replicated range-id local key span.
// 2. Range-local key span.
// 3. User key span.
func MakeReplicatedRangeSpansExceptLockTable(d *roachpb.RangeDescriptor) []roachpb.Span {
	return []roachpb.Span{
		MakeRangeIDLocalRangeSpan(d.RangeID, true /* replicatedOnly */),
		makeRangeLocalRangeSpan(d),
		MakeUserRangeSpan(d),
	}
}

// MakeReplicatedRangeSpansExceptRangeID returns all key spans that are fully
// Raft replicated for the given Range, except for the replicated range-id local
// key span. These are returned in the following sorted order:
//
// 1. Range-local key span.
// 2. Lock-table key span.
// 3. User key span.
func MakeReplicatedRangeSpansExceptRangeID(d *roachpb.RangeDescriptor) []roachpb.Span {
	rangeLockTable := makeLockTableRangeSpans(d)
	if len(rangeLockTable) != 2 {
		panic("unexpected number of lock table key spans")
	}
	return []roachpb.Span{
		makeRangeLocalRangeSpan(d),
		rangeLockTable[0],
		rangeLockTable[1],
		MakeUserRangeSpan(d),
	}
}

// MakeRangeIDLocalRangeSpan returns the range-id local key span of the given
// range. If replicatedOnly is true, then it returns only the replicated keys,
// otherwise, it only returns both the replicated and unreplicated keys.
func MakeRangeIDLocalRangeSpan(rangeID roachpb.RangeID, replicatedOnly bool) roachpb.Span {
	var sysRangeIDKey roachpb.Key
	if replicatedOnly {
		sysRangeIDKey = MakeRangeIDReplicatedPrefix(rangeID)
	} else {
		sysRangeIDKey = MakeRangeIDPrefix(rangeID)
	}
	return roachpb.Span{
		Key:    sysRangeIDKey,
		EndKey: sysRangeIDKey.PrefixEnd(),
	}
}

// makeRangeLocalRangeSpan returns the range local key span of the given range.
// Range-local keys are replicated keys that do not belong to the range they
// would naturally sort into. For example, /Local/Range/Table/1 would sort into
// [/Min, /System), but it actually belongs to [/Table/1, /Table/2).
func makeRangeLocalRangeSpan(d *roachpb.RangeDescriptor) roachpb.Span {
	return roachpb.Span{
		Key:    MakeRangeKeyPrefix(d.StartKey),
		EndKey: MakeRangeKeyPrefix(d.EndKey),
	}
}

// makeLockTableRangeSpans returns the 2 lock table key span for a range.
func makeLockTableRangeSpans(d *roachpb.RangeDescriptor) [2]roachpb.Span {
	// Handle doubly-local lock table keys since range descriptor key
	// is a range local key that can have a replicated lock acquired on it.
	startRangeLocal, _ := LockTableSingleKey(MakeRangeKeyPrefix(d.StartKey), nil)
	endRangeLocal, _ := LockTableSingleKey(MakeRangeKeyPrefix(d.EndKey), nil)
	// The first range in the global keyspace can start earlier than LocalMax,
	// at RKeyMin, but the actual data starts at LocalMax. We need to make this
	// adjustment here to prevent [startRangeLocal, endRangeLocal) and
	// [startGlobal, endGlobal) from overlapping.
	globalStartKey := d.StartKey.AsRawKey()
	if d.StartKey.Equal(roachpb.RKeyMin) {
		globalStartKey = LocalMax
	}
	startGlobal, _ := LockTableSingleKey(globalStartKey, nil)
	endGlobal, _ := LockTableSingleKey(roachpb.Key(d.EndKey), nil)
	return [2]roachpb.Span{
		{
			Key:    startRangeLocal,
			EndKey: endRangeLocal,
		},
		{
			Key:    startGlobal,
			EndKey: endGlobal,
		},
	}
}

// MakeUserRangeSpan returns the user key span of the given range.
func MakeUserRangeSpan(d *roachpb.RangeDescriptor) roachpb.Span {
	userKeys := d.KeySpan()
	return roachpb.Span{
		Key:    userKeys.Key.AsRawKey(),
		EndKey: userKeys.EndKey.AsRawKey(),
	}
}
