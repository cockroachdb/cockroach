// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keys

import "github.com/cockroachdb/cockroach/pkg/roachpb"

var (
	// EverythingSpan is a span that covers everything.
	EverythingSpan = roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax}

	// ExcludeFromBackupSpan is a span that covers the keyspace that we exclude
	// from full cluster backup and do not place protected timestamps on.
	ExcludeFromBackupSpan = roachpb.Span{Key: roachpb.KeyMin, EndKey: TableDataMin}

	// Meta1Span holds all first level addressing records.
	Meta1Span = roachpb.Span{Key: roachpb.KeyMin, EndKey: Meta2Prefix}

	// MetaSpan holds all first- and second-level addressing records.
	MetaSpan = roachpb.Span{Key: MetaMin, EndKey: MetaMax}

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

	// ScratchSpan is used in tests to write arbitrary data without
	// 	overlapping with meta, system or tenant ranges.
	ScratchSpan = roachpb.Span{Key: ScratchRangeMin, EndKey: ScratchRangeMax}

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
