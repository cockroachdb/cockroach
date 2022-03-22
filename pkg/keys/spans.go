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

	// SystemConfigSpan is the range of system objects which will be gossiped.
	SystemConfigSpan = roachpb.Span{Key: SystemConfigSplitKey, EndKey: SystemConfigTableDataMax}

	// NoSplitSpans describes the ranges that should never be split.
	// Meta1Span: needed to find other ranges.
	// Meta2MaxSpan: between meta and system ranges.
	// NodeLivenessSpan: liveness information on nodes in the cluster.
	// SystemConfigSpan: system objects which will be gossiped.
	NoSplitSpans = []roachpb.Span{Meta1Span, Meta2MaxSpan, NodeLivenessSpan, SystemConfigSpan}
)
