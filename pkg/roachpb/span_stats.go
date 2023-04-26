// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import "github.com/cockroachdb/cockroach/pkg/settings"

// Put span statistics cluster settings here to avoid import cycle.

const DefaultSpanStatsSpanLimit = 500

// SpanStatsBatchLimit registers the maximum number of spans allowed in a
// span stats request payload.
var SpanStatsBatchLimit = settings.RegisterIntSetting(
	settings.TenantWritable,
	"server.span_stats.span_batch_limit",
	"the maximum number of spans allowed in a request payload for span statistics",
	DefaultSpanStatsSpanLimit,
	settings.PositiveInt,
)

const defaultRangeStatsBatchLimit = 100

// RangeStatsBatchLimit registers the maximum number of ranges to be batched
// when fetching range stats for a span.
var RangeStatsBatchLimit = settings.RegisterIntSetting(
	settings.TenantWritable,
	"server.span_stats.range_batch_limit",
	"the maximum batch size when fetching ranges statistics for a span",
	defaultRangeStatsBatchLimit,
	settings.PositiveInt,
)
