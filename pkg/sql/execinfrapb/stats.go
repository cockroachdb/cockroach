// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfrapb

import "github.com/cockroachdb/cockroach/pkg/util/tracing"

// FlowIDTagKey is the key used for flow id tags in tracing spans.
const FlowIDTagKey = tracing.TagPrefix + "flowid"

// StreamIDTagKey is the key used for stream id tags in tracing spans.
const StreamIDTagKey = tracing.TagPrefix + "streamid"

// ProcessorIDTagKey is the key used for processor id tags in tracing spans.
const ProcessorIDTagKey = tracing.TagPrefix + "processorid"

// DistSQLSpanStats is a tracing.SpanStats that returns a list of stats to
// output on a query plan.
type DistSQLSpanStats interface {
	tracing.SpanStats
	StatsForQueryPlan() []string
}
