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

package distsqlpb

import "github.com/cockroachdb/cockroach/pkg/util/tracing"

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
