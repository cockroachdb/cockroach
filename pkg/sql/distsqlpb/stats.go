// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
