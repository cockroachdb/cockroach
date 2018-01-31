// Copyright 2017 The Cockroach Authors.
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

package compactor

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Metrics holds all metrics relating to a Compactor.
type Metrics struct {
	BytesQueued         *metric.Gauge
	BytesSkipped        *metric.Counter
	BytesCompacted      *metric.Counter
	CompactionSuccesses *metric.Counter
	CompactionFailures  *metric.Counter
	CompactingNanos     *metric.Counter
}

// MetricStruct implements the metrics.Struct interface.
func (Metrics) MetricStruct() {}

var _ metric.Struct = Metrics{}

var (
	metaBytesQueued = metric.Metadata{
		Name: "compactor.suggestionbytes.queued",
		Help: "Number of logical bytes in suggested compactions in the queue"}
	metaBytesSkipped = metric.Metadata{
		Name: "compactor.suggestionbytes.skipped",
		Help: "Number of logical bytes in suggested compactions which were not compacted"}
	metaBytesCompacted = metric.Metadata{
		Name: "compactor.suggestionbytes.compacted",
		Help: "Number of logical bytes compacted from suggested compactions"}
	metaCompactionSuccesses = metric.Metadata{
		Name: "compactor.compactions.success",
		Help: "Number of successful compaction requests sent to the storage engine"}
	metaCompactionFailures = metric.Metadata{
		Name: "compactor.compactions.failure",
		Help: "Number of failed compaction requests sent to the storage engine"}
	metaCompactingNanos = metric.Metadata{
		Name: "compactor.compactingnanos",
		Help: "Number of nanoseconds spent compacting ranges"}
)

// makeMetrics returns a Metrics struct.
func makeMetrics() Metrics {
	return Metrics{
		BytesQueued:         metric.NewGauge(metaBytesQueued),
		BytesSkipped:        metric.NewCounter(metaBytesSkipped),
		BytesCompacted:      metric.NewCounter(metaBytesCompacted),
		CompactionSuccesses: metric.NewCounter(metaCompactionSuccesses),
		CompactionFailures:  metric.NewCounter(metaCompactionFailures),
		CompactingNanos:     metric.NewCounter(metaCompactingNanos),
	}
}
