// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlstats

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs/eventagg"
	"github.com/cockroachdb/cockroach/pkg/obs/logstream"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type Stmt struct {
	// StmtFingerprintID is the fingerprint ID for the statement.
	// TODO(abarganier): StmtFingerprintID is included in the GroupingKey for this type.
	// Ideally, we would have some sort of struct tag that would allow us to leverage code
	// generation to derive the GroupingKey implementation. However, for now, we punt this
	// problem to avoid slowing down the prototyping process.
	StmtFingerprintID appstatspb.StmtFingerprintID
	// Statement is the redactable statement string.
	Statement string
	// ServiceLatency is the latency of serving the query, excluding miscellaneous
	// sources of the overhead (e.g. internal retries).
	ServiceLatency time.Duration
}

func (s *Stmt) MergeInto(aggregate *StmtStatistics) {
	aggregate.ExecCount++
	aggregate.ServiceLatency.RecordValue(s.ServiceLatency.Nanoseconds())
	aggregate.ServiceLatencyP99 = aggregate.ServiceLatency.CumulativeSnapshot().ValueAtQuantile(99)
}

func (s *Stmt) GroupingKey() appstatspb.StmtFingerprintID {
	return s.StmtFingerprintID
}

var _ eventagg.Mergeable[appstatspb.StmtFingerprintID, *StmtStatistics] = (*Stmt)(nil)

type StmtStatistics struct {
	// ExecCount is the count of total executions for the statement.
	// TODO(abarganier): Ideally, we find a code generation mechanism here to define
	// how an event is merged into the aggregate type via things like struct tags. However,
	// for now, we punt this problem to avoid slowing down the prototyping process.
	ExecCount int
	// ServiceLatency is a histogram used to aggregate service latencies of the
	// various Stmt's recorded into this StmtStatistics instance.
	// TODO(abarganier): If we're pulling quantiles from this histogram with every merge, or once on flush
	// for many values, we need a much more lightweight histogram implementation. Calculating quantiles from
	// the current histogram library is heavy on allocations.
	ServiceLatency metric.IHistogram `json:"-"`
	// ServiceLatencyP99 is the P99 latency for the statement, derived from ServiceLatency.
	ServiceLatencyP99 float64
}

// NewStmtStatsAggregator leverages the generic MapReduceAggregator to instantiate
// and return a new aggregator for StmtStatistic's..
//
// It is an example of a system that'd use the eventagg library to easily define aggregation
// rules/criteria, and how to process those results.
func NewStmtStatsAggregator() *eventagg.MapReduceAggregator[*Stmt, appstatspb.StmtFingerprintID, *StmtStatistics] {
	return eventagg.NewMapReduceAggregator[*Stmt, appstatspb.StmtFingerprintID, *StmtStatistics](
		func() *StmtStatistics {
			return &StmtStatistics{
				ServiceLatency: metric.NewHistogram(metric.HistogramOptions{
					Metadata: metric.Metadata{
						Name:        "stmt.svc.latency",
						Measurement: "Aggregate service latency of statement executions",
						Unit:        metric.Unit_NANOSECONDS,
					},
					Duration:     base.DefaultHistogramWindowInterval(),
					BucketConfig: metric.IOLatencyBuckets,
				}),
			}
		},
		eventagg.NewWindowedFlush(10*time.Second, timeutil.Now),
		eventagg.NewLogWriteConsumer[appstatspb.StmtFingerprintID, *StmtStatistics](log.STATEMENT_STATS), // We'd like to log all the aggregated results, as-is.
	)
}

type StmtStatProcessor struct{}

var _ eventagg.KVProcessor[appstatspb.StmtFingerprintID, *StmtStatistics] = (*StmtStatProcessor)(nil)

// InitStmtStatsProcessor initializes & registers a new logstream.Processor for the processing of statement statistics.
// It consumes streams of events flushed & logged from NewStmtStatsAggregator instances.
func InitStmtStatsProcessor(ctx context.Context) {
	processor := eventagg.NewEmittedKVProcessor[appstatspb.StmtFingerprintID, *StmtStatistics](&StmtStatProcessor{})
	logstream.RegisterProcessor(ctx, log.STATEMENT_STATS, processor)
}

// Process implements the eventagg.KVProcessor interface.
func (s *StmtStatProcessor) Process(
	_ context.Context,
	aggInfo eventagg.AggInfo,
	key appstatspb.StmtFingerprintID,
	val *StmtStatistics,
) error {
	fmt.Printf("FingerprintID: '%s'\tExec Count: %d\tP99 Latency (ms): %f\tStart: %s End %s\n",
		hex.EncodeToString(sqlstatsutil.EncodeUint64ToBytes(uint64(key))),
		val.ExecCount,
		val.ServiceLatencyP99/1000000,
		timeutil.Unix(0, aggInfo.StartTime).UTC().String(),
		timeutil.Unix(0, aggInfo.EndTime).UTC().String())
	return nil
}
