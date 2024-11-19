// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type Stmt struct {
	// StmtFingerprintID is the fingerprint ID for the statement.
	// TODO(abarganier): StmtFingerprintID is included in the GroupingKey for this type.
	// Ideally, we would have some sort of struct tag that would allow us to leverage code
	// generation to derive the GroupingKey implementation. However, for now, we punt this
	// problem to avoid slowing down the prototyping process.
	StmtFingerprintID appstatspb.StmtFingerprintID
	// Statement is the redactable statement string.
	Statement redact.RedactableString
	// ServiceLatency is the latency of serving the query, excluding miscellaneous
	// sources of the overhead (e.g. internal retries).
	ServiceLatency time.Duration
}

type StmtStatistics struct {
	// ExecCount is the count of total executions for the statement.
	// TODO(abarganier): Ideally, we find a code generation mechanism here to define
	// how an event is merged into the aggregate type via things like struct tags. However,
	// for now, we punt this problem to avoid slowing down the prototyping process.
	ExecCount int
	// ServiceLatency is a histogram used to aggregate service latencies of the
	// various Stmt's recorded into this StmtStatistics instance.
	ServiceLatency    metric.IHistogram
	ServiceLatencyP99 float64
}

func mergeStmt(s *Stmt, stats *StmtStatistics) {
	stats.ExecCount++
	stats.ServiceLatency.RecordValue(s.ServiceLatency.Nanoseconds())
}

func mapStmt(s *Stmt) appstatspb.StmtFingerprintID {
	return s.StmtFingerprintID
}

// NewStmtStatsAggregator leverages the generic MapReduceAggregator to instantiate
// and return a new aggregator for StmtStatistic's..
//
// It is an example of a system that'd use the eventagg library to easily define aggregation
// rules/criteria, and how to process those results.
func NewStmtStatsAggregator(
	stopper *stop.Stopper,
) *eventagg.MapReduceAggregator[*Stmt, appstatspb.StmtFingerprintID, *StmtStatistics] {
	return eventagg.NewMapReduceAggregator[*Stmt, appstatspb.StmtFingerprintID, *StmtStatistics](
		stopper,
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
		mapStmt,
		mergeStmt,
		eventagg.NewWindowedFlush(10*time.Second, timeutil.Now),                                          // Let's limit our aggregation windows to clock-aligned 10-second intervals.
		eventagg.NewLogWriteConsumer[appstatspb.StmtFingerprintID, *StmtStatistics](log.STATEMENT_STATS), // We'd like to log all the aggregated results, as-is.
	)
}

// SQLStatsLogProcessor is an example logstream.Processor implementation.
type SQLStatsLogProcessor struct {
	// Processors can be stateful! This is why we opt for using interface implementations as
	// opposed to anonymous functions.
}

// Process implements the logstream.Processor interface.
func (S *SQLStatsLogProcessor) Process(ctx context.Context, event any) error {
	e, ok := event.(eventagg.KeyValueLog[appstatspb.StmtFingerprintID, *StmtStatistics])
	if !ok {
		panic(errors.AssertionFailedf("Unexpected event type provided to SQLStatsLogProcessor: %v", event))
	}
	fmt.Printf("FingerprintID: '%s'\tExec Count: %d\tP99 Latency (ms): %f\tStart: %s End %s\n",
		hex.EncodeToString(sqlstatsutil.EncodeUint64ToBytes(uint64(e.Key))),
		e.Value.ExecCount,
		e.Value.ServiceLatencyP99/1000000,
		timeutil.Unix(0, e.AggInfo.StartTime).UTC().String(),
		timeutil.Unix(0, e.AggInfo.EndTime).UTC().String())
	return nil
}

var _ logstream.Processor = (*SQLStatsLogProcessor)(nil)

// InitStmtStatsProcessor initializes & registers a new logstream.Processor for the processing of statement statistics
// for the tenant associated with the given ctx.
//
// It consumes streams of events flushed & logged from NewStmtStatsAggregator instances belonging to the same tenant.
func InitStmtStatsProcessor(ctx context.Context, stopper *stop.Stopper) {
	logstream.RegisterProcessor(ctx, stopper, log.STATEMENT_STATS, &SQLStatsLogProcessor{})
}
