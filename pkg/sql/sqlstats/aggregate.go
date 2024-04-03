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
	"time"

	"github.com/cockroachdb/cockroach/pkg/obs/eventagg"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	ServiceLatency metric.ManualWindowHistogram
}

func mergeStmt(s *Stmt, stats *StmtStatistics) {
	stats.ExecCount++
	stats.ServiceLatency.RecordValue(float64(s.ServiceLatency.Nanoseconds()))
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
				ServiceLatency: metric.ManualWindowHistogram{}, // TODO: legitimate construction of histogram.
			}
		},
		mapStmt,
		mergeStmt,
		eventagg.NewWindowedFlush(10*time.Second, timeutil.Now),                                          // Let's limit our aggregation windows to clock-aligned 10-second intervals.
		eventagg.NewLogWriteConsumer[appstatspb.StmtFingerprintID, *StmtStatistics](log.STATEMENT_STATS), // We'd like to log all the aggregated results, as-is.
	)
}
