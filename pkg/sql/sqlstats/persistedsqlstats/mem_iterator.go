// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
)

// memStmtStatsIterator wraps a sslocal.StmtStatsIterator. Since in-memory
// statement statistics does not have aggregated_ts and aggregation_interval
// fields populated, memStmtStatsIterator overrides the
// sslocal.StmtStatsIterator's Cur() method to populate the aggregated_ts
// and aggregation_interval fields on the returning
// appstatspb.CollectedStatementStatistics.
type memStmtStatsIterator struct {
	sslocal.StmtStatsIterator
	aggregatedTs time.Time
	aggInterval  time.Duration
}

func newMemStmtStatsIterator(
	stats *sslocal.SQLStats,
	options sqlstats.IteratorOptions,
	aggregatedTS time.Time,
	aggInterval time.Duration,
) memStmtStatsIterator {
	return memStmtStatsIterator{
		StmtStatsIterator: stats.StmtStatsIterator(options),
		aggregatedTs:      aggregatedTS,
		aggInterval:       aggInterval,
	}
}

// Cur calls the m.StmtStatsIterator.Cur() and populates the c.AggregatedTs
// field and c.AggregationInterval field.
func (m *memStmtStatsIterator) Cur() *appstatspb.CollectedStatementStatistics {
	c := m.StmtStatsIterator.Cur()
	c.AggregatedTs = m.aggregatedTs
	c.AggregationInterval = m.aggInterval
	return c
}

// memTxnStatsIterator wraps a sslocal.TxnStatsIterator. Since in-memory
// transaction statistics does not have aggregated_ts and aggregation_interval
// fields populated, memTxnStatsIterator overrides the
// sslocal.TxnStatsIterator's Cur() method to populate the aggregated_ts and
// aggregatoin_interval fields fields on the returning
// appstatspb.CollectedTransactionStatistics.
type memTxnStatsIterator struct {
	sslocal.TxnStatsIterator
	aggregatedTs time.Time
	aggInterval  time.Duration
}

func newMemTxnStatsIterator(
	stats *sslocal.SQLStats,
	options sqlstats.IteratorOptions,
	aggregatedTS time.Time,
	aggInterval time.Duration,
) memTxnStatsIterator {
	return memTxnStatsIterator{
		TxnStatsIterator: stats.TxnStatsIterator(options),
		aggregatedTs:     aggregatedTS,
		aggInterval:      aggInterval,
	}
}

// Cur calls the m.TxnStatsIterator.Cur() and populates the stats.AggregatedTs
// and stats.AggregationInterval fields.
func (m *memTxnStatsIterator) Cur() *appstatspb.CollectedTransactionStatistics {
	stats := m.TxnStatsIterator.Cur()
	stats.AggregatedTs = m.aggregatedTs
	stats.AggregationInterval = m.aggInterval
	return stats
}
