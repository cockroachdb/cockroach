// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
)

// memStmtStatsIterator wraps a sslocal.StmtStatsIterator. Since in-memory
// statement statistics does not have aggregated_ts field populated,
// memStmtStatsIterator overrides the sslocal.StmtStatsIterator's Cur() method
// to populate the aggregated_ts field on the returning
// roachpb.CollectedStatementStatistics.
type memStmtStatsIterator struct {
	*sslocal.StmtStatsIterator
	aggregatedTs time.Time
}

func newMemStmtStatsIterator(
	stats *sslocal.SQLStats, options *sqlstats.IteratorOptions, aggregatedTS time.Time,
) *memStmtStatsIterator {
	return &memStmtStatsIterator{
		StmtStatsIterator: stats.StmtStatsIterator(options),
		aggregatedTs:      aggregatedTS,
	}
}

// Cur calls the m.StmtStatsIterator.Cur() and populates the m.aggregatedTs
// field.
func (m *memStmtStatsIterator) Cur() *roachpb.CollectedStatementStatistics {
	c := m.StmtStatsIterator.Cur()
	c.AggregatedTs = m.aggregatedTs
	return c
}

// memTxnStatsIterator wraps a sslocal.TxnStatsIterator. Since in-memory
// transaction statistics does not have aggregated_ts field populated,
// memTxnStatsIterator overrides the sslocal.TxnStatsIterator's Cur() method
// to populate the aggregated_ts field on the returning
// roachpb.CollectedTransactionStatistics.
type memTxnStatsIterator struct {
	*sslocal.TxnStatsIterator
	aggregatedTs time.Time
}

func newMemTxnStatsIterator(
	stats *sslocal.SQLStats, options *sqlstats.IteratorOptions, aggregatedTS time.Time,
) *memTxnStatsIterator {
	return &memTxnStatsIterator{
		TxnStatsIterator: stats.TxnStatsIterator(options),
		aggregatedTs:     aggregatedTS,
	}
}

// Cur calls the m.TxnStatsIterator.Cur() and populates the m.aggregatedTs
// field.
func (m *memTxnStatsIterator) Cur() *roachpb.CollectedTransactionStatistics {
	stats := m.TxnStatsIterator.Cur()
	stats.AggregatedTs = m.aggregatedTs
	return stats
}
