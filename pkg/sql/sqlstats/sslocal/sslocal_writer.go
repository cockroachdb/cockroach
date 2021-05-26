// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sslocal

import (
	"context"
	"errors"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var _ sqlstats.Writer = &appStats{}

// RecordStatement implements sqlstats.Writer interface.
// RecordStatement saves per-statement statistics.
//
// samplePlanDescription can be nil, as these are only sampled periodically
// per unique fingerprint.
// RecordStatement always returns a valid stmtID corresponding to the given
// stmt regardless of whether the statement is actually recorded or not.
//
// If the statement is not actually recorded due to either:
// 1. the memory budget has been exceeded
// 2. the unique statement fingerprint limit has been exceeded
// and error is being returned.
// Note: This error is only related to the operation of recording the statement
// statistics into in-memory structs. It is unrelated to the stmtErr in the
// arguments.
func (a *appStats) RecordStatement(
	ctx context.Context,
	anonymizedStmtStr string,
	samplePlanDescription *roachpb.ExplainTreePlanNode,
	distSQLUsed bool,
	vectorized bool,
	implicitTxn bool,
	fullScan bool,
	automaticRetryCount int,
	numRows int,
	stmtErr error,
	parseLat, planLat, runLat, svcLat, ovhLat float64,
	database string,
	stmtType tree.StatementType,
	bytesRead int64,
	rowsRead int64,
	nodes []int64,
) (roachpb.StmtID, error) {
	createIfNonExistent := true
	// If the statement is below the latency threshold, or stats aren't being
	// recorded we don't need to create an entry in the stmts map for it. We do
	// still need stmtID for transaction level metrics tracking.
	t := sqlstats.StatsCollectionLatencyThreshold.Get(&a.st.SV)
	if !sqlstats.StmtStatsEnable.Get(&a.st.SV) || (t > 0 && t.Seconds() >= svcLat) {
		createIfNonExistent = false
	}

	// Get the statistics object.
	s, statementKey, stmtID, created, throttled := a.getStatsForStmt(
		anonymizedStmtStr, implicitTxn, database,
		stmtErr != nil, createIfNonExistent,
	)

	// This means we have reached the limit of unique fingerprints. We don't
	// record anything and abort the operation.
	if throttled {
		return stmtID, errors.New("unique fingerprint limit has been reached")
	}

	// This statement was below the latency threshold or sql stats aren't being
	// recorded. Either way, we don't need to record anything in the stats object
	// for this statement, though we do need to return the statement ID for
	// transaction level metrics collection.
	if !createIfNonExistent {
		return stmtID, nil
	}

	// Collect the per-statement statistics.
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.data.Count++
	if stmtErr != nil {
		s.mu.data.SensitiveInfo.LastErr = stmtErr.Error()
	}
	// Only update MostRecentPlanDescription if we sampled a new PlanDescription.
	if samplePlanDescription != nil {
		s.mu.data.SensitiveInfo.MostRecentPlanDescription = *samplePlanDescription
		s.mu.data.SensitiveInfo.MostRecentPlanTimestamp = timeutil.Now()
	}
	if automaticRetryCount == 0 {
		s.mu.data.FirstAttemptCount++
	} else if int64(automaticRetryCount) > s.mu.data.MaxRetries {
		s.mu.data.MaxRetries = int64(automaticRetryCount)
	}
	s.mu.data.SQLType = stmtType.String()
	s.mu.data.NumRows.Record(s.mu.data.Count, float64(numRows))
	s.mu.data.ParseLat.Record(s.mu.data.Count, parseLat)
	s.mu.data.PlanLat.Record(s.mu.data.Count, planLat)
	s.mu.data.RunLat.Record(s.mu.data.Count, runLat)
	s.mu.data.ServiceLat.Record(s.mu.data.Count, svcLat)
	s.mu.data.OverheadLat.Record(s.mu.data.Count, ovhLat)
	s.mu.data.BytesRead.Record(s.mu.data.Count, float64(bytesRead))
	s.mu.data.RowsRead.Record(s.mu.data.Count, float64(rowsRead))
	s.mu.data.LastExecTimestamp = timeutil.Now()
	s.mu.data.Nodes = util.CombineUniqueInt64(s.mu.data.Nodes, nodes)
	// Note that some fields derived from tracing statements (such as
	// BytesSentOverNetwork) are not updated here because they are collected
	// on-demand.
	// TODO(asubiotto): Record the aforementioned fields here when always-on
	//  tracing is a thing.
	s.mu.vectorized = vectorized
	s.mu.distSQLUsed = distSQLUsed
	s.mu.fullScan = fullScan
	s.mu.database = database

	if created {
		// stats size + stmtKey size + hash of the statementKey
		estimatedMemoryAllocBytes := s.sizeUnsafe() + statementKey.size() + 8
		a.Lock()
		defer a.Unlock()
		// We attempt to account for all the memory we used. If we have exceeded our
		// memory budget, delete the entry that we just created and report the error.
		if err := a.acc.Grow(ctx, estimatedMemoryAllocBytes); err != nil {
			delete(a.stmts, statementKey)
			return s.ID, err
		}
	}

	return s.ID, nil
}

// RecordStatementExecStats implements sqlstats.Writer interface.
func (a *appStats) RecordStatementExecStats(
	fingerprint string,
	implicitTxn bool,
	database string,
	stmtErr error,
	stats execstats.QueryLevelStats,
) error {
	stmtStats, _, _, _, _ :=
		a.getStatsForStmt(fingerprint, implicitTxn, database, stmtErr != nil, false /* createIfNotExists */)
	if stmtStats == nil {
		return errors.New("stmtStats flushed before execution stats can be recorded")
	}
	stmtStats.recordExecStats(stats)
	return nil
}

// ShouldSaveLogicalPlanDesc implements sqlstats.Writer interface.
func (a *appStats) ShouldSaveLogicalPlanDesc(
	fingerprint string, implicitTxn bool, database string,
) bool {
	stmtStats, _, _, _, _ :=
		a.getStatsForStmt(fingerprint, implicitTxn, database, false /* failed */, false /* createIfNotExists */)
	return a.shouldSaveLogicalPlanDescription(stmtStats)
}

// RecordTransaction implements sqlstats.Writer interface and saves
// per-transaction statistics.
func (a *appStats) RecordTransaction(
	ctx context.Context,
	key sqlstats.TransactionFingerprintID,
	transactionTimeSec float64,
	committed bool,
	implicit bool,
	retryCount int64,
	statementIDs []roachpb.StmtID,
	serviceLat time.Duration,
	retryLat time.Duration,
	commitLat time.Duration,
	numRows int,
	collectedExecStats bool,
	execStats execstats.QueryLevelStats,
	rowsRead int64,
	bytesRead int64,
) error {
	a.recordTransactionHighLevelStats(transactionTimeSec, committed, implicit)

	if !sqlstats.TxnStatsEnable.Get(&a.st.SV) {
		return nil
	}
	// Do not collect transaction statistics if the stats collection latency
	// threshold is set, since our transaction UI relies on having stats for every
	// statement in the transaction.
	t := sqlstats.StatsCollectionLatencyThreshold.Get(&a.st.SV)
	if t > 0 {
		return nil
	}

	// Get the statistics object.
	s, created, throttled := a.getStatsForTxnWithKey(key, statementIDs, true /* createIfNonexistent */)

	if throttled {
		return errors.New("unique fingerprint limit has been reached")
	}

	// Collect the per-transaction statistics.
	s.mu.Lock()
	defer s.mu.Unlock()

	// If we have created a new entry successfully, we check if we have reached
	// the memory limit. If we have, then we delete the newly created entry and
	// return the memory allocation error.
	// If the entry is not created, this means we have reached the limit of unique
	// fingerprints for this app. We also abort the operation and return an error.
	if created {
		estimatedMemAllocBytes :=
			s.sizeUnsafe() + key.Size() + 8 /* hash of transaction key */
		a.Lock()
		if err := a.acc.Grow(ctx, estimatedMemAllocBytes); err != nil {
			delete(a.txns, key)
			a.Unlock()
			return err
		}
		a.Unlock()
	}

	s.mu.data.Count++

	s.mu.data.NumRows.Record(s.mu.data.Count, float64(numRows))
	s.mu.data.ServiceLat.Record(s.mu.data.Count, serviceLat.Seconds())
	s.mu.data.RetryLat.Record(s.mu.data.Count, retryLat.Seconds())
	s.mu.data.CommitLat.Record(s.mu.data.Count, commitLat.Seconds())
	if retryCount > s.mu.data.MaxRetries {
		s.mu.data.MaxRetries = retryCount
	}
	s.mu.data.RowsRead.Record(s.mu.data.Count, float64(rowsRead))
	s.mu.data.BytesRead.Record(s.mu.data.Count, float64(bytesRead))

	if collectedExecStats {
		s.mu.data.ExecStats.Count++
		s.mu.data.ExecStats.NetworkBytes.Record(s.mu.data.ExecStats.Count, float64(execStats.NetworkBytesSent))
		s.mu.data.ExecStats.MaxMemUsage.Record(s.mu.data.ExecStats.Count, float64(execStats.MaxMemUsage))
		s.mu.data.ExecStats.ContentionTime.Record(s.mu.data.ExecStats.Count, execStats.ContentionTime.Seconds())
		s.mu.data.ExecStats.NetworkMessages.Record(s.mu.data.ExecStats.Count, float64(execStats.NetworkMessages))
		s.mu.data.ExecStats.MaxDiskUsage.Record(s.mu.data.ExecStats.Count, float64(execStats.MaxDiskUsage))
	}

	return nil
}

func (a *appStats) recordTransactionHighLevelStats(
	transactionTimeSec float64, committed bool, implicit bool,
) {
	if !sqlstats.TxnStatsEnable.Get(&a.st.SV) {
		return
	}
	a.txnCounts.recordTransactionCounts(transactionTimeSec, committed, implicit)
}
