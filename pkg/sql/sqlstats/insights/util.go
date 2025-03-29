// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/redact"
)

func makeTxnInsight(value *sqlstats.RecordedTxnStats) *Transaction {
	var retryReason string
	if value.AutoRetryReason != nil {
		retryReason = value.AutoRetryReason.Error()
	}

	var cpuSQLNanos int64
	if value.ExecStats.CPUTime.Nanoseconds() >= 0 {
		cpuSQLNanos = value.ExecStats.CPUTime.Nanoseconds()
	}

	var errorCode string
	var errorMsg redact.RedactableString
	if value.TxnErr != nil {
		errorCode = pgerror.GetPGCode(value.TxnErr).String()
		errorMsg = redact.Sprint(value.TxnErr)
	}

	status := Transaction_Failed
	if value.Committed {
		status = Transaction_Completed
	}

	insight := &Transaction{
		ID:              value.TransactionID,
		FingerprintID:   value.FingerprintID,
		UserPriority:    value.Priority.String(),
		ImplicitTxn:     value.ImplicitTxn,
		Contention:      &value.ExecStats.ContentionTime,
		StartTime:       value.StartTime,
		EndTime:         value.EndTime,
		User:            value.UserNormalized,
		ApplicationName: value.Application,
		RowsRead:        value.RowsRead,
		RowsWritten:     value.RowsWritten,
		RetryCount:      value.RetryCount,
		AutoRetryReason: retryReason,
		CPUSQLNanos:     cpuSQLNanos,
		LastErrorCode:   errorCode,
		LastErrorMsg:    errorMsg,
		Status:          status,
	}

	return insight
}

func makeStmtInsight(value *sqlstats.RecordedStmtStats) *Statement {
	var autoRetryReason string
	if value.AutoRetryReason != nil {
		autoRetryReason = value.AutoRetryReason.Error()
	}

	var contention *time.Duration
	var cpuSQLNanos int64
	if value.ExecStats != nil {
		contention = &value.ExecStats.ContentionTime
		cpuSQLNanos = value.ExecStats.CPUTime.Nanoseconds()
	}

	var errorCode string
	var errorMsg redact.RedactableString
	if value.StatementError != nil {
		errorCode = pgerror.GetPGCode(value.StatementError).String()
		errorMsg = redact.Sprint(value.StatementError)
	}

	insight := &Statement{
		ID:                   value.StatementID,
		FingerprintID:        value.FingerprintID,
		LatencyInSeconds:     value.ServiceLatencySec,
		Query:                value.Query,
		Status:               getInsightStatus(value.StatementError),
		StartTime:            value.StartTime,
		EndTime:              value.EndTime,
		FullScan:             value.FullScan,
		PlanGist:             value.PlanGist,
		Retries:              int64(value.AutoRetryCount),
		AutoRetryReason:      autoRetryReason,
		RowsRead:             value.RowsRead,
		RowsWritten:          value.RowsWritten,
		Nodes:                value.Nodes,
		KVNodeIDs:            value.KVNodeIDs,
		Contention:           contention,
		IndexRecommendations: value.IndexRecommendations,
		Database:             value.Database,
		CPUSQLNanos:          cpuSQLNanos,
		ErrorCode:            errorCode,
		ErrorMsg:             errorMsg,
	}

	return insight
}

func getInsightStatus(statementError error) Statement_Status {
	if statementError == nil {
		return Statement_Completed
	}

	return Statement_Failed
}
