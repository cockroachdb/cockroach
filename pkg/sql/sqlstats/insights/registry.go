// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/redact"
)

// LockingRegistry registry is the central object in the insights subsystem. It observes
// statement execution to determine which statements are outliers and
// writes insights into the provided sink.
type LockingRegistry struct {
	statements   map[clusterunique.ID]*statementBuf
	detector     detector
	causes       *causes
	store        *LockingStore
	testingKnobs *TestingKnobs
}

func (r *LockingRegistry) Clear() {
	r.statements = make(map[clusterunique.ID]*statementBuf)
}

type statementBuf []*Statement

func (b *statementBuf) append(statement *Statement) {
	*b = append(*b, statement)
}

func (b *statementBuf) release() {
	for i, n := 0, len(*b); i < n; i++ {
		(*b)[i] = nil
	}
	*b = (*b)[:0]
	statementsBufPool.Put(b)
}

var statementsBufPool = sync.Pool{
	New: func() interface{} {
		return new(statementBuf)
	},
}

// Instead of creating and allocating a map to track duplicate
// causes each time, we just iterate through the array since
// we don't expect it to be very large.
func addCause(arr []Cause, n Cause) []Cause {
	for i := range arr {
		if arr[i] == n {
			return arr
		}
	}
	return append(arr, n)
}

// Instead of creating and allocating a map to track duplicate
// problems each time, we just iterate through the array since
// we don't expect it to be very large.
func addProblem(arr []Problem, n Problem) []Problem {
	for i := range arr {
		if arr[i] == n {
			return arr
		}
	}
	return append(arr, n)
}

func (r *LockingRegistry) ObserveTransaction(
	sessionID clusterunique.ID,
	txnID appstatspb.TransactionFingerprintID,
	transactionStats *sqlstats.RecordedTxnStats,
	statements []*sqlstats.RecordedStmtStats,
) {
	if !r.enabled() {
		return
	}
	if txnID.String() == "00000000-0000-0000-0000-000000000000" {
		return
	}

	// Mark statements which are detected as slow or have a failed status.
	var slowOrFailedStatements intsets.Fast
	for i, s := range statements {
		if !shouldIgnoreStatement(s) && (r.detector.isSlow(s) || isFailed(s)) {
			slowOrFailedStatements.Add(i)
		}
	}

	// So far this is the only case when a transactionStats is considered slow.
	// In the future, we may want to make a detector for transactions if there
	// are more cases.
	highContention := transactionStats.ExecStats.ContentionTime.Seconds() >= LatencyThreshold.Get(&r.causes.st.SV).Seconds()

	txnFailed := !transactionStats.Committed
	if slowOrFailedStatements.Empty() && !highContention && !txnFailed {
		// We only record an insight if we have slow statements, high txn contention, or failed executions.
		return
	}

	// Note that we'll record insights for every statement, not just for
	// the slow ones.
	transaction := makeTxnInsight(txnID, transactionStats)
	insight := makeInsight(sessionID, transaction)

	if highContention {
		insight.Transaction.Problems = addProblem(insight.Transaction.Problems, Problem_SlowExecution)
		insight.Transaction.Causes = addCause(insight.Transaction.Causes, Cause_HighContention)
	}

	if txnFailed {
		insight.Transaction.Problems = addProblem(insight.Transaction.Problems, Problem_FailedExecution)
	}

	// The transactionStats status will reflect the status of its statements; it will
	// default to completed unless a failed statement status is found. Note that
	// this does not take into account the "Cancelled" transactionStats status.
	var lastStmtErr redact.RedactableString
	var lastStmtErrCode string
	for i, recordedStmt := range statements {
		s := makeStmtInsight(recordedStmt.FingerprintID, recordedStmt)
		if slowOrFailedStatements.Contains(i) {
			switch s.Status {
			case Statement_Completed:
				s.Problem = Problem_SlowExecution
				s.Causes = r.causes.examine(s.Causes, s)
			case Statement_Failed:
				s.Problem = Problem_FailedExecution
				if transaction.LastErrorCode == "" {
					lastStmtErr = s.ErrorMsg
					lastStmtErrCode = s.ErrorCode
				}
			}

			// Bubble up stmt problems and causes.
			for i := range s.Causes {
				insight.Transaction.Causes = addCause(insight.Transaction.Causes, s.Causes[i])
			}
			insight.Transaction.Problems = addProblem(insight.Transaction.Problems, s.Problem)
		}

		insight.Transaction.StmtExecutionIDs = append(insight.Transaction.StmtExecutionIDs, s.ID)
		insight.Statements = append(insight.Statements, s)
	}

	if transaction.LastErrorCode == "" && lastStmtErrCode != "" {
		// Stmt failure equates to transactionStats failure. Sometimes we
		// can't propagate the error up to the transactionStats level so
		// we manually set the transactionStats's failure info here.
		transaction.LastErrorMsg = lastStmtErr
		transaction.LastErrorCode = lastStmtErrCode
	}

	r.store.addInsight(insight)
}

// clearSession removes the session from the registry and releases the
// associated statement buffer.
func (r *LockingRegistry) clearSession(sessionID clusterunique.ID) {
	if b, ok := r.statements[sessionID]; ok {
		delete(r.statements, sessionID)
		b.release()
		if r.testingKnobs != nil && r.testingKnobs.OnSessionClear != nil {
			r.testingKnobs.OnSessionClear(sessionID)
		}
	}
}

// TODO(todd):
//
//	Once we can handle sufficient throughput to live on the hot
//	execution path in #81021, we can probably get rid of this external
//	concept of "enabled" and let the detectors just decide for themselves
//	internally.
func (r *LockingRegistry) enabled() bool {
	return r.detector.enabled()
}

func newRegistry(
	st *cluster.Settings, detector detector, store *LockingStore, knobs *TestingKnobs,
) *LockingRegistry {
	return &LockingRegistry{
		statements:   make(map[clusterunique.ID]*statementBuf),
		detector:     detector,
		causes:       &causes{st: st},
		store:        store,
		testingKnobs: knobs,
	}
}

func makeTxnInsight(
	txnFingerprintID appstatspb.TransactionFingerprintID, value *sqlstats.RecordedTxnStats,
) *Transaction {
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
		FingerprintID:   txnFingerprintID,
		UserPriority:    value.Priority.String(),
		ImplicitTxn:     value.ImplicitTxn,
		Contention:      &value.ExecStats.ContentionTime,
		StartTime:       value.StartTime,
		EndTime:         value.EndTime,
		User:            value.SessionData.User().Normalized(),
		ApplicationName: value.SessionData.ApplicationName,
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

func makeStmtInsight(
	stmtFingerprintID appstatspb.StmtFingerprintID, value *sqlstats.RecordedStmtStats,
) *Statement {
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
		FingerprintID:        stmtFingerprintID,
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
