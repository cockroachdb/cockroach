// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/redact"
)

// lockingRegistry is the central object in the insights subsystem. It observes
// statement execution to determine which statements are outliers and
// writes insights into the provided sink.
type lockingRegistry struct {
	detector detector
	causes   *causes
	store    *LockingStore
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

// observeTransaction determines if a transaction is slow or failed and records
// an insight if so. It does so by examining the statements in the transaction
// and marking them as slow or failed if they are.
// the given sessionID. The buffer is returned to the pool along with the
// statements it contains.
func (r *lockingRegistry) observeTransaction(
	transactionStats *sqlstats.RecordedTxnStats, statements []*sqlstats.RecordedStmtStats,
) {
	if !r.enabled() {
		return
	}

	if transactionStats.TransactionID.Equal(uuid.Nil) {
		return
	}

	sessionID := transactionStats.SessionID

	// Mark statements which are detected as slow or have a failed status.
	var slowOrFailedStatements intsets.Fast
	for i, s := range statements {
		if !shouldIgnoreStatement(s) && (r.detector.isSlow(s) || s.Failed) {
			slowOrFailedStatements.Add(i)
		}
	}

	// So far this is the only case when a transaction is considered slow.
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
	transaction := makeTxnInsight(transactionStats)
	insight := makeInsight(sessionID, transaction)

	if highContention {
		insight.Transaction.Problems = addProblem(insight.Transaction.Problems, Problem_SlowExecution)
		insight.Transaction.Causes = addCause(insight.Transaction.Causes, Cause_HighContention)
	}

	if txnFailed {
		insight.Transaction.Problems = addProblem(insight.Transaction.Problems, Problem_FailedExecution)
	}

	// The transaction status will reflect the status of its statements; it will
	// default to completed unless a failed statement status is found. Note that
	// this does not take into account the "Cancelled" transaction status.
	var lastStmtErr redact.RedactableString
	var lastStmtErrCode string
	for i, recordedStmt := range statements {
		s := makeStmtInsight(recordedStmt)
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
		// Stmt failure equates to transaction failure. Sometimes we
		// can't propagate the error up to the transaction level so
		// we manually set the transaction's failure info here.
		transaction.LastErrorMsg = lastStmtErr
		transaction.LastErrorCode = lastStmtErrCode
	}

	r.store.addInsight(insight)
}

// TODO(todd):
//
//	Once we can handle sufficient throughput to live on the hot
//	execution path in #81021, we can probably get rid of this external
//	concept of "enabled" and let the detectors just decide for themselves
//	internally.
func (r *lockingRegistry) enabled() bool {
	return r.detector.enabled()
}

func newRegistry(st *cluster.Settings, detector detector, store *LockingStore) *lockingRegistry {
	return &lockingRegistry{
		detector: detector,
		causes:   &causes{st: st},
		store:    store,
	}
}
