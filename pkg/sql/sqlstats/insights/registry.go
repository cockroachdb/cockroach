// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/redact"
)

// This registry is the central object in the insights subsystem. It observes
// statement execution to determine which statements are outliers and
// writes insights into the provided sink.
type lockingRegistry struct {
	statements   map[clusterunique.ID]*statementBuf
	detector     detector
	causes       *causes
	store        *LockingStore
	testingKnobs *TestingKnobs
}

func (r *lockingRegistry) Clear() {
	r.statements = make(map[clusterunique.ID]*statementBuf)
}

func (r *lockingRegistry) ObserveStatement(sessionID clusterunique.ID, statement *Statement) {
	if !r.enabled() {
		return
	}
	b, ok := r.statements[sessionID]
	if !ok {
		b = statementsBufPool.Get().(*statementBuf)
		r.statements[sessionID] = b
	}
	b.append(statement)
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

func (r *lockingRegistry) ObserveTransaction(sessionID clusterunique.ID, transaction *Transaction) {
	if !r.enabled() {
		return
	}
	if transaction.ID.String() == "00000000-0000-0000-0000-000000000000" {
		return
	}
	statements, ok := func() (*statementBuf, bool) {
		statements, ok := r.statements[sessionID]
		if !ok {
			return nil, false
		}
		delete(r.statements, sessionID)
		return statements, true
	}()
	if !ok {
		return
	}
	defer statements.release()

	// Mark statements which are detected as slow or have a failed status.
	var slowOrFailedStatements intsets.Fast
	for i, s := range *statements {
		if !shouldIgnoreStatement(s) && (r.detector.isSlow(s) || isFailed(s)) {
			slowOrFailedStatements.Add(i)
		}
	}

	// So far this is the only case when a transaction is considered slow.
	// In the future, we may want to make a detector for transactions if there
	// are more cases.
	highContention := false
	if transaction.Contention != nil {
		highContention = transaction.Contention.Seconds() >= LatencyThreshold.Get(&r.causes.st.SV).Seconds()
	}

	txnFailed := transaction.Status == Transaction_Failed
	if slowOrFailedStatements.Empty() && !highContention && !txnFailed {
		// We only record an insight if we have slow statements, high txn contention, or failed executions.
		return
	}

	// Note that we'll record insights for every statement, not just for
	// the slow ones.
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
	for i, s := range *statements {
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

// clearSession removes the session from the registry and releases the
// associated statement buffer.
func (r *lockingRegistry) clearSession(sessionID clusterunique.ID) {
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
func (r *lockingRegistry) enabled() bool {
	return r.detector.enabled()
}

func newRegistry(
	st *cluster.Settings, detector detector, store *LockingStore, knobs *TestingKnobs,
) *lockingRegistry {
	return &lockingRegistry{
		statements:   make(map[clusterunique.ID]*statementBuf),
		detector:     detector,
		causes:       &causes{st: st},
		store:        store,
		testingKnobs: knobs,
	}
}
