// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// Return a new stmt with the added values.
func newStmtWithProblemAndCauses(stmt *Statement, problem Problem, causes []Cause) *Statement {
	newStmt := *stmt
	newStmt.Problem = problem
	newStmt.Causes = causes
	return &newStmt
}

func TestRegistry(t *testing.T) {
	ctx := context.Background()
	session := Session{ID: clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))}

	type rawSQLStats struct {
		sessionID clusterunique.ID
		txn       *sqlstats.RecordedTxnStats
		stmts     []*sqlstats.RecordedStmtStats
	}

	t.Run("slow detection", func(t *testing.T) {
		txns := []rawSQLStats{
			{
				sessionID: session.ID,
				txn: &sqlstats.RecordedTxnStats{
					TransactionID: uuid.MakeV4(),
					Committed:     true,
				},
				stmts: []*sqlstats.RecordedStmtStats{
					{
						StatementID:       clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
						FingerprintID:     appstatspb.StmtFingerprintID(100),
						ServiceLatencySec: 2,
					},
				},
			},
		}
		expectedStatement := &Statement{
			ID:               txns[0].stmts[0].StatementID,
			FingerprintID:    txns[0].stmts[0].FingerprintID,
			LatencyInSeconds: 2,
			Status:           Statement_Completed,
			Problem:          Problem_SlowExecution,
		}
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store, nil)

		registry.ObserveStatement(session.ID, txns[0].stmts[0])
		registry.ObserveTransaction(session.ID, txns[0].txn)

		expected := []*Insight{{
			Session:     session,
			Transaction: makeCompletedTxn(txns[0].txn),
			Statements:  []*Statement{expectedStatement},
		}}
		var actual []*Insight

		store.IterateInsights(
			context.Background(),
			func(ctx context.Context, o *Insight) {
				actual = append(actual, o)
			},
		)

		assertInsightsEqual(t, actual, expected)
	})

	t.Run("failure detection", func(t *testing.T) {
		// Verify that statement error info gets bubbled up to the transaction
		// when the transaction does not have this information.
		txn := &sqlstats.RecordedTxnStats{TransactionID: uuid.MakeV4(), Committed: false}
		stmt := &sqlstats.RecordedStmtStats{
			StatementID:       clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:     appstatspb.StmtFingerprintID(100),
			ServiceLatencySec: 2,
			Failed:            true,
			StatementError:    pgerror.New(pgcode.DivisionByZero, "division by zero"),
		}
		expectedTxnInsight := &Transaction{
			ID:            txn.TransactionID,
			Status:        Transaction_Failed,
			LastErrorCode: pgcode.DivisionByZero.String(),
			LastErrorMsg:  "division by zero",
		}
		expectedStmtInsight := &Statement{
			ID:               clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:    appstatspb.StmtFingerprintID(100),
			LatencyInSeconds: 2,
			Status:           Statement_Failed,
			ErrorCode:        "22012",
			ErrorMsg:         "division by zero",
			Problem:          Problem_FailedExecution,
		}

		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store, nil)
		registry.ObserveStatement(session.ID, stmt)
		// Transaction status is set during transaction stats recorded based on
		// if the transaction committed. We'll inject the failure here to align
		// it with the test. The insights integration tests will verify that this
		// field is set properly.
		registry.ObserveTransaction(session.ID, txn)

		expected := []*Insight{{
			Session:     session,
			Transaction: expectedTxnInsight,
			Statements: []*Statement{
				expectedStmtInsight,
			},
		}}
		var actual []*Insight

		store.IterateInsights(
			context.Background(),
			func(ctx context.Context, o *Insight) {
				actual = append(actual, o)
			},
		)

		assertInsightsEqual(t, actual, expected)
	})

	t.Run("disabled", func(t *testing.T) {
		transaction := &sqlstats.RecordedTxnStats{TransactionID: uuid.MakeV4(), Committed: true}
		statement := &sqlstats.RecordedStmtStats{
			StatementID:       clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:     appstatspb.StmtFingerprintID(100),
			ServiceLatencySec: 2,
		}
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 0)
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store, nil)
		registry.ObserveStatement(session.ID, statement)
		registry.ObserveTransaction(session.ID, transaction)

		var actual []*Insight
		store.IterateInsights(
			context.Background(),
			func(ctx context.Context, o *Insight) {
				actual = append(actual, o)
			},
		)
		require.Empty(t, actual)
	})

	t.Run("too fast", func(t *testing.T) {
		transaction := &sqlstats.RecordedTxnStats{TransactionID: uuid.MakeV4(), Committed: true}
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		stmt := &sqlstats.RecordedStmtStats{
			StatementID:       clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:     appstatspb.StmtFingerprintID(100),
			ServiceLatencySec: 0.5,
		}
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store, nil)
		registry.ObserveStatement(session.ID, stmt)
		registry.ObserveTransaction(session.ID, transaction)

		var actual []*Insight
		store.IterateInsights(context.Background(),
			func(ctx context.Context, o *Insight) {
				actual = append(actual, o)
			},
		)
		require.Empty(t, actual)
	})

	t.Run("buffering statements per session", func(t *testing.T) {
		otherSession := Session{ID: clusterunique.IDFromBytes([]byte("cccccccccccccccccccccccccccccccc"))}

		// 2 transactions with 1 statement each. Both will create an insight,
		// as both statements are over the latency threshold.
		txns := []rawSQLStats{
			{
				sessionID: session.ID,
				txn: &sqlstats.RecordedTxnStats{
					TransactionID: uuid.MakeV4(),
					Committed:     true,
				},
				stmts: []*sqlstats.RecordedStmtStats{
					{
						StatementID:       clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
						FingerprintID:     appstatspb.StmtFingerprintID(100),
						ServiceLatencySec: 2,
					},
				},
			},
			{
				sessionID: otherSession.ID,
				txn: &sqlstats.RecordedTxnStats{
					TransactionID: uuid.MakeV4(),
					Committed:     true,
				},
				stmts: []*sqlstats.RecordedStmtStats{
					{
						StatementID:       clusterunique.IDFromBytes([]byte("dddddddddddddddddddddddddddddddd")),
						FingerprintID:     appstatspb.StmtFingerprintID(101),
						ServiceLatencySec: 3,
					},
				},
			},
		}

		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store, nil)

		expected := []*Insight{{
			Session:     session,
			Transaction: makeCompletedTxn(txns[0].txn),
			Statements: []*Statement{
				{
					ID:               txns[0].stmts[0].StatementID,
					FingerprintID:    txns[0].stmts[0].FingerprintID,
					LatencyInSeconds: txns[0].stmts[0].ServiceLatencySec,
					Status:           Statement_Completed,
					Problem:          Problem_SlowExecution,
				},
			},
		}, {
			Session:     otherSession,
			Transaction: makeCompletedTxn(txns[1].txn),
			Statements: []*Statement{
				{
					ID:               txns[1].stmts[0].StatementID,
					FingerprintID:    txns[1].stmts[0].FingerprintID,
					LatencyInSeconds: txns[1].stmts[0].ServiceLatencySec,
					Status:           Statement_Completed,
					Problem:          Problem_SlowExecution,
				},
			},
		}}

		for _, txn := range txns {
			for _, stmt := range txn.stmts {
				registry.ObserveStatement(txn.sessionID, stmt)
			}
			registry.ObserveTransaction(txn.sessionID, txn.txn)
		}

		var actual []*Insight
		store.IterateInsights(
			context.Background(),
			func(ctx context.Context, o *Insight) {
				actual = append(actual, o)
			},
		)

		// IterateInsights doesn't specify its iteration order, so we sort here for a stable test.
		sort.Slice(actual, func(i, j int) bool {
			return bytes.Compare(actual[i].Session.ID.GetBytes(), actual[j].Session.ID.GetBytes()) < 0
		})

		assertInsightsEqual(t, actual, expected)
	})

	t.Run("sibling statements without problems", func(t *testing.T) {
		transaction := &sqlstats.RecordedTxnStats{TransactionID: uuid.MakeV4(), Committed: true}
		statement := &sqlstats.RecordedStmtStats{
			StatementID:       clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:     appstatspb.StmtFingerprintID(100),
			ServiceLatencySec: 2,
		}
		siblingStatement := &sqlstats.RecordedStmtStats{
			StatementID:   clusterunique.IDFromBytes([]byte("dddddddddddddddddddddddddddddddd")),
			FingerprintID: appstatspb.StmtFingerprintID(101),
		}

		expected := []*Insight{
			{
				Session:     session,
				Transaction: &Transaction{ID: transaction.TransactionID},
				Statements: []*Statement{
					{
						ID:            statement.StatementID,
						FingerprintID: statement.FingerprintID,
						Status:        Statement_Completed,
						Problem:       Problem_SlowExecution,
					},
					{
						ID:            siblingStatement.StatementID,
						FingerprintID: siblingStatement.FingerprintID,
						Status:        Statement_Completed,
					},
				},
			},
		}

		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store, nil)
		registry.ObserveStatement(session.ID, statement)
		registry.ObserveStatement(session.ID, siblingStatement)
		registry.ObserveTransaction(session.ID, transaction)

		var actual []*Insight
		store.IterateInsights(
			context.Background(),
			func(ctx context.Context, o *Insight) {
				actual = append(actual, o)
			},
		)

		assertInsightsEqual(t, actual, expected)
	})

	t.Run("txn with no stmts", func(t *testing.T) {
		transaction := &sqlstats.RecordedTxnStats{TransactionID: uuid.MakeV4(), Committed: true}
		st := cluster.MakeTestingClusterSettings()
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, newStore(st), nil)
		require.NotPanics(t, func() { registry.ObserveTransaction(session.ID, transaction) })
	})

	t.Run("txn with high accumulated contention without high single stmt contention", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store, nil)
		contentionDuration := 10 * time.Second
		statement := &sqlstats.RecordedStmtStats{
			StatementID:       clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:     appstatspb.StmtFingerprintID(100),
			ServiceLatencySec: 0.00001,
		}
		txnHighContention := &sqlstats.RecordedTxnStats{
			Committed:     true,
			TransactionID: uuid.MakeV4(),
			ExecStats: execstats.QueryLevelStats{
				ContentionTime: contentionDuration,
			},
		}

		registry.ObserveStatement(session.ID, statement)
		registry.ObserveTransaction(session.ID, txnHighContention)

		expected := []*Insight{
			{
				Session: session,
				Transaction: &Transaction{
					ID:         txnHighContention.TransactionID,
					Contention: &contentionDuration,
					Problems:   []Problem{Problem_SlowExecution},
					Causes:     []Cause{Cause_HighContention}},
				Statements: []*Statement{
					{
						Status:           Statement_Completed,
						ID:               clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
						FingerprintID:    appstatspb.StmtFingerprintID(100),
						LatencyInSeconds: 0.00001,
					},
				},
			},
		}

		var actual []*Insight
		store.IterateInsights(context.Background(), func(ctx context.Context, o *Insight) {
			actual = append(actual, o)
		},
		)

		assertInsightsEqual(t, actual, expected)
	})

	t.Run("statement that is slow but should be ignored", func(t *testing.T) {
		transaction := &sqlstats.RecordedTxnStats{TransactionID: uuid.MakeV4(), Committed: true}
		stmts := []*sqlstats.RecordedStmtStats{
			// copy the statement objects below:
			{
				StatementID:       clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
				FingerprintID:     appstatspb.StmtFingerprintID(100),
				ServiceLatencySec: 2,
				Query:             "SELECT * FROM users",
			},
			{
				StatementID:       clusterunique.IDFromBytes([]byte("dddddddddddddddddddddddddddddddd")),
				FingerprintID:     appstatspb.StmtFingerprintID(101),
				ServiceLatencySec: 2,
				Query:             "SET vectorize = '_'",
			},
			{
				StatementID:       clusterunique.IDFromBytes([]byte("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")),
				FingerprintID:     appstatspb.StmtFingerprintID(102),
				ServiceLatencySec: 2,
				Query:             "EXPLAIN SELECT * FROM users",
			},
		}

		statementNotIgnored := &Statement{
			Status:           Statement_Completed,
			ID:               clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:    appstatspb.StmtFingerprintID(100),
			LatencyInSeconds: 2,
			Query:            "SELECT * FROM users",
		}
		statementIgnoredSet := &Statement{
			ID:               clusterunique.IDFromBytes([]byte("dddddddddddddddddddddddddddddddd")),
			FingerprintID:    appstatspb.StmtFingerprintID(101),
			LatencyInSeconds: 2,
			Query:            "SET vectorize = '_'",
		}
		statementIgnoredExplain := &Statement{
			ID:               clusterunique.IDFromBytes([]byte("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")),
			FingerprintID:    appstatspb.StmtFingerprintID(102),
			LatencyInSeconds: 2,
			Query:            "EXPLAIN SELECT * FROM users",
		}

		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store, nil)
		for _, s := range stmts {
			registry.ObserveStatement(session.ID, s)
		}
		registry.ObserveTransaction(session.ID, transaction)

		expected := []*Insight{
			{
				Session:     session,
				Transaction: &Transaction{ID: transaction.TransactionID},
				Statements: []*Statement{
					newStmtWithProblemAndCauses(statementNotIgnored, Problem_SlowExecution, nil),
					statementIgnoredSet,
					statementIgnoredExplain,
				},
			},
		}
		var actual []*Insight
		store.IterateInsights(context.Background(), func(ctx context.Context, o *Insight) {
			actual = append(actual, o)
		},
		)

		assertInsightsEqual(t, actual, expected)
	})
}

func TestInsightsRegistry_Clear(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	t.Run("clears cache", func(t *testing.T) {
		// Initialize the registry.
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store, nil)
		// Create some test data.
		sessionA := Session{ID: clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))}
		sessionB := Session{ID: clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))}
		statement := &sqlstats.RecordedStmtStats{
			StatementID:       clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:     appstatspb.StmtFingerprintID(100),
			ServiceLatencySec: 2,
		}
		// Record the test data, assert it's cached.
		registry.ObserveStatement(sessionA.ID, statement)
		registry.ObserveStatement(sessionB.ID, statement)
		expLenStmts := 2
		// No need to acquire the lock here, as the registry is not attached to anything.
		require.Len(t, registry.statements, expLenStmts)
		// Now clear the cache, assert it's cleared.
		registry.Clear()
		require.Empty(t, registry.statements)
	})
}

func TestInsightsRegistry_ClearSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Initialize the registry.
	st := cluster.MakeTestingClusterSettings()
	store := newStore(st)
	registry := newRegistry(st, &latencyThresholdDetector{st: st}, store, nil)

	// Create some test data.
	sessionA := Session{ID: clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))}
	sessionB := Session{ID: clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))}
	statement := &sqlstats.RecordedStmtStats{
		Failed:            false,
		StatementID:       clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
		FingerprintID:     appstatspb.StmtFingerprintID(100),
		ServiceLatencySec: 2,
	}

	// Record the test data, assert it's cached.
	registry.ObserveStatement(sessionA.ID, statement)
	registry.ObserveStatement(sessionB.ID, statement)
	// No need to acquire the lock here, as the registry is not attached to anything.
	require.Len(t, registry.statements, 2)

	// Clear the cache, assert it's cleared.
	registry.clearSession(sessionA.ID)

	// sessionA should be removed, sessionB should still be present.
	b, ok := registry.statements[sessionA.ID]
	require.False(t, ok)
	require.Nil(t, b)
	require.Len(t, registry.statements, 1)
	require.NotEmpty(t, registry.statements[sessionB.ID])
}

func assertInsightsEqual(t *testing.T, actual []*Insight, expected []*Insight) {
	require.Equal(t, len(expected), len(actual))

	for i, insight := range actual {
		require.Equal(t, expected[i].Transaction.ID, insight.Transaction.ID)
		require.Equal(t, expected[i].Session.ID, insight.Session.ID)
		require.Equal(t, expected[i].Transaction.Status, insight.Transaction.Status)
		require.Equal(t, expected[i].Transaction.LastErrorCode, insight.Transaction.LastErrorCode)
		require.Equal(t, expected[i].Transaction.LastErrorMsg, insight.Transaction.LastErrorMsg)
		for j, statement := range insight.Statements {
			require.Equalf(t, expected[i].Statements[j].ID, statement.ID, "statement ids not equal for. stmt1: %v, stmt2: %v", expected[i].Statements[j], statement)
			require.Equal(t, expected[i].Statements[j].Status, statement.Status)
			require.Equal(t, expected[i].Statements[j].Query, statement.Query)
			require.Equal(t, expected[i].Statements[j].Problem, statement.Problem)
		}
	}
}

func makeCompletedTxn(txn *sqlstats.RecordedTxnStats) *Transaction {
	status := Transaction_Failed
	if txn.Committed {
		status = Transaction_Completed
	}
	return &Transaction{
		ID:     txn.TransactionID,
		Status: status,
	}
}
