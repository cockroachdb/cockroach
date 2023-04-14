// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

// Return a new failed statement.
func newFailedStmt(stmt *Statement) *Statement {
	newStmt := *stmt
	newStmt.Problem = Problem_FailedExecution
	return &newStmt
}

func TestRegistry(t *testing.T) {
	ctx := context.Background()

	session := Session{ID: clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))}
	transaction := &Transaction{ID: uuid.FastMakeV4()}

	t.Run("slow detection", func(t *testing.T) {
		statement := &Statement{
			Status:           Statement_Completed,
			ID:               clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:    appstatspb.StmtFingerprintID(100),
			LatencyInSeconds: 2,
		}
		expectedStatement :=
			newStmtWithProblemAndCauses(statement, Problem_SlowExecution, nil)
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store)
		registry.ObserveStatement(session.ID, statement)
		registry.ObserveTransaction(session.ID, transaction)

		expected := []*Insight{{
			Session:     session,
			Transaction: transaction,
			Statements:  []*Statement{expectedStatement},
		}}
		var actual []*Insight

		store.IterateInsights(
			context.Background(),
			func(ctx context.Context, o *Insight) {
				actual = append(actual, o)
			},
		)

		require.Equal(t, expected[0], actual[0])
		require.Equal(t, transaction.Status, Transaction_Status(statement.Status))
	})

	t.Run("failure detection", func(t *testing.T) {
		statement := &Statement{
			ID:               clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:    appstatspb.StmtFingerprintID(100),
			LatencyInSeconds: 2,
			Status:           Statement_Failed,
			ErrorCode:        "22012",
		}

		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store)
		registry.ObserveStatement(session.ID, statement)
		registry.ObserveTransaction(session.ID, transaction)

		expected := []*Insight{{
			Session:     session,
			Transaction: transaction,
			Statements: []*Statement{
				newFailedStmt(statement),
			},
		}}
		var actual []*Insight

		store.IterateInsights(
			context.Background(),
			func(ctx context.Context, o *Insight) {
				actual = append(actual, o)
			},
		)

		require.Equal(t, expected, actual)
		require.Equal(t, transaction.LastErrorCode, statement.ErrorCode)
		require.Equal(t, transaction.Status, Transaction_Status(statement.Status))
	})

	t.Run("disabled", func(t *testing.T) {
		statement := &Statement{
			Status:           Statement_Completed,
			ID:               clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:    appstatspb.StmtFingerprintID(100),
			LatencyInSeconds: 2,
		}
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 0)
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store)
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
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		statement2 := &Statement{
			ID:               clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:    appstatspb.StmtFingerprintID(100),
			LatencyInSeconds: 0.5,
		}
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store)
		registry.ObserveStatement(session.ID, statement2)
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

	t.Run("buffering statements per session", func(t *testing.T) {
		statement := &Statement{
			Status:           Statement_Completed,
			ID:               clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:    appstatspb.StmtFingerprintID(100),
			LatencyInSeconds: 2,
		}
		otherSession := Session{ID: clusterunique.IDFromBytes([]byte("cccccccccccccccccccccccccccccccc"))}
		otherTransaction := &Transaction{ID: uuid.FastMakeV4()}
		otherStatement := &Statement{
			ID:               clusterunique.IDFromBytes([]byte("dddddddddddddddddddddddddddddddd")),
			FingerprintID:    appstatspb.StmtFingerprintID(101),
			LatencyInSeconds: 3,
		}

		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store)
		registry.ObserveStatement(session.ID, statement)
		registry.ObserveStatement(otherSession.ID, otherStatement)
		registry.ObserveTransaction(session.ID, transaction)
		registry.ObserveTransaction(otherSession.ID, otherTransaction)

		expected := []*Insight{{
			Session:     session,
			Transaction: transaction,
			Statements: []*Statement{
				newStmtWithProblemAndCauses(statement, Problem_SlowExecution, nil),
			},
		}, {
			Session:     otherSession,
			Transaction: otherTransaction,
			Statements: []*Statement{
				newStmtWithProblemAndCauses(otherStatement, Problem_SlowExecution, nil),
			},
		}}
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

		require.Equal(t, expected, actual)
	})

	t.Run("sibling statements without problems", func(t *testing.T) {
		statement := &Statement{
			Status:           Statement_Completed,
			ID:               clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:    appstatspb.StmtFingerprintID(100),
			LatencyInSeconds: 2,
		}
		siblingStatement := &Statement{
			ID:            clusterunique.IDFromBytes([]byte("dddddddddddddddddddddddddddddddd")),
			FingerprintID: appstatspb.StmtFingerprintID(101),
		}

		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store)
		registry.ObserveStatement(session.ID, statement)
		registry.ObserveStatement(session.ID, siblingStatement)
		registry.ObserveTransaction(session.ID, transaction)

		expected := []*Insight{
			{
				Session:     session,
				Transaction: transaction,
				Statements: []*Statement{
					newStmtWithProblemAndCauses(statement, Problem_SlowExecution, nil),
					siblingStatement,
				},
			},
		}
		var actual []*Insight
		store.IterateInsights(
			context.Background(),
			func(ctx context.Context, o *Insight) {
				actual = append(actual, o)
			},
		)

		require.Equal(t, expected, actual)
		require.Equal(t, transaction.Status, Transaction_Status(statement.Status))
	})

	t.Run("txn with no stmts", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, newStore(st))
		require.NotPanics(t, func() { registry.ObserveTransaction(session.ID, transaction) })
	})

	t.Run("txn with high accumulated contention without high single stmt contention", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store)
		contentionDuration := 10 * time.Second
		statement := &Statement{
			Status:           Statement_Completed,
			ID:               clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:    appstatspb.StmtFingerprintID(100),
			LatencyInSeconds: 0.00001,
		}
		txnHighContention := &Transaction{ID: uuid.FastMakeV4(), Contention: &contentionDuration}

		registry.ObserveStatement(session.ID, statement)
		registry.ObserveTransaction(session.ID, txnHighContention)

		expected := []*Insight{
			{
				Session: session,
				Transaction: &Transaction{
					ID:               txnHighContention.ID,
					Contention:       &contentionDuration,
					StmtExecutionIDs: txnHighContention.StmtExecutionIDs,
					Problems:         []Problem{Problem_SlowExecution},
					Causes:           []Cause{Cause_HighContention}},
				Statements: []*Statement{
					newStmtWithProblemAndCauses(statement, Problem_None, nil),
				},
			},
		}

		var actual []*Insight
		store.IterateInsights(
			context.Background(),
			func(ctx context.Context, o *Insight) {
				actual = append(actual, o)
			},
		)

		require.Equal(t, expected, actual)
		require.Equal(t, transaction.Status, Transaction_Status(statement.Status))
	})

	t.Run("statement that is slow but should be ignored", func(t *testing.T) {
		statementNotIgnored := &Statement{
			Status:           Statement_Completed,
			ID:               clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:    appstatspb.StmtFingerprintID(100),
			LatencyInSeconds: 2,
			Query:            "SELECT * FROM users",
		}
		statementIgnored := &Statement{
			ID:               clusterunique.IDFromBytes([]byte("dddddddddddddddddddddddddddddddd")),
			FingerprintID:    appstatspb.StmtFingerprintID(101),
			LatencyInSeconds: 2,
			Query:            "SET vectorize = '_'",
		}

		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		store := newStore(st)
		registry := newRegistry(st, &latencyThresholdDetector{st: st}, store)
		registry.ObserveStatement(session.ID, statementNotIgnored)
		registry.ObserveStatement(session.ID, statementIgnored)
		registry.ObserveTransaction(session.ID, transaction)

		expected := []*Insight{
			{
				Session:     session,
				Transaction: transaction,
				Statements: []*Statement{
					newStmtWithProblemAndCauses(statementNotIgnored, Problem_SlowExecution, nil),
					statementIgnored,
				},
			},
		}
		var actual []*Insight
		store.IterateInsights(
			context.Background(),
			func(ctx context.Context, o *Insight) {
				actual = append(actual, o)
			},
		)

		require.Equal(t, expected, actual)
		require.Equal(t, transaction.Status, Transaction_Status(statementNotIgnored.Status))
	})
}
