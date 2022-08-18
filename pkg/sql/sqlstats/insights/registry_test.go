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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestRegistry(t *testing.T) {
	ctx := context.Background()

	session := &Session{ID: clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))}
	transaction := &Transaction{ID: uuid.FastMakeV4()}
	statement := &Statement{
		ID:               clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
		FingerprintID:    roachpb.StmtFingerprintID(100),
		LatencyInSeconds: 2,
	}

	t.Run("detection", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		registry := newRegistry(st, NewMetrics())
		registry.ObserveStatement(session.ID, statement)
		registry.ObserveTransaction(session.ID, transaction)

		expected := []*Insight{{
			Session:     session,
			Transaction: transaction,
			Statement:   statement,
			Problems:    []Problem{Problem_Unknown},
		}}
		var actual []*Insight

		registry.IterateInsights(
			context.Background(),
			func(ctx context.Context, o *Insight) {
				actual = append(actual, o)
			},
		)

		require.Equal(t, expected, actual)
	})

	t.Run("disabled", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 0)
		registry := newRegistry(st, NewMetrics())
		registry.ObserveStatement(session.ID, statement)
		registry.ObserveTransaction(session.ID, transaction)

		var actual []*Insight
		registry.IterateInsights(
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
			FingerprintID:    roachpb.StmtFingerprintID(100),
			LatencyInSeconds: 0.5,
		}
		registry := newRegistry(st, NewMetrics())
		registry.ObserveStatement(session.ID, statement2)
		registry.ObserveTransaction(session.ID, transaction)

		var actual []*Insight
		registry.IterateInsights(
			context.Background(),
			func(ctx context.Context, o *Insight) {
				actual = append(actual, o)
			},
		)
		require.Empty(t, actual)
	})

	t.Run("buffering statements per session", func(t *testing.T) {
		otherSession := &Session{ID: clusterunique.IDFromBytes([]byte("cccccccccccccccccccccccccccccccc"))}
		otherTransaction := &Transaction{ID: uuid.FastMakeV4()}
		otherStatement := &Statement{
			ID:               clusterunique.IDFromBytes([]byte("dddddddddddddddddddddddddddddddd")),
			FingerprintID:    roachpb.StmtFingerprintID(101),
			LatencyInSeconds: 3,
		}

		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		registry := newRegistry(st, NewMetrics())
		registry.ObserveStatement(session.ID, statement)
		registry.ObserveStatement(otherSession.ID, otherStatement)
		registry.ObserveTransaction(session.ID, transaction)
		registry.ObserveTransaction(otherSession.ID, otherTransaction)

		expected := []*Insight{{
			Session:     session,
			Transaction: transaction,
			Statement:   statement,
			Problems:    []Problem{Problem_Unknown},
		}, {
			Session:     otherSession,
			Transaction: otherTransaction,
			Statement:   otherStatement,
			Problems:    []Problem{Problem_Unknown},
		}}
		var actual []*Insight
		registry.IterateInsights(
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

	t.Run("retention", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(ctx, &st.SV, 100*time.Millisecond)
		slow := 2 * LatencyThreshold.Get(&st.SV).Seconds()
		r := newRegistry(st, NewMetrics())

		// With the ExecutionInsightsCapacity set to 5, we retain the 5 most recently-seen insights.
		ExecutionInsightsCapacity.Override(ctx, &st.SV, 5)
		for id := 0; id < 10; id++ {
			observeStatementExecution(r, uint64(id), slow)
		}
		assertInsightStatementIDs(t, r, []uint64{9, 8, 7, 6, 5})

		// Lowering the ExecutionInsightsCapacity requires having a new insight to evict the others.
		ExecutionInsightsCapacity.Override(ctx, &st.SV, 2)
		assertInsightStatementIDs(t, r, []uint64{9, 8, 7, 6, 5})
		observeStatementExecution(r, 10, slow)
		assertInsightStatementIDs(t, r, []uint64{10, 9})
	})
}

func observeStatementExecution(registry Registry, idBase uint64, latencyInSeconds float64) {
	sessionID := clusterunique.ID{Uint128: uint128.FromInts(2, 0)}
	txnID := uuid.FromUint128(uint128.FromInts(1, idBase))
	stmtID := clusterunique.ID{Uint128: uint128.FromInts(0, idBase)}
	registry.ObserveStatement(sessionID, &Statement{ID: stmtID, LatencyInSeconds: latencyInSeconds})
	registry.ObserveTransaction(sessionID, &Transaction{ID: txnID})
}

func assertInsightStatementIDs(t *testing.T, registry Registry, expected []uint64) {
	var actual []uint64
	registry.IterateInsights(context.Background(), func(ctx context.Context, insight *Insight) {
		actual = append(actual, insight.Statement.ID.Lo)
	})
	require.ElementsMatch(t, expected, actual)
}
