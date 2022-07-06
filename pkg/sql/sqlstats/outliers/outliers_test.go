// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package outliers_test

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/outliers"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestOutliers(t *testing.T) {
	ctx := context.Background()

	sessionID := clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	session := &outliers.Session{ID: &sessionID}
	txnID := uuid.FastMakeV4()
	transaction := &outliers.Transaction{ID: &txnID}
	statementID := clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
	statement := &outliers.Statement{
		ID:               &statementID,
		FingerprintID:    roachpb.StmtFingerprintID(100),
		LatencyInSeconds: 2,
	}

	t.Run("detection", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		outliers.LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		registry := outliers.New(st, outliers.NewMetrics())
		registry.ObserveStatement(sessionID, statement)
		registry.ObserveTransaction(sessionID, transaction)

		expected := []*outliers.Outlier{{
			Session:     session,
			Transaction: transaction,
			Statement:   statement,
		}}
		var actual []*outliers.Outlier

		registry.IterateOutliers(
			context.Background(),
			func(ctx context.Context, o *outliers.Outlier) {
				actual = append(actual, o)
			},
		)

		require.Equal(t, expected, actual)
	})

	t.Run("disabled", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		outliers.LatencyThreshold.Override(ctx, &st.SV, 0)
		registry := outliers.New(st, outliers.NewMetrics())
		registry.ObserveStatement(sessionID, statement)
		registry.ObserveTransaction(sessionID, transaction)

		var actual []*outliers.Outlier
		registry.IterateOutliers(
			context.Background(),
			func(ctx context.Context, o *outliers.Outlier) {
				actual = append(actual, o)
			},
		)
		require.Empty(t, actual)
	})

	t.Run("too fast", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		outliers.LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		statement2ID := clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
		statement2 := &outliers.Statement{
			ID:               &statement2ID,
			FingerprintID:    roachpb.StmtFingerprintID(100),
			LatencyInSeconds: 0.5,
		}
		registry := outliers.New(st, outliers.NewMetrics())
		registry.ObserveStatement(sessionID, statement2)
		registry.ObserveTransaction(sessionID, transaction)

		var actual []*outliers.Outlier
		registry.IterateOutliers(
			context.Background(),
			func(ctx context.Context, o *outliers.Outlier) {
				actual = append(actual, o)
			},
		)
		require.Empty(t, actual)
	})

	t.Run("buffering statements per session", func(t *testing.T) {
		otherSessionID := clusterunique.IDFromBytes([]byte("cccccccccccccccccccccccccccccccc"))
		otherSession := &outliers.Session{ID: &otherSessionID}
		otherTxnID := uuid.FastMakeV4()
		otherTransaction := &outliers.Transaction{ID: &otherTxnID}
		otherStatementID := clusterunique.IDFromBytes([]byte("dddddddddddddddddddddddddddddddd"))
		otherStatement := &outliers.Statement{
			ID:               &otherStatementID,
			FingerprintID:    roachpb.StmtFingerprintID(101),
			LatencyInSeconds: 3,
		}

		st := cluster.MakeTestingClusterSettings()
		outliers.LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		registry := outliers.New(st, outliers.NewMetrics())
		registry.ObserveStatement(sessionID, statement)
		registry.ObserveStatement(otherSessionID, otherStatement)
		registry.ObserveTransaction(sessionID, transaction)
		registry.ObserveTransaction(otherSessionID, otherTransaction)

		expected := []*outliers.Outlier{{
			Session:     session,
			Transaction: transaction,
			Statement:   statement,
		}, {
			Session:     otherSession,
			Transaction: otherTransaction,
			Statement:   otherStatement,
		}}
		var actual []*outliers.Outlier
		registry.IterateOutliers(
			context.Background(),
			func(ctx context.Context, o *outliers.Outlier) {
				actual = append(actual, o)
			},
		)

		// IterateOutliers doesn't specify its iteration order, so we sort here for a stable test.
		sort.Slice(actual, func(i, j int) bool {
			return bytes.Compare(actual[i].Session.ID.GetBytes(), actual[j].Session.ID.GetBytes()) < 0
		})

		require.Equal(t, expected, actual)
	})
}
