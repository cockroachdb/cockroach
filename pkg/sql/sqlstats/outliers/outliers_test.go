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

	session := &outliers.Session{ID: clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))}
	transaction := &outliers.Transaction{ID: uuid.FastMakeV4()}
	statement := &outliers.Statement{
		ID:               clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
		FingerprintID:    roachpb.StmtFingerprintID(100),
		LatencyInSeconds: 2,
	}

	t.Run("detection", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		outliers.LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		registry := outliers.New(st, outliers.NewMetrics())
		registry.ObserveStatement(session.ID, statement)
		registry.ObserveTransaction(session.ID, transaction)

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
		registry.ObserveStatement(session.ID, statement)
		registry.ObserveTransaction(session.ID, transaction)

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
		statement2 := &outliers.Statement{
			ID:               clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
			FingerprintID:    roachpb.StmtFingerprintID(100),
			LatencyInSeconds: 0.5,
		}
		registry := outliers.New(st, outliers.NewMetrics())
		registry.ObserveStatement(session.ID, statement2)
		registry.ObserveTransaction(session.ID, transaction)

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
		otherSession := &outliers.Session{ID: clusterunique.IDFromBytes([]byte("cccccccccccccccccccccccccccccccc"))}
		otherTransaction := &outliers.Transaction{ID: uuid.FastMakeV4()}
		otherStatement := &outliers.Statement{
			ID:               clusterunique.IDFromBytes([]byte("dddddddddddddddddddddddddddddddd")),
			FingerprintID:    roachpb.StmtFingerprintID(101),
			LatencyInSeconds: 3,
		}

		st := cluster.MakeTestingClusterSettings()
		outliers.LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
		registry := outliers.New(st, outliers.NewMetrics())
		registry.ObserveStatement(session.ID, statement)
		registry.ObserveStatement(otherSession.ID, otherStatement)
		registry.ObserveTransaction(session.ID, transaction)
		registry.ObserveTransaction(otherSession.ID, otherTransaction)

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
