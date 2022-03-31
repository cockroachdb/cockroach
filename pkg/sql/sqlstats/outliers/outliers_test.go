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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/outliers"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

var (
	sessionID = clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	txnID     = uuid.FastMakeV4()
	stmtID    = clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
)

func TestOutliers(t *testing.T) {
	ctx := context.Background()

	st := cluster.MakeTestingClusterSettings()
	outliers.LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
	registry := outliers.New(st)
	registry.ObserveStatement(sessionID, stmtID, 2)
	registry.ObserveTransaction(sessionID, txnID)

	expected := []*outliers.Outlier{{
		Session: &outliers.Outlier_Session{
			ID: sessionID.GetBytes(),
		},
		Transaction: &outliers.Outlier_Transaction{
			ID: &txnID,
		},
		Statement: &outliers.Outlier_Statement{
			ID:               stmtID.GetBytes(),
			LatencyInSeconds: 2,
		},
	}}
	var actual []*outliers.Outlier

	registry.IterateOutliers(
		context.Background(),
		func(ctx context.Context, o *outliers.Outlier) {
			actual = append(actual, o)
		},
	)

	require.Equal(t, expected, actual)
}

func TestOutliersDisabled(t *testing.T) {
	ctx := context.Background()

	st := cluster.MakeTestingClusterSettings()
	outliers.LatencyThreshold.Override(ctx, &st.SV, 0)
	registry := outliers.New(st)
	registry.ObserveStatement(sessionID, stmtID, 2)
	registry.ObserveTransaction(sessionID, txnID)

	var actual []*outliers.Outlier
	registry.IterateOutliers(
		context.Background(),
		func(ctx context.Context, o *outliers.Outlier) {
			actual = append(actual, o)
		},
	)
	require.Empty(t, actual)
}

func TestOutliersTooFast(t *testing.T) {
	ctx := context.Background()

	st := cluster.MakeTestingClusterSettings()
	outliers.LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
	registry := outliers.New(st)
	registry.ObserveStatement(sessionID, stmtID, 0.5)
	registry.ObserveTransaction(sessionID, txnID)

	var actual []*outliers.Outlier
	registry.IterateOutliers(
		context.Background(),
		func(ctx context.Context, o *outliers.Outlier) {
			actual = append(actual, o)
		},
	)
	require.Empty(t, actual)
}

func TestOutliersConcurrentAccess(t *testing.T) {
	ctx := context.Background()

	otherSessionID := clusterunique.IDFromBytes([]byte("cccccccccccccccccccccccccccccccc"))
	otherTxnID := uuid.FastMakeV4()
	otherStmtID := clusterunique.IDFromBytes([]byte("dddddddddddddddddddddddddddddddd"))

	st := cluster.MakeTestingClusterSettings()
	outliers.LatencyThreshold.Override(ctx, &st.SV, 1*time.Second)
	registry := outliers.New(st)
	registry.ObserveStatement(sessionID, stmtID, 2)
	registry.ObserveStatement(otherSessionID, otherStmtID, 3)
	registry.ObserveTransaction(sessionID, txnID)
	registry.ObserveTransaction(otherSessionID, otherTxnID)

	expected := []*outliers.Outlier{{
		Session: &outliers.Outlier_Session{
			ID: sessionID.GetBytes(),
		},
		Transaction: &outliers.Outlier_Transaction{
			ID: &txnID,
		},
		Statement: &outliers.Outlier_Statement{
			ID:               stmtID.GetBytes(),
			LatencyInSeconds: 2,
		},
	}, {
		Session: &outliers.Outlier_Session{
			ID: otherSessionID.GetBytes(),
		},
		Transaction: &outliers.Outlier_Transaction{
			ID: &otherTxnID,
		},
		Statement: &outliers.Outlier_Statement{
			ID:               otherStmtID.GetBytes(),
			LatencyInSeconds: 3,
		},
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
		return bytes.Compare(actual[i].Session.ID, actual[j].Session.ID) < 0
	})

	require.Equal(t, expected, actual)
}
