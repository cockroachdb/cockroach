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
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestIngester(t *testing.T) {
	testCases := []struct {
		name         string
		observations []testEvent
		insights     []testEvent
	}{
		{
			name: "One Session",
			observations: []testEvent{
				{sessionID: 1, statementID: 10},
				{sessionID: 1, transactionID: 100},
			},
			insights: []testEvent{
				{sessionID: 1, transactionID: 100, statementID: 10},
			},
		},
		{
			name: "Interleaved Sessions",
			observations: []testEvent{
				{sessionID: 1, statementID: 10},
				{sessionID: 2, statementID: 20},
				{sessionID: 1, statementID: 11},
				{sessionID: 2, statementID: 21},
				{sessionID: 1, transactionID: 100},
				{sessionID: 2, transactionID: 200},
			},
			insights: []testEvent{
				{sessionID: 1, transactionID: 100, statementID: 10},
				{sessionID: 1, transactionID: 100, statementID: 11},
				{sessionID: 2, transactionID: 200, statementID: 20},
				{sessionID: 2, transactionID: 200, statementID: 21},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			// We use a fakeDetector claiming *every* statement is slow, so
			// that we can assert on the generated insights to make sure all
			// the events came through properly.
			provider := newConcurrentBufferIngester(
				newRegistry(
					cluster.MakeTestingClusterSettings(), &fakeDetector{
						stubEnabled: true,
						stubIsSlow:  true,
					}),
			)

			provider.Start(ctx, stopper)
			writer := provider.Writer()
			reader := provider.Reader()

			for _, e := range tc.observations {
				if e.statementID != 0 {
					writer.ObserveStatement(e.SessionID(), &Statement{ID: e.StatementID()})
				} else {
					writer.ObserveTransaction(e.SessionID(), &Transaction{ID: e.TransactionID()})
				}
			}

			// Wait for the insights to come through.
			require.Eventually(t, func() bool {
				var numInsights int
				reader.IterateInsights(ctx, func(context.Context, *Insight) {
					numInsights++
				})
				return numInsights == len(tc.insights)
			}, 1*time.Second, 50*time.Millisecond)

			// See that the insights we were expecting are the ones that
			// arrived. We allow the provider to do whatever it needs to, so
			// long as it can properly match statements with their
			// transactions.
			var actual []testEvent
			reader.IterateInsights(ctx, func(ctx context.Context, insight *Insight) {
				actual = append(actual, testEvent{
					sessionID:     insight.Session.ID.Lo,
					transactionID: insight.Transaction.ID.ToUint128().Lo,
					statementID:   insight.Statement.ID.Lo,
				})
			})

			require.ElementsMatch(t, tc.insights, actual)
		})
	}
}

func TestIngester_Disabled(t *testing.T) {
	// It's important that we be able to disable all of the insights machinery
	// should something go wrong. Here we peek at the internals of the ingester
	// to make sure it doesn't hold onto any statement or transaction info if
	// the underlying registry is currently disabled.
	ingester := newConcurrentBufferIngester(newRegistry(cluster.MakeTestingClusterSettings(), &fakeDetector{}))
	ingester.ObserveStatement(clusterunique.ID{}, &Statement{})
	ingester.ObserveTransaction(clusterunique.ID{}, &Transaction{})
	require.Nil(t, ingester.guard.eventBuffer[0])
}

type testEvent struct {
	sessionID, transactionID, statementID uint64
}

func (s testEvent) SessionID() clusterunique.ID {
	return clusterunique.ID{Uint128: uint128.FromInts(0, s.sessionID)}
}

func (s testEvent) TransactionID() uuid.UUID {
	return uuid.FromBytesOrNil(uint128.FromInts(0, s.transactionID).GetBytes())
}

func (s testEvent) StatementID() clusterunique.ID {
	return clusterunique.ID{Uint128: uint128.FromInts(0, s.statementID)}
}
