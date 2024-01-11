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

	"github.com/cockroachdb/cockroach/pkg/obs"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestIngester(t *testing.T) {
	testCases := []struct {
		name             string
		totalTxnInsights int
		observations     []testEvent
		insights         []testEvent
	}{
		{
			name:             "One Session",
			totalTxnInsights: 1,
			observations: []testEvent{
				{sessionID: 1, statementID: 10},
				{sessionID: 1, transactionID: 100},
			},
			insights: []testEvent{
				{sessionID: 1, transactionID: 100, statementID: 10},
			},
		},
		{
			name:             "Interleaved Sessions",
			totalTxnInsights: 2,
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
			st := cluster.MakeTestingClusterSettings()
			store := newStore(st, obs.NoopEventsExporter{})
			ingester := newConcurrentBufferIngester(
				newRegistry(st, &fakeDetector{
					stubEnabled: true,
					stubIsSlow:  true,
				}, store),
			)

			ingester.Start(ctx, stopper)
			for _, e := range tc.observations {
				if e.statementID != 0 {
					ingester.ObserveStatement(e.SessionID(), &Statement{ID: e.StatementID()})
				} else {
					ingester.ObserveTransaction(ctx, e.SessionID(), &Transaction{ID: e.TransactionID()})
				}
			}

			// Wait for the insights to come through.
			require.Eventually(t, func() bool {
				var numInsights int
				store.IterateInsights(ctx, func(context.Context, *Insight) {
					numInsights++
				})
				return numInsights == tc.totalTxnInsights
			}, 1*time.Second, 50*time.Millisecond)

			// See that the insights we were expecting are the ones that
			// arrived. We allow the provider to do whatever it needs to, so
			// long as it can properly match statements with their
			// transactions.
			var actual []testEvent
			store.IterateInsights(ctx, func(ctx context.Context, insight *Insight) {
				for _, s := range insight.Statements {
					actual = append(actual, testEvent{
						sessionID:     insight.Session.ID.Lo,
						transactionID: insight.Transaction.ID.ToUint128().Lo,
						statementID:   s.ID.Lo,
					})
				}
			})

			require.ElementsMatch(t, tc.insights, actual)
		})
	}
}

func TestIngester_Clear(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()

	t.Run("clears buffer", func(t *testing.T) {
		// Initialize ingester, prevent flushes while we fill the buffer using testKnobs.
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		store := newStore(settings, obs.NoopEventsExporter{})
		ingester := newConcurrentBufferIngester(
			newRegistry(settings, &fakeDetector{
				stubEnabled: true,
				stubIsSlow:  true,
			}, store))
		ingester.testKnobs.noFlush = true
		ingester.Start(ctx, stopper)
		// Fill the ingester's buffer with some data, preventing auto flushes while we do so.
		// Make sure we have statements without associated txns, as txns trigger flushes of
		// statements within the lockingRegistry. We want there to be some leftover data within
		// the lockingRegistry so we can assert that it was cleared as well.
		observations := []testEvent{
			{sessionID: 1, statementID: 10},
			{sessionID: 2, statementID: 20},
			{sessionID: 1, statementID: 11},
			{sessionID: 2, statementID: 21},
			{sessionID: 3, statementID: 31}, // No associated txn.
			{sessionID: 4, statementID: 41}, // No associated txn.
			{sessionID: 1, transactionID: 100},
			{sessionID: 2, transactionID: 200},
		}
		for _, o := range observations {
			if o.statementID != 0 {
				ingester.ObserveStatement(o.SessionID(), &Statement{ID: o.StatementID()})
			} else {
				ingester.ObserveTransaction(ctx, o.SessionID(), &Transaction{ID: o.TransactionID()})
			}
		}
		// Verify buffer isn't empty.
		require.NotEqual(t, event{}, ingester.guard.eventBuffer[0])
		// Now call Clear() to verify it clears the buffer and the underlying registry.
		ingester.Clear(ctx)
		require.Equal(t, event{}, ingester.guard.eventBuffer[0])
		require.Empty(t, ingester.registry.statements)
	})
}

func TestIngester_Disabled(t *testing.T) {
	// It's important that we be able to disable all of the insights machinery
	// should something go wrong. Here we peek at the internals of the ingester
	// to make sure it doesn't hold onto any statement or transaction info if
	// the underlying registry is currently disabled.
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	ingester := newConcurrentBufferIngester(newRegistry(st, &fakeDetector{}, newStore(st, obs.NoopEventsExporter{})))
	ingester.ObserveStatement(clusterunique.ID{}, &Statement{})
	ingester.ObserveTransaction(ctx, clusterunique.ID{}, &Transaction{})
	require.Equal(t, event{}, ingester.guard.eventBuffer[0])
}

func TestIngester_DoesNotBlockWhenReceivingManyObservationsAfterShutdown(t *testing.T) {
	// We have seen some tests hanging in CI, implicating this ingester in
	// their goroutine dumps. We reproduce what we think is happening here,
	// observing high volumes of SQL traffic after our consumer has shut down.
	// - https://github.com/cockroachdb/cockroach/issues/87673
	// - https://github.com/cockroachdb/cockroach/issues/88087
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	registry := newRegistry(st, &fakeDetector{stubEnabled: true}, newStore(st, obs.NoopEventsExporter{}))
	ingester := newConcurrentBufferIngester(registry)
	ingester.Start(ctx, stopper)

	// Simulate a shutdown and wait for the consumer of the ingester's channel to stop.
	stopper.Stop(ctx)
	<-stopper.IsStopped()

	// Send a high volume of SQL observations into the ingester.
	done := make(chan struct{})
	go func() {
		// We push enough observations to fill the ingester's channel at least
		// twice. With no consumer of the channel running and no safeguards in
		// place, this operation would block, which would be bad.
		for i := 0; i < 2*bufferSize+1; i++ {
			ingester.ObserveStatement(clusterunique.ID{}, &Statement{})
		}
		done <- struct{}{}
	}()

	select {
	case <-done:
		// Success!
	case <-time.After(time.Second):
		t.Fatal("Did not finish writing observations into the ingester within the expected time; the operation is probably blocked.")
	}
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
