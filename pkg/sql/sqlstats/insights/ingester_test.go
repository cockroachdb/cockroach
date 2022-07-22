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
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestIngester(t *testing.T) {
	testCases := []struct {
		name   string
		events []testEvent
	}{
		{
			name: "One Session",
			events: []testEvent{
				{sessionID: 1, statementID: 10},
				{sessionID: 1, transactionID: 100},
			},
		},
		{
			name: "Interleaved Sessions",
			events: []testEvent{
				{sessionID: 1, statementID: 10},
				{sessionID: 2, statementID: 20},
				{sessionID: 1, statementID: 11},
				{sessionID: 2, statementID: 21},
				{sessionID: 1, transactionID: 100},
				{sessionID: 2, transactionID: 200},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			delegate := &fakeRegistry{enable: true}
			ingester := newConcurrentBufferIngester(delegate)
			ingester.Start(ctx, stopper)

			for _, e := range testCase.events {
				if e.statementID != 0 {
					ingester.ObserveStatement(e.SessionID(), &Statement{ID: e.StatementID()})
				} else {
					ingester.ObserveTransaction(e.SessionID(), &Transaction{ID: e.TransactionID()})
				}
			}

			// Wait for the events to come through.
			require.Eventually(t, func() bool {
				return len(delegate.events) == len(testCase.events)
			}, 1*time.Second, 50*time.Millisecond)

			// We allow the ingester to do whatever it needs to, so long as the ordering of statements
			// and transactions for a given session is preserved.
			sort.SliceStable(testCase.events, func(i, j int) bool { return testCase.events[i].sessionID < testCase.events[j].sessionID })
			sort.SliceStable(delegate.events, func(i, j int) bool { return delegate.events[i].sessionID < delegate.events[j].sessionID })

			require.EqualValues(t, testCase.events, delegate.events)
		})
	}
}

func TestIngester_Disabled(t *testing.T) {
	// It's important that we be able to disable all of the insights machinery
	// should something go wrong. Here we peek at the internals of the ingester
	// to make sure it doesn't hold onto any statement or transaction info if
	// the underlying registry is currently disabled.
	ingester := newConcurrentBufferIngester(&fakeRegistry{enable: false})
	ingester.ObserveStatement(clusterunique.ID{}, &Statement{})
	ingester.ObserveTransaction(clusterunique.ID{}, &Transaction{})
	require.Nil(t, ingester.(*concurrentBufferIngester).guard.eventBuffer[0])
}

type fakeRegistry struct {
	enable bool
	events []testEvent
}

func (r *fakeRegistry) Start(context.Context, *stop.Stopper) {
	// No-op.
}

func (r *fakeRegistry) ObserveStatement(sessionID clusterunique.ID, statement *Statement) {
	// Rebuild the testEvent, so that we can assert on what we saw.
	r.events = append(r.events, testEvent{
		sessionID:   sessionID.Lo,
		statementID: statement.ID.Lo,
	})
}

func (r *fakeRegistry) ObserveTransaction(sessionID clusterunique.ID, transaction *Transaction) {
	// Rebuild the testEvent, so that we can assert on what we saw.
	r.events = append(r.events, testEvent{
		sessionID:     sessionID.Lo,
		transactionID: transaction.ID.ToUint128().Lo,
	})
}

func (r *fakeRegistry) IterateInsights(context.Context, func(context.Context, *Insight)) {
	// No-op.
}

func (r *fakeRegistry) enabled() bool {
	return r.enable
}

type testEvent struct {
	sessionID, statementID, transactionID uint64
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
