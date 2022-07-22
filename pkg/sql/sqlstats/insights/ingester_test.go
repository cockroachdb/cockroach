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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			r := &fakeRegistry{enable: true}
			ingester := newConcurrentBufferIngester(r)
			ingester.Start(ctx, stopper)

			for _, e := range tc.events {
				if e.statementID != 0 {
					ingester.ObserveStatement(e.SessionID(), &Statement{ID: e.StatementID()})
				} else {
					ingester.ObserveTransaction(e.SessionID(), &Transaction{ID: e.TransactionID()})
				}
			}

			// Wait for the events to come through.
			require.Eventually(t, func() bool {
				r.mu.RLock()
				defer r.mu.RUnlock()
				return len(r.mu.events) == len(tc.events)
			}, 1*time.Second, 50*time.Millisecond)

			// See that the events we were expecting are the ones that arrived.
			// We allow the ingester to do whatever it needs to, so long as the ordering of statements
			// and transactions for a given session is preserved.
			sort.SliceStable(tc.events, func(i, j int) bool {
				return tc.events[i].sessionID < tc.events[j].sessionID
			})

			sort.SliceStable(r.mu.events, func(i, j int) bool {
				r.mu.Lock()
				defer r.mu.Unlock()
				return r.mu.events[i].sessionID < r.mu.events[j].sessionID
			})

			r.mu.RLock()
			defer r.mu.RUnlock()
			require.EqualValues(t, tc.events, r.mu.events)
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

	mu struct {
		syncutil.RWMutex
		events []testEvent
	}
}

func (r *fakeRegistry) Start(context.Context, *stop.Stopper) {
	// No-op.
}

func (r *fakeRegistry) ObserveStatement(sessionID clusterunique.ID, statement *Statement) {
	// Rebuild the testEvent, so that we can assert on what we saw.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.events = append(r.mu.events, testEvent{
		sessionID:   sessionID.Lo,
		statementID: statement.ID.Lo,
	})
}

func (r *fakeRegistry) ObserveTransaction(sessionID clusterunique.ID, transaction *Transaction) {
	// Rebuild the testEvent, so that we can assert on what we saw.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.events = append(r.mu.events, testEvent{
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
