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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestIngester(t *testing.T) {
	ingesters := []struct {
		name        string
		constructor func(Registry) Registry
	}{
		{"channelIngester", newChannelIngester},
		{"concurrentBufferIngester", newConcurrentBufferIngester},
	}

	tests := []struct {
		name   string
		events []testEvent
	}{
		{
			name: "One Session",
			events: []testEvent{
				testStatement{sessionID: 1, statementID: 10},
				testTransaction{sessionID: 1, transactionID: 100},
			},
		},
		{
			name: "Interleaved Sessions",
			events: []testEvent{
				testStatement{sessionID: 1, statementID: 10},
				testStatement{sessionID: 2, statementID: 20},
				testStatement{sessionID: 1, statementID: 11},
				testStatement{sessionID: 2, statementID: 21},
				testTransaction{sessionID: 1, transactionID: 100},
				testTransaction{sessionID: 2, transactionID: 200},
			},
		},
	}

	for _, ingester := range ingesters {
		t.Run(ingester.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					registry := &fakeRegistry{enable: true}
					ingester := ingester.constructor(registry)

					ctx := context.Background()
					stopper := stop.NewStopper()
					defer stopper.Stop(ctx)
					ingester.Start(ctx, stopper)

					for _, event := range test.events {
						event.Apply(ingester)
					}

					// Wait for the events to come through.
					require.Eventually(t, func() bool {
						return len(registry.events) == len(test.events)
					}, 1*time.Second, 50*time.Millisecond)

					// We allow the ingester to do whatever it needs to, so long as the ordering of statements
					// and transactions for a given session is preserved.
					sort.SliceStable(test.events, func(i, j int) bool {
						return test.events[i].SessionID() < test.events[j].SessionID()
					})
					sort.SliceStable(registry.events, func(i, j int) bool {
						return registry.events[i].SessionID() < registry.events[j].SessionID()
					})

					require.EqualValues(t, test.events, registry.events)
				})
			}
		})
	}
}

func TestIngesterDisabled(t *testing.T) {
	t.Run("channelIngester", func(t *testing.T) {
		registry := newChannelIngester(&fakeRegistry{enable: false})
		registry.ObserveStatement(clusterunique.ID{}, &Statement{})
		registry.ObserveTransaction(clusterunique.ID{}, &Transaction{})
		require.Empty(t, registry.(*channelIngester).events)
	})
}

func BenchmarkIngester(b *testing.B) {
	b.Run("channelIngester", benchmarkIngester(newChannelIngester))
	b.Run("concurrentBufferIngester", benchmarkIngester(newConcurrentBufferIngester))
	//b.Run("multipleConcurrentBufferIngester", benchmarkIngester(newConcurrentBufferIngester))
	b.Run("nullIngester", benchmarkIngester(newNullIngester))
}

func benchmarkIngester(constructor func(Registry) Registry) func(b *testing.B) {
	return func(b *testing.B) {
		ingester := constructor(&fakeRegistry{enable: true})

		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		ingester.Start(ctx, stopper)

		sessionCount := 100
		transactionCount := b.N
		var sessions sync.WaitGroup
		sessions.Add(sessionCount)

		for i := 0; i < sessionCount; i++ {
			sessionID := clusterunique.IDFromBytes(idBytes(int64(i)))
			statement := &Statement{}
			transaction := &Transaction{}
			go func() {
				for j := 0; j < transactionCount; j++ {
					ingester.ObserveStatement(sessionID, statement)
					ingester.ObserveTransaction(sessionID, transaction)
				}
				sessions.Done()
			}()
		}

		sessions.Wait()
	}
}

func BenchmarkConcurrentBufferIngester(b *testing.B) {
	registry := &fakeRegistry{enable: true}
	ingester := newConcurrentBufferIngester(registry)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	ingester.Start(ctx, stopper)

	sessionCount := 100
	transactionCount := b.N
	var sessions sync.WaitGroup
	sessions.Add(sessionCount)

	for i := 0; i < sessionCount; i++ {
		sessionID := clusterunique.IDFromBytes(idBytes(int64(i)))
		statement := &Statement{}
		transaction := &Transaction{}
		go func() {
			for j := 0; j < transactionCount; j++ {
				ingester.ObserveStatement(sessionID, statement)
				ingester.ObserveTransaction(sessionID, transaction)
			}
			sessions.Done()
		}()
	}

	sessions.Wait()

	i := ingester.(*concurrentBufferIngester)
	b.ReportMetric(float64(len(i.guard.block)), "block")
	b.ReportMetric(float64(len(i.sink)), "sink")
	b.ReportMetric(float64(len(registry.events)), "events")
}

type fakeRegistry struct {
	enable bool
	events []testEvent
}

func (r *fakeRegistry) Start(context.Context, *stop.Stopper) {
	// No-op.
}

func (r *fakeRegistry) ObserveStatement(sessionID clusterunique.ID, statement *Statement) {
	r.events = append(r.events, testStatement{
		sessionID:   int64(sessionID.Lo),
		statementID: int64(statement.ID.Lo),
	})
}

func (r *fakeRegistry) ObserveTransaction(sessionID clusterunique.ID, transaction *Transaction) {
	r.events = append(r.events, testTransaction{
		sessionID:     int64(sessionID.Lo),
		transactionID: int64(transaction.ID.ToUint128().Lo),
	})
}

func (r *fakeRegistry) IterateInsights(context.Context, func(context.Context, *Insight)) {
}

func (r *fakeRegistry) enabled() bool {
	return r.enable
}

type testEvent interface {
	Apply(r Registry)
	SessionID() int64
}

type testStatement struct {
	sessionID, statementID int64
}

func (s testStatement) Apply(r Registry) {
	r.ObserveStatement(
		clusterunique.IDFromBytes(idBytes(s.sessionID)),
		&Statement{
			ID: clusterunique.IDFromBytes(idBytes(s.statementID)),
		},
	)
}

func (s testStatement) SessionID() int64 {
	return s.sessionID
}

type testTransaction struct {
	sessionID, transactionID int64
}

func (t testTransaction) Apply(r Registry) {
	r.ObserveTransaction(
		clusterunique.IDFromBytes(idBytes(t.sessionID)),
		&Transaction{
			ID: uuid.FromBytesOrNil(idBytes(t.transactionID)),
		},
	)
}

func (t testTransaction) SessionID() int64 {
	return t.sessionID
}

func idBytes(id int64) []byte {
	return uint128.FromInts(0, uint64(id)).GetBytes()
}

type channelIngester struct {
	events   chan event
	delegate Registry
}

var _ Registry = &channelIngester{}

func (i *channelIngester) Start(ctx context.Context, stopper *stop.Stopper) {
	if err := stopper.RunAsyncTask(ctx, "outliers-channel-ingester", func(ctx context.Context) {
		i.loop(stopper)
	}); err != nil {
		panic(err) // FIXME
	}
}

func (i *channelIngester) ObserveStatement(sessionID clusterunique.ID, statement *Statement) {
	if i.enabled() {
		i.events <- event{sessionID: sessionID, statement: statement}
	}
}

func (i *channelIngester) ObserveTransaction(sessionID clusterunique.ID, transaction *Transaction) {
	if i.enabled() {
		i.events <- event{sessionID: sessionID, transaction: transaction}
	}
}

func (i *channelIngester) IterateInsights(
	ctx context.Context, visitor func(context.Context, *Insight),
) {
	i.delegate.IterateInsights(ctx, visitor)
}

func (i *channelIngester) enabled() bool {
	return i.delegate.enabled()
}

func (i *channelIngester) loop(stopper *stop.Stopper) {
	for {
		select {
		case e := <-i.events:
			if e.transaction != nil {
				i.delegate.ObserveTransaction(e.sessionID, e.transaction)
			} else {
				i.delegate.ObserveStatement(e.sessionID, e.statement)
			}
		case <-stopper.ShouldQuiesce():
			return
		}
	}
}

func newChannelIngester(delegate Registry) Registry {
	return &channelIngester{
		events:   make(chan event, 1000),
		delegate: delegate,
	}
}

// This nullIngester type lets us benchmark an absolute floor for performance.
type nullIngester struct {
}

func (n nullIngester) Start(context.Context, *stop.Stopper) {
	// no-op
}

func (n nullIngester) ObserveStatement(clusterunique.ID, *Statement) {
	// no-op
}

func (n nullIngester) ObserveTransaction(clusterunique.ID, *Transaction) {
	// no-op
}

func (n nullIngester) IterateInsights(context.Context, func(context.Context, *Insight)) {
	// no-op
}

func (n nullIngester) enabled() bool {
	return true
}

func newNullIngester(_ Registry) Registry {
	return &nullIngester{}
}
