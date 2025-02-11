// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sslocal

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// SQLConcurrentBufferIngester amortizes the locking cost of writing to
// the sql stats container concurrently from multiple goroutines.
// Built around contentionutils.ConcurrentBufferGuard.
type SQLConcurrentBufferIngester struct {
	guard struct {
		*contentionutils.ConcurrentBufferGuard
		eventBuffer *eventBuffer
	}

	opts struct {
		// noTimedFlush prevents time-triggered flushes from being scheduled.
		noTimedFlush bool
	}

	sqlStatsSink *SQLStats
	// We buffer statements during the ingestion process by session id.

	statements map[clusterunique.ID]*statementBuf

	insightsRegistry *insights.LockingRegistry

	eventBufferCh chan eventBufChPayload
	clearRegistry uint32

	closeCh      chan struct{}
	testingKnobs *sqlstats.TestingKnobs
}

type eventBufChPayload struct {
	clearRegistry bool
	events        *eventBuffer
}

type statementBuf []*sqlstats.RecordedStmtStats

func (b *statementBuf) append(statement *sqlstats.RecordedStmtStats) {
	*b = append(*b, statement)
}

func (b *statementBuf) release() {
	for i, n := 0, len(*b); i < n; i++ {
		sqlstats.ReleaseStmtStats((*b)[i])
	}
	*b = (*b)[:0]
	statementsBufPool.Put(b)
}

var statementsBufPool = sync.Pool{
	New: func() interface{} {
		return new(statementBuf)
	},
}

// SQLConcurrentBufferIngester buffers the "events" it sees (via ObserveStatement
// and IngestTransaction) and passes them along to the underlying registry
// once its buffer is full. (Or once a timeout has passed, for low-traffic
// clusters and tests.)
//
// The bufferSize was set at 8192 after experimental micro-benchmarking ramping
// up the number of goroutines writing through the ingester concurrently.
// Performance was deemed acceptable under 10,000 concurrent goroutines.
const bufferSize = 8192

type eventBuffer [bufferSize]event

var eventBufferPool = sync.Pool{
	New: func() interface{} { return new(eventBuffer) },
}

type event struct {
	sessionID   clusterunique.ID
	transaction *sqlstats.RecordedTxnStats
	statement   *sqlstats.RecordedStmtStats
}

type BufferOpt func(i *SQLConcurrentBufferIngester)

//// WithoutTimedFlush prevents the SQLConcurrentBufferIngester from performing
//// timed flushes to the underlying registry. Generally only useful for
//// testing purposes.
//func WithoutTimedFlush() BufferOpt {
//	return func(i *SQLConcurrentBufferIngester) {
//		i.opts.noTimedFlush = true
//	}
//}

func (i *SQLConcurrentBufferIngester) Start(
	ctx context.Context, stopper *stop.Stopper, opts ...BufferOpt,
) {
	for _, opt := range opts {
		opt(i)
	}
	_ = stopper.RunAsyncTask(ctx, "sql-stats-ingester", func(ctx context.Context) {

		for {
			select {
			case payload := <-i.eventBufferCh:
				i.ingest(ctx, payload.events) // note that ingest clears the buffer
				if payload.clearRegistry {
					i.Clear()
				}
				eventBufferPool.Put(payload.events)
			case <-stopper.ShouldQuiesce():
				close(i.closeCh)
				return
			}
		}
	})

	if !i.opts.noTimedFlush {
		// This task eagerly flushes partial buffers into the channel, to avoid
		// delays identifying insights in low-traffic clusters and tests.
		_ = stopper.RunAsyncTask(ctx, "sql-stats-ingester-flush", func(ctx context.Context) {
			ticker := time.NewTicker(1 * time.Second)

			for {
				select {
				case <-ticker.C:
					i.guard.ForceSync()
				case <-stopper.ShouldQuiesce():
					ticker.Stop()
					return
				}
			}
		})
	}
}

// Clear flushes the underlying buffer, and signals the underlying registry
// to clear any remaining cached data afterward. This is an async operation.
func (i *SQLConcurrentBufferIngester) Clear() {
	i.guard.ForceSyncExec(func() {
		// Our flush function defined on the guard is responsible for setting clearRegistry back to 0.
		atomic.StoreUint32(&i.clearRegistry, 1)
	})
}

func (i *SQLConcurrentBufferIngester) ingest(ctx context.Context, events *eventBuffer) {
	for idx, e := range events {
		// Because an eventBuffer is a fixed-size array, rather than a slice,
		// we do not know how full it is until we hit a nil entry.
		if e == (event{}) {
			break
		}
		if e.statement != nil {
			i.processStatement(e.statement.SessionID, e.statement)
		} else if e.transaction != nil {
			i.processTransaction(ctx, e.transaction.SessionID, e.transaction)
		} else {
			i.clearSession(e.sessionID)
		}
		events[idx] = event{}
	}
}

func (i *SQLConcurrentBufferIngester) IngestStatement(statement *sqlstats.RecordedStmtStats) {
	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.eventBuffer[writerIdx] = event{
			sessionID: statement.SessionID,
			statement: statement,
		}
	})
}

func (i *SQLConcurrentBufferIngester) IngestTransaction(transaction *sqlstats.RecordedTxnStats) {
	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.eventBuffer[writerIdx] = event{
			transaction: transaction,
		}
	})
}

// ClearSession sends a signal to the underlying registry to clear any cached
// data associated with the given sessionID. This is an async operation.
func (i *SQLConcurrentBufferIngester) ClearSession(sessionID clusterunique.ID) {
	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.eventBuffer[writerIdx] = event{
			sessionID: sessionID,
		}
	})
}

func NewSQLConcurrentBufferIngester(
	sqlStats *SQLStats, insights *insights.LockingRegistry,
) *SQLConcurrentBufferIngester {
	i := &SQLConcurrentBufferIngester{
		// A channel size of 1 is sufficient to avoid unnecessarily
		// synchronizing producer (our clients) and consumer (the underlying
		// registry): moving from 0 to 1 here resulted in a 25% improvement
		// in the micro-benchmarks, but further increases had no effect.
		// Otherwise, we rely solely on the size of the eventBuffer for
		// adjusting our carrying capacity.
		eventBufferCh:    make(chan eventBufChPayload, 2),
		closeCh:          make(chan struct{}),
		statements:       make(map[clusterunique.ID]*statementBuf),
		sqlStatsSink:     sqlStats,
		insightsRegistry: insights,
	}

	i.guard.eventBuffer = eventBufferPool.Get().(*eventBuffer)
	i.guard.ConcurrentBufferGuard = contentionutils.NewConcurrentBufferGuard(
		func() int64 {
			return bufferSize
		},
		func(currentWriterIndex int64) {
			clearRegistry := atomic.LoadUint32(&i.clearRegistry) == 1
			if clearRegistry {
				defer func() {
					atomic.StoreUint32(&i.clearRegistry, 0)
				}()
			}
			select {
			case i.eventBufferCh <- eventBufChPayload{
				clearRegistry: clearRegistry,
				events:        i.guard.eventBuffer,
			}:
			case <-i.closeCh:
			}
			i.guard.eventBuffer = eventBufferPool.Get().(*eventBuffer)
		},
	)
	return i
}

// clearSession removes the session from the registry and releases the
// associated statement buffer.
func (i *SQLConcurrentBufferIngester) clearSession(sessionID clusterunique.ID) {
	if b, ok := i.statements[sessionID]; ok {
		delete(i.statements, sessionID)
		b.release()
	}
}

func (i *SQLConcurrentBufferIngester) processStatement(
	sessionID clusterunique.ID, statement *sqlstats.RecordedStmtStats,
) {
	b, ok := i.statements[sessionID]
	if !ok {
		b = statementsBufPool.Get().(*statementBuf)
		i.statements[sessionID] = b
	}
	b.append(statement)
}

func (i *SQLConcurrentBufferIngester) processTransaction(
	ctx context.Context, sessionID clusterunique.ID, transaction *sqlstats.RecordedTxnStats,
) {
	statements, ok := func() (*statementBuf, bool) {
		statements, ok := i.statements[sessionID]
		if !ok {
			return nil, false
		}
		delete(i.statements, sessionID)
		return statements, true
	}()
	if !ok {
		return
	}
	defer statements.release()

	if len(*statements) == 0 {
		return
	}

	// Write statements.
	appStats := i.sqlStatsSink.GetApplicationStats(transaction.SessionData.ApplicationName)
	// Sum txn fingerprint.
	txnFingerprintID := util.FNV64{}
	txnFingerprintID.Init()
	for i, stmt := range *statements {
		stmt.FingerprintID = appstatspb.ConstructStatementFingerprintID(stmt.Query, stmt.ImplicitTxn, stmt.Database)
		transaction.StatementFingerprintIDs[i] = stmt.FingerprintID
		txnFingerprintID.Add(uint64(stmt.FingerprintID))
	}
	txnFingerprintIDFinal := appstatspb.TransactionFingerprintID(txnFingerprintID.Sum())
	for _, stmt := range *statements {
		stmt.TransactionFingerprintID = txnFingerprintIDFinal
		_, err := appStats.RecordStatement(ctx, stmt)
		if err != nil {
			// todo
		}
	}

	i.insightsRegistry.ObserveTransaction(sessionID, txnFingerprintIDFinal, transaction, *statements)

	if err := appStats.RecordTransaction(ctx, txnFingerprintIDFinal, *transaction); err != nil {
		// todo
	}
}
