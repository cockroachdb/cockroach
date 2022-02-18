// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contention

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	eventBatchSize   = 64
	eventChannelSize = 24
)

// eventWriter provides interfaces to write contention event into eventStore.
type eventWriter interface {
	addEvent(contentionpb.ExtendedContentionEvent)
}

// eventReader provides interface to read contention events from eventStore.
type eventReader interface {
	// ForEachEvent executes the callback function on every single contention
	// event. If an error is returned from the callback, the iteration is aborted
	// and the error returned by the callback is bubbled up. The contention event
	// is first copied out from the store before being passed into the callback.
	// This means ForEachEvent is thread-safe.
	ForEachEvent(func(*contentionpb.ExtendedContentionEvent) error) error
}

type timeSource func() time.Time

// eventBatch is used to batch up multiple contention events to amortize the
// cost of acquiring a mutex.
type eventBatch [eventBatchSize]contentionpb.ExtendedContentionEvent

func (b *eventBatch) len() int {
	for i := 0; i < eventBatchSize; i++ {
		if !b[i].Valid() {
			return i
		}
	}
	return eventBatchSize
}

var eventBatchPool = &sync.Pool{
	New: func() interface{} {
		return &eventBatch{}
	},
}

// eventStore is a contention event store that performs asynchronous contention
// event collection. It subsequently resolves the transaction ID reported in the
// contention event into transaction fingerprint ID.
// eventStore relies on two background goroutines:
// 1. intake goroutine: this goroutine is responsible for inserting batched
//    contention events into the in-memory store, and then queue the batched
//    events into the resolver. This means that the contention events can be
//    immediately visible as early as possible to the readers of the eventStore
//    before the txn id resolution is performed.
// 2. resolver goroutine: this goroutine runs on a timer (controlled via
//    sql.contention.event_store.resolution_interval cluster setting).
//    Periodically, the timer fires and resolver attempts to contact remote
//    nodes to resolve the transaction IDs in the queued contention events
//    into transaction fingerprint IDs. If the attempt is successful, the
//    resolver goroutine will update the stored contention events with the
//    transaction fingerprint IDs.
type eventStore struct {
	st *cluster.Settings

	guard struct {
		*contentionutils.ConcurrentBufferGuard

		// buffer is used to store a batch of contention events to amortize the
		// cost of acquiring mutex. It is used in conjunction with the concurrent
		// buffer guard.
		buffer *eventBatch
	}

	eventBatchChan chan *eventBatch
	closeCh        chan struct{}

	resolver resolverQueue

	mu struct {
		syncutil.RWMutex

		// store is the main in-memory FIFO contention event store.
		store *cache.UnorderedCache
	}

	atomic struct {
		// storageSize is used to determine when to start evicting the older
		// contention events.
		storageSize int64
	}

	timeSrc timeSource
}

var (
	_ eventWriter = &eventStore{}
	_ eventReader = &eventStore{}
)

func newEventStore(
	st *cluster.Settings, endpoint ResolverEndpoint, timeSrc timeSource,
) *eventStore {
	s := &eventStore{
		st:             st,
		resolver:       newResolver(endpoint, eventBatchSize /* sizeHint */),
		eventBatchChan: make(chan *eventBatch, eventChannelSize),
		closeCh:        make(chan struct{}),
		timeSrc:        timeSrc,
	}

	s.mu.store = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(_ int, _, _ interface{}) bool {
			capacity := StoreCapacity.Get(&st.SV)
			size := atomic.LoadInt64(&s.atomic.storageSize)
			return size > capacity
		},
		OnEvictedEntry: func(entry *cache.Entry) {
			event := entry.Value.(contentionpb.ExtendedContentionEvent)
			entrySize := int64(entryBytes(&event))
			atomic.AddInt64(&s.atomic.storageSize, -entrySize)
		},
	})

	s.guard.buffer = eventBatchPool.Get().(*eventBatch)
	s.guard.ConcurrentBufferGuard = contentionutils.NewConcurrentBufferGuard(
		func() int64 {
			return eventBatchSize
		}, /* limiter */
		func(_ int64) {
			select {
			case s.eventBatchChan <- s.guard.buffer:
			case <-s.closeCh:
			}
			s.guard.buffer = eventBatchPool.Get().(*eventBatch)
		}, /* onBufferFullSync */
	)

	return s
}

// start runs both background goroutines used by eventStore.
func (s *eventStore) start(ctx context.Context, stopper *stop.Stopper) {
	s.startEventIntake(ctx, stopper)
	s.startResolver(ctx, stopper)
}

func (s *eventStore) startEventIntake(ctx context.Context, stopper *stop.Stopper) {
	handleInsert := func(batch []contentionpb.ExtendedContentionEvent) {
		s.resolver.enqueue(batch)
		s.upsertBatch(batch)
	}

	consumeBatch := func(batch *eventBatch) {
		batchLen := batch.len()
		handleInsert(batch[:batchLen])
		*batch = eventBatch{}
		eventBatchPool.Put(batch)
	}

	if err := stopper.RunAsyncTask(ctx, "contention-event-intake", func(ctx context.Context) {
		for {
			select {
			case batch := <-s.eventBatchChan:
				consumeBatch(batch)
			case <-stopper.ShouldQuiesce():
				close(s.closeCh)
				return
			}
		}
	}); err != nil {
		close(s.closeCh)
	}
}

func (s *eventStore) startResolver(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "contention-event-resolver", func(ctx context.Context) {
		// Handles resolution interval changes.
		var resolutionIntervalChanged = make(chan struct{}, 1)
		TxnIDResolutionInterval.SetOnChange(&s.st.SV, func(ctx context.Context) {
			resolutionIntervalChanged <- struct{}{}
		})

		initialDelay := s.resolutionIntervalWithJitter()
		timer := timeutil.NewTimer()
		timer.Reset(initialDelay)

		for {
			waitInterval := s.resolutionIntervalWithJitter()
			timer.Reset(waitInterval)

			select {
			case <-timer.C:
				if err := s.flushAndResolve(ctx); err != nil {
					if log.V(1) {
						log.Warningf(ctx, "unexpected error encountered when performing "+
							"txn id resolution %s", err)
					}
				}
				timer.Read = true
			case <-resolutionIntervalChanged:
				continue
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// addEvent implements the eventWriter interface.
func (s *eventStore) addEvent(e contentionpb.ExtendedContentionEvent) {
	if TxnIDResolutionInterval.Get(&s.st.SV) == 0 {
		return
	}
	s.guard.AtomicWrite(func(writerIdx int64) {
		e.CollectionTs = s.timeSrc()
		s.guard.buffer[writerIdx] = e
	})
}

// ForEachEvent implements the eventReader interface.
func (s *eventStore) ForEachEvent(
	op func(event *contentionpb.ExtendedContentionEvent) error,
) error {
	// First we read all the keys in the eventStore, and then immediately release
	// the read lock. This is to minimize the time we need to hold the lock. This
	// is important since the op() callback can take arbitrary long to execute,
	// we should not be holding the lock while op() is executing.
	s.mu.RLock()
	keys := make([]uint64, 0, s.mu.store.Len())
	s.mu.store.Do(func(entry *cache.Entry) {
		keys = append(keys, entry.Key.(uint64))
	})
	s.mu.RUnlock()

	for i := range keys {
		event, ok := s.getEventByEventHash(keys[i])
		if !ok {
			// The event might have been evicted between reading the keys and
			// getting the event. In this case we simply ignore it.
			continue
		}
		if err := op(&event); err != nil {
			return err
		}
	}

	return nil
}

func (s *eventStore) getEventByEventHash(
	hash uint64,
) (_ contentionpb.ExtendedContentionEvent, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	event, ok := s.mu.store.Get(hash)
	return event.(contentionpb.ExtendedContentionEvent), ok
}

// flushAndResolve is the main method called by the resolver goroutine each
// time the timer fires. This method does two things:
// 1. it triggers the batching buffer to flush its content into the intake
//    goroutine. This is to ensure that in the case where we have very low
//    rate of contentions, the contention events won't be permanently trapped
//    in the batching buffer.
// 2. it invokes the dequeue() method on the resolverQueue. This cause the
//    resolver to perform txnID resolution. See inline comments on the method
//    for details.
func (s *eventStore) flushAndResolve(ctx context.Context) error {
	// This forces the write-buffer flushes its batch into the intake goroutine.
	// The intake goroutine will asynchronously add all events in the batch
	// into the in-memory store and the resolverQueue.
	s.guard.ForceSync()

	// Since call the ForceSync() is asynchronous, it is possible that the
	// events is not yet available in the resolver queue yet. This is fine,
	// since those events will be processed in the next iteration of the resolver
	// goroutine.
	result, err := s.resolver.dequeue(ctx)

	// Ensure that all the resolved contention events are added to the store
	// before we bubble up the error.
	s.upsertBatch(result)

	return err
}

// upsertBatch update or insert a batch of contention events into the in-memory
// store
func (s *eventStore) upsertBatch(events []contentionpb.ExtendedContentionEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range events {
		blockingTxnID := events[i].BlockingEvent.TxnMeta.ID
		_, ok := s.mu.store.Get(blockingTxnID)
		if !ok {
			atomic.AddInt64(&s.atomic.storageSize, int64(entryBytes(&events[i])))
		}
		s.mu.store.Add(events[i].Hash(), events[i])
	}
}

func (s *eventStore) resolutionIntervalWithJitter() time.Duration {
	baseInterval := TxnIDResolutionInterval.Get(&s.st.SV)

	// Jitter the interval a by +/- 15%.
	frac := 1 + (2*rand.Float64()-1)*0.15
	jitteredInterval := time.Duration(frac * float64(baseInterval.Nanoseconds()))
	return jitteredInterval
}

func entryBytes(event *contentionpb.ExtendedContentionEvent) int {
	// Since we store the event's hash as the key to the unordered cache,
	// this is means we are storing another copy of uint64 (8 bytes).
	return event.Size() + 8
}
