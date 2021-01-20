// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tstracker

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestTracker(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := context.Background()
	tr := NewTracker()

	// No requests are evaluating, so LowerBound() returns zero val.
	require.Zero(t, tr.LowerBound())
	tok10 := tr.Track(ctx, 10)
	require.Equal(t, int64(10), tr.LowerBound())

	tok20 := tr.Track(ctx, 20)
	require.Equal(t, int64(10), tr.LowerBound())
	tr.Untrack(ctx, tok10)
	require.Equal(t, int64(20), tr.LowerBound())

	tok30 := tr.Track(ctx, 30)
	tok25 := tr.Track(ctx, 25)
	require.Equal(t, int64(20), tr.LowerBound())
	tr.Untrack(ctx, tok20)
	require.Equal(t, int64(25), tr.LowerBound())
	tr.Untrack(ctx, tok25)
	require.Equal(t, int64(25), tr.LowerBound())
	tr.Untrack(ctx, tok30)
	require.Zero(t, tr.LowerBound())
}

// Test the tracker by throwing random requests at it. We verify that, at all
// times, Tracker.LowerBound()'s error is small (i.e. the lower bound is not
// much lower than the lowest timestamp at which a request is currently
// evaluating).
func TestTrackerRandomStress(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := context.Background()
	tr := NewTracker()

	// The test takes a few seconds (configured below).
	skip.UnderShort(t)

	// How many producers?
	const numProducers = 10
	// How many outstanding requests can each producer have?
	const maxConcurrentRequestsPerProducer = 50
	// Maximum evaluation duration for a request. Each request will evaluate for a
	// random duration with this upper bound.
	const maxReqDurationMillis = 5
	// Maximum time in the past that a request can evaluate at. Each request will
	// evaluate at a timestamp in the past, with this lower bound.
	const maxReqTrailingMillis = 10
	// How long will this test take?
	const testDurationMillis = 5000

	// Maximum tolerated error between what the tracker says is the lower bound of
	// currently evaluated request is, and what the reality was. This is on the
	// order of the maximum time it takes a request to evaluate.
	const maxTrackerErrorMillis = int64(10)

	// We'll generate requests at random timestamps on multiple producer
	// goroutines. The test will keep track of what requests are currently
	// evaluating, so it can check the tracker's responses.
	// At the same time, there a consumer goroutine (taking an exclusive lock),
	// and a checker goroutine.
	stopT := time.After(testDurationMillis * time.Millisecond)
	stop := make(chan struct{})
	// Adapt the timer channel to a channel of struct{}.
	go func() {
		<-stopT
		close(stop)
	}()

	// This mutex is protecting the Tracker. Producers will lock it in read mode
	// and consumers in write mode. This matches how the ProposalBuffer
	// synchronizes access to the Tracker.
	var mu syncutil.RWMutex
	var rs requestsCollection

	g := ctxgroup.WithContext(ctx)

	for i := 0; i < numProducers; i++ {
		p := makeRequestProducer(
			stop, mu.RLocker(),
			maxReqDurationMillis, maxReqTrailingMillis, maxConcurrentRequestsPerProducer,
			tr, &rs)
		g.GoCtx(func(ctx context.Context) error {
			p.run(ctx)
			return nil
		})
	}

	c := makeRequestConsumer(stop, &mu, tr, &rs)
	g.GoCtx(func(ctx context.Context) error {
		c.run(ctx)
		return nil
	})

	checker := makeTrackerChecker(stop, &mu, tr, &rs)
	g.GoCtx(checker.run)

	<-stop
	require.NoError(t, g.Wait())

	for _, req := range rs.mu.rs {
		tr.Untrack(ctx, req.tok)
	}
	require.Zero(t, tr.LowerBound())

	maxOvershotMillis := checker.maxOvershotNanos / 1000000
	require.Lessf(t, maxOvershotMillis, maxTrackerErrorMillis,
		"maximum tracker lowerbound error was %dms, above maximum tolerated %dms",
		maxOvershotMillis, maxTrackerErrorMillis)
	log.Infof(ctx, "maximum lower bound error: %dms", maxOvershotMillis)
}

// requests is a collection of requests, ordered by finish time (not by
// evaluation time).
//
// Most, but not all, methods are thread-safe.
type requestsCollection struct {
	mu struct {
		syncutil.Mutex
		rs requestsHeap
	}
}

type requestsHeap []request

var _ heap.Interface = &requestsHeap{}

func (rs *requestsCollection) Insert(r request) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	heap.Push(&rs.mu.rs, r)
}

func (rs *requestsCollection) Len() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.mu.rs.Len()
}

// all returns the inner requests. This cannot be called concurrently with any
// other methods; the caller must coordinate exclusive access to the requests.
func (rs *requestsCollection) all() []request {
	return rs.mu.rs
}

// PopMin removes the first request (i.e. the request with the lowest finish
// time) from the collection.
func (rs *requestsCollection) PopMin() request {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return heap.Pop(&rs.mu.rs).(request)
}

// PeekFirstFinish returns the timestamp when the first request scheduled to
// finish will finish. It is illegal to call this without ensuring that there's
// at least one request in the collection.
func (rs *requestsCollection) PeekFirstFinish() time.Time {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.mu.rs[0].finish
}

// Len is part of heap.Interface.
func (rs requestsHeap) Len() int {
	return len(rs)
}

// Less is part of heap.Interface.
func (rs requestsHeap) Less(i, j int) bool {
	return rs[i].finish.Before(rs[j].finish)
}

// Swap is part of heap.Interface.
func (rs requestsHeap) Swap(i, j int) {
	r := rs[i]
	rs[i] = rs[j]
	rs[j] = r
}

// Push is part of heap.Interface.
func (rs *requestsHeap) Push(x interface{}) {
	r := x.(request)
	*rs = append(*rs, r)
}

// Pop is part of heap.Interface.
func (rs *requestsHeap) Pop() interface{} {
	r := (*rs)[len(*rs)-1]
	*rs = (*rs)[0 : len(*rs)-1]
	return r
}

// requestProducer is an actor that constantly starts tracking requests
// until signaled to stop. It doesn't untrack any of the requests.
//
// Requests are tracked in both the Tracker and in a requestsCollection.
type requestProducer struct {
	reqMaxDurationMillis int
	reqMaxTrailingMillis int

	stop <-chan struct{}
	// semaphore enforcing a maximum number of concurrent requests.
	sem chan struct{}
	mu  struct {
		sync.Locker
		t *Tracker
	}
	requests *requestsCollection
}

type request struct {
	// The time at which the request is scheduled to finish evaluation.
	finish time.Time
	// The time at which the request is writing.
	wtsNanos int64

	// The semaphore to release when the request is completed.
	sem chan struct{}
	// The tok used to untrack the request.
	tok Token
}

// release signals the producer that produced this request.
func (r request) release() {
	<-r.sem
}

func makeRequestProducer(
	stop <-chan struct{},
	mu sync.Locker,
	maxReqDurationMillis int,
	maxReqTrailingMillis int,
	maxConcurrentRequest int,
	t *Tracker,
	rs *requestsCollection,
) requestProducer {
	p := requestProducer{
		reqMaxDurationMillis: maxReqDurationMillis,
		reqMaxTrailingMillis: maxReqTrailingMillis,
		stop:                 stop,
		sem:                  make(chan struct{}, maxConcurrentRequest),
		requests:             rs,
	}
	p.mu.Locker = mu
	p.mu.t = t
	return p
}

func (p *requestProducer) wait() bool {
	select {
	case <-p.stop:
		return false
	case p.sem <- struct{}{}:
		return true
	}
}

func (p *requestProducer) run(ctx context.Context) {
	for {
		if !p.wait() {
			return
		}
		p.mu.Lock()

		reqDurationMillis := 1 + rand.Intn(p.reqMaxDurationMillis)
		reqEndTime := timeutil.Now().Add(time.Duration(reqDurationMillis) * time.Millisecond)
		wtsTrailMillis := rand.Intn(p.reqMaxTrailingMillis)
		wts := timeutil.Now().UnixNano() - (int64(wtsTrailMillis) * 1000000)

		tok := p.mu.t.Track(ctx, wts)
		req := request{
			finish:   reqEndTime,
			wtsNanos: wts,
			sem:      p.sem,
			tok:      tok,
		}
		p.requests.Insert(req)

		p.mu.Unlock()
	}
}

type requestConsumer struct {
	stop <-chan struct{}
	mu   struct {
		sync.Locker
		t        *Tracker
		requests *requestsCollection
	}
}

func makeRequestConsumer(
	stop <-chan struct{}, mu sync.Locker, t *Tracker, rs *requestsCollection,
) requestConsumer {
	c := requestConsumer{
		stop: stop,
	}
	c.mu.Locker = mu
	c.mu.t = t
	c.mu.requests = rs
	return c
}

func (c *requestConsumer) run(ctx context.Context) {
	for {
		select {
		case <-c.stop:
			return
		case <-time.After(100 * time.Microsecond):
		}
		c.mu.Lock()
		for c.mu.requests.Len() > 0 && c.mu.requests.PeekFirstFinish().Before(timeutil.Now()) {
			req := c.mu.requests.PopMin()
			c.mu.t.Untrack(ctx, req.tok)
			req.release()
		}
		c.mu.Unlock()
	}
}

type trackerChecker struct {
	stop <-chan struct{}
	mu   struct {
		sync.Locker
		t        *Tracker
		requests *requestsCollection
	}
	maxOvershotNanos int64
}

func makeTrackerChecker(
	stop <-chan struct{}, mu sync.Locker, t *Tracker, rs *requestsCollection,
) trackerChecker {
	checker := trackerChecker{
		stop: stop,
	}
	checker.mu.Locker = mu
	checker.mu.t = t
	checker.mu.requests = rs
	return checker
}

func (c *trackerChecker) run(ctx context.Context) error {
	for {
		select {
		case <-c.stop:
			return nil
		case <-time.After(10 * time.Millisecond):
		}
		c.mu.Lock()
		lbNanos := c.mu.t.LowerBound()
		minEvalTS := int64(math.MaxInt64)
		for _, req := range c.mu.requests.all() {
			if req.wtsNanos < lbNanos {
				c.mu.Unlock()
				return fmt.Errorf("bad lower bound %d > req: %d", lbNanos, req.wtsNanos)
			}
			if req.wtsNanos < minEvalTS {
				minEvalTS = req.wtsNanos
			}
		}
		if c.mu.requests.Len() == 0 {
			minEvalTS = 0
		}
		c.mu.Unlock()

		overshotNanos := minEvalTS - lbNanos
		if c.maxOvershotNanos < overshotNanos {
			c.maxOvershotNanos = overshotNanos
		}
		// log.Infof(ctx, "lower bound error: %dms", overshotNanos/1000000)
	}
}
