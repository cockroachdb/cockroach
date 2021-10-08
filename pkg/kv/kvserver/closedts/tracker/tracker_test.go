// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracker

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestLockfreeTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tr := NewLockfreeTracker()
	testTracker(ctx, t, tr)
}

func TestHeapTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tr := newHeapTracker()
	testTracker(ctx, t, tr)
}

func testTracker(ctx context.Context, t *testing.T, tr Tracker) {
	ts := func(nanos int64) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: nanos,
		}
	}

	// No requests are evaluating, so LowerBound() returns zero val.
	require.True(t, tr.LowerBound(ctx).IsEmpty())
	tok10 := tr.Track(ctx, ts(10))
	require.Equal(t, int64(10), tr.LowerBound(ctx).WallTime)

	tok20 := tr.Track(ctx, ts(20))
	require.Equal(t, int64(10), tr.LowerBound(ctx).WallTime)
	tr.Untrack(ctx, tok10)
	require.Equal(t, int64(20), tr.LowerBound(ctx).WallTime)

	tok30 := tr.Track(ctx, ts(30))
	tok25 := tr.Track(ctx, ts(25))
	require.Equal(t, int64(20), tr.LowerBound(ctx).WallTime)
	tr.Untrack(ctx, tok20)
	require.Equal(t, int64(25), tr.LowerBound(ctx).WallTime)
	tr.Untrack(ctx, tok25)
	// Here we hackily have different logic for the different trackers. The
	// lockfree one is not accurate, so it returns a lower LowerBound than the
	// heap one.
	if _, ok := tr.(*lockfreeTracker); ok {
		require.Equal(t, int64(25), tr.LowerBound(ctx).WallTime)
	} else {
		require.Equal(t, int64(30), tr.LowerBound(ctx).WallTime)
	}
	tr.Untrack(ctx, tok30)
	require.True(t, tr.LowerBound(ctx).IsEmpty())

	// Check that synthetic timestamps are tracked as such.
	synthTS := hlc.Timestamp{
		WallTime:  10,
		Synthetic: true,
	}
	tok := tr.Track(ctx, synthTS)
	require.Equal(t, synthTS, tr.LowerBound(ctx))
	// Check that after the Tracker is emptied, lowerbounds are not synthetic any
	// more.
	tr.Untrack(ctx, tok)
	tr.Track(ctx, ts(10))
	require.Equal(t, ts(10), tr.LowerBound(ctx))
}

// Test the tracker by throwing random requests at it. We verify that, at all
// times, Tracker.LowerBound()'s error is small (i.e. the lower bound is not
// much lower than the lowest timestamp at which a request is currently
// evaluating).
func TestLockfreeTrackerRandomStress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tr := NewLockfreeTracker().(*lockfreeTracker)

	// The test takes a few seconds (configured below). It's also hard on the CPU.
	skip.UnderShort(t)

	// How long to stress for.
	const testDuration = 2 * time.Second
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

	// We'll generate requests at random timestamps on multiple producer
	// goroutines. The test will keep track of what requests are currently
	// evaluating, so it can check the tracker's responses.
	// At the same time, there a consumer goroutine (taking an exclusive lock),
	// and a checker goroutine.
	stopT := time.After(testDuration)
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
	require.Zero(t, tr.LowerBound(ctx))

	maxOvershotMillis := checker.maxOvershotNanos / 1000000
	// Maximum tolerated error between what the tracker said the lower bound of
	// currently evaluating request was, and what the reality was. This is on the
	// order of the maximum time it takes a request to evaluate. Note that the
	// requestConsumer measure how long "evaluation" actually took; we don't just
	// rely on maxReqDurationMillis. This is in order to make the test resilient
	// to the consumer goroutine being starved for a while. The error is highest
	// when the Tracker's second bucket has gotten to be really deep (covering
	// requests over a long time window). If a consumption step happens then, it
	// will clear out the first bucket and most, but possibly not all, of the
	// second bucket. If something is left in the second bucket, the lower bound
	// error will be large - on the order of a request evaluation time.
	// TODO(andrei): I think the 3x below is too conservative; the maximum error
	// should be on the order of 1x maxEvaluationTime. And yet 2x fails after
	// hours of stressrace on GCE worker (and it failed once on CI stress too).
	// Figure out why.
	maxToleratedErrorMillis := 3 * c.maxEvaluationTime.Milliseconds()
	log.Infof(ctx, "maximum lower bound error: %dms. maximum request evaluation time: %s",
		maxOvershotMillis, c.maxEvaluationTime)
	require.Lessf(t, maxOvershotMillis, maxToleratedErrorMillis,
		"maximum tracker lowerbound error was %dms, above maximum tolerated %dms",
		maxOvershotMillis, maxToleratedErrorMillis)
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
		t *lockfreeTracker
	}
	requests *requestsCollection
}

type request struct {
	// The time when the request was created.
	start time.Time
	// The time at which the request is scheduled to finish evaluation.
	finish time.Time
	// The time at which the request is writing.
	wtsNanos int64

	// The semaphore to release when the request is completed.
	sem chan struct{}
	// The tok used to untrack the request.
	tok RemovalToken
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
	t *lockfreeTracker,
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
		wts := hlc.Timestamp{
			WallTime: timeutil.Now().UnixNano() - (int64(wtsTrailMillis) * 1000000),
		}

		tok := p.mu.t.Track(ctx, wts)
		req := request{
			finish:   reqEndTime,
			start:    timeutil.Now(),
			wtsNanos: wts.WallTime,
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
		t        *lockfreeTracker
		requests *requestsCollection
	}
	// The maximum time a request took from when it was created to when it was
	// consumed.
	maxEvaluationTime time.Duration
}

func makeRequestConsumer(
	stop <-chan struct{}, mu sync.Locker, t *lockfreeTracker, rs *requestsCollection,
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
		var consumed int
		for c.mu.requests.Len() > 0 && c.mu.requests.PeekFirstFinish().Before(timeutil.Now()) {
			req := c.mu.requests.PopMin()
			c.mu.t.Untrack(ctx, req.tok)
			req.release()
			consumed++
			evalTime := timeutil.Now().Sub(req.start)
			if c.maxEvaluationTime < evalTime {
				c.maxEvaluationTime = evalTime
			}
		}
		c.mu.Unlock()
	}
}

type trackerChecker struct {
	stop <-chan struct{}
	mu   struct {
		sync.Locker
		t        *lockfreeTracker
		requests *requestsCollection
	}
	maxOvershotNanos int64
}

func makeTrackerChecker(
	stop <-chan struct{}, mu sync.Locker, t *lockfreeTracker, rs *requestsCollection,
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
		lbNanos := c.mu.t.LowerBound(ctx).WallTime
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
		log.VInfof(ctx, 1, "lower bound error: %dms", overshotNanos/1000000)
	}
}

// Results on go 1.15.5 on a Macbook Pro 2.3 GHz 8-Core Intel Core i9 (16 threads):
//
// BenchmarkTracker       	38833928	        30.9 ns/op
// BenchmarkTracker-2     	14426193	        71.2 ns/op
// BenchmarkTracker-4     	17354930	        61.5 ns/op
// BenchmarkTracker-8     	24115866	        49.7 ns/op
// BenchmarkTracker-16    	24667039	        45.5 ns/op
//
// The drop in throughput from 1 CPU to 2 CPUs mimics what
// happens for a simple RWMutex.RLock/RUnlock pair.
// TODO(andrei): investigate distributed RWMutexes like
// https://github.com/jonhoo/drwmutex.
func BenchmarkLockfreeTracker(b *testing.B) {
	ctx := context.Background()
	benchmarkTracker(ctx, b, NewLockfreeTracker())
}

// Results on go 1.15.5 on a Macbook Pro 2.3 GHz 8-Core Intel Core i9 (16 threads):
//
// BenchmarkHeapTracker       	 4646006	       355 ns/op
// BenchmarkHeapTracker-2     	 4602415	       235 ns/op
// BenchmarkHeapTracker-4     	 5170777	       229 ns/op
// BenchmarkHeapTracker-8     	 4938172	       244 ns/op
// BenchmarkHeapTracker-16    	 4223288	       268 ns/op
func BenchmarkHeapTracker(b *testing.B) {
	ctx := context.Background()
	benchmarkTracker(ctx, b, newHeapTracker())
}

// benchmarkTracker benchmarks a Tracker.
func benchmarkTracker(ctx context.Context, b *testing.B, t Tracker) {
	// This matches what RunParallel does.
	numGoRoutines := runtime.GOMAXPROCS(0)
	toks := make([][]RemovalToken, numGoRoutines)
	for i := range toks {
		toks[i] = make([]RemovalToken, 0, 1000000)
	}
	// Regardless of whether the Tracker being benchmarked
	// is fully thread-safe or not, we need locking to synchronize
	// access to toks above. Insertions can be done under a read lock,
	// consumption is done under a write lock.
	var mu syncutil.RWMutex

	// Run a consumer goroutine that periodically consumes everything.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Microsecond):
			}

			// Consume all the requests.
			mu.Lock()
			var n int
			for i := range toks {
				n += len(toks[i])
				for _, tok := range toks[i] {
					t.Untrack(ctx, tok)
					// Throw in a call to LowerBound per request. This matches the propBuf
					// use.
					t.LowerBound(ctx)
				}
				toks[i] = toks[i][:0]
			}
			mu.Unlock()
			log.VInfof(ctx, 1, "cleared %d reqs", n)
		}
	}()

	var goroutineID int32 // atomic
	b.RunParallel(func(b *testing.PB) {
		myid := atomic.AddInt32(&goroutineID, 1)
		myid-- // go to 0-based index
		i := hlc.Timestamp{}
		for b.Next() {
			i.WallTime++
			mu.RLock()
			tok := t.Track(ctx, i)
			toks[myid] = append(toks[myid], tok)
			mu.RUnlock()
		}
	})
	close(stop)
}
