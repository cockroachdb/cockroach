// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowcontroller

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Aliases to make the code below slightly easier to read.
const regular, elastic = admissionpb.RegularWorkClass, admissionpb.ElasticWorkClass

// Controller is a concrete implementation of the kvflowcontrol.Controller
// interface. It provides flow control for replication traffic in KV and is
// typically held at the node-level.
type Controller struct {
	mu struct {
		syncutil.Mutex
		// Token limit per work class, tracking
		// kvadmission.flow_controller.{regular,elastic}_tokens_per_stream.
		limit tokensPerWorkClass

		// We maintain flow token buckets for {regular,elastic} work along each
		// stream. This is lazily instantiated. mu is held wen adding to the map.
		// Readers only need to hold mu, if they don't want to miss a concurrently
		// added entry.
		//
		// TODO(irfansharif): Sort out the GC story for these buckets. When
		// streams get closed permanently (tenants get deleted, nodes removed)
		// or when completely inactive (no tokens deducted/returned over 30+
		// minutes), clear these out.
		buckets     syncutil.Map[kvflowcontrol.Stream, bucket]
		bucketCount int
	}
	metrics  *metrics
	clock    *hlc.Clock
	settings *cluster.Settings
}

var _ kvflowcontrol.Controller = &Controller{}

// New constructs a new Controller.
func New(registry *metric.Registry, settings *cluster.Settings, clock *hlc.Clock) *Controller {
	c := &Controller{
		clock:    clock,
		settings: settings,
	}

	regularTokens := kvflowcontrol.Tokens(kvflowcontrol.RegularTokensPerStream.Get(&settings.SV))
	elasticTokens := kvflowcontrol.Tokens(kvflowcontrol.ElasticTokensPerStream.Get(&settings.SV))
	c.mu.limit = tokensPerWorkClass{
		regular: regularTokens,
		elastic: elasticTokens,
	}
	onChangeFunc := func(ctx context.Context) {
		c.mu.Lock()
		defer c.mu.Unlock()

		before := c.mu.limit
		now := tokensPerWorkClass{
			regular: kvflowcontrol.Tokens(kvflowcontrol.RegularTokensPerStream.Get(&settings.SV)),
			elastic: kvflowcontrol.Tokens(kvflowcontrol.ElasticTokensPerStream.Get(&settings.SV)),
		}
		adjustment := tokensPerWorkClass{
			regular: now.regular - before.regular,
			elastic: now.elastic - before.elastic,
		}
		c.mu.limit = now
		c.mu.buckets.Range(func(_ kvflowcontrol.Stream, b *bucket) bool {
			// NB: We're holding the controller mutex here, which guards against
			// new buckets being added, synchronization we don't get out of
			// syncutil.Map.Range() directly.
			adj, _ := b.adjust(
				ctx, admissionpb.RegularWorkClass, adjustment.regular, now, true, c.clock.PhysicalTime())
			if adj.elastic != 0 {
				panic(errors.AssertionFailedf("elastic should not be adjusted"))
			}
			if buildutil.CrdbTestBuild && adj.regular != adjustment.regular {
				panic(errors.AssertionFailedf(
					"unequal adjustments %d != %d", adj.regular, adjustment.regular))
			}
			adj, _ = b.adjust(
				ctx, admissionpb.ElasticWorkClass, adjustment.elastic, now, true, c.clock.PhysicalTime())
			if adj.regular != 0 {
				panic(errors.AssertionFailedf("regular should not be adjusted"))
			}
			if buildutil.CrdbTestBuild && adj.elastic != adjustment.elastic {
				panic(errors.AssertionFailedf(
					"unequal adjustments %d != %d", adj.elastic, adjustment.elastic))
			}
			c.metrics.onTokenAdjustment(adjustment)
			return true
		})
	}
	kvflowcontrol.RegularTokensPerStream.SetOnChange(&settings.SV, onChangeFunc)
	kvflowcontrol.ElasticTokensPerStream.SetOnChange(&settings.SV, onChangeFunc)
	c.metrics = newMetrics(c)
	registry.AddMetricStruct(c.metrics)
	return c
}

func (c *Controller) mode() kvflowcontrol.ModeT {
	return kvflowcontrol.Mode.Get(&c.settings.SV)
}

// Admit is part of the kvflowcontrol.Controller interface. It blocks until
// there are flow tokens available for replication over the given stream for
// work of the given priority.
func (c *Controller) Admit(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	_ time.Time,
	connection kvflowcontrol.ConnectedStream,
) (bool, error) {
	class := admissionpb.WorkClassFromPri(pri)
	c.metrics.onWaiting(class)

	tstart := c.clock.PhysicalTime()

	// In addition to letting requests through when there are tokens
	// being available, we'll also let them through if we're not
	// applying flow control to their specific work class.
	bypass := c.mode() == kvflowcontrol.ApplyToElastic && class == admissionpb.RegularWorkClass
	waitEndState := waitSuccess
	waited := false
	if !bypass {
		b := c.getBucket(connection.Stream())
		waitEndState, waited = b.wait(ctx, class, connection.Disconnected())
	}
	var waitDuration time.Duration
	if waited {
		waitDuration = c.clock.PhysicalTime().Sub(tstart)
	}
	// Else, did not wait (common case), so waitDuration stays 0.
	// Unconditionally computing the waitDuration as the elapsed time pollutes
	// the wait duration metrics with CPU scheduling artifacts, causing
	// confusion.

	if waitEndState == waitSuccess {
		const formatStr = "admitted request (pri=%s stream=%s wait-duration=%s mode=%s)"
		if waited {
			// Always trace if there is any waiting.
			log.VEventf(ctx, 2, formatStr, pri, connection.Stream(), waitDuration, c.mode())
		} else if log.V(2) {
			log.Infof(ctx, formatStr, pri, connection.Stream(), waitDuration, c.mode())
		}
		// Else, common case, did not wait and logging verbosity is not high.

		c.metrics.onAdmitted(class, waitDuration)
		if bypass {
			return false, nil
		} else {
			return true, nil
		}
	} else if waitEndState == contextCanceled {
		const formatStr = "canceled after waiting (pri=%s stream=%s wait-duration=%s mode=%s)"
		log.VEventf(ctx, 2, formatStr, pri, connection.Stream(), waitDuration, c.mode())
		c.metrics.onErrored(class, waitDuration)
		err := errors.Newf(formatStr, pri, connection.Stream(), waitDuration, c.mode())
		return false, errors.CombineErrors(err, ctx.Err())
	} else {
		log.VEventf(ctx, 2,
			"bypassed as stream disconnected (pri=%s stream=%s wait-duration=%s mode=%s)",
			pri, connection.Stream(), waitDuration, c.mode())
		c.metrics.onBypassed(class, waitDuration)
		return true, nil
	}
	// TODO(irfansharif): Use CreateTime for ordering among waiting
	// requests, integrate it with epoch-LIFO. See I12 from
	// kvflowcontrol/doc.go.
}

// DeductTokens is part of the kvflowcontrol.Controller interface.
func (c *Controller) DeductTokens(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	tokens kvflowcontrol.Tokens,
	stream kvflowcontrol.Stream,
) {
	if tokens < 0 {
		log.Fatalf(ctx, "malformed argument: -ve tokens deducted (pri=%s tokens=%s stream=%s)",
			pri, tokens, stream)
	}
	c.adjustTokens(ctx, pri, -tokens, stream)
}

// ReturnTokens is part of the kvflowcontrol.Controller interface.
func (c *Controller) ReturnTokens(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	tokens kvflowcontrol.Tokens,
	stream kvflowcontrol.Stream,
) {
	if tokens < 0 {
		log.Fatalf(ctx, "malformed argument: -ve tokens returned (pri=%s tokens=%s stream=%s)",
			pri, tokens, stream)
	}
	if tokens == 0 {
		return // nothing to do
	}
	c.adjustTokens(ctx, pri, +tokens, stream)
}

// Inspect is part of the kvflowcontrol.Controller interface.
func (c *Controller) Inspect(ctx context.Context) []kvflowinspectpb.Stream {
	// NB: we are not acquiring c.mu since we don't care about streams that are
	// being concurrently added to the map.
	var streams []kvflowinspectpb.Stream
	c.mu.buckets.Range(func(stream kvflowcontrol.Stream, b *bucket) bool {
		b.mu.RLock()
		streams = append(streams, kvflowinspectpb.Stream{
			TenantID:                   stream.TenantID,
			StoreID:                    stream.StoreID,
			AvailableEvalRegularTokens: int64(b.tokensLocked(regular)),
			AvailableEvalElasticTokens: int64(b.tokensLocked(elastic)),
		})
		b.mu.RUnlock()
		return true
	})
	slices.SortFunc(streams, func(a, b kvflowinspectpb.Stream) int { // for determinism
		return cmp.Or(
			cmp.Compare(a.TenantID.ToUint64(), b.TenantID.ToUint64()),
			cmp.Compare(a.StoreID, b.StoreID),
		)
	})
	return streams
}

// InspectStream is part of the kvflowcontrol.Controller interface.
func (c *Controller) InspectStream(
	_ context.Context, stream kvflowcontrol.Stream,
) kvflowinspectpb.Stream {
	tokens := c.getTokensForStream(stream)
	return kvflowinspectpb.Stream{
		TenantID:                   stream.TenantID,
		StoreID:                    stream.StoreID,
		AvailableEvalRegularTokens: int64(tokens.regular),
		AvailableEvalElasticTokens: int64(tokens.elastic),
	}
}

func (c *Controller) adjustTokens(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	delta kvflowcontrol.Tokens,
	stream kvflowcontrol.Stream,
) {
	class := admissionpb.WorkClassFromPri(pri)

	// TODO(irfansharif,aaditya): Double check that there are no more
	// alloc_objects (for the tokensPerWorkClass instances being bussed around
	// below) when running kv0/enc=false/nodes=3/cpu=96. Do this as part of
	// #104154.
	b := c.getBucket(stream)
	adjustment, unaccounted :=
		b.adjust(ctx, class, delta, c.mu.limit, false, c.clock.PhysicalTime())
	c.metrics.onTokenAdjustment(adjustment)
	if unaccounted.regular > 0 || unaccounted.elastic > 0 {
		c.metrics.onUnaccounted(unaccounted)
	}

	if log.V(2) {
		b.mu.RLock()
		log.Infof(ctx, "adjusted flow tokens (pri=%s stream=%s delta=%s): regular=%s elastic=%s",
			pri, stream, delta, b.tokensLocked(regular), b.tokensLocked(elastic))
		b.mu.RUnlock()
	}
}

func (c *Controller) getBucket(stream kvflowcontrol.Stream) *bucket {
	// NB: syncutil.Map is more expensive CPU wise as per BenchmarkController
	// for reads, ~250ns vs. ~350ns, though better for mutex contention when
	// looking at kv0/enc=false/nodes=3/cpu=9. The syncutil.Map does show up in
	// CPU profiles more prominently though. If we want to go back to it being a
	// mutex-backed map, we could use a read-Lock when trying to read the bucket
	// and then swapping for a write-lock when optionally creating the bucket.
	b, ok := c.mu.buckets.Load(stream)
	if !ok {
		c.mu.Lock()
		var loaded bool
		b, loaded = c.mu.buckets.LoadOrStore(stream, newBucket(c.mu.limit, c.clock.PhysicalTime()))
		if !loaded {
			c.mu.bucketCount += 1
		}
		c.mu.Unlock()
	}
	return b
}

// bucketPerWorkClass is a helper struct for implementing bucket. tokens and
// stats are protected by the mutex in bucket. Operations on the signalCh may
// not be protected by that mutex -- see the comment below.
type bucketPerWorkClass struct {
	wc     admissionpb.WorkClass
	tokens kvflowcontrol.Tokens
	// Waiting requests do so by waiting on signalCh without holding a mutex.
	//
	// Requests first check for available tokens (by acquiring and releasing the
	// mutex), and then wait if tokens for their work class are unavailable. The
	// risk in such waiting after releasing the mutex is the following race:
	// tokens become available after the waiter releases the mutex and before it
	// starts waiting. We handle this race by ensuring that signalCh always has
	// an entry if tokens are available:
	//
	// - Whenever tokens are returned, signalCh is signaled, waking up a single
	//   waiting request. If the request finds no available tokens, it starts
	//   waiting again.
	// - Whenever a request gets admitted, it signals the next waiter if any.
	//
	// So at least one request that observed unavailable tokens will get
	// unblocked, which will in turn unblock others. This turn by turn admission
	// provides some throttling to over-admission since the goroutine scheduler
	// needs to schedule the goroutine that got the entry for it to unblock
	// another. Hopefully, before we over-admit much, some of the scheduled
	// goroutines will complete proposal evaluation and deduct the tokens they
	// need.
	//
	// TODO(irfansharif): Right now we continue forwarding admission grants to
	// request while the available tokens > 0, which can lead to over-admission
	// since deduction is deferred (see testdata/simulation/over_admission).
	//
	// A. One mitigation could be terminating grant forwarding if the
	//    'tentatively deducted tokens' exceeds some amount (say, 16
	//    MiB). When tokens are actually deducted, we'll reduce from
	//    this 'tentatively deducted' count. We can re-signal() on every
	//    actual token deduction where available tokens is still > 0.
	// B. The pathological case with A is when all these tentatively
	//    deducted tokens are due to requests that are waiting on
	//    latches and locks. And the next request that wants to be
	//    admitted is not contending with those latches/locks but gets
	//    stuck behind it anyway. We could instead count the # of
	//    requests that go through this bucket and are also past the
	//    latch/lock acquisition but not yet evaluated, and block if
	//    that count is greater than some small multiple of GOMAXPROCS.

	signalCh chan struct{}
	stats    struct {
		deltaStats
		noTokenStartTime time.Time
	}
}

type deltaStats struct {
	noTokenDuration time.Duration
	tokensDeducted  kvflowcontrol.Tokens
}

func makeBucketPerWorkClass(
	wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens, now time.Time,
) bucketPerWorkClass {
	bwc := bucketPerWorkClass{
		wc:       wc,
		tokens:   tokens,
		signalCh: make(chan struct{}, 1),
	}
	bwc.stats.noTokenStartTime = now
	return bwc
}

func (bwc *bucketPerWorkClass) adjustTokensLocked(
	ctx context.Context,
	delta kvflowcontrol.Tokens,
	limit kvflowcontrol.Tokens,
	admin bool,
	now time.Time,
) (adjustment, unaccounted kvflowcontrol.Tokens) {
	before := bwc.tokens
	bwc.tokens += delta
	if delta > 0 {
		if bwc.tokens > limit {
			unaccounted = bwc.tokens - limit
			bwc.tokens = limit
		}
		if before <= 0 && bwc.tokens > 0 {
			bwc.signal()
			bwc.stats.noTokenDuration += now.Sub(bwc.stats.noTokenStartTime)
		}
	} else {
		bwc.stats.deltaStats.tokensDeducted -= delta
		if before > 0 && bwc.tokens <= 0 {
			bwc.stats.noTokenStartTime = now
		}
	}
	if buildutil.CrdbTestBuild && !admin && unaccounted != 0 {
		log.Fatalf(ctx, "unaccounted[%s]=%d delta=%d limit=%d", bwc.wc, unaccounted, delta, limit)
	}
	adjustment = bwc.tokens - before
	return adjustment, unaccounted
}

func (bwc *bucketPerWorkClass) signal() {
	select {
	case bwc.signalCh <- struct{}{}: // non-blocking channel write that ensures it's topped up to 1 entry
	default:
	}
}

type waitEndState int32

const (
	waitSuccess waitEndState = iota
	contextCanceled
	stopWaitSignaled
)

func (bwc *bucketPerWorkClass) wait(ctx context.Context, stopWaitCh <-chan struct{}) waitEndState {
	select {
	case <-bwc.signalCh:
		return waitSuccess
	case <-stopWaitCh:
		return stopWaitSignaled
	case <-ctx.Done():
		return contextCanceled
	}
}

func (bwc *bucketPerWorkClass) getAndResetStats(now time.Time) deltaStats {
	stats := bwc.stats.deltaStats
	if bwc.tokens <= 0 {
		stats.noTokenDuration += now.Sub(bwc.stats.noTokenStartTime)
	}
	bwc.stats.deltaStats = deltaStats{}
	// Doesn't matter if bwc.tokens is actually > 0 since in that case we won't
	// use this value.
	bwc.stats.noTokenStartTime = now
	return stats
}

// bucket holds flow tokens for {regular,elastic} traffic over a
// kvflowcontrol.Stream. It's used to synchronize handoff between threads
// returning and waiting for flow tokens.
type bucket struct {
	mu struct {
		syncutil.RWMutex
		buckets [admissionpb.NumWorkClasses]bucketPerWorkClass
	}
}

func newBucket(tokensPerWorkClass tokensPerWorkClass, now time.Time) *bucket {
	b := bucket{}
	b.mu.buckets[admissionpb.RegularWorkClass] = makeBucketPerWorkClass(
		admissionpb.RegularWorkClass, tokensPerWorkClass.regular, now)
	b.mu.buckets[admissionpb.ElasticWorkClass] = makeBucketPerWorkClass(
		admissionpb.ElasticWorkClass, tokensPerWorkClass.elastic, now)
	return &b
}

func (b *bucket) tokens(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.tokensLocked(wc)
}

func (b *bucket) tokensLocked(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	return b.mu.buckets[wc].tokens
}

// wait causes the caller that has work with work class wc to wait until
// either tokens are available for work class wc, or the context is cancelled,
// or stopWaitCh is signaled. The waitEndState return value indicates which
// one of these terminated the wait. The waited bool is set to true only if
// there was some waiting, since the common case is that there will be tokens
// available and there will be no waiting.
//
// The implementation of wait uses the bucketPerWorkClass instance for the
// work class wc. The only reason we don't encapsulate the logic inside the
// bucketPerWorkClass struct is the shared mutex (bucket.mu). See the long
// comment in the bucketPerWorkClass declaration describing the waiting and
// signaling scheme.
func (b *bucket) wait(
	ctx context.Context, wc admissionpb.WorkClass, stopWaitCh <-chan struct{},
) (state waitEndState, waited bool) {
	waitedWithWaitSuccess := false
	for {
		b.mu.RLock()
		tokens := b.tokensLocked(wc)
		b.mu.RUnlock()
		if tokens > 0 {
			// No need to wait.
			if waitedWithWaitSuccess {
				// Waited in a previous iteration of the loop, so must have consumed
				// the entry in the channel. Unblock another waiter.
				b.mu.buckets[wc].signal()
			}
			return waitSuccess, waited
		}
		waited = true
		state = b.mu.buckets[wc].wait(ctx, stopWaitCh)
		if state != waitSuccess {
			// We could have previously removed the entry in the channel, if in a
			// previous iteration waitedWithWaitSuccess became true. But we don't
			// need to add an entry here, since we've sampled the tokens after
			// consuming that entry and still found tokens <= 0. So whoever
			// transitions tokens to a value greater than 0 will add an entry.
			return state, waited
		} else {
			waitedWithWaitSuccess = true
			// Continue in the loop to see if tokens are now available (common
			// case).
		}
	}
}

// admin is set to true when this method is called because of a settings
// change. In that case the class is interpreted narrowly as only updating the
// tokens for that class.
func (b *bucket) adjust(
	ctx context.Context,
	class admissionpb.WorkClass,
	delta kvflowcontrol.Tokens,
	limit tokensPerWorkClass,
	admin bool,
	now time.Time,
) (adjustment, unaccounted tokensPerWorkClass) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// TODO(irfansharif,aaditya): On kv0/enc=false/nodes=3/cpu=96 this mutex is
	// responsible for ~1.8% of the mutex contention. Maybe address it as part
	// of #104154. We want to effectively increment two values but cap each at
	// some limit, and when incrementing, figure out what the adjustment was.
	// What if reads always capped it at the limit? And when incrementing
	// atomically by +delta, if we're over the limit, since we tried to increase
	// the value by +delta, at most we need to adjust back down by -delta.
	// Something like it.
	//
	//	var c int64 = 0
	//	var limit int64 = rand.Int63n(10000000000)
	//	for i := 0; i < 50; i++ {
	//		go func() {
	//			for j := 0; j < 2000; j++ {
	//				delta := rand.Int63()
	//				v := atomic.AddInt64(&c, delta)
	//				if v > limit {
	//					overlimit := v - limit
	//					var adjustment int64 = overlimit
	//					if delta < overlimit {
	//						adjustment = delta
	//					}
	//					n := atomic.AddInt64(&c, -adjustment)
	//					fmt.Printf("%d > %d by %d, adjusted by %d to %d)\n",
	//						v, limit, v-limit, -adjustment, n)
	//				}
	//			}
	//		}()
	//	}

	switch class {
	case regular:
		adjustment.regular, unaccounted.regular =
			b.mu.buckets[admissionpb.RegularWorkClass].adjustTokensLocked(
				ctx, delta, limit.regular, admin, now)
		if !admin {
			// Regular {deductions,returns} also affect elastic flow tokens.
			adjustment.elastic, unaccounted.elastic =
				b.mu.buckets[admissionpb.ElasticWorkClass].adjustTokensLocked(
					ctx, delta, limit.elastic, admin, now)
		}
	case elastic:
		// Elastic {deductions,returns} only affect elastic flow tokens.
		adjustment.elastic, unaccounted.elastic =
			b.mu.buckets[admissionpb.ElasticWorkClass].adjustTokensLocked(
				ctx, delta, limit.elastic, admin, now)
	}
	return adjustment, unaccounted
}

func (b *bucket) getAndResetStats(now time.Time) (regularStats, elasticStats deltaStats) {
	b.mu.Lock()
	defer b.mu.Unlock()
	regularStats = b.mu.buckets[admissionpb.RegularWorkClass].getAndResetStats(now)
	elasticStats = b.mu.buckets[admissionpb.ElasticWorkClass].getAndResetStats(now)
	return regularStats, elasticStats
}

func (b *bucket) testingGetChannel(wc admissionpb.WorkClass) <-chan struct{} {
	return b.mu.buckets[wc].signalCh
}

func (b *bucket) testingSignalChannel(wc admissionpb.WorkClass) {
	b.mu.buckets[wc].signal()
}

type tokensPerWorkClass struct {
	regular, elastic kvflowcontrol.Tokens
}

func (c *Controller) getTokensForStream(stream kvflowcontrol.Stream) tokensPerWorkClass {
	ret := tokensPerWorkClass{}
	b := c.getBucket(stream)

	b.mu.RLock()
	ret.regular = b.tokensLocked(regular)
	ret.elastic = b.tokensLocked(elastic)
	b.mu.RUnlock()
	return ret
}

func (c *Controller) testingGetLimit() tokensPerWorkClass {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.limit
}

// TestingNonBlockingAdmit is a non-blocking alternative to Admit() for use in
// tests.
// - it checks if we have a non-zero number of flow tokens
// - if we do, we return immediately with admitted=true
// - if we don't, we return admitted=false and two callbacks:
//   - signaled, which can be polled to check whether we're ready to try and
//     admitting again;
//   - admit, which can be used to try and admit again. If still not admitted,
//     callers are to wait until they're signaled again.
//
// TODO(irfansharif): Fold in ctx cancelation into this non-blocking interface
// (signaled return true if ctx is canceled), and admit can increment the right
// errored metric underneath. We'll have to plumb this to the (test) caller too
// to prevent it from deducting tokens for canceled requests.
func (c *Controller) TestingNonBlockingAdmit(
	pri admissionpb.WorkPriority, connection kvflowcontrol.ConnectedStream,
) (admitted bool, signaled func() bool, admit func() bool) {
	class := admissionpb.WorkClassFromPri(pri)
	c.metrics.onWaiting(class)

	tstart := c.clock.PhysicalTime()
	admit = func() bool {
		select {
		case <-connection.Disconnected():
			c.metrics.onBypassed(class, c.clock.PhysicalTime().Sub(tstart))
			return true
		default:
		}

		b := c.getBucket(connection.Stream())
		tokens := b.tokens(class)
		if tokens <= 0 {
			return false
		}
		// Signal a waiter, if any, since we may have removed an entry from the
		// channel via testingSignaled.
		b.testingSignalChannel(class)
		c.metrics.onAdmitted(class, c.clock.PhysicalTime().Sub(tstart))
		return true
	}

	if admit() {
		return true, nil, nil
	}

	b := c.testingGetBucket(connection.Stream())
	return false, b.testingSignaled(connection, class), admit
}

// TestingAdjustTokens exports adjustTokens for testing purposes.
func (c *Controller) TestingAdjustTokens(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	delta kvflowcontrol.Tokens,
	stream kvflowcontrol.Stream,
) {
	c.adjustTokens(ctx, pri, delta, stream)
}

// TestingMetrics returns the underlying metrics struct for testing purposes.
func (c *Controller) TestingMetrics() interface{} {
	return c.metrics
}

func (c *Controller) testingGetBucket(stream kvflowcontrol.Stream) *bucket {
	return c.getBucket(stream)
}

func (b *bucket) testingSignaled(
	connection kvflowcontrol.ConnectedStream, wc admissionpb.WorkClass,
) func() bool {
	return func() bool {
		select {
		case <-connection.Disconnected():
			return true
		case <-b.testingGetChannel(wc): // check if signaled
			return true
		default:
			return false
		}
	}
}
