// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowcontroller

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// regularTokensPerStream determines the flow tokens available for regular work
// on a per-stream basis.
var regularTokensPerStream = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kvadmission.flow_controller.regular_tokens_per_stream",
	"flow tokens available for regular work on a per-stream basis",
	16<<20, // 16 MiB
	validateTokenRange,
)

// elasticTokensPerStream determines the flow tokens available for elastic work
// on a per-stream basis.
var elasticTokensPerStream = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kvadmission.flow_controller.elastic_tokens_per_stream",
	"flow tokens available for elastic work on a per-stream basis",
	8<<20, // 8 MiB
	validateTokenRange,
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
		// stream. This is lazily instantiated.
		//
		// TODO(irfansharif): Sort out the GC story for these buckets. When
		// streams get closed permanently (tenants get deleted, nodes removed)
		// or when completely inactive (no tokens deducted/returned over 30+
		// minutes), clear these out.
		buckets     sync.Map // kvflowcontrol.Stream => *bucket
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

	regularTokens := kvflowcontrol.Tokens(regularTokensPerStream.Get(&settings.SV))
	elasticTokens := kvflowcontrol.Tokens(elasticTokensPerStream.Get(&settings.SV))
	c.mu.limit = tokensPerWorkClass{
		regular: regularTokens,
		elastic: elasticTokens,
	}
	c.mu.buckets = sync.Map{}
	regularTokensPerStream.SetOnChange(&settings.SV, func(ctx context.Context) {
		c.mu.Lock()
		defer c.mu.Unlock()

		before := c.mu.limit
		now := tokensPerWorkClass{
			regular: kvflowcontrol.Tokens(regularTokensPerStream.Get(&settings.SV)),
			elastic: kvflowcontrol.Tokens(elasticTokensPerStream.Get(&settings.SV)),
		}
		adjustment := tokensPerWorkClass{
			regular: now.regular - before.regular,
			elastic: now.elastic - before.elastic,
		}
		c.mu.limit = now
		c.mu.buckets.Range(func(_, value any) bool {
			// NB: We're holding the controller mutex here, which guards against
			// new buckets being added, synchronization we don't get out of
			// sync.Map.Range() directly.
			b := value.(*bucket)
			b.mu.Lock()
			b.mu.tokensPerWorkClass.regular += adjustment.regular
			b.mu.tokensPerWorkClass.elastic += adjustment.elastic
			b.mu.Unlock()
			c.metrics.onTokenAdjustment(adjustment)
			if adjustment.regular > 0 || adjustment.elastic > 0 {
				b.signal() // signal a waiter, if any
			}
			return true
		})
	})
	c.metrics = newMetrics(c)
	registry.AddMetricStruct(c.metrics)
	return c
}

func (c *Controller) mode() kvflowcontrol.ModeT {
	return kvflowcontrol.ModeT(kvflowcontrol.Mode.Get(&c.settings.SV))
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

	logged := false
	tstart := c.clock.PhysicalTime()
	for {
		b := c.getBucket(connection.Stream())

		tokens := b.tokens(class)
		// In addition to letting requests through when there are tokens
		// being available, we'll also let them through if we're not
		// applying flow control to their specific work class.
		bypass := c.mode() == kvflowcontrol.ApplyToElastic && class == admissionpb.RegularWorkClass
		if tokens > 0 || bypass {
			if log.V(2) {
				log.Infof(ctx, "admitted request (pri=%s stream=%s tokens=%s wait-duration=%s mode=%s)",
					pri, connection.Stream(), tokens, c.clock.PhysicalTime().Sub(tstart), c.mode())
			}

			// TODO(irfansharif): Right now we continue forwarding admission
			// grants to request while the available tokens > 0, which can lead
			// to over-admission since deduction is deferred (see
			// testdata/simulation/over_admission).
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

			b.signal() // signal a waiter, if any
			c.metrics.onAdmitted(class, c.clock.PhysicalTime().Sub(tstart))
			if bypass {
				return false, nil
			}
			return true, nil
		}

		if !logged && log.V(2) {
			log.Infof(ctx, "waiting for flow tokens (pri=%s stream=%s tokens=%s)",
				pri, connection.Stream(), tokens)
			logged = true
		}

		select {
		case <-b.wait(): // wait for a signal
		case <-connection.Disconnected():
			c.metrics.onBypassed(class, c.clock.PhysicalTime().Sub(tstart))
			return true, nil
		case <-ctx.Done():
			if ctx.Err() != nil {
				c.metrics.onErrored(class, c.clock.PhysicalTime().Sub(tstart))
			}
			return false, ctx.Err()
		}
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
	c.mu.Lock()
	defer c.mu.Unlock()

	var streams []kvflowinspectpb.Stream
	c.mu.buckets.Range(func(key, value any) bool {
		stream := key.(kvflowcontrol.Stream)
		b := value.(*bucket)

		b.mu.RLock()
		streams = append(streams, kvflowinspectpb.Stream{
			TenantID:               stream.TenantID,
			StoreID:                stream.StoreID,
			AvailableRegularTokens: int64(b.tokensLocked(regular)),
			AvailableElasticTokens: int64(b.tokensLocked(elastic)),
		})
		b.mu.RUnlock()
		return true
	})
	sort.Slice(streams, func(i, j int) bool { // for determinism
		if streams[i].TenantID != streams[j].TenantID {
			return streams[i].TenantID.ToUint64() < streams[j].TenantID.ToUint64()
		}
		return streams[i].StoreID < streams[j].StoreID
	})
	return streams
}

// InspectStream is part of the kvflowcontrol.Controller interface.
func (c *Controller) InspectStream(
	_ context.Context, stream kvflowcontrol.Stream,
) kvflowinspectpb.Stream {
	tokens := c.getTokensForStream(stream)
	return kvflowinspectpb.Stream{
		TenantID:               stream.TenantID,
		StoreID:                stream.StoreID,
		AvailableRegularTokens: int64(tokens.regular),
		AvailableElasticTokens: int64(tokens.elastic),
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
	adjustment, unaccounted := b.adjust(ctx, class, delta, c.mu.limit)
	c.metrics.onTokenAdjustment(adjustment)
	if unaccounted.regular > 0 || unaccounted.elastic > 0 {
		c.metrics.onUnaccounted(unaccounted)
	}
	if adjustment.regular > 0 || adjustment.elastic > 0 {
		b.signal() // signal a waiter, if any
	}

	if log.V(2) {
		b.mu.RLock()
		log.Infof(ctx, "adjusted flow tokens (pri=%s stream=%s delta=%s): regular=%s elastic=%s",
			pri, stream, delta, b.tokensLocked(regular), b.tokensLocked(elastic))
		b.mu.RUnlock()
	}
}

func (c *Controller) getBucket(stream kvflowcontrol.Stream) *bucket {
	// NB: sync.map is more expensive CPU wise as per BenchmarkController
	// for reads, ~250ns vs. ~350ns, though better for mutex contention when
	// looking at kv0/enc=false/nodes=3/cpu=9. The sync.Map does show up in CPU
	// profiles more prominently though. If we want to go back to it being a
	// mutex-backed map, we could use a read-Lock when trying to read the bucket
	// and then swapping for a write-lock when optionally creating the bucket.
	b, ok := c.mu.buckets.Load(stream)
	if !ok {
		c.mu.Lock()
		var loaded bool
		b, loaded = c.mu.buckets.LoadOrStore(stream, newBucket(c.mu.limit))
		if !loaded {
			c.mu.bucketCount += 1
		}
		c.mu.Unlock()
	}
	return b.(*bucket)
}

// bucket holds flow tokens for {regular,elastic} traffic over a
// kvflowcontrol.Stream. It's used to synchronize handoff between threads
// returning and waiting for flow tokens.
type bucket struct {
	mu struct {
		syncutil.RWMutex
		tokensPerWorkClass tokensPerWorkClass
	}

	// Waiting requests do so by waiting on signalCh without holding mutexes.
	// Requests first check for available tokens, waiting if unavailable.
	// - Whenever tokens are returned, signalCh is signaled, waking up a single
	//   waiting request. If the request finds no available tokens, it starts
	//   waiting again.
	// - Whenever a request gets admitted, it signals the next waiter if any.
	// The invariant we are ensuring is that whenever there are tokens
	// available, the channel will have an entry, so at least one request that
	// observed unavailable tokens will get unblocked, which will in turn
	// unblock others. The concurrent request that adds tokens also tops up the
	// channel so that the at least one waiter that observed unavailable tokens
	// will find the channel signaled.
	signalCh chan struct{}
}

func newBucket(tokensPerWorkClass tokensPerWorkClass) *bucket {
	b := bucket{
		signalCh: make(chan struct{}, 1),
	}
	b.mu.tokensPerWorkClass = tokensPerWorkClass
	return &b
}

func (b *bucket) tokens(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.tokensLocked(wc)
}

func (b *bucket) tokensLocked(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	if wc == regular {
		return b.mu.tokensPerWorkClass.regular
	}
	return b.mu.tokensPerWorkClass.elastic
}

func (b *bucket) signal() {
	select {
	case b.signalCh <- struct{}{}: // non-blocking channel write that ensures it's topped up to 1 entry
	default:
	}
}

func (b *bucket) wait() chan struct{} {
	return b.signalCh
}

func (b *bucket) adjust(
	ctx context.Context,
	class admissionpb.WorkClass,
	delta kvflowcontrol.Tokens,
	limit tokensPerWorkClass,
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

	unaccounted = tokensPerWorkClass{}
	before := b.mu.tokensPerWorkClass

	switch class {
	case elastic:
		// Elastic {deductions,returns} only affect elastic flow tokens.
		b.mu.tokensPerWorkClass.elastic += delta
		if delta > 0 && b.mu.tokensPerWorkClass.elastic > limit.elastic {
			unaccounted.elastic = b.mu.tokensPerWorkClass.elastic - limit.elastic
			b.mu.tokensPerWorkClass.elastic = limit.elastic // enforce ceiling
		}
	case regular:
		b.mu.tokensPerWorkClass.regular += delta
		if delta > 0 && b.mu.tokensPerWorkClass.regular > limit.regular {
			unaccounted.regular = b.mu.tokensPerWorkClass.regular - limit.regular
			b.mu.tokensPerWorkClass.regular = limit.regular // enforce ceiling
		}

		b.mu.tokensPerWorkClass.elastic += delta
		if delta > 0 && b.mu.tokensPerWorkClass.elastic > limit.elastic {
			unaccounted.elastic = b.mu.tokensPerWorkClass.elastic - limit.elastic
			b.mu.tokensPerWorkClass.elastic = limit.elastic // enforce ceiling
		}
	}

	if buildutil.CrdbTestBuild && (unaccounted.regular != 0 || unaccounted.elastic != 0) {
		log.Fatalf(ctx, "unaccounted[regular]=%s unaccounted[elastic]=%s for class=%s delta=%s limit[regular]=%s limit[elastic]=%s",
			unaccounted.regular, unaccounted.elastic, class, delta, limit.regular, limit.elastic)
	}

	adjustment = tokensPerWorkClass{
		regular: b.mu.tokensPerWorkClass.regular - before.regular,
		elastic: b.mu.tokensPerWorkClass.elastic - before.elastic,
	}
	return adjustment, unaccounted
}

type tokensPerWorkClass struct {
	regular, elastic kvflowcontrol.Tokens
}

const (
	minTokensPerStream kvflowcontrol.Tokens = 1 << 20  // 1 MiB
	maxTokensPerStream kvflowcontrol.Tokens = 64 << 20 // 64 MiB
)

var validateTokenRange = settings.WithValidateInt(func(b int64) error {
	t := kvflowcontrol.Tokens(b)
	if t < minTokensPerStream {
		return fmt.Errorf("minimum flowed tokens allowed is %s, got %s", minTokensPerStream, t)
	}
	if t > maxTokensPerStream {
		return fmt.Errorf("maximum flow tokens allowed is %s, got %s", maxTokensPerStream, t)
	}
	return nil
})

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

		b.signal() // signal a waiter, if any
		c.metrics.onAdmitted(class, c.clock.PhysicalTime().Sub(tstart))
		return true
	}

	if admit() {
		return true, nil, nil
	}

	b := c.testingGetBucket(connection.Stream())
	return false, b.testingSignaled(connection), admit
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

func (b *bucket) testingSignaled(connection kvflowcontrol.ConnectedStream) func() bool {
	return func() bool {
		select {
		case <-connection.Disconnected():
			return true
		case <-b.wait(): // check if signaled
			return true
		default:
			return false
		}
	}
}
