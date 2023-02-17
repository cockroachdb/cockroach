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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var regularTokensPerStream = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kvadmission.flow_controller.regular_tokens_per_stream",
	"flow tokens available for regular work on a per-stream basis",
	16<<20, // 16 MiB
	validateTokenRange,
)

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
		buckets map[kvflowcontrol.Stream]bucket
	}
	metrics *metrics
	clock   *hlc.Clock
}

var _ kvflowcontrol.Controller = &Controller{}

// New constructs a new Controller.
func New(registry *metric.Registry, settings *cluster.Settings, clock *hlc.Clock) *Controller {
	c := &Controller{
		clock: clock,
	}

	regularTokens := kvflowcontrol.Tokens(regularTokensPerStream.Get(&settings.SV))
	elasticTokens := kvflowcontrol.Tokens(elasticTokensPerStream.Get(&settings.SV))
	c.mu.limit = map[admissionpb.WorkClass]kvflowcontrol.Tokens{
		regular: regularTokens,
		elastic: elasticTokens,
	}
	c.mu.buckets = make(map[kvflowcontrol.Stream]bucket)
	regularTokensPerStream.SetOnChange(&settings.SV, func(ctx context.Context) {
		c.mu.Lock()
		defer c.mu.Unlock()

		before := tokensPerWorkClass{
			regular: c.mu.limit[regular],
			elastic: c.mu.limit[elastic],
		}
		now := tokensPerWorkClass{
			regular: kvflowcontrol.Tokens(regularTokensPerStream.Get(&settings.SV)),
			elastic: kvflowcontrol.Tokens(elasticTokensPerStream.Get(&settings.SV)),
		}
		adjustment := tokensPerWorkClass{
			regular: now[regular] - before[regular],
			elastic: now[elastic] - before[elastic],
		}
		c.mu.limit = now
		for _, b := range c.mu.buckets {
			b.tokens[regular] += adjustment[regular]
			b.tokens[elastic] += adjustment[elastic]
			c.metrics.onTokenAdjustment(adjustment)
			if adjustment[regular] > 0 || adjustment[elastic] > 0 {
				b.signal() // signal a waiter, if any
			}
		}
	})
	c.metrics = newMetrics(c)
	registry.AddMetricStruct(c.metrics)
	return c
}

// Admit is part of the kvflowcontrol.Controller interface. It blocks until
// there are flow tokens available for replication over the given stream for
// work of the given priority.
func (c *Controller) Admit(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	_ time.Time,
	connection kvflowcontrol.ConnectedStream,
) error {
	class := admissionpb.WorkClassFromPri(pri)
	c.metrics.onWaiting(class)

	logged := false
	tstart := c.clock.PhysicalTime()
	for {
		c.mu.Lock()
		b := c.getBucketLocked(connection.Stream())
		tokens := b.tokens[class]
		c.mu.Unlock()

		if tokens > 0 {
			if log.ExpensiveLogEnabled(ctx, 2) {
				log.Infof(ctx, "flow tokens available (pri=%s stream=%s tokens=%s wait-duration=%s)",
					pri, connection.Stream(), tokens, c.clock.PhysicalTime().Sub(tstart))
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
			return nil
		}

		if !logged && log.ExpensiveLogEnabled(ctx, 2) {
			log.Infof(ctx, "waiting for flow tokens (pri=%s stream=%s tokens=%s)",
				pri, connection.Stream(), tokens)
			logged = true
		}

		select {
		case <-b.wait(): // wait for a signal
		case <-connection.Disconnected():
			c.metrics.onBypassed(class, c.clock.PhysicalTime().Sub(tstart))
			return nil
		case <-ctx.Done():
			if ctx.Err() != nil {
				c.metrics.onErrored(class, c.clock.PhysicalTime().Sub(tstart))
			}
			return ctx.Err()
		}
	}

	// TODO(irfansharif): Use the create time for ordering among waiting
	// requests. Integrate it with epoch-LIFO.
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

func (c *Controller) adjustTokens(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	delta kvflowcontrol.Tokens,
	stream kvflowcontrol.Stream,
) {
	class := admissionpb.WorkClassFromPri(pri)

	c.mu.Lock()
	defer c.mu.Unlock()

	b := c.getBucketLocked(stream)
	adjustment, unaccounted := b.adjust(ctx, class, delta, c.mu.limit)
	c.metrics.onTokenAdjustment(adjustment)
	c.metrics.onUnaccounted(unaccounted)
	if adjustment[regular] > 0 || adjustment[elastic] > 0 {
		b.signal() // signal a waiter, if any
	}

	if log.ExpensiveLogEnabled(ctx, 2) {
		log.Infof(ctx, "adjusted flow tokens (pri=%s stream=%s delta=%s): regular=%s elastic=%s",
			pri, stream, delta, b.tokens[regular], b.tokens[elastic])
	}
}

func (c *Controller) getBucketLocked(stream kvflowcontrol.Stream) bucket {
	b, ok := c.mu.buckets[stream]
	if !ok {
		b = newBucket(c.mu.limit)
		c.mu.buckets[stream] = b
	}
	return b
}

// bucket holds flow tokens for {regular,elastic} traffic over a
// kvflowcontrol.Stream. It's used to synchronize handoff between threads
// returning and waiting for flow tokens.
type bucket struct {
	tokens tokensPerWorkClass // protected by Controller.mu
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

func newBucket(t tokensPerWorkClass) bucket {
	return bucket{
		tokens: map[admissionpb.WorkClass]kvflowcontrol.Tokens{
			regular: t[regular],
			elastic: t[elastic],
		},
		signalCh: make(chan struct{}, 1),
	}
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
	unaccounted = tokensPerWorkClass{
		regular: 0,
		elastic: 0,
	}

	before := tokensPerWorkClass{
		regular: b.tokens[regular],
		elastic: b.tokens[elastic],
	}

	switch class {
	case elastic:
		// Elastic {deductions,returns} only affect elastic flow tokens.
		b.tokens[class] += delta
		if delta > 0 && b.tokens[class] > limit[class] {
			unaccounted[class] = b.tokens[class] - limit[class]
			b.tokens[class] = limit[class] // enforce ceiling
		}
	case regular:
		b.tokens[class] += delta
		if delta > 0 && b.tokens[class] > limit[class] {
			unaccounted[class] = b.tokens[class] - limit[class]
			b.tokens[class] = limit[class] // enforce ceiling
		}

		b.tokens[elastic] += delta
		if delta > 0 && b.tokens[elastic] > limit[elastic] {
			unaccounted[elastic] = b.tokens[elastic] - limit[elastic]
			b.tokens[elastic] = limit[elastic] // enforce ceiling
		}
	}

	if buildutil.CrdbTestBuild && (unaccounted[regular] != 0 || unaccounted[elastic] != 0) {
		log.Fatalf(ctx, "unaccounted[regular]=%s unaccounted[elastic]=%s for class=%s delta=%s limit[regular]=%s limit[elastic]=%s",
			unaccounted[regular], unaccounted[elastic], class, delta, limit[regular], limit[elastic])
	}

	adjustment = tokensPerWorkClass{
		regular: b.tokens[regular] - before[regular],
		elastic: b.tokens[elastic] - before[elastic],
	}
	return adjustment, unaccounted
}

type tokensPerWorkClass map[admissionpb.WorkClass]kvflowcontrol.Tokens

const (
	minTokensPerStream kvflowcontrol.Tokens = 1 << 20  // 1 MiB
	maxTokensPerStream kvflowcontrol.Tokens = 64 << 20 // 64 MiB
)

func validateTokenRange(b int64) error {
	t := kvflowcontrol.Tokens(b)
	if t < minTokensPerStream {
		return fmt.Errorf("minimum flowed tokens allowed is %s, got %s", minTokensPerStream, t)
	}
	if t > maxTokensPerStream {
		return fmt.Errorf("maximum flow tokens allowed is %s, got %s", maxTokensPerStream, t)
	}
	return nil
}

func (c *Controller) testingGetTokensForStream(stream kvflowcontrol.Stream) tokensPerWorkClass {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := make(map[admissionpb.WorkClass]kvflowcontrol.Tokens)
	for wc, c := range c.getBucketLocked(stream).tokens {
		ret[wc] = c
	}
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

		c.mu.Lock()
		b := c.getBucketLocked(connection.Stream())
		tokens := b.tokens[class]
		c.mu.Unlock()

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

func (c *Controller) testingGetBucket(stream kvflowcontrol.Stream) bucket {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.getBucketLocked(stream)
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
