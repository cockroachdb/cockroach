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

// Controller is a concrete implementation of the kvflowcontrol.Controller
// interface. It provides flow control for replication traffic in KV and is
// typically held at the node-level.
type Controller struct {
	mu struct {
		syncutil.Mutex
		limit tokensPerWorkClass
		// We maintain flow token buckets for {regular,elastic} work along each
		// stream. This is lazily instantiated.
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

	// TODO(irfansharif): React to these cluster settings changing. Changing the
	// limits is easy enough, but we also want to resize existing flow control
	// buckets. Should we just reset everything across the board, relying on
	// lazy (re-)instantiation? We'll likely want to enforce a ceiling when
	// returning tokens out of an abundance of caution, so resetting all
	// buckets seems fine given the possibility of in-flight token returns.

	regularTokens := kvflowcontrol.Tokens(regularTokensPerStream.Get(&settings.SV))
	elasticTokens := kvflowcontrol.Tokens(elasticTokensPerStream.Get(&settings.SV))
	c.mu.limit = map[admissionpb.WorkClass]kvflowcontrol.Tokens{
		admissionpb.RegularWorkClass: regularTokens,
		admissionpb.ElasticWorkClass: elasticTokens,
	}
	c.mu.buckets = make(map[kvflowcontrol.Stream]bucket)
	c.metrics = newMetrics(c)
	registry.AddMetricStruct(c.metrics)
	return c
}

// Admit is part of the kvflowcontrol.Controller interface. It blocks until
// there are flow tokens available for replication over the given stream for
// work of the given priority.
func (c *Controller) Admit(
	ctx context.Context, pri admissionpb.WorkPriority, _ time.Time, stream kvflowcontrol.Stream,
) error {
	class := admissionpb.WorkClassFromPri(pri)
	c.metrics.onWaiting(class)

	logged := false
	tstart := c.clock.PhysicalTime()
	for {
		c.mu.Lock()
		b := c.getBucketLocked(stream)
		tokens := b.tokens[class]
		c.mu.Unlock()

		if tokens > 0 {
			if log.ExpensiveLogEnabled(ctx, 2) {
				log.Infof(ctx, "flow tokens available (pri=%s stream=%s tokens=%s wait-duration=%s)",
					pri, stream, tokens, c.clock.PhysicalTime().Sub(tstart))
			}
			b.signal() // signal a waiter, if any
			c.metrics.onAdmitted(class, c.clock.PhysicalTime().Sub(tstart))
			return nil
		}

		if !logged && log.ExpensiveLogEnabled(ctx, 2) {
			log.Infof(ctx, "waiting for flow tokens (pri=%s stream=%s tokens=%s)",
				pri, stream, tokens)
			logged = true
		}

		select {
		case <-b.wait(): // wait for a signal
		case <-ctx.Done():
			if ctx.Err() != nil {
				c.metrics.onErrored(class)
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
) bool {
	if tokens < 0 {
		log.Fatalf(ctx, "malformed argument: -ve tokens deducted (pri=%s tokens=%s stream=%s)",
			pri, tokens, stream)
	}
	c.adjustTokens(ctx, pri, -tokens, stream)
	return true
}

// ReturnTokens is part of the kvflowcontrol.Controller interface.
func (c *Controller) ReturnTokens(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	tokens kvflowcontrol.Tokens,
	stream kvflowcontrol.Stream,
) {
	if tokens < 0 {
		log.Fatalf(ctx, "malformed argument: -ve tokens deducted (pri=%s tokens=%s stream=%s)",
			pri, tokens, stream)
	}
	c.adjustTokens(ctx, pri, tokens, stream)
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
	adjustment := b.adjust(class, delta, c.mu.limit)
	c.metrics.onTokenAdjustment(adjustment)
	if adjustment[admissionpb.RegularWorkClass] > 0 || adjustment[admissionpb.ElasticWorkClass] > 0 {
		b.signal() // signal a waiter, if any
	}

	if log.ExpensiveLogEnabled(ctx, 2) {
		log.Infof(ctx, "adjusted flow tokens (pri=%s stream=%s delta=%s): regular=%s elastic=%s",
			pri, stream, delta, b.tokens[admissionpb.RegularWorkClass], b.tokens[admissionpb.ElasticWorkClass])
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
	tokens          tokensPerWorkClass
	waitForTokensCh chan struct{}
}

func newBucket(t tokensPerWorkClass) bucket {
	return bucket{
		tokens: map[admissionpb.WorkClass]kvflowcontrol.Tokens{
			admissionpb.RegularWorkClass: t[admissionpb.RegularWorkClass],
			admissionpb.ElasticWorkClass: t[admissionpb.ElasticWorkClass],
		},
		waitForTokensCh: make(chan struct{}, 1),
	}
}

func (b *bucket) signal() {
	select {
	case b.waitForTokensCh <- struct{}{}:
	default:
	}
}

func (b *bucket) wait() chan struct{} {
	return b.waitForTokensCh
}

func (b *bucket) adjust(
	class admissionpb.WorkClass, delta kvflowcontrol.Tokens, limit tokensPerWorkClass,
) (adjustment tokensPerWorkClass) {
	// Aliases to make the code below slightly easier to read.
	const regular, elastic = admissionpb.RegularWorkClass, admissionpb.ElasticWorkClass

	// TODO(irfansharif): Stop enforcing the ceiling under test builds. Under
	// production builds should we also perhaps enforce some floor? Say -16 MiB
	// for regular work, -24 MiB for elastic.

	before := tokensPerWorkClass{
		regular: b.tokens[regular],
		elastic: b.tokens[elastic],
	}

	switch class {
	case elastic:
		// Elastic {deductions,returns} only affect elastic flow tokens.
		b.tokens[class] += delta
		if delta > 0 {
			b.tokens[class] = min(b.tokens[class], limit[class]) // enforce ceiling
		}
	case regular:
		b.tokens[class] += delta
		if delta > 0 {
			b.tokens[class] = min(b.tokens[class], limit[class]) // enforce ceiling

			if b.tokens[regular] > limit[elastic] {
				// "Top half" of regular additions, also add to elastic. For
				// symmetry with the deductions case below.
				b.tokens[elastic] += min(delta, b.tokens[regular]-limit[elastic])
				b.tokens[elastic] = min(b.tokens[elastic], limit[elastic]) // enforce ceiling
			}
		} else {
			if before[regular] > limit[elastic] {
				// "Top half" of regular deductions, also deduct from elastic.
				// Allows elastic flow tokens to go -ve.
				b.tokens[elastic] -= min(before[regular]-limit[elastic], -delta)
			}
		}
	}
	return tokensPerWorkClass{
		regular: b.tokens[regular] - before[regular],
		elastic: b.tokens[elastic] - before[elastic],
	}
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

// testingNonBlockingAdmit is a non-blocking alternative to Admit() for use in
// tests.
// - it checks if we have a non-zero number of flow tokens
// - if we do, we return immediately with admitted=true
// - if we don't, we return admitted=false and two callbacks:
//   - signaled, which can be polled to check whether we're ready to try and
//     admitting again;
//   - admit, which can be used to try and admit again. If still not admitted,
//     callers are to wait until they're signaled again.
func (c *Controller) testingNonBlockingAdmit(
	pri admissionpb.WorkPriority, stream kvflowcontrol.Stream,
) (admitted bool, signaled func() bool, admit func() bool) {
	class := admissionpb.WorkClassFromPri(pri)
	c.metrics.onWaiting(class)

	tstart := c.clock.PhysicalTime()
	admit = func() bool {
		c.mu.Lock()
		b := c.getBucketLocked(stream)
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

	b := c.testingGetBucket(stream)
	return false, b.testingSignaled, admit
}

func (c *Controller) testingGetBucket(stream kvflowcontrol.Stream) bucket {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.getBucketLocked(stream)
}

func (b *bucket) testingSignaled() bool {
	select {
	case <-b.wait(): // check if signaled
		return true
	default:
		return false
	}
}

func min(i, j kvflowcontrol.Tokens) kvflowcontrol.Tokens {
	if i < j {
		return i
	}
	return j
}
