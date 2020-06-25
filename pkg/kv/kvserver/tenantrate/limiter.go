// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantrate

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/errors"
)

// Limiter implements a token-bucket style rate limiter.
// It has the added feature that quota acquired from the pool can be returned
// in the case that they end up not getting used.
type Limiter struct {
	parent   *LimiterFactory
	tenantID roachpb.TenantID
	qp       *quotapool.QuotaPool
	metrics  tenantMetrics
}

// newLimiter constructs a new Limiter.
func (rl *Limiter) init(
	parent *LimiterFactory,
	tenantID roachpb.TenantID,
	conf LimitConfigs,
	metrics tenantMetrics,
	options ...quotapool.Option,
) {
	*rl = Limiter{
		parent:   parent,
		tenantID: tenantID,
		metrics:  metrics,
	}
	buckets := tokenBuckets{
		requests:   makeTokenBucket(conf.Requests),
		readBytes:  makeTokenBucket(conf.ReadBytes),
		writeBytes: makeTokenBucket(conf.WriteBytes),
	}
	options = append(options, quotapool.OnAcquisition(func(
		ctx context.Context, poolName string, r quotapool.Request, start time.Time,
	) {
		req := r.(*waitRequest)
		rl.metrics.writeBytesAdmitted.Inc(req.writeBytes)
	}))
	rl.qp = quotapool.New(tenantID.String(), &buckets, options...)
	buckets.clock = rl.qp.TimeSource()
	buckets.lastUpdated = buckets.clock.Now()
}

// Wait acquires n quota from the Limiter. This acquisition cannot be
// released. Each call to wait will wait will consume 1 read byte, 1 request,
// and writeBytes from the token buckets. Calls to Wait will block until the
// buckets contain adequate resources. If a request attempts to write more
// than the burst limit, it will wait until the bucket is completely full
// before acquiring the requested quantity and putting the limiter in debt.
//
// The only errors which should be returned are due to the context.
func (rl *Limiter) Wait(ctx context.Context, writeBytes int64) error {
	rl.metrics.currentBlocked.Inc(1)
	defer rl.metrics.currentBlocked.Dec(1)
	r := newWaitRequest(writeBytes)
	defer putWaitRequest(r)
	if err := rl.qp.Acquire(ctx, r); err != nil {
		return err
	}
	return nil
}

// RecordRead subtracts the bytes read by a request from the token bucket.
func (rl *Limiter) RecordRead(readBytes int64) {
	rb := newReadBytesResource(readBytes)
	defer putReadBytesResource(rb)
	rl.qp.Add(rb)
}

// updateLimits is used by the factory to inform the Limiter of a new
// configuration.
func (rl *Limiter) updateLimits(limits LimitConfigs) {
	rl.qp.Add((rateLimitsResource)(limits))
}

// tokenBuckets is the implementation of Resource which remains in the quotapool
// for a Limiter.
type tokenBuckets struct {
	clock       quotapool.TimeSource
	lastUpdated time.Time
	requests    tokenBucket
	readBytes   tokenBucket
	writeBytes  tokenBucket
}

var _ quotapool.Resource = (*tokenBuckets)(nil)

func (rb *tokenBuckets) update() {
	now := rb.clock.Now()

	// Update token bucket capacity given the passage of clock.
	// TODO(ajwerner): Consider instituting a minimum update frequency to avoid
	// spinning too fast on timers for tons of tiny allocations at a fast rate.
	if since := now.Sub(rb.lastUpdated); since > 0 {
		rb.requests.update(since)
		rb.readBytes.update(since)
		rb.writeBytes.update(since)
		rb.lastUpdated = now
	}
}

// check determines whether a request can be fulfilled by the given tokens in
// the bucket. If not, it determines when the buckets will be adequately full
// to fulfill the request.
func (rb *tokenBuckets) check(req *waitRequest) (fulfilled bool, tryAgainAfter time.Duration) {
	fulfilled = true
	check := func(t *tokenBucket, needed int64) {
		if ok, after := t.check(needed); !ok {
			fulfilled = false
			if after > tryAgainAfter {
				tryAgainAfter = after
			}
		}
	}
	check(&rb.requests, req.requests)
	check(&rb.readBytes, req.readBytes)
	check(&rb.writeBytes, req.writeBytes)
	return fulfilled, tryAgainAfter
}

func (rb *tokenBuckets) subtract(req *waitRequest) {
	rb.requests.tokens -= float64(req.requests)
	rb.readBytes.tokens -= float64(req.readBytes)
	rb.writeBytes.tokens -= float64(req.writeBytes)
}

func (rb *tokenBuckets) Merge(other quotapool.Resource) bool {
	switch toAdd := other.(type) {
	case rateLimitsResource:
		// Account for the accumulation since lastUpdate and now under the old
		// configuration.
		rb.update()

		rb.requests.setConf(toAdd.Requests)
		rb.readBytes.setConf(toAdd.ReadBytes)
		rb.writeBytes.setConf(toAdd.WriteBytes)
		return true
	case *readBytesResource:
		rb.readBytes.tokens -= float64(*toAdd)
		// Do not notify the head of the queue. In the best case we did not disturb
		// the time at which it can be fulfilled and in the worst case, we made it
		// further in the future.
		return false
	default:
		panic(errors.AssertionFailedf("Merge not implemented for %T", other))
	}
}

// tokenBucket represents a token bucket for a given resource and its associated
// configuration.
type tokenBucket struct {
	LimitConfig
	tokens float64
}

func makeTokenBucket(rl LimitConfig) tokenBucket {
	return tokenBucket{
		LimitConfig: rl,
		tokens:      float64(rl.Burst),
	}
}

// update applies the positive time delta update for the resource.
func (t *tokenBucket) update(deltaT time.Duration) {
	t.tokens += float64(t.Rate) * deltaT.Seconds()
	t.clampTokens()
}

// checkQuota returns whether needed will be satisfied by quota. Not that the
// definition of satisfied is either that the integer part of quota exceeds
// needed or that quota is equal to the burst. This is because we want to
// have request put the rate limiter in debt rather than prevent execution of
// requests.
//
// If the request is not satisfied, the amount of clock that must be waited for
// the request to be satisfied at the current rate is returned.
func (t *tokenBucket) check(needed int64) (fulfilled bool, tryAgainAfter time.Duration) {
	if q := int64(t.tokens); needed <= q || q == t.Burst {
		return true, 0
	}

	// We'll calculate the amount of clock until the quota is full if we're
	// requesting more than the burst limit.
	if needed > t.Burst {
		needed = t.Burst
	}
	delta := float64(needed) - t.tokens
	tryAgainAfter = time.Duration((delta * float64(time.Second)) / float64(t.Rate))
	return false, tryAgainAfter
}

// setConf updates the configuration for a tokenBucket.
//
// TODO(ajwerner): It seems possible that when adding or reducing the burst
// values that we might want to remove those values from the token bucket.
// It's not obvious that we want to add tokens when increasing the burst as
// that might lead to a big spike in load immediately upon increasing this
// limit.
func (t *tokenBucket) setConf(rl LimitConfig) {
	t.LimitConfig = rl
	t.clampTokens()
}

// clampTokens ensures that tokens does not exceed burst.
func (t *tokenBucket) clampTokens() {
	if burst := float64(t.Burst); t.tokens > burst {
		t.tokens = burst
	}
}

// waitRequest is used to wait for adequate resources in the tokenBuckets.
type waitRequest struct {
	requests   int64
	writeBytes int64
	readBytes  int64
}

var waitRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(waitRequest) },
}

// newWaitRequest allocates a waitRequest from the sync.Pool.
// It should be returned with putWaitRequest.
func newWaitRequest(writeBytes int64) *waitRequest {
	r := waitRequestSyncPool.Get().(*waitRequest)
	*r = waitRequest{
		requests:   1,
		readBytes:  1,
		writeBytes: writeBytes,
	}
	return r
}

func putWaitRequest(r *waitRequest) {
	*r = waitRequest{}
	waitRequestSyncPool.Put(r)
}

type readBytesResource int64

var _ quotapool.Resource = (*readBytesResource)(nil)

func (rb *readBytesResource) Merge(other quotapool.Resource) bool {
	panic("nothing should get merged into me")
}

var readBytesResourceSyncPool = sync.Pool{
	New: func() interface{} { return new(readBytesResource) },
}

func newReadBytesResource(readBytes int64) *readBytesResource {
	rb := readBytesResourceSyncPool.Get().(*readBytesResource)
	*rb = readBytesResource(readBytes)
	return rb
}

func putReadBytesResource(rb *readBytesResource) {
	*rb = 0
	readBytesResourceSyncPool.Put(rb)
}

func (req *waitRequest) Acquire(
	ctx context.Context, res quotapool.Resource,
) (fulfilled bool, unused quotapool.Resource, tryAgainAfter time.Duration) {
	r := res.(*tokenBuckets)
	r.update()
	if fulfilled, tryAgainAfter = r.check(req); !fulfilled {
		return false, r, tryAgainAfter
	}
	r.subtract(req)
	return true, r, 0
}

func (req *waitRequest) ShouldWait() bool {
	return true
}

type rateLimitsResource LimitConfigs

func (r rateLimitsResource) Merge(other quotapool.Resource) bool {
	panic("nothing should get merged into me")
}

var _ quotapool.Resource = (*rateLimitsResource)(nil)
