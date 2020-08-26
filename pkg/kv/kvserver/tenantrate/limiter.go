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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Limiter is used to rate-limit KV requests for a given tenant.
//
// The use of an interface permits a different implementation for the system
// tenant and other tenants. The remaining commentary will pertain to the
// implementation used for non-system tenants. The limiter is implemented as
// a multi-dimensional token-bucket. The dimensions are Requests, WriteBytes,
// and ReadBytes. Each dimension has a rate and burst limit per tenant. These
// limits are controlled via cluster settings.
//
// Calls to Wait interact with all three dimensions. The Wait call takes a
// quantity of writeBytes which it attempts to consume from the Limiter. It also
// attempts to consume 1 request and 1 readByte. This readByte "courtesy"
// allocation is used in conjunction with the RecordRead method to control the
// rate of reads despite not knowing how much data a request will read a priori.
// If a request attempts to consume more than the burst limit in any dimension,
// it can proceed only if the token bucket is completely full in that dimension.
// In that case, the acquisition will put the Limiter into debt in that
// dimension, meaning that its current quota is negative. Future acquisitions
// will need to wait until the debt is paid off. Calls to RecordRead subtract
// the indicated byte quantity from the token bucket regardless of its current
// value. RecordRead can push the limiter into debt, blocking future requests
// until that debt is paid.
//
// The Limiter is backed by a FIFO queue which provides fairness.
type Limiter interface {

	// Wait acquires n quota from the limiter. This acquisition cannot be
	// released. Each call to wait will consume 1 read or write request
	// depending on isWrite, 1 read byte, and writeBytes from the token buckets.
	// Calls to Wait will block until the buckets contain adequate resources. If
	// a request attempts to write more than the burst limit, it will wait until
	// the bucket is completely full before acquiring the requested quantity and
	// putting the limiter in debt.
	//
	// The only errors which should be returned are due to the context.
	Wait(ctx context.Context, isWrite bool, writeBytes int64) error

	// RecordRead subtracts the bytes read by a request from the token bucket.
	// This call may push the Limiter into debt in the ReadBytes dimensions
	// forcing subsequent Wait calls to block until the debt is paid.
	// However, RecordRead itself will never block.
	RecordRead(ctx context.Context, readBytes int64)
}

type limiter struct {
	parent   *LimiterFactory
	tenantID roachpb.TenantID
	qp       *quotapool.QuotaPool
	metrics  tenantMetrics
}

// init initializes a new limiter.
func (rl *limiter) init(
	parent *LimiterFactory,
	tenantID roachpb.TenantID,
	conf LimitConfigs,
	metrics tenantMetrics,
	options ...quotapool.Option,
) {
	*rl = limiter{
		parent:   parent,
		tenantID: tenantID,
		metrics:  metrics,
	}
	buckets := tokenBuckets{
		readRequests:  makeTokenBucket(conf.ReadRequests),
		writeRequests: makeTokenBucket(conf.WriteRequests),
		readBytes:     makeTokenBucket(conf.ReadBytes),
		writeBytes:    makeTokenBucket(conf.WriteBytes),
	}
	options = append(options, quotapool.OnAcquisition(func(
		ctx context.Context, poolName string, r quotapool.Request, start time.Time,
	) {
		req := r.(*waitRequest)
		if req.readRequests > 0 {
			rl.metrics.readRequestsAdmitted.Inc(req.readRequests)
		}
		if req.writeRequests > 0 {
			rl.metrics.writeRequestsAdmitted.Inc(req.writeRequests)
		}
		// Accounted for in limiter.RecordRead.
		// if req.readBytes > 0 {
		// 	rl.metrics.readBytesAdmitted.Inc(req.readBytes)
		// }
		if req.writeBytes > 0 {
			rl.metrics.writeBytesAdmitted.Inc(req.writeBytes)
		}
	}))
	rl.qp = quotapool.New(tenantID.String(), &buckets, options...)
	buckets.clock = rl.qp.TimeSource()
	buckets.lastUpdated = buckets.clock.Now()
}

func (rl *limiter) Wait(ctx context.Context, isWrite bool, writeBytes int64) error {
	rl.metrics.currentBlocked.Inc(1)
	defer rl.metrics.currentBlocked.Dec(1)
	r := newWaitRequest(isWrite, writeBytes)
	defer putWaitRequest(r)
	if err := rl.qp.Acquire(ctx, r); err != nil {
		return err
	}
	return nil
}

func (rl *limiter) RecordRead(ctx context.Context, readBytes int64) {
	rb := newReadBytesResource(readBytes)
	defer putReadBytesResource(rb)
	rl.metrics.readBytesAdmitted.Inc(readBytes)
	rl.qp.Add(rb)
}

// updateLimits is used by the factory to inform the limiter of a new
// configuration.
func (rl *limiter) updateLimits(limits LimitConfigs) {
	rl.qp.Add(limits)
}

// tokenBuckets is the implementation of Resource which remains in the quotapool
// for a limiter.
type tokenBuckets struct {
	clock         timeutil.TimeSource
	lastUpdated   time.Time
	readRequests  tokenBucket
	writeRequests tokenBucket
	readBytes     tokenBucket
	writeBytes    tokenBucket
}

var _ quotapool.Resource = (*tokenBuckets)(nil)

func (rb *tokenBuckets) update() {
	now := rb.clock.Now()

	// Update token bucket capacity given the passage of clock.
	// TODO(ajwerner): Consider instituting a minimum update frequency to avoid
	// spinning too fast on timers for tons of tiny allocations at a fast rate.
	if since := now.Sub(rb.lastUpdated); since > 0 {
		rb.readRequests.update(since)
		rb.writeRequests.update(since)
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
	check(&rb.readRequests, req.readRequests)
	check(&rb.writeRequests, req.writeRequests)
	check(&rb.readBytes, req.readBytes)
	check(&rb.writeBytes, req.writeBytes)
	return fulfilled, tryAgainAfter
}

func (rb *tokenBuckets) subtract(req *waitRequest) {
	rb.readRequests.tokens -= float64(req.readRequests)
	rb.writeRequests.tokens -= float64(req.writeRequests)
	rb.readBytes.tokens -= float64(req.readBytes)
	rb.writeBytes.tokens -= float64(req.writeBytes)
}

func (rb *tokenBuckets) Merge(val interface{}) (shouldNotify bool) {
	switch toAdd := val.(type) {
	case LimitConfigs:
		// Account for the accumulation since lastUpdate and now under the old
		// configuration.
		rb.update()

		rb.readRequests.setConf(toAdd.ReadRequests)
		rb.writeRequests.setConf(toAdd.WriteRequests)
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
		panic(errors.AssertionFailedf("merge not implemented for %T", val))
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

// checkQuota returns whether needed will be satisfied by quota. Note that the
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
	readRequests  int64
	writeRequests int64
	writeBytes    int64
	readBytes     int64
}

var waitRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(waitRequest) },
}

// newWaitRequest allocates a waitRequest from the sync.Pool.
// It should be returned with putWaitRequest.
func newWaitRequest(isWrite bool, writeBytes int64) *waitRequest {
	r := waitRequestSyncPool.Get().(*waitRequest)
	*r = waitRequest{
		readRequests:  0,
		writeRequests: 0,
		readBytes:     1,
		writeBytes:    writeBytes,
	}
	if isWrite {
		r.writeRequests = 1
	} else {
		r.readRequests = 1
	}
	return r
}

func putWaitRequest(r *waitRequest) {
	*r = waitRequest{}
	waitRequestSyncPool.Put(r)
}

type readBytesResource int64

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
) (fulfilled bool, tryAgainAfter time.Duration) {
	r := res.(*tokenBuckets)
	r.update()
	if fulfilled, tryAgainAfter = r.check(req); !fulfilled {
		return false, tryAgainAfter
	}
	r.subtract(req)
	return true, 0
}

func (req *waitRequest) ShouldWait() bool {
	return true
}
