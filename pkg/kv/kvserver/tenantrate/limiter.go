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

	// Wait acquires the quota necessary to admit a read or write request. This
	// acquisition cannot be released.  Calls to Wait will block until the buckets
	// contain adequate resources. If a request attempts to write more than the
	// burst limit, it will wait until the bucket is completely full before
	// acquiring the requested quantity and putting the limiter in debt.
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
	config Config,
	metrics tenantMetrics,
	options ...quotapool.Option,
) {
	*rl = limiter{
		parent:   parent,
		tenantID: tenantID,
		metrics:  metrics,
	}
	// Note: if multiple token buckets are needed, consult the history of
	// this file as of 0e70529f84 for a sample implementation.
	bucket := makeTokenBucket(config)
	rl.qp = quotapool.New(tenantID.String(), &bucket, options...)
	bucket.clock = rl.qp.TimeSource()
	bucket.lastUpdated = bucket.clock.Now()
}

// Wait is part of the Limiter interface.
func (rl *limiter) Wait(ctx context.Context, isWrite bool, writeBytes int64) error {
	// TODO(radu): find a way to omit these atomic operations in the case when we
	// don't have to wait.
	rl.metrics.currentBlocked.Inc(1)
	defer rl.metrics.currentBlocked.Dec(1)

	r := newWaitRequest(isWrite, writeBytes)
	defer putWaitRequest(r)

	if err := rl.qp.Acquire(ctx, r); err != nil {
		return err
	}

	if isWrite {
		rl.metrics.writeRequestsAdmitted.Inc(1)
		rl.metrics.writeBytesAdmitted.Inc(writeBytes)
	} else {
		// We don't know how much we will read; the bytes will be accounted for
		// after the fact in RecordRead.
		rl.metrics.readRequestsAdmitted.Inc(1)
	}

	return nil
}

// RecordRead is part of the Limiter interface.
func (rl *limiter) RecordRead(ctx context.Context, readBytes int64) {
	rb := newReadBytesResource(readBytes)
	defer putReadBytesResource(rb)
	rl.metrics.readBytesAdmitted.Inc(readBytes)
	rl.qp.Add(rb)
}

// updateConfig is used by the factory to inform the limiter of a new
// configuration.
func (rl *limiter) updateConfig(config Config) {
	rl.qp.Add(config)
}

// tokenBucket represents the token bucket for KV Compute Units and its
// associated configuration. It implements quotapool.Resource.
type tokenBucket struct {
	config      Config
	clock       timeutil.TimeSource
	lastUpdated time.Time
	// Current number of tokens, in KV Compute Units.
	tokens float64
}

var _ quotapool.Resource = (*tokenBucket)(nil)

func makeTokenBucket(config Config) tokenBucket {
	return tokenBucket{
		config: config,
		tokens: float64(config.Burst),
	}
}

// update accounts for the passing of time.
func (tb *tokenBucket) update() {
	now := tb.clock.Now()

	if since := now.Sub(tb.lastUpdated); since > 0 {
		tb.tokens += float64(tb.config.Rate) * since.Seconds()
		tb.clampTokens()
		tb.lastUpdated = now
	}
}

// tryToFulfill calculates the number of KV Compute Units needed for the
// request and tries to remove them from the bucket.
//
// If the request can be fulfilled, the current token amount is adjusted. Note
// if the current amount is equal to Burst, then we allow any request to be
// fulfilled. This is because we want to have request put the rate limiter
// in debt rather than prevent execution of requests.
//
// If the request is not satisfied, the amount of clock that must be waited for
// the request to be satisfied at the current rate is returned.
func (tb *tokenBucket) tryToFulfill(
	req *waitRequest,
) (fulfilled bool, tryAgainAfter time.Duration) {
	var needed float64
	if req.isWrite {
		needed = tb.config.WriteRequestUnits + float64(req.writeBytes)*tb.config.WriteUnitsPerByte
	} else {
		// We don't know the size of the read upfront; we will adjust the bucket
		// after the fact in RecordRead.
		needed = tb.config.ReadRequestUnits
	}
	if q := tb.tokens; needed <= q || q == tb.config.Burst {
		tb.tokens -= needed
		return true, 0
	}

	// We'll calculate the amount of clock until the quota is full if we're
	// requesting more than the burst limit.
	if needed > tb.config.Burst {
		needed = tb.config.Burst
	}
	delta := needed - tb.tokens
	tryAgainAfter = time.Duration((delta * float64(time.Second)) / tb.config.Rate)
	return false, tryAgainAfter
}

// setConf updates the configuration for a tokenBucket.
//
// TODO(ajwerner): It seems possible that when adding or reducing the burst
// values that we might want to remove those values from the token bucket.
// It's not obvious that we want to add tokens when increasing the burst as
// that might lead to a big spike in load immediately upon increasing this
// limit.
func (tb *tokenBucket) updateConfig(config Config) {
	tb.config = config
	tb.clampTokens()
}

// clampTokens ensures that tokens does not exceed burst.
func (tb *tokenBucket) clampTokens() {
	if tb.tokens > tb.config.Burst {
		tb.tokens = tb.config.Burst
	}
}

// Merge is part of quotapool.Resource.
func (tb *tokenBucket) Merge(val interface{}) (shouldNotify bool) {
	switch val := val.(type) {
	case Config:
		// Account for the accumulation since lastUpdate and now under the old
		// configuration.
		tb.update()

		tb.updateConfig(val)
		return true

	case *readBytesResource:
		tb.tokens -= float64(val.readBytes) * tb.config.ReadUnitsPerByte
		// Do not notify the head of the queue. In the best case we did not disturb
		// the time at which it can be fulfilled and in the worst case, we made it
		// further in the future.
		return false

	default:
		panic(errors.AssertionFailedf("merge not implemented for %T", val))
	}
}

// waitRequest is used to wait for adequate resources in the tokenBuckets.
type waitRequest struct {
	isWrite    bool
	writeBytes int64
}

var _ quotapool.Request = (*waitRequest)(nil)

var waitRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(waitRequest) },
}

// newWaitRequest allocates a waitRequest from the sync.Pool.
// It should be returned with putWaitRequest.
func newWaitRequest(isWrite bool, writeBytes int64) *waitRequest {
	r := waitRequestSyncPool.Get().(*waitRequest)
	*r = waitRequest{
		isWrite:    isWrite,
		writeBytes: writeBytes,
	}
	return r
}

func putWaitRequest(r *waitRequest) {
	*r = waitRequest{}
	waitRequestSyncPool.Put(r)
}

// Acquire is part of quotapool.Request.
func (req *waitRequest) Acquire(
	ctx context.Context, res quotapool.Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
	r := res.(*tokenBucket)
	r.update()
	return r.tryToFulfill(req)
}

// ShouldWait is part of quotapool.Request.
func (req *waitRequest) ShouldWait() bool {
	return true
}

type readBytesResource struct {
	readBytes int64
}

var readBytesResourceSyncPool = sync.Pool{
	New: func() interface{} { return new(readBytesResource) },
}

func newReadBytesResource(readBytes int64) *readBytesResource {
	rb := readBytesResourceSyncPool.Get().(*readBytesResource)
	*rb = readBytesResource{
		readBytes: readBytes,
	}
	return rb
}

func putReadBytesResource(rb *readBytesResource) {
	*rb = readBytesResource{}
	readBytesResourceSyncPool.Put(rb)
}
