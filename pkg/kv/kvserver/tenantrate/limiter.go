// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantrate

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/tokenbucket"
)

// Limiter is used to rate-limit KV requests for a given tenant.
//
// The intention is to limit a single tenant from using a large percentage of a
// KV machine, which could lead to very significant variation in observed
// performance depending how many other tenants are using the node.
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
	Wait(ctx context.Context, reqInfo tenantcostmodel.BatchInfo) error

	// RecordRead subtracts the bytes read by a request from the token bucket.
	// This call may push the Limiter into debt in the ReadBytes dimensions
	// forcing subsequent Wait calls to block until the debt is paid.
	// However, RecordRead itself will never block.
	RecordRead(ctx context.Context, respInfo tenantcostmodel.BatchInfo)
}

type limiter struct {
	parent   *LimiterFactory
	tenantID roachpb.TenantID
	qp       *quotapool.AbstractPool
	metrics  tenantMetrics

	// authorizer is used to determine of the tenant should be unlimited or not.
	//
	// If this starts showing up in profiles, we could cache the result and create
	// some infrastructure to update the cache when the capability actually
	// changes.
	authorizer tenantcapabilities.Authorizer
}

// init initializes a new limiter.
func (rl *limiter) init(
	parent *LimiterFactory,
	tenantID roachpb.TenantID,
	config Config,
	metrics tenantMetrics,
	authorizer tenantcapabilities.Authorizer,
	options ...quotapool.Option,
) {
	*rl = limiter{
		parent:     parent,
		tenantID:   tenantID,
		metrics:    metrics,
		authorizer: authorizer,
	}
	// Note: if multiple token buckets are needed, consult the history of
	// this file as of 0e70529f84 for a sample implementation.
	bucket := &tokenBucket{}

	options = append(options,
		quotapool.OnWaitStart(
			func(ctx context.Context, poolName string, r quotapool.Request) {
				rl.metrics.currentBlocked.Inc(1)
			}),
		quotapool.OnWaitFinish(
			func(ctx context.Context, poolName string, r quotapool.Request, _ time.Time) {
				rl.metrics.currentBlocked.Dec(1)
			}),
	)

	// There is a lot of overlap with quotapool.RateLimiter, but we can't use it
	// directly without separate synchronization for the Config.
	rl.qp = quotapool.New(tenantID.String(), bucket, options...)
	bucket.init(config, rl.qp.TimeSource())
}

// Wait is part of the Limiter interface.
func (rl *limiter) Wait(ctx context.Context, reqInfo tenantcostmodel.BatchInfo) error {
	exempt := rl.authorizer.IsExemptFromRateLimiting(ctx, rl.tenantID)
	if !exempt {
		r := newWaitRequest(reqInfo)
		defer putWaitRequest(r)

		if err := rl.qp.Acquire(ctx, r); err != nil {
			return err
		}
	}

	if reqInfo.WriteCount > 0 {
		rl.metrics.writeBatchesAdmitted.Inc(1)
		rl.metrics.writeRequestsAdmitted.Inc(reqInfo.WriteCount)
		rl.metrics.writeBytesAdmitted.Inc(reqInfo.WriteBytes)
	}

	return nil
}

// RecordRead is part of the Limiter interface.
func (rl *limiter) RecordRead(ctx context.Context, respInfo tenantcostmodel.BatchInfo) {
	exempt := rl.authorizer.IsExemptFromRateLimiting(ctx, rl.tenantID)

	rl.metrics.readBatchesAdmitted.Inc(1)
	rl.metrics.readRequestsAdmitted.Inc(respInfo.ReadCount)
	rl.metrics.readBytesAdmitted.Inc(respInfo.ReadBytes)
	if !exempt {
		rl.qp.Update(func(res quotapool.Resource) (shouldNotify bool) {
			tb := res.(*tokenBucket)
			amount := tb.config.ReadBatchUnits
			amount += float64(respInfo.ReadCount) * tb.config.ReadRequestUnits
			amount += float64(respInfo.ReadBytes) * tb.config.ReadUnitsPerByte
			tb.Adjust(tokenbucket.Tokens(-amount))
			// Do not notify the head of the queue. In the best case we did not disturb
			// the time at which it can be fulfilled and in the worst case, we made it
			// further in the future.
			return false
		})
	}
}

// Release cleans up resources reserved for this limiter.
func (rl *limiter) Release() {
	rl.metrics.unlink()
	rl.qp.Close("released")
}

func (rl *limiter) TenantID() roachpb.TenantID {
	return rl.tenantID
}

func (rl *limiter) updateConfig(config Config) {
	rl.qp.Update(func(res quotapool.Resource) (shouldNotify bool) {
		tb := res.(*tokenBucket)
		tb.config = config
		tb.UpdateConfig(tokenbucket.TokensPerSecond(config.Rate), tokenbucket.Tokens(config.Burst))
		return true
	})
}

// tokenBucket represents the token bucket for KV Compute Units and its
// associated configuration. It implements quotapool.Resource.
type tokenBucket struct {
	tokenbucket.TokenBucket

	config Config
}

var _ quotapool.Resource = (*tokenBucket)(nil)

func (tb *tokenBucket) init(config Config, timeSource timeutil.TimeSource) {
	tb.TokenBucket.InitWithNowFn(
		tokenbucket.TokensPerSecond(config.Rate), tokenbucket.Tokens(config.Burst), timeSource.Now,
	)
	tb.config = config
}

// waitRequest is used to wait for adequate resources in the tokenBuckets.
type waitRequest struct {
	info tenantcostmodel.BatchInfo
}

var _ quotapool.Request = (*waitRequest)(nil)

var waitRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(waitRequest) },
}

// newWaitRequest allocates a waitRequest from the sync.Pool.
// It should be returned with putWaitRequest.
func newWaitRequest(info tenantcostmodel.BatchInfo) *waitRequest {
	r := waitRequestSyncPool.Get().(*waitRequest)
	*r = waitRequest{info: info}
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
	tb := res.(*tokenBucket)
	var needed float64
	if req.info.WriteCount > 0 {
		needed = tb.config.WriteBatchUnits
		needed += float64(req.info.WriteCount) * tb.config.WriteRequestUnits
		needed += float64(req.info.WriteBytes) * tb.config.WriteUnitsPerByte
	} else {
		// Only acquire tokens for read requests once the response has been
		// received. However, TryToFulfill still needs to be called with a zero
		// value, in case the quota pool is in debt and the read should block.
		needed = 0
	}
	return tb.TryToFulfill(tokenbucket.Tokens(needed))
}

// ShouldWait is part of quotapool.Request.
func (req *waitRequest) ShouldWait() bool {
	return true
}
