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
	"github.com/cockroachdb/cockroach/pkg/util/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	qp       *quotapool.AbstractPool
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
	bucket := &tokenBucket{}

	options = append(options, quotapool.OnWait(
		func(ctx context.Context, poolName string, r quotapool.Request) {
			rl.metrics.currentBlocked.Inc(1)
		},
		func(ctx context.Context, poolName string, r quotapool.Request) {
			rl.metrics.currentBlocked.Dec(1)
		},
	))

	// There is a lot of overlap with quotapool.RateLimiter, but we can't use it
	// directly without separate synchronization for the Config.
	rl.qp = quotapool.New(tenantID.String(), bucket, options...)
	bucket.init(config, rl.qp.TimeSource())
}

// Wait is part of the Limiter interface.
func (rl *limiter) Wait(ctx context.Context, isWrite bool, writeBytes int64) error {
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
	rl.metrics.readBytesAdmitted.Inc(readBytes)
	rl.qp.Update(func(res quotapool.Resource) (shouldNotify bool) {
		tb := res.(*tokenBucket)
		amount := float64(readBytes) * float64(tb.config.CostModel.KVReadByte)
		tb.Adjust(quotapool.Tokens(-amount))
		// Do not notify the head of the queue. In the best case we did not disturb
		// the time at which it can be fulfilled and in the worst case, we made it
		// further in the future.
		return false
	})
}

func (rl *limiter) updateConfig(config Config) {
	rl.qp.Update(func(res quotapool.Resource) (shouldNotify bool) {
		tb := res.(*tokenBucket)
		tb.config = config
		tb.UpdateConfig(quotapool.TokensPerSecond(config.Rate), quotapool.Tokens(config.Burst))
		return true
	})
}

// tokenBucket represents the token bucket for KV Compute Units and its
// associated configuration. It implements quotapool.Resource.
type tokenBucket struct {
	quotapool.TokenBucket

	config Config
}

var _ quotapool.Resource = (*tokenBucket)(nil)

func (tb *tokenBucket) init(config Config, timeSource timeutil.TimeSource) {
	tb.TokenBucket.Init(
		quotapool.TokensPerSecond(config.Rate), quotapool.Tokens(config.Burst), timeSource,
	)
	tb.config = config
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
	tb := res.(*tokenBucket)
	var needed tenantcostmodel.RU
	if req.isWrite {
		needed = tb.config.CostModel.KVWriteCost(req.writeBytes)
	} else {
		// We don't know the size of the read upfront; we will adjust the bucket
		// after the fact in RecordRead.
		needed = tb.config.CostModel.KVReadRequest
	}
	return tb.TryToFulfill(quotapool.Tokens(needed))
}

// ShouldWait is part of quotapool.Request.
func (req *waitRequest) ShouldWait() bool {
	return true
}
