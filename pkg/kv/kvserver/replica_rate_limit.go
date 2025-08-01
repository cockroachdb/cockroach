// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/errors"
)

// maybeRateLimitBatch may block the batch waiting to be rate-limited. Note that
// the replica must be initialized and thus there is no synchronization issue
// on the tenantRateLimiter.
func (r *Replica) maybeRateLimitBatch(ctx context.Context, ba *kvpb.BatchRequest) error {
	if r.tenantLimiter == nil {
		return nil
	}
	tenantID, ok := roachpb.ClientTenantFromContext(ctx)
	if !ok || tenantID == roachpb.SystemTenantID {
		return nil
	}

	var info tenantcostmodel.BatchInfo
	for i := range ba.Requests {
		req := ba.Requests[i].GetInner()
		if !kvpb.IsReadOnly(req) {
			info.WriteCount++
			if swr, isSizedWrite := req.(kvpb.SizedWriteRequest); isSizedWrite {
				info.WriteBytes += swr.WriteBytes()
			}
		}
	}

	// Request object only needs to account for writeCount and writeBytes. All
	// the others are only used to calculate usage, and not for rate limiting.
	err := r.tenantLimiter.Wait(ctx, info)

	// For performance reasons, we do not hold any Replica's mutexes while waiting
	// on the tenantLimiter, and so we are racing with the Replica lifecycle. The
	// Replica can be destroyed and release the limiter before or during the Wait
	// call. In this case Wait returns an ErrClosed error. Instead of ErrClosed,
	// return the destruction status error which upper layers recognize.
	if err != nil && errors.HasType(err, (*quotapool.ErrClosed)(nil)) {
		if _, err := r.IsDestroyed(); err != nil {
			return err
		}
		return errors.AssertionFailedf("replica not marked as destroyed but limiter is closed: %v", r)
	}

	return err
}

// recordImpactOnRateLimiter is used to record a read against the tenant rate
// limiter.
func (r *Replica) recordImpactOnRateLimiter(
	ctx context.Context, br *kvpb.BatchResponse, isReadOnly bool,
) {
	if r.tenantLimiter == nil || br == nil || !isReadOnly {
		return
	}

	var info tenantcostmodel.BatchInfo
	for i := range br.Responses {
		resp := br.Responses[i].GetInner()
		info.ReadCount++
		info.ReadBytes += resp.Header().NumBytes
	}

	r.tenantLimiter.RecordRead(ctx, info)
}
