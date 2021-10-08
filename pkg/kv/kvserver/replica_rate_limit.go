// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// maybeRateLimitBatch may block the batch waiting to be rate-limited. Note that
// the replica must be initialized and thus there is no synchronization issue
// on the tenantRateLimiter.
func (r *Replica) maybeRateLimitBatch(ctx context.Context, ba *roachpb.BatchRequest) error {
	if r.tenantLimiter == nil {
		return nil
	}
	tenantID, ok := roachpb.TenantFromContext(ctx)
	if !ok || tenantID == roachpb.SystemTenantID {
		return nil
	}
	return r.tenantLimiter.Wait(ctx, tenantcostmodel.MakeRequestInfo(ba))
}

// recordImpactOnRateLimiter is used to record a read against the tenant rate limiter.
func (r *Replica) recordImpactOnRateLimiter(ctx context.Context, br *roachpb.BatchResponse) {
	if r.tenantLimiter == nil || br == nil {
		return
	}

	r.tenantLimiter.RecordRead(ctx, tenantcostmodel.MakeResponseInfo(br))
}
