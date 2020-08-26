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
	return r.tenantLimiter.Wait(ctx, ba.IsWrite(), bytesWrittenFromRequest(ba))
}

// bytesWrittenFromBatchRequest returns an approximation of the number of bytes
// written by a batch request.
func bytesWrittenFromRequest(ba *roachpb.BatchRequest) int64 {
	var writeBytes int64
	for _, ru := range ba.Requests {
		if swr, isSizedWrite := ru.GetInner().(roachpb.SizedWriteRequest); isSizedWrite {
			writeBytes += swr.WriteBytes()
		}
	}
	return writeBytes
}

// recordImpactOnRateLimiter is used to record a read against the tenant rate limiter.
func (r *Replica) recordImpactOnRateLimiter(ctx context.Context, br *roachpb.BatchResponse) {
	if r.tenantLimiter == nil || br == nil {
		return
	}

	r.tenantLimiter.RecordRead(ctx, bytesReadFromResponse(br))
}

func bytesReadFromResponse(br *roachpb.BatchResponse) int64 {
	var readBytes int64
	for _, ru := range br.Responses {
		readBytes += ru.GetInner().Header().NumBytes
	}
	return readBytes
}
