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
//
// TODO(ajwerner,tbg): When we have client information in the context, we'd like
// to use it to ensure that we only rate limit batches due to this tenant and
// not batches due to the system tenant or internal traffic.
func (r *Replica) maybeRateLimitBatch(ctx context.Context, ba *roachpb.BatchRequest) error {
	if r.tenantLimiter == nil {
		return nil
	}

	return r.tenantLimiter.Wait(ctx, bytesWrittenFromRequest(ba))
}

// bytesWrittenFromBatchRequest returns an approximation of the number of bytes
// written by a batch request.
//
// TODO(ajwerner): Come up with a better methodology to compute the write
// bytes. This is extraordinarily crude but it probably captures the intent
// reasonably.
func bytesWrittenFromRequest(ba *roachpb.BatchRequest) int64 {
	for _, ru := range ba.Requests {
		if !roachpb.IsReadOnly(ru.GetInner()) {
			return int64(ba.Size())
		}
	}
	return 0
}

// maybeRecordRead is used to record a read against the tenant rate limiter.
// TODO(ajwerner): Consider whether this needs to only run on batch requests
// for which canBackpressure is true.
func (r *Replica) maybeRecordRead(ctx context.Context, br *roachpb.BatchResponse) {
	if r.tenantLimiter == nil || br == nil {
		return
	}
	// TODO(ajwerner): Come up with a better methodology to compute the read
	// bytes.
	r.tenantLimiter.RecordRead(ctx, int64(br.Size()))
}
