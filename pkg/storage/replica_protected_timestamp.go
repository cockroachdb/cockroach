// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/gc"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// protectedTimestampRecordApplies returns true if it is this case that the
// record which protects the `protected` timestamp. It returns false if it may
// not. If the state of the cache  is not sufficiently new to determine whether
// the record will apply, the cache is refreshed and then the check is performed
// again. See r.protectedTimestampRecordCurrentlyApplies() for more details.
func (r *Replica) protectedTimestampRecordApplies(
	ctx context.Context, protected, recordAliveAt hlc.Timestamp, id uuid.UUID,
) (willApply bool, _ error) {
	// Check the state of the cache without a refresh.
	willApply, cacheTooOld, err := r.protectedTimestampRecordCurrentlyApplies(ctx, protected, recordAliveAt, id)
	if err != nil {
		return false, err
	}
	if !cacheTooOld {
		return willApply, nil
	}
	// Refresh the cache so that we know that the next time we come around we're
	// certain to either see the record or see a timestamp for readAt that is
	// greater than or equal to recordAliveAt.
	if err := r.store.protectedtsCache.Refresh(ctx, recordAliveAt); err != nil {
		return false, err
	}
	willApply, cacheTooOld, err = r.protectedTimestampRecordCurrentlyApplies(ctx, protected, recordAliveAt, id)
	if err != nil {
		return false, err
	}
	if cacheTooOld {
		return false, errors.AssertionFailedf("cache was not updated after being refreshed")
	}
	return willApply, nil
}

// protectedTimestampRecordCurrentlyApplies determines whether a record with
// the specified ID which protects `protected` and is known to exist at
// `recordAliveAt` will apply given the current state of the cache. This method
// is called by `r.protectedTimestampRecordApplies()`. It may be the case that
// the current state of the cache is too old to determine whether the record
// will apply. In such cases the cache should be refreshed to recordAliveAt and
// then this method should be called again.
func (r *Replica) protectedTimestampRecordCurrentlyApplies(
	ctx context.Context, protected, recordAliveAt hlc.Timestamp, id uuid.UUID,
) (willApply, cacheTooOld bool, _ error) {
	// We first need to check that we're the current leaseholder.
	// TODO(ajwerner): what other conditions with regards to time do we need to
	// check? I don't think there are any. If the recordAliveAt is after our
	// liveness expiration that's okay because we're either going to find the
	// record or we're not and if we don't then we'll push the cache and re-assert
	// that we're still the leaseholder. If somebody else becomes the leaseholder
	// then they will have to go through the same process.
	ls, pErr := r.redirectOnOrAcquireLease(ctx)
	if pErr != nil {
		return false, false, pErr.GoError()
	}

	// NB: It should be the case that the recordAliveAt timestamp
	// is before the current time and that the above lease check means that
	// the replica is the leaseholder at the current time. If recordAliveAt
	// happened to be newer than the current time we'd need to make sure that
	// the current Replica will be live at that time. Given that recordAliveAt
	// has to be before the batch timestamp for this request and we should
	// have forwarded the local clock to the batch timestamp this can't
	// happen.
	// TODO(ajwerner): do we need to assert that indeed the recordAliveAt precedes
	// the batch timestamp? Probably not a bad sanity check.

	r.mu.RLock()
	defer r.mu.RUnlock()
	if protected.LessEq(*r.mu.state.GCThreshold) {
		return false, false, nil
	}
	if recordAliveAt.Less(ls.Lease.Start) {
		return true, false, nil
	}

	// Now we're in the case where maybe it is possible that we're going to later
	// attempt to set the GC threshold above our protected point so to prevent
	// that we add some state to the replica.
	r.protectedTimestampMu.Lock()
	defer r.protectedTimestampMu.Unlock()
	if protected.Less(r.protectedTimestampMu.pendingGCThreshold) {
		return false, false, nil
	}

	var seen bool
	desc := r.mu.state.Desc
	readAt := r.store.protectedtsCache.Iterate(ctx,
		roachpb.Key(desc.StartKey), roachpb.Key(desc.EndKey),
		func(r *ptpb.Record) (wantMore bool) {
			if r.ID == id {
				seen = true
			}
			return !seen
		})
	// If we observed the record in question then we know that all future attempts
	// to run GC will observe the Record if it still exists. The one hazard we
	// need to avoid is a race whereby an attempt to run GC first checks the
	// protected timestamp state and then attempts to increase the GC threshold.
	// We set the minStateReadTimestamp here to avoid such races. The GC queue
	// will call markPendingGC just prior to sending a request to update the GC
	// threshold which will verify the safety of the new value relative to
	// minStateReadTimestamp.
	if seen {
		r.protectedTimestampMu.minStateReadTimestamp = readAt
		return true, false, nil
	}

	// Protected timestamp state has progressed past the point at which we
	// should see this record. This implies that the record has been removed.
	return false, readAt.Less(recordAliveAt), nil
}

// checkProtectedTimestampsForGC determines whether the Replica can run GC.
// If the Replica can run GC, this method returns the latest timestamp which
// can be used to determine a valid new GCThreshold. The policy is passed in
// rather than read from the replica state to ensure that the same value used
// for this calculation is used later.
//
// In the case that GC can proceed, three timestamps are returned: The timestamp
// corresponding to the state of the cache used to make the determination (used
// for markPendingGC when actually performing GC), the timestamp used as the
// basis to calculate the new gc threshold (used for scoring and reporting), and
// the new gc threshold itself.
func (r *Replica) checkProtectedTimestampsForGC(
	ctx context.Context, policy zonepb.GCPolicy,
) (canGC bool, cacheTimestamp, gcTimestamp, newThreshold hlc.Timestamp) {
	r.mu.RLock()
	desc := r.descRLocked()
	gcThreshold := *r.mu.state.GCThreshold
	lease := *r.mu.state.Lease
	r.mu.RUnlock()
	// earliestValidRecord is the record with the earliest timestamp which is
	// greater than the existing gcThreshold.
	var earliestValidRecord *ptpb.Record
	cacheTimestamp = r.store.protectedtsCache.Iterate(ctx,
		roachpb.Key(desc.StartKey),
		roachpb.Key(desc.EndKey),
		func(rec *ptpb.Record) (wantMore bool) {
			// Check if we've already GC'd past the timestamp this record was trying
			// to protect, in which case we know that the record does not apply.
			// Note that when we implement PROTECT_AT, we'll need to consult some
			// replica state here to determine whether the record indeed has been
			// applied.
			if gcThreshold.LessEq(rec.Timestamp) &&
				(earliestValidRecord == nil || rec.Timestamp.Less(earliestValidRecord.Timestamp)) {
				earliestValidRecord = rec
			}
			return true
		})
	gcTimestamp = cacheTimestamp
	if earliestValidRecord != nil {
		// NB: we want to allow GC up to the timestamp preceding the earliest valid
		// record.
		impliedGCTimestamp := gc.TimestampForThreshold(earliestValidRecord.Timestamp.Prev(), policy)
		if impliedGCTimestamp.Less(gcTimestamp) {
			gcTimestamp = impliedGCTimestamp
		}
	}

	if gcTimestamp.Less(lease.Start) {
		log.VEventf(ctx, 1, "not gc'ing replica %v due to new lease %v started after %v",
			r, lease, gcTimestamp)
		return false, hlc.Timestamp{}, hlc.Timestamp{}, hlc.Timestamp{}
	}

	newThreshold = gc.CalculateThreshold(gcTimestamp, policy)

	// If we've already GC'd right up to this record, there's no reason to
	// gc again.
	if newThreshold.Equal(gcThreshold) {
		return false, hlc.Timestamp{}, hlc.Timestamp{}, hlc.Timestamp{}
	}

	return true, cacheTimestamp, gcTimestamp, newThreshold
}

// markPendingGC is called just prior to sending the GC request to increase the
// GC threshold during GC queue processing. This method synchronizes such
// requests with the processing of AdminVerifyProtectedTimestamp requests. Such
// synchronization is important to prevent races where the protected timestamp
// state is read from a stale point in time and then concurrently, a
// verification request arrives which applies under a later cache state and then
// the gc queue, acting on older cache state, attempts to set the gc threshold
// above a successfully verified record.
func (r *Replica) markPendingGC(readAt, newThreshold hlc.Timestamp) error {
	r.protectedTimestampMu.Lock()
	defer r.protectedTimestampMu.Unlock()
	if readAt.Less(r.protectedTimestampMu.minStateReadTimestamp) {
		return errors.Errorf("cannot set gc threshold to %v because read at %v < min %v",
			newThreshold, readAt, r.protectedTimestampMu.minStateReadTimestamp)
	}
	r.protectedTimestampMu.pendingGCThreshold = newThreshold
	return nil
}
