// Copyright 2019 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/gc"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// cachedProtectedTimestampState is used to cache information about the state
// of protected timestamps as they pertain to this replica. The data is
// refreshed when the replica examines protected timestamps when being
// considered for gc or when verifying a protected timestamp record.
// It is consulted when determining whether a request can be served.
type cachedProtectedTimestampState struct {
	// readAt denotes the timestamp at which this record was read.
	// It is used to coordinate updates to this field. It is also used to
	// ensure that the protected timestamp subsystem can be relied upon. If
	// the cache state is older than the lease start time then it is possible
	// that protected timestamps have not been observed. In this case we must
	// assume that any protected timestamp could exist to provide the contract
	// on verify.
	readAt         hlc.Timestamp
	earliestRecord *ptpb.Record
}

// clearIfNotNewer clears the state in ts if it is not newer than the passed
// value. This is used in conjunction with Replica.maybedUpdateCachedProtectedTS().
// This optimization allows most interactions with protected timestamps to
// operate using a shared lock. Only in cases where the cached value is known to
// be older will the update be attempted.
func (ts *cachedProtectedTimestampState) clearIfNotNewer(existing cachedProtectedTimestampState) {
	if !existing.readAt.Less(ts.readAt) {
		*ts = cachedProtectedTimestampState{}
	}
}

// maybeUpdateCachedProtectedTS is used to optimize updates. We learn about
// needs to update the cache while holding Replica.mu for reading but need to
// perform the update with the exclusive lock. This function is intended to
// be deferred.
func (r *Replica) maybeUpdateCachedProtectedTS(ts *cachedProtectedTimestampState) {
	if *ts == (cachedProtectedTimestampState{}) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.cachedProtectedTS.readAt.Less(ts.readAt) {
		r.mu.cachedProtectedTS = *ts
	}
}

// protectedTimestampRecordApplies returns true if it is this case that the
// record which protects the `protected` timestamp. It returns false if it may
// not. If the state of the cache  is not sufficiently new to determine whether
// the record will apply, the cache is refreshed and then the check is performed
// again. See r.protectedTimestampRecordCurrentlyApplies() for more details.
func (r *Replica) protectedTimestampRecordApplies(
	ctx context.Context, args *roachpb.AdminVerifyProtectedTimestampRequest,
) (willApply bool, doesNotApplyReason string, _ error) {
	// Check the state of the cache without a refresh.
	willApply, cacheTooOld, doesNotApplyReason, err := r.protectedTimestampRecordCurrentlyApplies(
		ctx, args)
	if err != nil {
		return false, doesNotApplyReason, err
	}
	if !cacheTooOld {
		return willApply, doesNotApplyReason, nil
	}
	// Refresh the cache so that we know that the next time we come around we're
	// certain to either see the record or see a timestamp for readAt that is
	// greater than or equal to recordAliveAt.
	if err := r.store.protectedtsCache.Refresh(ctx, args.RecordAliveAt); err != nil {
		return false, doesNotApplyReason, err
	}
	willApply, cacheTooOld, doesNotApplyReason, err = r.protectedTimestampRecordCurrentlyApplies(
		ctx, args)
	if err != nil {
		return false, doesNotApplyReason, err
	}
	if cacheTooOld {
		return false, doesNotApplyReason, errors.AssertionFailedf(
			"cache was not updated after being refreshed")
	}
	return willApply, doesNotApplyReason, nil
}

func (r *Replica) readProtectedTimestampsRLocked(
	ctx context.Context, f func(r *ptpb.Record),
) (ts cachedProtectedTimestampState) {
	desc := r.descRLocked()
	gcThreshold := *r.mu.state.GCThreshold

	ts.readAt = r.store.protectedtsCache.Iterate(ctx,
		roachpb.Key(desc.StartKey),
		roachpb.Key(desc.EndKey),
		func(rec *ptpb.Record) (wantMore bool) {
			// Check if we've already GC'd past the timestamp this record was trying
			// to protect, in which case we know that the record does not apply.
			// Note that when we implement PROTECT_AT, we'll need to consult some
			// replica state here to determine whether the record indeed has been
			// applied.
			if isValid := gcThreshold.LessEq(rec.Timestamp); !isValid {
				return true
			}
			if f != nil {
				f(rec)
			}
			if ts.earliestRecord == nil || rec.Timestamp.Less(ts.earliestRecord.Timestamp) {
				ts.earliestRecord = rec
			}
			return true
		})
	return ts
}

// protectedTimestampRecordCurrentlyApplies determines whether a record with
// the specified ID which protects `protected` and is known to exist at
// `recordAliveAt` will apply given the current state of the cache. This method
// is called by `r.protectedTimestampRecordApplies()`. It may be the case that
// the current state of the cache is too old to determine whether the record
// will apply. In such cases the cache should be refreshed to recordAliveAt and
// then this method should be called again.
// In certain cases we return a doesNotApplyReason explaining why the protected
// ts record does not currently apply. We do not want to return an error so that
// we can aggregate the reasons across multiple
// AdminVerifyProtectedTimestampRequest, as explained in
// adminVerifyProtectedTimestamp.
func (r *Replica) protectedTimestampRecordCurrentlyApplies(
	ctx context.Context, args *roachpb.AdminVerifyProtectedTimestampRequest,
) (willApply, cacheTooOld bool, doesNotApplyReason string, _ error) {
	// We first need to check that we're the current leaseholder.
	// TODO(ajwerner): what other conditions with regards to time do we need to
	// check? I don't think there are any. If the recordAliveAt is after our
	// liveness expiration that's okay because we're either going to find the
	// record or we're not and if we don't then we'll push the cache and re-assert
	// that we're still the leaseholder. If somebody else becomes the leaseholder
	// then they will have to go through the same process.
	ls, pErr := r.redirectOnOrAcquireLease(ctx)
	if pErr != nil {
		return false, false, "", pErr.GoError()
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

	// We may be reading the protected timestamp cache while we're holding
	// the Replica.mu for reading. If we do so and find newer state in the cache
	// then we want to, update the replica's cache of its state. The guarantee
	// we provide is that if a record is successfully verified then the Replica's
	// cachedProtectedTS will have a readAt value high enough to include that
	// record.
	var read cachedProtectedTimestampState
	defer r.maybeUpdateCachedProtectedTS(&read)
	r.mu.RLock()
	defer r.mu.RUnlock()
	defer read.clearIfNotNewer(r.mu.cachedProtectedTS)

	// If the key that routed this request to this range is now out of this
	// range's bounds, return an error for the client to try again on the
	// correct range.
	desc := r.descRLocked()
	if !kvserverbase.ContainsKeyRange(desc, args.Key, args.EndKey) {
		return false, false, "", roachpb.NewRangeKeyMismatchError(ctx, args.Key, args.EndKey, desc,
			r.mu.state.Lease)
	}
	if args.Protected.LessEq(*r.mu.state.GCThreshold) {
		gcReason := fmt.Sprintf("protected ts: %s is less than equal to the GCThreshold: %s for the"+
			" range %s - %s", args.Protected.String(), r.mu.state.GCThreshold.String(),
			desc.StartKey.String(), desc.EndKey.String())
		return false, false, gcReason, nil
	}
	if args.RecordAliveAt.Less(ls.Lease.Start.ToTimestamp()) {
		return true, false, "", nil
	}

	// Now we're in the case where maybe it is possible that we're going to later
	// attempt to set the GC threshold above our protected point so to prevent
	// that we add some state to the replica.
	r.protectedTimestampMu.Lock()
	defer r.protectedTimestampMu.Unlock()
	if args.Protected.Less(r.protectedTimestampMu.pendingGCThreshold) {
		gcReason := fmt.Sprintf(
			"protected ts: %s is less than the pending GCThreshold: %s for the range %s - %s",
			args.Protected.String(), r.protectedTimestampMu.pendingGCThreshold.String(),
			desc.StartKey.String(), desc.EndKey.String())
		return false, false, gcReason, nil
	}

	var seen bool
	read = r.readProtectedTimestampsRLocked(ctx, func(r *ptpb.Record) {
		if r.ID == args.RecordID {
			seen = true
		}
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
		r.protectedTimestampMu.minStateReadTimestamp = read.readAt
		return true, false, "", nil
	}

	isCacheTooOld := read.readAt.Less(args.RecordAliveAt)
	// Protected timestamp state has progressed past the point at which we
	// should see this record. This implies that the record has been removed.
	if !isCacheTooOld {
		recordRemovedReason := "protected ts record has been removed"
		return false, false, recordRemovedReason, nil
	}
	// Retry, since the cache is too old.
	return false, true, "", nil
}

// checkProtectedTimestampsForGC determines whether the Replica can run GC. If
// the Replica can run GC, this method returns the latest timestamp which can be
// used to determine a valid new GCThreshold. The policy is passed in rather
// than read from the replica state to ensure that the same value used for this
// calculation is used later.
//
// In the case that GC can proceed, four timestamps are returned: The timestamp
// corresponding to the state of the cache used to make the determination (used
// for markPendingGC when actually performing GC), the timestamp used as the
// basis to calculate the new gc threshold (used for scoring and reporting), the
// old gc threshold, and the new gc threshold.
func (r *Replica) checkProtectedTimestampsForGC(
	ctx context.Context, policy zonepb.GCPolicy,
) (canGC bool, cacheTimestamp, gcTimestamp, oldThreshold, newThreshold hlc.Timestamp) {

	// We may be reading the protected timestamp cache while we're holding
	// the Replica.mu for reading. If we do so and find newer state in the cache
	// then we want to, update the replica's cache of its state. The guarantee
	// we provide is that if a record is successfully verified then the Replica's
	// cachedProtectedTS will have a readAt value high enough to include that
	// record.
	var read cachedProtectedTimestampState
	defer r.maybeUpdateCachedProtectedTS(&read)
	r.mu.RLock()
	defer r.mu.RUnlock()
	defer read.clearIfNotNewer(r.mu.cachedProtectedTS)

	oldThreshold = *r.mu.state.GCThreshold
	lease := *r.mu.state.Lease

	// read.earliestRecord is the record with the earliest timestamp which is
	// greater than the existing gcThreshold.
	read = r.readProtectedTimestampsRLocked(ctx, nil)
	gcTimestamp = read.readAt
	if read.earliestRecord != nil {
		// NB: we want to allow GC up to the timestamp preceding the earliest valid
		// record.
		impliedGCTimestamp := gc.TimestampForThreshold(read.earliestRecord.Timestamp.Prev(), policy)
		if impliedGCTimestamp.Less(gcTimestamp) {
			gcTimestamp = impliedGCTimestamp
		}
	}

	if gcTimestamp.Less(lease.Start.ToTimestamp()) {
		log.VEventf(ctx, 1, "not gc'ing replica %v due to new lease %v started after %v",
			r, lease, gcTimestamp)
		return false, hlc.Timestamp{}, hlc.Timestamp{}, hlc.Timestamp{}, hlc.Timestamp{}
	}

	newThreshold = gc.CalculateThreshold(gcTimestamp, policy)

	return true, read.readAt, gcTimestamp, oldThreshold, newThreshold
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
