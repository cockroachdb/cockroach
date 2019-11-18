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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

// protectedTimestampRecordWillApply returns true if it is this case that the
// record which protects the specified time and was created at the specified
// time will apply. It returns false if it may not. There is an additional case
// where this Replica promises that it will wait to hear about this Record
// before performing any more GC.
func (r *Replica) protectedTimestampRecordWillApply(
	ctx context.Context, protected, recordAliveAt hlc.Timestamp, id uuid.UUID,
) (willApply bool, _ error) {
	ls, pErr := r.redirectOnOrAcquireLease(ctx)
	if pErr != nil {
		return false, pErr.GoError()
	}

	// NB: It should be the case that the recordAliveAt timestamp
	// is before the current time and that the above lease check means that
	// the replica is the leaseholder at the current time. If recordAliveAt
	// happened to be newer than the current time we'd need to make sure that
	// the current Replica will be live at that time. Given that recordAliveAt
	// has to be before the batch timestamp for this request and we should
	// have forwarded the local clock to the batch timestamp this can't
	// happen.

	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.mu.state.GCThreshold.Less(protected) {
		return false, nil
	}
	if recordAliveAt.Less(ls.Lease.Start) {
		return true, nil
	}

	// Now we're in the case where maybe it is possible that we're going to later
	// attempt to set the GC threshold above our protected point so to prevent
	// that we add some state to the replica.
	r.protectedTimestampMu.Lock()
	defer r.protectedTimestampMu.Unlock()
	if protected.Less(r.protectedTimestampMu.pendingGCThreshold) {
		return false, nil
	}

	var seen bool
	desc := r.mu.state.Desc
	readAt := r.store.protectedtsTracker.ProtectedBy(ctx, roachpb.Span{
		Key:    roachpb.Key(desc.StartKey),
		EndKey: roachpb.Key(desc.EndKey),
	}, func(r *ptpb.Record) {
		if r.ID == id {
			seen = true
		}
	})
	if seen {
		return true, nil
	}
	// Protected timestamp state has progressed past the point at which we
	// should see this record. This implies that the record has been removed.
	if recordAliveAt.Less(readAt) {
		return false, nil
	}

	r.protectedTimestampMu.minStateReadTimestamp.Forward(recordAliveAt)
	r.protectedTimestampMu.promisedIDs[id] = recordAliveAt
	return true, nil
}

// checkProtectedTimestampsForGC determines whether the Replica can run GC.
// If the Replica can run GC, this method returns the latest timestamp which
// can be used to determine a valid new GCThreshold.
func (r *Replica) checkProtectedTimestampsForGC(
	ctx context.Context, now hlc.Timestamp,
) (canGC bool, gcTimestamp hlc.Timestamp) {
	r.mu.RLock()
	desc := r.descRLocked()
	gcThreshold := *r.mu.state.GCThreshold
	lease := *r.mu.state.Lease
	r.mu.RUnlock()
	var overlapping []*ptpb.Record
	gcTimestamp = r.store.protectedtsTracker.ProtectedBy(ctx, roachpb.Span{
		Key:    roachpb.Key(desc.StartKey),
		EndKey: roachpb.Key(desc.EndKey),
	}, func(rec *ptpb.Record) {

		r.protectedTimestampMu.Lock()
		if _, ok := r.protectedTimestampMu.promisedIDs[rec.ID]; ok {
			delete(r.protectedTimestampMu.promisedIDs, rec.ID)
		}
		r.protectedTimestampMu.Unlock()

		// Check if we've already GC'd past the timestamp this record was trying
		// to protect, in which case we know that the record does not apply.
		// Note that when we implement PROTECT_AT, we'll need to consult some
		// replica state here to determine whether the record indeed has been
		// applied.
		if rec.Timestamp.Less(gcThreshold) {
			return
		}
		overlapping = append(overlapping, rec)
	})

	// Now we need to go visit our promises. We should be able to clear some of them out.
	// In particular, all of the promises which have a timestamp earlier than what
	// we just read we can now throw out.
	r.protectedTimestampMu.Lock()
	var promisedTimestamp hlc.Timestamp
	for id, ts := range r.protectedTimestampMu.promisedIDs {
		if ts.Less(gcTimestamp) {
			delete(r.protectedTimestampMu.promisedIDs, id)
		} else if promisedTimestamp.Less(ts) {
			promisedTimestamp = ts
		}
	}
	r.protectedTimestampMu.Unlock()

	if len(overlapping) > 0 {
		log.VEventf(ctx, 1, "not gc'ing replica %v due to protected timestamps %v as of %v",
			r, overlapping, gcTimestamp)
		return false, hlc.Timestamp{}
	}
	if gcTimestamp.Less(promisedTimestamp) {
		log.VEventf(ctx, 1, "not gc'ing replica %v due to new promised timestamp %v after read timestamp %v",
			r, promisedTimestamp, gcTimestamp)
		return false, hlc.Timestamp{}
	}
	// TODO(ajwerner): Is there any need to check the end of the lease?
	// It seems like there shouldn't be given the fact that the GC request
	// will fail if this replica does not have the lease.
	if gcTimestamp.Less(lease.Start) {
		log.VEventf(ctx, 1, "not gc'ing replica %v due to new lease %v started after %v",
			r, lease, gcTimestamp)
		return false, hlc.Timestamp{}
	}
	return true, gcTimestamp
}

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
