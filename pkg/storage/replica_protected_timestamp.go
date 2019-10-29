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

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

// recordWillApply returns true if it is this case that the record which
// protects the specified time and was created at the specified time will
// apply. It returns false if it may not.
func (r *Replica) recordWillApply(
	ctx context.Context, protected, recordCreatedAt hlc.Timestamp,
) (willApply bool, _ error) {
	ls, pErr := r.redirectOnOrAcquireLease(ctx)
	if pErr != nil {
		return false, pErr.GoError()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.mu.state.GCThreshold.Less(protected) {
		return false, nil
	}
	if recordCreatedAt.Less(ls.Lease.Start) {
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
	r.protectedTimestampMu.minStateReadTimestamp = recordCreatedAt
	return true, nil
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
