// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package batcheval

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
)

func newFailedLeaseTrigger(isTransfer bool) result.Result {
	var trigger result.Result
	trigger.Local.LeaseMetricsResult = new(result.LeaseMetricsType)
	if isTransfer {
		*trigger.Local.LeaseMetricsResult = result.LeaseTransferError
	} else {
		*trigger.Local.LeaseMetricsResult = result.LeaseRequestError
	}
	return trigger
}

// evalNewLease checks that the lease contains a valid interval and that
// the new lease holder is still a member of the replica set, and then proceeds
// to write the new lease to the batch, emitting an appropriate trigger.
//
// The new lease might be a lease for a range that didn't previously have an
// active lease, might be an extension or a lease transfer.
//
// isExtension should be set if the lease holder does not change with this
// lease. If it doesn't change, we don't need the application of this lease to
// block reads.
//
// TODO(tschottdorf): refactoring what's returned from the trigger here makes
// sense to minimize the amount of code intolerant of rolling updates.
func evalNewLease(
	ctx context.Context,
	rec EvalContext,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	lease roachpb.Lease,
	prevLease roachpb.Lease,
	isExtension bool,
	isTransfer bool,
) (result.Result, error) {
	// When returning an error from this method, must always return
	// a newFailedLeaseTrigger() to satisfy stats.

	// Ensure either an Epoch is set or Start < Expiration.
	if (lease.Type() == roachpb.LeaseExpiration && !lease.Start.Less(lease.GetExpiration())) ||
		(lease.Type() == roachpb.LeaseEpoch && lease.Expiration != nil) {
		// This amounts to a bug.
		return newFailedLeaseTrigger(isTransfer),
			&roachpb.LeaseRejectedError{
				Existing:  prevLease,
				Requested: lease,
				Message: fmt.Sprintf("illegal lease: epoch=%d, interval=[%s, %s)",
					lease.Epoch, lease.Start, lease.Expiration),
			}
	}

	// Verify that requesting replica is part of the current replica set.
	desc := rec.Desc()
	if _, ok := desc.GetReplicaDescriptor(lease.Replica.StoreID); !ok {
		return newFailedLeaseTrigger(isTransfer),
			&roachpb.LeaseRejectedError{
				Existing:  prevLease,
				Requested: lease,
				Message:   "replica not found",
			}
	}

	// Store the lease to disk & in-memory.
	if err := MakeStateLoader(rec).SetLease(ctx, batch, ms, lease); err != nil {
		return newFailedLeaseTrigger(isTransfer), err
	}

	var pd result.Result
	// If we didn't block concurrent reads here, there'd be a chance that
	// reads could sneak in on a new lease holder between setting the lease
	// and updating the low water mark. This in itself isn't a consistency
	// violation, but it's a bit suspicious and did make
	// TestRangeTransferLease flaky. We err on the side of caution for now, but
	// at least we don't do it in case of an extension.
	//
	// TODO(tschottdorf): Maybe we shouldn't do this at all. Need to think
	// through potential consequences.
	pd.Replicated.BlockReads = !isExtension
	pd.Replicated.State = &storagebase.ReplicaState{
		Lease: &lease,
	}
	pd.Local.LeaseMetricsResult = new(result.LeaseMetricsType)
	if isTransfer {
		*pd.Local.LeaseMetricsResult = result.LeaseTransferSuccess
	} else {
		*pd.Local.LeaseMetricsResult = result.LeaseRequestSuccess
	}
	return pd, nil
}
