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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
)

func declareKeysRequestLease(
	desc roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: keys.RangeLeaseKey(header.RangeID)})
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
}

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

	// Requests should not set the sequence number themselves.
	if lease.Sequence != 0 {
		return newFailedLeaseTrigger(isTransfer),
			&roachpb.LeaseRejectedError{
				Existing:  prevLease,
				Requested: lease,
				Message:   "sequence number should not be set",
			}
	}

	// Set the lease's sequence number once VersionLeaseSequence is the minimum
	// version. We don't want to do it once VersionLeaseSequence IsActive but
	// not IsMinSupported because that means that the cluster could be
	// downgraded. This could lead to lease sequence numbers not being
	// incremented again and could even lead to sequence numbers being lost on
	// some nodes if older binaries were run again, because proto3 does not
	// preserve unknown fields (see github.com/gogo/protobuf/issues/275).
	//
	// We also set the lease's sequence number if the previous lease already had
	// a sequence number set. This ensures that even if the updated cluster
	// version is not known locally (since version updates are gossiped), we'll
	// never stop setting lease sequences once a range has started to.
	if prevLease.Sequence != 0 ||
		rec.ClusterSettings().Version.IsMinSupported(cluster.VersionLeaseSequence) {

		if prevLease.Equivalent(lease) {
			// If the proposed lease is equivalent to the previous lease, it is
			// given the same sequence number. This is subtle, but is important
			// to ensure that leases which are meant to be considered the same
			// lease for the purpose of matching leases during command execution
			// (see Lease.Equivalent) will be considered so. For example, an
			// extension to an expiration-based lease will result in a new lease
			// with the same sequence number.
			lease.Sequence = prevLease.Sequence
		} else {
			// We set the new lease sequence to one more than the previous lease
			// sequence. This is safe and will never result in repeated lease
			// sequences because the sequence check beneath Raft acts as an atomic
			// compare-and-swap of sorts. If two lease requests are proposed in
			// parallel, both with the same previous lease, only one will be
			// accepted and the other will get a LeaseRejectedError and need to
			// retry with a different sequence number. This is actually exactly what
			// the sequence number is used to enforce!
			lease.Sequence = prevLease.Sequence + 1
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

	if rec.ClusterSettings().Version.IsMinSupported(cluster.VersionProposedTSLeaseRequest) {
		pd.Replicated.PrevLeaseProposal = prevLease.ProposedTS
	}

	pd.Local.LeaseMetricsResult = new(result.LeaseMetricsType)
	if isTransfer {
		*pd.Local.LeaseMetricsResult = result.LeaseTransferSuccess
	} else {
		*pd.Local.LeaseMetricsResult = result.LeaseRequestSuccess
	}
	return pd, nil
}
