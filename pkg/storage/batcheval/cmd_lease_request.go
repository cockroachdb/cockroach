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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
)

func init() {
	RegisterCommand(roachpb.RequestLease, declareKeysRequestLease, RequestLease)
}

// RequestLease sets the range lease for this range. The command fails
// only if the desired start timestamp collides with a previous lease.
// Otherwise, the start timestamp is wound back to right after the expiration
// of the previous lease (or zero). If this range replica is already the lease
// holder, the expiration will be extended or shortened as indicated. For a new
// lease, all duties required of the range lease holder are commenced, including
// clearing the command queue and timestamp cache.
func RequestLease(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.RequestLeaseRequest)
	// When returning an error from this method, must always return
	// a newFailedLeaseTrigger() to satisfy stats.
	prevLease, _ := cArgs.EvalCtx.GetLease()

	rErr := &roachpb.LeaseRejectedError{
		Existing:  prevLease,
		Requested: args.Lease,
	}

	// MIGRATION(tschottdorf): needed to apply Raft commands which got proposed
	// before the StartStasis field was introduced.
	newLease := args.Lease
	if newLease.DeprecatedStartStasis == nil {
		newLease.DeprecatedStartStasis = newLease.Expiration
	}

	isExtension := prevLease.Replica.StoreID == newLease.Replica.StoreID
	effectiveStart := newLease.Start

	// Wind the start timestamp back as far towards the previous lease as we
	// can. That'll make sure that when multiple leases are requested out of
	// order at the same replica (after all, they use the request timestamp,
	// which isn't straight out of our local clock), they all succeed unless
	// they have a "real" issue with a previous lease. Example: Assuming no
	// previous lease, one request for [5, 15) followed by one for [0, 15)
	// would fail without this optimization. With it, the first request
	// effectively gets the lease for [0, 15), which the second one can commit
	// again (even extending your own lease is possible; see below).
	//
	// If this is our lease (or no prior lease exists), we effectively absorb
	// the old lease. This allows multiple requests from the same replica to
	// merge without ticking away from the minimal common start timestamp. It
	// also has the positive side-effect of fixing #3561, which was caused by
	// the absence of replay protection.
	if prevLease.Replica.StoreID == 0 || isExtension {
		effectiveStart.Backward(prevLease.Start)
		// If the lease holder promised to not propose any commands below
		// MinProposedTS, it must also not be allowed to extend a lease before that
		// timestamp. We make sure that when a node restarts, its earlier in-flight
		// commands (which are not tracked by the command queue post restart)
		// receive an error under the new lease by making sure the sequence number
		// of that lease is higher. This in turn is achieved by forwarding its start
		// time here, which makes it not Equivalent() to the preceding lease for the
		// same store.
		//
		// Note also that leastPostApply makes sure to update the timestamp cache in
		// this case: even though the lease holder does not change, the the sequence
		// number does and this triggers a low water mark bump.
		//
		// The bug prevented with this is unlikely to occur in practice
		// since earlier commands usually apply before this lease will.
		if ts := args.MinProposedTS; isExtension && ts != nil {
			effectiveStart.Forward(*ts)
		}

	} else if prevLease.Type() == roachpb.LeaseExpiration {
		effectiveStart.Backward(prevLease.Expiration.Next())
	}

	if isExtension {
		if effectiveStart.Less(prevLease.Start) {
			rErr.Message = "extension moved start timestamp backwards"
			return newFailedLeaseTrigger(false /* isTransfer */), rErr
		}
		if newLease.Type() == roachpb.LeaseExpiration {
			// NB: Avoid mutating pointers in the argument which might be shared with
			// the caller.
			t := *newLease.Expiration
			newLease.Expiration = &t
			newLease.Expiration.Forward(prevLease.GetExpiration())
		}
	} else if prevLease.Type() == roachpb.LeaseExpiration && effectiveStart.Less(prevLease.GetExpiration()) {
		rErr.Message = "requested lease overlaps previous lease"
		return newFailedLeaseTrigger(false /* isTransfer */), rErr
	}
	newLease.Start = effectiveStart
	return evalNewLease(ctx, cArgs.EvalCtx, batch, cArgs.Stats,
		newLease, prevLease, isExtension, false /* isTransfer */)
}
