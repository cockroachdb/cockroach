// Copyright 2014 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
)

// AdminSplit divides the range into into two ranges using args.SplitKey.
func (r *Replica) AdminSplit(
	ctx context.Context, args roachpb.AdminSplitRequest, reason string,
) (reply roachpb.AdminSplitResponse, _ *roachpb.Error) {
	if len(args.SplitKey) == 0 {
		return roachpb.AdminSplitResponse{}, roachpb.NewErrorf("cannot split range with no key provided")
	}

	err := r.executeAdminCommandWithDescriptor(ctx, func(desc *roachpb.RangeDescriptor) error {
		var err error
		reply, err = r.adminSplitWithDescriptor(ctx, args, desc, true /* delayable */, reason)
		return err
	})
	return reply, err
}

func maybeDescriptorChangedError(
	desc *roachpb.RangeDescriptor, err error,
) (ok bool, expectedDesc *roachpb.RangeDescriptor) {
	if detail := (*roachpb.ConditionFailedError)(nil); errors.As(err, &detail) {
		// Provide a better message in the common case that the range being changed
		// was already changed by a concurrent transaction.
		var actualDesc roachpb.RangeDescriptor
		if !detail.ActualValue.IsPresent() {
			return true, nil
		} else if err := detail.ActualValue.GetProto(&actualDesc); err == nil && !desc.Equal(&actualDesc) {
			return true, &actualDesc
		}
	}
	return false, nil
}

func splitSnapshotWarningStr(rangeID roachpb.RangeID, status *raft.Status) string {
	var s string
	if status != nil && status.RaftState == raft.StateLeader {
		for replicaID, pr := range status.Progress {
			if replicaID == status.Lead {
				// TODO(tschottdorf): remove this line once we have picked up
				// https://github.com/etcd-io/etcd/pull/10279
				continue
			}
			if pr.State == tracker.StateReplicate {
				// This follower is in good working order.
				continue
			}
			s += fmt.Sprintf("; r%d/%d is ", rangeID, replicaID)
			switch pr.State {
			case tracker.StateSnapshot:
				// If the Raft snapshot queue is backed up, replicas can spend
				// minutes or worse until they are caught up.
				s += "waiting for a Raft snapshot"
			case tracker.StateProbe:
				// Assuming the split has already been delayed for a little bit,
				// seeing a follower that is probing hints at some problem with
				// Raft or Raft message delivery. (Of course it's possible that
				// the follower *just* entered probing state).
				s += "being probed (may or may not need a Raft snapshot)"
			default:
				// Future proofing.
				s += "in unknown state " + pr.State.String()
			}
		}
	}
	return s
}

// prepareSplitDescs returns the left and right descriptor of the split whose
// right side is assigned rightRangeID and starts at splitKey. The supplied
// expiration is the "sticky bit" stored on the right descriptor.
func prepareSplitDescs(
	ctx context.Context,
	st *cluster.Settings,
	rightRangeID roachpb.RangeID,
	splitKey roachpb.RKey,
	expiration hlc.Timestamp,
	leftDesc *roachpb.RangeDescriptor,
) (*roachpb.RangeDescriptor, *roachpb.RangeDescriptor) {
	// Create right hand side range descriptor.
	rightDesc := roachpb.NewRangeDescriptor(rightRangeID, splitKey, leftDesc.EndKey, leftDesc.Replicas())

	// Init updated version of existing range descriptor.
	{
		tmp := *leftDesc
		leftDesc = &tmp
	}

	leftDesc.IncrementGeneration()
	leftDesc.EndKey = splitKey

	// Set the generation of the right hand side descriptor to match that of the
	// (updated) left hand side. See the comment on the field for an explanation
	// of why generations are useful.
	rightDesc.Generation = leftDesc.Generation

	setStickyBit(rightDesc, expiration)
	return leftDesc, rightDesc
}

func setStickyBit(desc *roachpb.RangeDescriptor, expiration hlc.Timestamp) {
	// TODO(jeffreyxiao): Remove this check in 20.1.
	// Note that the client API for splitting has expiration time as
	// non-nullable, but the internal representation of a sticky bit is nullable
	// for backwards compatibility. If expiration time is the zero timestamp, we
	// must be sure not to set the sticky bit to the zero timestamp because the
	// byte representation of setting the stickyBit to nil is different than
	// setting it to hlc.Timestamp{}. This check ensures that CPuts would not
	// fail on older versions.
	if !expiration.IsEmpty() {
		desc.StickyBit = &expiration
	}
}

func splitTxnAttempt(
	ctx context.Context,
	store *Store,
	txn *kv.Txn,
	rightRangeID roachpb.RangeID,
	splitKey roachpb.RKey,
	expiration hlc.Timestamp,
	oldDesc *roachpb.RangeDescriptor,
) error {
	txn.SetDebugName(splitTxnName)

	_, dbDescValue, err := conditionalGetDescValueFromDB(
		ctx, txn, oldDesc.StartKey, false /* forUpdate */, checkDescsEqual(oldDesc))
	if err != nil {
		return err
	}
	// TODO(tbg): return desc from conditionalGetDescValueFromDB and don't pass
	// in oldDesc any more (just the start key).
	desc := oldDesc
	oldDesc = nil // prevent accidental use

	leftDesc, rightDesc := prepareSplitDescs(
		ctx, store.ClusterSettings(), rightRangeID, splitKey, expiration, desc)

	// Update existing range descriptor for left hand side of
	// split. Note that we mutate the descriptor for the left hand
	// side of the split first to locate the txn record there.
	{
		b := txn.NewBatch()
		leftDescKey := keys.RangeDescriptorKey(leftDesc.StartKey)
		if err := updateRangeDescriptor(ctx, b, leftDescKey, dbDescValue, leftDesc); err != nil {
			return err
		}
		// Commit this batch first to ensure that the transaction record
		// is created in the right place (split trigger relies on this).
		// Sending the batch containing only the first write guarantees
		// the transaction record is written first, preventing cases
		// where splits are aborted early due to conflicts with meta
		// intents (see #9265).
		log.Event(ctx, "updating LHS descriptor")
		if err := txn.Run(ctx, b); err != nil {
			return err
		}
	}

	// Log the split into the range event log.
	if err := store.logSplit(ctx, txn, *leftDesc, *rightDesc); err != nil {
		return err
	}

	b := txn.NewBatch()

	// Write range descriptor for right hand side of the split.
	rightDescKey := keys.RangeDescriptorKey(rightDesc.StartKey)
	if err := updateRangeDescriptor(ctx, b, rightDescKey, nil, rightDesc); err != nil {
		return err
	}

	// Update range descriptor addressing record(s).
	if err := splitRangeAddressing(b, rightDesc, leftDesc); err != nil {
		return err
	}

	// End the transaction manually, instead of letting RunTransaction
	// loop do it, in order to provide a split trigger.
	b.AddRawRequest(&roachpb.EndTxnRequest{
		Commit: true,
		InternalCommitTrigger: &roachpb.InternalCommitTrigger{
			SplitTrigger: &roachpb.SplitTrigger{
				LeftDesc:  *leftDesc,
				RightDesc: *rightDesc,
			},
		},
	})

	// Commit txn with final batch (RHS descriptor and meta).
	log.Event(ctx, "commit txn with batch containing RHS descriptor and meta records")
	return txn.Run(ctx, b)
}

func splitTxnStickyUpdateAttempt(
	ctx context.Context, txn *kv.Txn, desc *roachpb.RangeDescriptor, expiration hlc.Timestamp,
) error {
	_, dbDescValue, err := conditionalGetDescValueFromDB(
		ctx, txn, desc.StartKey, false /* forUpdate */, checkDescsEqual(desc))
	if err != nil {
		return err
	}
	newDesc := *desc
	setStickyBit(&newDesc, expiration)

	b := txn.NewBatch()
	descKey := keys.RangeDescriptorKey(desc.StartKey)
	if err := updateRangeDescriptor(ctx, b, descKey, dbDescValue, &newDesc); err != nil {
		return err
	}
	if err := updateRangeAddressing(b, &newDesc); err != nil {
		return err
	}
	// End the transaction manually, instead of letting RunTransaction loop
	// do it, in order to provide a sticky bit trigger.
	b.AddRawRequest(&roachpb.EndTxnRequest{
		Commit: true,
		InternalCommitTrigger: &roachpb.InternalCommitTrigger{
			StickyBitTrigger: &roachpb.StickyBitTrigger{
				StickyBit: newDesc.GetStickyBit(),
			},
		},
	})
	return txn.Run(ctx, b)
}

// adminSplitWithDescriptor divides the range into into two ranges, using
// either args.SplitKey (if provided) or an internally computed key that aims
// to roughly equipartition the range by size. The split is done inside of a
// distributed txn which writes updated left and new right hand side range
// descriptors, and updates the range addressing metadata. The handover of
// responsibility for the reassigned key range is carried out seamlessly
// through a split trigger carried out as part of the commit of that
// transaction.
//
// The supplied RangeDescriptor is used as a form of optimistic lock. An
// operation which might split a range should obtain a copy of the range's
// current descriptor before making the decision to split. If the decision is
// affirmative the descriptor is passed to AdminSplit, which performs a
// Conditional Put on the RangeDescriptor to ensure that no other operation has
// modified the range in the time the decision was being made.
// TODO(tschottdorf): should assert that split key is not a local key.
//
// See the comment on splitTrigger for details on the complexities.
func (r *Replica) adminSplitWithDescriptor(
	ctx context.Context,
	args roachpb.AdminSplitRequest,
	desc *roachpb.RangeDescriptor,
	delayable bool,
	reason string,
) (roachpb.AdminSplitResponse, error) {
	var err error
	// The split queue doesn't care about the set of replicas, so if we somehow
	// are being handed one that's in a joint state, finalize that before
	// continuing.
	desc, err = maybeLeaveAtomicChangeReplicas(ctx, r.store, desc)
	if err != nil {
		return roachpb.AdminSplitResponse{}, err
	}

	var reply roachpb.AdminSplitResponse

	// Determine split key if not provided with args. This scan is
	// allowed to be relatively slow because admin commands don't block
	// other commands.
	log.Event(ctx, "split begins")
	var splitKey roachpb.RKey
	{
		var foundSplitKey roachpb.Key
		if len(args.SplitKey) == 0 {
			// Find a key to split by size.
			var err error
			targetSize := r.GetMaxBytes() / 2
			foundSplitKey, err = storage.MVCCFindSplitKey(
				ctx, r.store.engine, desc.StartKey, desc.EndKey, targetSize)
			if err != nil {
				return reply, errors.Errorf("unable to determine split key: %s", err)
			}
			if foundSplitKey == nil {
				// No suitable split key could be found.
				return reply, unsplittableRangeError{}
			}
		} else {
			// If the key that routed this request to this range is now out of this
			// range's bounds, return an error for the client to try again on the
			// correct range.
			if !kvserverbase.ContainsKey(desc, args.Key) {
				l, _ := r.GetLease()
				return reply, roachpb.NewRangeKeyMismatchError(ctx, args.Key, args.Key, desc, &l)
			}
			foundSplitKey = args.SplitKey
		}

		if !kvserverbase.ContainsKey(desc, foundSplitKey) {
			return reply, errors.Errorf("requested split key %s out of bounds of %s", args.SplitKey, r)
		}

		var err error
		splitKey, err = keys.Addr(foundSplitKey)
		if err != nil {
			return reply, err
		}
		if !splitKey.Equal(foundSplitKey) {
			return reply, errors.Errorf("cannot split range at range-local key %s", splitKey)
		}
		if !storage.IsValidSplitKey(foundSplitKey) {
			return reply, errors.Errorf("cannot split range at key %s", splitKey)
		}
	}

	// If the range starts at the splitKey, we treat the AdminSplit
	// as a no-op and return success instead of throwing an error.
	if desc.StartKey.Equal(splitKey) {
		if len(args.SplitKey) == 0 {
			log.Fatal(ctx, "MVCCFindSplitKey returned start key of range")
		}
		log.Event(ctx, "range already split")
		// Even if the range is already split, we should still update the sticky
		// bit if it has a later expiration time.
		if desc.GetStickyBit().Less(args.ExpirationTime) {
			err := r.store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				return splitTxnStickyUpdateAttempt(ctx, txn, desc, args.ExpirationTime)
			})
			// The ConditionFailedError can occur because the descriptors acting as
			// expected values in the CPuts used to update the range descriptor are
			// picked outside the transaction. Return ConditionFailedError in the
			// error detail so that the command can be retried.
			if ok, actualDesc := maybeDescriptorChangedError(desc, err); ok {
				// NB: we have to wrap the existing error here as consumers of this code
				// look at the root cause to sniff out the changed descriptor.
				err = &benignError{wrapDescChangedError(err, desc, actualDesc)}
			}
			return reply, err
		}
		return reply, nil
	}
	log.Event(ctx, "found split key")

	// Create right hand side range descriptor.
	rightRangeID, err := r.store.AllocateRangeID(ctx)
	if err != nil {
		return reply, errors.Wrap(err, "unable to allocate range id for right hand side")
	}

	var extra string
	if delayable {
		extra += maybeDelaySplitToAvoidSnapshot(ctx, (*splitDelayHelper)(r))
	}
	extra += splitSnapshotWarningStr(r.RangeID, r.RaftStatus())

	log.Infof(ctx, "initiating a split of this range at key %s [r%d] (%s)%s",
		splitKey.StringWithDirs(nil /* valDirs */, 50 /* maxLen */), rightRangeID, reason, extra)

	if err := r.store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return splitTxnAttempt(ctx, r.store, txn, rightRangeID, splitKey, args.ExpirationTime, desc)
	}); err != nil {
		// The ConditionFailedError can occur because the descriptors acting
		// as expected values in the CPuts used to update the left or right
		// range descriptors are picked outside the transaction. Return
		// ConditionFailedError in the error detail so that the command can be
		// retried.
		if ok, actualDesc := maybeDescriptorChangedError(desc, err); ok {
			// NB: we have to wrap the existing error here as consumers of this code
			// look at the root cause to sniff out the changed descriptor.
			err = &benignError{wrapDescChangedError(err, desc, actualDesc)}
		}
		return reply, errors.Wrapf(err, "split at key %s failed", splitKey)
	}
	return reply, nil
}

// AdminUnsplit removes the sticky bit of the range specified by the
// args.Key.
func (r *Replica) AdminUnsplit(
	ctx context.Context, args roachpb.AdminUnsplitRequest, reason string,
) (roachpb.AdminUnsplitResponse, *roachpb.Error) {
	var reply roachpb.AdminUnsplitResponse
	err := r.executeAdminCommandWithDescriptor(ctx, func(desc *roachpb.RangeDescriptor) error {
		var err error
		reply, err = r.adminUnsplitWithDescriptor(ctx, args, desc, reason)
		return err
	})
	return reply, err
}

func (r *Replica) adminUnsplitWithDescriptor(
	ctx context.Context,
	args roachpb.AdminUnsplitRequest,
	desc *roachpb.RangeDescriptor,
	reason string,
) (roachpb.AdminUnsplitResponse, error) {
	var reply roachpb.AdminUnsplitResponse
	if !bytes.Equal(desc.StartKey.AsRawKey(), args.Header().Key) {
		return reply, errors.Errorf("key %s is not the start of a range", args.Header().Key)
	}

	// If the range's sticky bit is already hlc.Timestamp{}, we treat the unsplit
	// command as a no-op and return success instead of throwing an error. On
	// mixed version clusters that don't support StickyBit, all range descriptor
	// sticky bits are guaranteed to be nil, so we can skip checking the cluster
	// version.
	if desc.GetStickyBit().IsEmpty() {
		return reply, nil
	}

	if err := r.store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, dbDescValue, err := conditionalGetDescValueFromDB(
			ctx, txn, desc.StartKey, false /* forUpdate */, checkDescsEqual(desc))
		if err != nil {
			return err
		}

		newDesc := *desc
		// Use nil instead of &zero until 20.1; this field is new in 19.2. We
		// could use &zero here because the sticky bit will never be populated
		// before the cluster version reaches 19.2 and the early return above
		// already handles that case, but nothing is won in doing so.
		newDesc.StickyBit = nil
		descKey := keys.RangeDescriptorKey(newDesc.StartKey)

		b := txn.NewBatch()
		if err := updateRangeDescriptor(ctx, b, descKey, dbDescValue, &newDesc); err != nil {
			return err
		}
		if err := updateRangeAddressing(b, &newDesc); err != nil {
			return err
		}
		// End the transaction manually in order to provide a sticky bit trigger.
		b.AddRawRequest(&roachpb.EndTxnRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				StickyBitTrigger: &roachpb.StickyBitTrigger{
					// Setting StickyBit to the zero timestamp ensures that it is always
					// eligible for automatic merging.
					StickyBit: hlc.Timestamp{},
				},
			},
		})
		return txn.Run(ctx, b)
	}); err != nil {
		// The ConditionFailedError can occur because the descriptors acting as
		// expected values in the CPuts used to update the range descriptor are
		// picked outside the transaction. Return ConditionFailedError in the error
		// detail so that the command can be retried.
		if ok, actualDesc := maybeDescriptorChangedError(desc, err); ok {
			// NB: we have to wrap the existing error here as consumers of this code
			// look at the root cause to sniff out the changed descriptor.
			err = &benignError{wrapDescChangedError(err, desc, actualDesc)}
		}
		return reply, err
	}
	return reply, nil
}

// executeAdminCommandWithDescriptor wraps a read-modify-write operation for RangeDescriptors in a
// retry loop.
func (r *Replica) executeAdminCommandWithDescriptor(
	ctx context.Context, updateDesc func(*roachpb.RangeDescriptor) error,
) *roachpb.Error {
	// Retry forever as long as we see errors we know will resolve.
	retryOpts := base.DefaultRetryOptions()
	// Randomize quite a lot just in case someone else also interferes with us
	// in a retry loop. Note that this is speculative; there wasn't an incident
	// that suggested this.
	retryOpts.RandomizationFactor = 0.5
	var lastErr error
	for retryable := retry.StartWithCtx(ctx, retryOpts); retryable.Next(); {
		// The replica may have been destroyed since the start of the retry loop.
		// We need to explicitly check this condition. Having a valid lease, as we
		// verify below, does not imply that the range still exists: even after a
		// range has been merged into its left-hand neighbor, its final lease
		// (i.e., the lease we have in r.mu.state.Lease) can remain valid
		// indefinitely.
		if _, err := r.IsDestroyed(); err != nil {
			return roachpb.NewError(err)
		}

		// Admin commands always require the range lease to begin (see
		// executeAdminBatch), but we may have lost it while in this retry loop.
		// Without the lease, a replica's local descriptor can be arbitrarily
		// stale, which will result in a ConditionFailedError. To avoid this, we
		// make sure that we still have the lease before each attempt.
		if _, pErr := r.redirectOnOrAcquireLease(ctx); pErr != nil {
			return pErr
		}

		lastErr = updateDesc(r.Desc())
		// On seeing a ConditionFailedError or an AmbiguousResultError, retry the
		// command with the updated descriptor.
		if !errors.HasType(lastErr, (*roachpb.ConditionFailedError)(nil)) &&
			!errors.HasType(lastErr, (*roachpb.AmbiguousResultError)(nil)) {
			break
		}
	}
	return roachpb.NewError(lastErr)
}

// AdminMerge extends this range to subsume the range that comes next
// in the key space. The merge is performed inside of a distributed
// transaction which writes the left hand side range descriptor (the
// subsuming range) and deletes the range descriptor for the right
// hand side range (the subsumed range). It also updates the range
// addressing metadata. The handover of responsibility for the
// reassigned key range is carried out seamlessly through a merge
// trigger carried out as part of the commit of that transaction. A
// merge requires that the two ranges are collocated on the same set
// of replicas.
//
// The supplied RangeDescriptor is used as a form of optimistic lock. See the
// comment of "AdminSplit" for more information on this pattern.
func (r *Replica) AdminMerge(
	ctx context.Context, args roachpb.AdminMergeRequest, reason string,
) (roachpb.AdminMergeResponse, *roachpb.Error) {
	var reply roachpb.AdminMergeResponse

	runMergeTxn := func(txn *kv.Txn) error {
		log.Event(ctx, "merge txn begins")
		txn.SetDebugName(mergeTxnName)

		// If we aren't certain that all possible nodes in the cluster support a
		// range merge transaction refreshing its reads while the RHS range is
		// subsumed, observe the commit timestamp to force a client-side retry.
		if !r.ClusterSettings().Version.IsActive(ctx, clusterversion.PriorReadSummaries) {
			_ = txn.CommitTimestamp()
		}

		// Pipelining might send QueryIntent requests to the RHS after the RHS has
		// noticed the merge and started blocking all traffic. This causes the merge
		// transaction to deadlock. Just turn pipelining off; the structure of the
		// merge transaction means pipelining provides no performance benefit
		// anyway.
		if err := txn.DisablePipelining(); err != nil {
			return err
		}

		origLeftDesc := r.Desc()
		if origLeftDesc.EndKey.Equal(roachpb.RKeyMax) {
			// Merging the final range doesn't make sense.
			return errors.New("cannot merge final range")
		}

		// Retrieve the current left hand side's range descriptor and confirm
		// that it matches our expectation. Do so using a locking read. Locking
		// the descriptor early (i.e. on the read instead of the write of the
		// read-modify-write operation) helps prevent multiple concurrent Range
		// merges from thrashing. Thrashing is especially detrimental for Range
		// merges because any restart results in an abort (see the retry loop
		// below), so thrashing can result in indefinite livelock.
		//
		// Because this is a locking read, this also dictates the location of
		// the merge's transaction record. It is critical to the range merge
		// protocol that the transaction record be placed on the the left hand
		// side's descriptor, as the MergeTrigger depends on this.
		_, dbOrigLeftDescValue, err := conditionalGetDescValueFromDB(
			ctx, txn, origLeftDesc.StartKey, true /* forUpdate */, checkDescsEqual(origLeftDesc))
		if err != nil {
			return err
		}

		// Do a consistent read of the right hand side's range descriptor.
		// Again, use a locking read because we intend to update this key
		// shortly.
		var rightDesc roachpb.RangeDescriptor
		rightDescKey := keys.RangeDescriptorKey(origLeftDesc.EndKey)
		dbRightDescKV, err := txn.GetForUpdate(ctx, rightDescKey)
		if err != nil {
			return err
		}
		if err := dbRightDescKV.ValueProto(&rightDesc); err != nil {
			return err
		}

		// Verify that the two ranges are mergeable.
		if !bytes.Equal(origLeftDesc.EndKey, rightDesc.StartKey) {
			// Should never happen, but just in case.
			return errors.Errorf("ranges are not adjacent; %s != %s", origLeftDesc.EndKey, rightDesc.StartKey)
		}
		// For simplicity, don't handle learner replicas or joint states, expect
		// the caller to resolve them first. (Defensively, we check that there
		// are no non-voter replicas, in case some third type is later added).
		// This behavior can be changed later if the complexity becomes worth
		// it, but it's not right now.
		//
		// NB: the merge queue transitions out of any joint states and removes
		// any learners it sees. It's sort of silly that we don't do that here
		// instead; effectively any caller of AdminMerge that is not the merge
		// queue won't be able to recover from these cases (though the replicate
		// queues should fix things up quickly).
		lReplicas, rReplicas := origLeftDesc.Replicas(), rightDesc.Replicas()

		if len(lReplicas.VoterFullAndNonVoterDescriptors()) != len(lReplicas.Descriptors()) {
			return errors.Errorf("cannot merge ranges when lhs is in a joint state or has learners: %s",
				lReplicas)
		}
		if len(rReplicas.VoterFullAndNonVoterDescriptors()) != len(rReplicas.Descriptors()) {
			return errors.Errorf("cannot merge ranges when rhs is in a joint state or has learners: %s",
				rReplicas)
		}
		if !replicasCollocated(lReplicas.Descriptors(), rReplicas.Descriptors()) {
			return errors.Errorf("ranges not collocated; %s != %s", lReplicas, rReplicas)
		}

		// Ensure that every current replica of the LHS has been initialized.
		// Otherwise there is a rare race where the replica GC queue can GC a
		// replica of the RHS too early. The comment on
		// TestStoreRangeMergeUninitializedLHSFollower explains the situation in full.
		if err := waitForReplicasInit(
			ctx, r.store.cfg.NodeDialer, origLeftDesc.RangeID, origLeftDesc.Replicas().Descriptors(),
		); err != nil {
			return errors.Wrap(err, "waiting for all left-hand replicas to initialize")
		}
		// Out of an abundance of caution, also ensure that replicas of the RHS have
		// all been initialized. If for whatever reason the initial upreplication
		// snapshot for a NON_VOTER on the RHS fails, it will have to get picked up
		// by the raft snapshot queue to upreplicate and may be uninitialized at
		// this point. As such, if we send a subsume request to the RHS in this sort
		// of state, we will wastefully and unintentionally block all traffic on it
		// for 5 seconds.
		if err := waitForReplicasInit(
			ctx, r.store.cfg.NodeDialer, rightDesc.RangeID, rightDesc.Replicas().Descriptors(),
		); err != nil {
			return errors.Wrap(err, "waiting for all right-hand replicas to initialize")
		}

		mergeReplicas := lReplicas.Descriptors()

		updatedLeftDesc := *origLeftDesc
		// lhs.Generation = max(rhs.Generation, lhs.Generation)+1.
		// See the comment on the Generation field for why generation are useful.
		if updatedLeftDesc.Generation < rightDesc.Generation {
			updatedLeftDesc.Generation = rightDesc.Generation
		}
		updatedLeftDesc.IncrementGeneration()
		updatedLeftDesc.EndKey = rightDesc.EndKey
		log.Infof(ctx, "initiating a merge of %s into this range (%s)", &rightDesc, reason)

		// Log the merge into the range event log.
		// TODO(spencer): event logging API should accept a batch
		// instead of a transaction; there's no reason this logging
		// shouldn't be done in parallel via the batch with the updated
		// range addressing.
		if err := r.store.logMerge(ctx, txn, updatedLeftDesc, rightDesc); err != nil {
			return err
		}

		b := txn.NewBatch()

		// Update the meta addressing records.
		if err := mergeRangeAddressing(b, origLeftDesc, &updatedLeftDesc); err != nil {
			return err
		}

		// Update the range descriptor for the receiving range.
		leftDescKey := keys.RangeDescriptorKey(updatedLeftDesc.StartKey)
		if err := updateRangeDescriptor(ctx, b, leftDescKey,
			dbOrigLeftDescValue, /* oldValue */
			&updatedLeftDesc,    /* newDesc */
		); err != nil {
			return err
		}

		// Remove the range descriptor for the deleted range.
		if err := updateRangeDescriptor(ctx, b, rightDescKey,
			dbRightDescKV.Value.TagAndDataBytes(), /* oldValue */
			nil,                                   /* newDesc */
		); err != nil {
			return err
		}

		// Send off this batch, ensuring that intents are placed on both the local
		// copy and meta2's copy of the right-hand side range descriptor before we
		// send the Subsume request below. This is the precondition for sending a
		// Subsume request; see the godoc on batcheval.Subsume for details.
		if err := txn.Run(ctx, b); err != nil {
			return err
		}

		// Intents have been placed, so the merge is now in its critical phase. Get
		// a consistent view of the data from the right-hand range. If the merge
		// commits, we'll write this data to the left-hand range in the merge
		// trigger.
		br, pErr := kv.SendWrapped(ctx, r.store.DB().NonTransactionalSender(),
			&roachpb.SubsumeRequest{
				RequestHeader: roachpb.RequestHeader{Key: rightDesc.StartKey.AsRawKey()},
				LeftDesc:      *origLeftDesc,
				RightDesc:     rightDesc,
			})
		if pErr != nil {
			return pErr.GoError()
		}
		rhsSnapshotRes := br.(*roachpb.SubsumeResponse)

		err = waitForApplication(
			ctx, r.store.cfg.NodeDialer, rightDesc.RangeID, mergeReplicas,
			rhsSnapshotRes.LeaseAppliedIndex)
		if err != nil {
			return errors.Wrap(err, "waiting for all right-hand replicas to catch up")
		}

		// Successful subsume, so we're guaranteed that the right-hand range will
		// not serve another request unless this transaction aborts. End the
		// transaction manually in order to provide a merge trigger.
		b = txn.NewBatch()
		b.AddRawRequest(&roachpb.EndTxnRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				MergeTrigger: &roachpb.MergeTrigger{
					LeftDesc:             updatedLeftDesc,
					RightDesc:            rightDesc,
					RightMVCCStats:       rhsSnapshotRes.MVCCStats,
					FreezeStart:          rhsSnapshotRes.FreezeStart,
					RightClosedTimestamp: rhsSnapshotRes.ClosedTimestamp,
					RightReadSummary:     rhsSnapshotRes.ReadSummary,
				},
			},
		})
		log.Event(ctx, "attempting commit")
		return txn.Run(ctx, b)
	}

	// If the merge transaction encounters an error, we need to trigger a full
	// abort and try again with a new transaction. Why? runMergeTxn has the side
	// effect of sending a Subsume request to the right-hand range, which blocks
	// the right-hand range from serving any traffic until the transaction commits
	// or aborts. If we retry using the same transaction (i.e., a "transaction
	// restart"), we'll send requests to the blocked right-hand range and
	// deadlock. The right-hand range will see that the transaction is still
	// pending and refuse to respond, but the transaction cannot commit until the
	// right-hand range responds. By instead marking the transaction as aborted,
	// we'll unlock the right-hand range, giving the next, fresh transaction a
	// chance to succeed.
	//
	// A second reason to eschew kv.DB.Txn() is that the API to disable	pipelining
	// is finicky and only allows disabling pipelining before any operations have
	// been sent, even in prior epochs. Calling DisablePipelining() on a restarted
	// transaction yields an error.
	for {
		txn := kv.NewTxn(ctx, r.store.DB(), r.NodeID())
		err := runMergeTxn(txn)
		if err != nil {
			log.VEventf(ctx, 2, "merge txn failed: %s", err)
			txn.CleanupOnError(ctx, err)
		}
		if !errors.HasType(err, (*roachpb.TransactionRetryWithProtoRefreshError)(nil)) {
			if err != nil {
				return reply, roachpb.NewErrorf("merge failed: %s", err)
			}
			return reply, nil
		}
	}
}

func waitForApplication(
	ctx context.Context,
	dialer *nodedialer.Dialer,
	rangeID roachpb.RangeID,
	replicas []roachpb.ReplicaDescriptor,
	leaseIndex uint64,
) error {
	return contextutil.RunWithTimeout(ctx, "wait for application", 5*time.Second, func(ctx context.Context) error {
		g := ctxgroup.WithContext(ctx)
		for _, repl := range replicas {
			repl := repl // copy for goroutine
			g.GoCtx(func(ctx context.Context) error {
				conn, err := dialer.Dial(ctx, repl.NodeID, rpc.DefaultClass)
				if err != nil {
					return errors.Wrapf(err, "could not dial n%d", repl.NodeID)
				}
				_, err = NewPerReplicaClient(conn).WaitForApplication(ctx, &WaitForApplicationRequest{
					StoreRequestHeader: StoreRequestHeader{NodeID: repl.NodeID, StoreID: repl.StoreID},
					RangeID:            rangeID,
					LeaseIndex:         leaseIndex,
				})
				return err
			})
		}
		return g.Wait()
	})
}

// waitForReplicasInit blocks until it has proof that the replicas listed in
// desc are initialized on their respective stores. It may return a false
// negative, i.e., claim that a replica is uninitialized when it is, in fact,
// initialized, but it will never return a false positive.
func waitForReplicasInit(
	ctx context.Context,
	dialer *nodedialer.Dialer,
	rangeID roachpb.RangeID,
	replicas []roachpb.ReplicaDescriptor,
) error {
	return contextutil.RunWithTimeout(ctx, "wait for replicas init", 5*time.Second, func(ctx context.Context) error {
		g := ctxgroup.WithContext(ctx)
		for _, repl := range replicas {
			repl := repl // copy for goroutine
			g.GoCtx(func(ctx context.Context) error {
				conn, err := dialer.Dial(ctx, repl.NodeID, rpc.DefaultClass)
				if err != nil {
					return errors.Wrapf(err, "could not dial n%d", repl.NodeID)
				}
				_, err = NewPerReplicaClient(conn).WaitForReplicaInit(ctx, &WaitForReplicaInitRequest{
					StoreRequestHeader: StoreRequestHeader{NodeID: repl.NodeID, StoreID: repl.StoreID},
					RangeID:            rangeID,
				})
				return err
			})
		}
		return g.Wait()
	})
}

// ChangeReplicas atomically changes the replicas that are members of a range.
// The change is performed in a distributed transaction and takes effect when
// that transaction is committed. This transaction confirms that the supplied
// RangeDescriptor is up to date and that the supplied slice of
// ReplicationChanges is a valid transition, meaning that replicas being added
// are not present, that replicas being removed are present, that no replica is
// altered more than once, and that no attempt is made at removing the
// leaseholder (which in particular implies that we can never remove all
// replicas).
//
// The returned RangeDescriptor is the new value of the range's descriptor
// following the successful commit of the transaction.
//
// In general, ChangeReplicas will carry out the following steps.
//
// 1. Run a distributed transaction that adds all new replicas as learner replicas.
//    Learner replicas receive the log, but do not have voting rights. They are
//    used to catch up these new replicas before turning them into voters, which
//    is important for the continued availability of the range throughout the
//    replication change. Learners are added (and removed) one by one due to a
//    technicality (see https://github.com/cockroachdb/cockroach/pull/40268).
//
//    The distributed transaction updates both copies of the range descriptor
//    (the one on the range and that in the meta ranges) to that effect, and
//    commits with a special trigger instructing Raft (via ProposeConfChange) to
//    tie a corresponding replication configuration change which goes into
//    effect (on each replica) when the transaction commit is applied to the
//    state. Applying the command also updates each replica's local view of
//    the state to reflect the new descriptor.
//
//    If no replicas are being added, this first step is elided. If non-voting
//    replicas (which are also learners in etcd/raft) are being added, then this
//    step is all we need. The rest of the steps only apply if voter replicas
//    are being added.
//
// 2. Send Raft snapshots to all learner replicas. This would happen
//    automatically by the existing recovery mechanisms (raft snapshot queue), but
//    it is done explicitly as a convenient way to ensure learners are caught up
//    before the next step is entered. (We ensure that work is not duplicated
//    between the snapshot queue and the explicit snapshot via the
//    snapshotLogTruncationConstraints map). Snapshots are subject to both
//    bandwidth rate limiting and throttling.
//
//    If no replicas are being added, this step is similarly elided.
//
// 3. Carry out a distributed transaction similar to that which added the
//    learner replicas, except this time it (atomically) changes all learners to
//    voters and removes any replicas for which this was requested; voters are
//    demoted before actually being removed to avoid bug in etcd/raft:
//    See https://github.com/cockroachdb/cockroach/pull/40268.
//
//    If only one replica is being added, raft can chose the simple
//    configuration change protocol; otherwise it has to use joint consensus. In
//    this latter mechanism, a first configuration change is made which results
//    in a configuration ("joint configuration") in which a quorum of both the
//    old replicas and the new replica sets is required for decision making.
//    Transitioning into this joint configuration, the RangeDescriptor (which is
//    the source of truth of the replication configuration) is updated with
//    corresponding replicas of type VOTER_INCOMING and VOTER_DEMOTING.
//    Immediately after committing this change, a second transition updates the
//    descriptor with and activates the final configuration.
//
// Concretely, if the initial members of the range are s1/1, s2/2, and s3/3, and
// an atomic membership change were to add s4/4 and s5/5 while removing s1/1 and
// s2/2, the following range descriptors would form the overall transition:
//
// 1. s1/1 s2/2 s3/3 (VOTER_FULL is implied)
// 2. s1/1 s2/2 s3/3 s4/4LEARNER
// 3. s1/1 s2/2 s3/3 s4/4LEARNER s5/5LEARNER
// 4. s1/1VOTER_DEMOTING_LEARNER s2/2VOTER_DEMOTING_LEARNER s3/3 s4/4VOTER_INCOMING s5/5VOTER_INCOMING
// 5. s1/1LEARNER s2/2LEARNER s3/3 s4/4 s5/5
// 6. s2/2LEARNER s3/3 s4/4 s5/5
// 7. s3/3 s4/4 s5/5
//
// A replica that learns that it was removed will queue itself for replicaGC.
// Note that a removed replica may never apply the configuration change removing
// itself and thus this trigger may not fire. This is because said replica may
// not have been a part of the quorum that committed the configuration change;
// nodes that apply the change will stop sending messages to the removed
// replica. At that point, the removed replica will typically campaign (since it
// receives no more heartbeats from the leader) and its former peers respond via
// a RaftGroupDeletedError (from the Raft transport) as a signal to queue to
// replicaGC. This second mechanism fails if all peers have rapidly moved
// elsewhere as well; in that last and rare case, replica GC queue will
// eventually discover the replica on its own; it has optimizations that handle
// "abandoned-looking" replicas more eagerly than healthy ones.
func (r *Replica) ChangeReplicas(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	priority SnapshotRequest_Priority,
	reason kvserverpb.RangeLogEventReason,
	details string,
	chgs roachpb.ReplicationChanges,
) (updatedDesc *roachpb.RangeDescriptor, _ error) {
	if desc == nil {
		// TODO(tbg): is this check just FUD?
		return nil, errors.Errorf("%s: the current RangeDescriptor must not be nil", r)
	}

	// If in testing (for lack of a better mechanism, we restrict to race builds),
	// try to catch tests that use manual replication while the replication queue
	// is active. Such tests are often flaky.
	if knobs := r.store.TestingKnobs(); util.RaceEnabled &&
		!knobs.DisableReplicateQueue &&
		!knobs.AllowUnsynchronizedReplicationChanges {
		bq := r.store.replicateQueue.baseQueue
		bq.mu.Lock()
		disabled := bq.mu.disabled
		bq.mu.Unlock()
		if !disabled {
			return nil, errors.New("must disable replicate queue to use ChangeReplicas manually")
		}
	}
	return r.changeReplicasImpl(ctx, desc, priority, reason, details, chgs)
}

func (r *Replica) changeReplicasImpl(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	priority SnapshotRequest_Priority,
	reason kvserverpb.RangeLogEventReason,
	details string,
	chgs roachpb.ReplicationChanges,
) (updatedDesc *roachpb.RangeDescriptor, _ error) {
	var err error
	// If in a joint config, clean up. The assumption here is that the caller
	// of ChangeReplicas didn't even realize that they were holding on to a
	// joint descriptor and would rather not have to deal with that fact.
	desc, err = maybeLeaveAtomicChangeReplicas(ctx, r.store, desc)
	if err != nil {
		return nil, err
	}

	if err := validateReplicationChanges(desc, chgs); err != nil {
		return nil, errors.Mark(err, errMarkInvalidReplicationChange)
	}
	targets := synthesizeTargetsByChangeType(chgs)

	// NB: As of the time of this writing,`AdminRelocateRange` will only execute
	// replication changes one by one. Thus, the order in which we execute the
	// changes we've synthesized doesn't matter that much. However, this
	// limitation might be lifted soon (see #58752, for instance).
	//
	// We choose to execute changes in the following order:
	// 1. Promotions / demotions / swaps between voters and non-voters
	// 2. Voter additions
	// 3. Voter removals
	// 4. Non-voter additions
	// 5. Non-voter removals
	//
	// This order is meant to be symmetric with how the allocator prioritizes
	// these actions. Broadly speaking, we first want to add a missing voter (and
	// promoting an existing non-voter, or swapping with one, is the fastest way
	// to do that). Then, we consider rebalancing/removing voters. Finally, we
	// handle non-voter additions & removals.

	// We perform promotions of non-voting replicas to voting replicas, and
	// likewise, demotions of voting replicas to non-voting replicas. If both
	// these types of operations are being applied to a (voter, non-voter) pair,
	// it will execute an atomic swap using joint consensus.
	//
	// NB: Promotions and demotions of LEARNER replicas are handled implicitly
	// during the addition or removal of voting replicas and are not handled
	// here.
	swaps := getInternalChangesForExplicitPromotionsAndDemotions(targets.voterDemotions, targets.nonVoterPromotions)
	if len(swaps) > 0 {
		desc, err = execChangeReplicasTxn(ctx, desc, reason, details, swaps, changeReplicasTxnArgs{
			db:                                   r.store.DB(),
			liveAndDeadReplicas:                  r.store.allocator.storePool.liveAndDeadReplicas,
			logChange:                            r.store.logChange,
			testForceJointConfig:                 r.store.TestingKnobs().ReplicationAlwaysUseJointConfig,
			testAllowDangerousReplicationChanges: r.store.TestingKnobs().AllowDangerousReplicationChanges,
		})
		if err != nil {
			return nil, err
		}
	}

	if adds := targets.voterAdditions; len(adds) > 0 {
		// For all newly added voters, first add LEARNER replicas. They accept raft
		// traffic (so they can catch up) but don't get to vote (so they don't
		// affect quorum and thus don't introduce fragility into the system). For
		// details see:
		_ = roachpb.ReplicaSet.LearnerDescriptors
		var err error
		desc, err = r.initializeRaftLearners(
			ctx, desc, priority, reason, details, adds, roachpb.LEARNER,
		)
		if err != nil {
			return nil, err
		}

		if fn := r.store.cfg.TestingKnobs.VoterAddStopAfterLearnerSnapshot; fn != nil && fn(adds) {
			return desc, nil
		}
	}

	if len(targets.voterAdditions)+len(targets.voterRemovals) > 0 {
		desc, err = r.execReplicationChangesForVoters(
			ctx, desc, reason, details,
			targets.voterAdditions, targets.voterRemovals,
		)
		if err != nil {
			// If the error occurred while transitioning out of an atomic replication
			// change, try again here with a fresh descriptor; this is a noop
			// otherwise.
			if _, err := maybeLeaveAtomicChangeReplicas(ctx, r.store, r.Desc()); err != nil {
				return nil, err
			}
			if fn := r.store.cfg.TestingKnobs.ReplicaAddSkipLearnerRollback; fn != nil && fn() {
				return nil, err
			}
			// Don't leave a learner replica lying around if we didn't succeed in
			// promoting it to a voter.
			if adds := targets.voterAdditions; len(adds) > 0 {
				log.Infof(ctx, "could not promote %v to voter, rolling back: %v", adds, err)
				for _, target := range adds {
					r.tryRollbackRaftLearner(ctx, r.Desc(), target, reason, details)
				}
			}
			return nil, err
		}
	}

	if adds := targets.nonVoterAdditions; len(adds) > 0 {
		// Add all non-voters and send them initial snapshots since some callers of
		// `AdminChangeReplicas` (notably the mergeQueue, via `AdminRelocateRange`)
		// care about only dealing with replicas that are mostly caught up with the
		// raft leader. Not attempting to upreplicate these non-voters here can lead
		// to spurious interactions that can fail range merges and cause severe
		// disruption to foreground traffic. See
		// https://github.com/cockroachdb/cockroach/issues/63199 for an example.
		desc, err = r.initializeRaftLearners(
			ctx, desc, priority, reason, details, adds, roachpb.NON_VOTER,
		)
		if err != nil {
			return nil, err
		}
		if fn := r.store.TestingKnobs().NonVoterAfterInitialization; fn != nil {
			fn()
		}
	}

	if removals := targets.nonVoterRemovals; len(removals) > 0 {
		for _, rem := range removals {
			iChgs := []internalReplicationChange{{target: rem, typ: internalChangeTypeRemove}}
			var err error
			desc, err = execChangeReplicasTxn(ctx, desc, reason, details, iChgs,
				changeReplicasTxnArgs{
					db:                                   r.store.DB(),
					liveAndDeadReplicas:                  r.store.allocator.storePool.liveAndDeadReplicas,
					logChange:                            r.store.logChange,
					testForceJointConfig:                 r.store.TestingKnobs().ReplicationAlwaysUseJointConfig,
					testAllowDangerousReplicationChanges: r.store.TestingKnobs().AllowDangerousReplicationChanges,
				})
			if err != nil {
				return nil, err
			}
		}
	}

	if len(targets.voterDemotions) > 0 {
		// If we demoted or swapped any voters with non-voters, we likely are in a
		// joint config or have learners on the range. Let's exit the joint config
		// and remove the learners.
		return maybeLeaveAtomicChangeReplicasAndRemoveLearners(ctx, r.store, desc)
	}
	return desc, nil
}

type targetsForReplicationChanges struct {
	voterDemotions, nonVoterPromotions  []roachpb.ReplicationTarget
	voterAdditions, voterRemovals       []roachpb.ReplicationTarget
	nonVoterAdditions, nonVoterRemovals []roachpb.ReplicationTarget
}

// synthesizeTargetsByChangeType groups replication changes in the
// manner they are intended to be executed by AdminChangeReplicas.
//
// In particular, it coalesces ReplicationChanges of types ADD_VOTER and
// REMOVE_NON_VOTER on a given target as promotions of non-voters into voters
// and likewise, ADD_NON_VOTER and REMOVE_VOTER changes for a given target as
// demotions of voters into non-voters. The rest of the changes are handled
// distinctly and are thus segregated in the return result.
func synthesizeTargetsByChangeType(
	chgs roachpb.ReplicationChanges,
) (result targetsForReplicationChanges) {
	// Isolate the promotions to voters and the demotions to non-voters from the
	// rest of the changes, since we want to handle these together and execute
	// atomic swaps of voters <-> non-voters if possible.
	result.voterDemotions = intersectTargets(chgs.VoterRemovals(), chgs.NonVoterAdditions())
	result.nonVoterPromotions = intersectTargets(chgs.NonVoterRemovals(), chgs.VoterAdditions())

	// Synthesize the additions and removals that shouldn't get executed as
	// promotions of non-voters or demotions of voters.
	result.voterAdditions = subtractTargets(chgs.VoterAdditions(), chgs.NonVoterRemovals())
	result.voterRemovals = subtractTargets(chgs.VoterRemovals(), chgs.NonVoterAdditions())
	result.nonVoterAdditions = subtractTargets(chgs.NonVoterAdditions(), chgs.VoterRemovals())
	result.nonVoterRemovals = subtractTargets(chgs.NonVoterRemovals(), chgs.VoterAdditions())

	return result
}

// maybeLeaveAtomicChangeReplicas transitions out of the joint configuration if
// the descriptor indicates one. This involves running a distributed transaction
// updating said descriptor, the result of which will be returned. The
// descriptor returned from this method will contain replicas of type LEARNER,
// NON_VOTER, and VOTER_FULL only.
func maybeLeaveAtomicChangeReplicas(
	ctx context.Context, s *Store, desc *roachpb.RangeDescriptor,
) (*roachpb.RangeDescriptor, error) {
	// We want execChangeReplicasTxn to be able to make sure it's only tasked
	// with leaving a joint state when it's in one, so make sure we don't call
	// it if we're not.
	if !desc.Replicas().InAtomicReplicationChange() {
		return desc, nil
	}
	// NB: this is matched on in TestMergeQueueSeesLearner.
	log.Eventf(ctx, "transitioning out of joint configuration %s", desc)

	// NB: reason and detail won't be used because no range log event will be
	// emitted.
	//
	// TODO(tbg): reconsider this.
	return execChangeReplicasTxn(
		ctx, desc, kvserverpb.ReasonUnknown /* unused */, "", nil, /* iChgs */
		changeReplicasTxnArgs{
			db:                                   s.DB(),
			liveAndDeadReplicas:                  s.allocator.storePool.liveAndDeadReplicas,
			logChange:                            s.logChange,
			testForceJointConfig:                 s.TestingKnobs().ReplicationAlwaysUseJointConfig,
			testAllowDangerousReplicationChanges: s.TestingKnobs().AllowDangerousReplicationChanges,
		})
}

// maybeLeaveAtomicChangeReplicasAndRemoveLearners transitions out of the joint
// config (if there is one), and then removes all learners. After this function
// returns, all remaining replicas will be of type VOTER_FULL or NON_VOTER.
func maybeLeaveAtomicChangeReplicasAndRemoveLearners(
	ctx context.Context, store *Store, desc *roachpb.RangeDescriptor,
) (*roachpb.RangeDescriptor, error) {
	desc, err := maybeLeaveAtomicChangeReplicas(ctx, store, desc)
	if err != nil {
		return nil, err
	}
	// Now the config isn't joint any more, but we may have demoted some voters
	// into learners. These learners should go as well.

	learners := desc.Replicas().LearnerDescriptors()
	if len(learners) == 0 {
		return desc, nil
	}
	targets := make([]roachpb.ReplicationTarget, len(learners))
	for i := range learners {
		targets[i].NodeID = learners[i].NodeID
		targets[i].StoreID = learners[i].StoreID
	}
	log.VEventf(ctx, 2, `removing learner replicas %v from %v`, targets, desc)
	// NB: unroll the removals because at the time of writing, we can't atomically
	// remove multiple learners. This will be fixed in:
	//
	// https://github.com/cockroachdb/cockroach/pull/40268
	origDesc := desc
	for _, target := range targets {
		var err error
		desc, err = execChangeReplicasTxn(
			ctx, desc, kvserverpb.ReasonAbandonedLearner, "",
			[]internalReplicationChange{{target: target, typ: internalChangeTypeRemove}},
			changeReplicasTxnArgs{db: store.DB(),
				liveAndDeadReplicas:                  store.allocator.storePool.liveAndDeadReplicas,
				logChange:                            store.logChange,
				testForceJointConfig:                 store.TestingKnobs().ReplicationAlwaysUseJointConfig,
				testAllowDangerousReplicationChanges: store.TestingKnobs().AllowDangerousReplicationChanges,
			},
		)
		if err != nil {
			return nil, errors.Wrapf(err, `removing learners from %s`, origDesc)
		}
	}
	return desc, nil
}

// validateAdditionsPerStore ensures that we're not trying to add the same type
// of replica to a store that already has one or that we're not trying to add
// any type of replica to a store that has a LEARNER.
func validateAdditionsPerStore(
	desc *roachpb.RangeDescriptor, chgsByStoreID changesByStoreID,
) error {
	for storeID, chgs := range chgsByStoreID {
		for _, chg := range chgs {
			if chg.ChangeType.IsRemoval() {
				continue
			}
			// If the replica already exists, check that we're not trying to add the
			// same type of replica again.
			//
			// NB: Trying to add a different type of replica, for instance, a
			// NON_VOTER to a store that already has a VOTER is fine when we're trying
			// to swap a VOTER with a NON_VOTER. Ensuring that this is indeed the case
			// is outside the scope of this particular helper method. See
			// validatePromotionsAndDemotions for how that is checked.
			replDesc, found := desc.GetReplicaDescriptor(storeID)
			if !found {
				// The store we're trying to add to doesn't already have a replica, all
				// good.
				continue
			}
			switch t := replDesc.GetType(); t {
			case roachpb.LEARNER:
				// Looks like we found a learner with the same store and node id. One of
				// the following is true:
				// 1. some previous leaseholder was trying to add it with the
				// learner+snapshot+voter cycle and got interrupted.
				// 2. we hit a race between the replicate queue and AdminChangeReplicas.
				// 3. We're trying to swap a voting replica with a non-voting replica
				// before the voting replica has been upreplicated and switched from
				// LEARNER to VOTER_FULL.
				return errors.AssertionFailedf(
					"trying to add(%+v) to a store that already has a %s", chg, t)
			case roachpb.VOTER_FULL:
				if chg.ChangeType == roachpb.ADD_VOTER {
					return errors.AssertionFailedf(
						"trying to add a voter to a store that already has a %s", t)
				}
			case roachpb.NON_VOTER:
				if chg.ChangeType == roachpb.ADD_NON_VOTER {
					return errors.AssertionFailedf(
						"trying to add a non-voter to a store that already has a %s", t)
				}
			default:
				return errors.AssertionFailedf("store(%d) being added to already contains a"+
					" replica of an unexpected type: %s", storeID, t)
			}
		}
	}
	return nil
}

// validateRemovals ensures that replicas being removed actually exist and that
// the type of replica being removed matches the type of the removal.
func validateRemovals(desc *roachpb.RangeDescriptor, chgsByStoreID changesByStoreID) error {
	for storeID, chgs := range chgsByStoreID {
		for _, chg := range chgs {
			if chg.ChangeType.IsAddition() {
				continue
			}
			replDesc, found := desc.GetReplicaDescriptor(storeID)
			if !found {
				return errors.AssertionFailedf("trying to remove a replica that doesn't exist: %+v", chg)
			}
			// Ensure that the type of replica being removed is the same as the type
			// of replica present in the range descriptor.
			switch t := replDesc.GetType(); t {
			case roachpb.VOTER_FULL, roachpb.LEARNER:
				if chg.ChangeType != roachpb.REMOVE_VOTER {
					return errors.AssertionFailedf("type of replica being removed (%s) does not match"+
						" expectation for change: %+v", t, chg)
				}
			case roachpb.NON_VOTER:
				if chg.ChangeType != roachpb.REMOVE_NON_VOTER {
					return errors.AssertionFailedf("type of replica being removed (%s) does not match"+
						" expectation for change: %+v", t, chg)
				}
			default:
				return errors.AssertionFailedf("unexpected replica type for removal %+v: %s", chg, t)
			}
		}
	}
	return nil
}

// validatePromotionsAndDemotions ensures the following:
// 1. All additions of voters to stores that already have a non-voter are
// accompanied by a removal of that non-voter (which is interpreted as a
// promotion of a non-voter to a voter).
// 2. All additions of non-voters to stores that already have a voter are
// accompanied by a removal of that voter (which is interpreted as a demotion of
// a voter to a non-voter)
func validatePromotionsAndDemotions(
	desc *roachpb.RangeDescriptor, chgsByStoreID changesByStoreID,
) error {
	for storeID, chgs := range chgsByStoreID {
		replDesc, found := desc.GetReplicaDescriptor(storeID)
		switch len(chgs) {
		case 0:
			continue
		case 1:
			if chgs[0].ChangeType.IsAddition() {
				// If there's only one addition on this store, without an accompanying
				// removal, then the change cannot correspond to a promotion/demotion.
				// Thus, the store must not already have a replica.
				if found {
					return errors.AssertionFailedf("trying to add(%+v) to a store(%s) that already"+
						" has a replica(%s)", chgs[0], storeID, replDesc.GetType())
				}
			}
		case 2:
			c1, c2 := chgs[0], chgs[1]
			if !found {
				return errors.AssertionFailedf("found 2 changes(%+v) for a store(%d)"+
					" that has no replicas", chgs, storeID)
			}
			if c1.ChangeType.IsAddition() && c2.ChangeType.IsRemoval() {
				// There's only two legal possibilities here:
				// 1. Promotion: ADD_VOTER, REMOVE_NON_VOTER
				// 2. Demotion: ADD_NON_VOTER, REMOVE_VOTER
				//
				// We reject everything else.
				isPromotion := c1.ChangeType == roachpb.ADD_VOTER && c2.ChangeType == roachpb.REMOVE_NON_VOTER
				isDemotion := c1.ChangeType == roachpb.ADD_NON_VOTER && c2.ChangeType == roachpb.REMOVE_VOTER
				if !(isPromotion || isDemotion) {
					return errors.AssertionFailedf("trying to add-remove the same replica(%s):"+
						" %+v", replDesc.GetType(), chgs)
				}
			} else {
				// NB: validateOneReplicaPerNode has a stronger version of this check,
				// but we check it here anyway for the sake of a more precise error
				// message.
				return errors.AssertionFailedf("the only permissible order of operations within a"+
					" store is add-remove; got %+v", chgs)
			}
		default:
			return errors.AssertionFailedf("more than 2 changes referring to the same store: %+v", chgs)
		}
	}
	return nil
}

// validateOneReplicaPerNode ensures that there are no more than 2 changes for
// any given node and if a node already has a replica, then adding a second
// replica is prohibited unless the existing replica is being removed with it.
func validateOneReplicaPerNode(desc *roachpb.RangeDescriptor, chgsByNodeID changesByNodeID) error {
	replsByNodeID := make(map[roachpb.NodeID]int)
	for _, repl := range desc.Replicas().Descriptors() {
		replsByNodeID[repl.NodeID]++
	}

	for nodeID, chgs := range chgsByNodeID {
		if len(chgs) > 2 {
			return errors.AssertionFailedf("more than 2 changes for the same node(%d): %+v",
				nodeID, chgs)
		}
		switch replsByNodeID[nodeID] {
		case 0:
			// If there are no existing replicas on the node, a rebalance is not
			// possible and there must not be more than 1 change for it.
			//
			// NB: We don't care _what_ kind of change it is. If it's a removal, it
			// will be invalidated by `validateRemovals`.
			if len(chgs) > 1 {
				return errors.AssertionFailedf("unexpected set of changes(%+v) for node %d, which has"+
					" no existing replicas for the range", chgs, nodeID)
			}
		case 1:
			// If the node has exactly one replica, then the only changes allowed on
			// the node are:
			// 1. An addition and a removal (constituting a rebalance within the node)
			// 2. Removal
			switch n := len(chgs); n {
			case 1:
				// Must be a removal unless the range only has a single replica. Ranges
				// with only one replica cannot be atomically rebalanced, and must go
				// through addition and then removal separately. See #40333.
				if !chgs[0].ChangeType.IsRemoval() && len(desc.Replicas().Descriptors()) > 1 {
					return errors.AssertionFailedf("node %d already has a replica; only valid actions"+
						" are a removal or a rebalance(add/remove); got %+v", nodeID, chgs)
				}
			case 2:
				// Must be an addition then removal
				c1, c2 := chgs[0], chgs[1]
				if !(c1.ChangeType.IsAddition() && c2.ChangeType.IsRemoval()) {
					return errors.AssertionFailedf("node %d already has a replica; only valid actions"+
						" are a removal or a rebalance(add/remove); got %+v", nodeID, chgs)
				}
			default:
				panic(fmt.Sprintf("unexpected number of changes for node %d: %+v", nodeID, chgs))
			}
		case 2:
			// If there are 2 replicas on any given node, a removal is the only legal
			// thing to do.
			if !(len(chgs) == 1 && chgs[0].ChangeType.IsRemoval()) {
				return errors.AssertionFailedf("node %d has 2 replicas, expected exactly one of them"+
					" to be removed; got %+v", nodeID, chgs)
			}
		default:
			return errors.AssertionFailedf("node %d unexpectedly has more than 2 replicas: %s",
				nodeID, desc.Replicas().Descriptors())
		}
	}
	return nil
}

// validateReplicationChanges runs a series of validation checks against the
// given range descriptor and the proposed set of replication changes on the
// range.
//
// It ensures the following:
// 1. There are no duplicate changes: we shouldn't be adding or removing a
// replica twice.
// 2. We're not adding a replica on a store that already has one, unless it's
// for a promotion or demotion.
// 3. If there are two changes for a single store, the first one must be an
// addition and the second one must be a removal.
// 4. We're not adding a replica on a node that already has one, unless the
// range only has one replica.
// 5. We're not removing a replica that doesn't exist.
// 6. Additions to stores that already contain a replica are strictly the ones
// that correspond to a voter demotion and/or a non-voter promotion
func validateReplicationChanges(
	desc *roachpb.RangeDescriptor, chgs roachpb.ReplicationChanges,
) error {
	chgsByStoreID := getChangesByStoreID(chgs)
	chgsByNodeID := getChangesByNodeID(chgs)

	if err := validateAdditionsPerStore(desc, chgsByStoreID); err != nil {
		return err
	}
	if err := validateRemovals(desc, chgsByStoreID); err != nil {
		return err
	}
	if err := validatePromotionsAndDemotions(desc, chgsByStoreID); err != nil {
		return err
	}
	if err := validateOneReplicaPerNode(desc, chgsByNodeID); err != nil {
		return err
	}

	return nil
}

// changesByStoreID represents a map from StoreID to a slice of replication
// changes on that store.
type changesByStoreID map[roachpb.StoreID][]roachpb.ReplicationChange

// changesByNodeID represents a map from NodeID to a slice of replication
// changes on that node.
type changesByNodeID map[roachpb.NodeID][]roachpb.ReplicationChange

func getChangesByStoreID(chgs roachpb.ReplicationChanges) changesByStoreID {
	chgsByStoreID := make(map[roachpb.StoreID][]roachpb.ReplicationChange, len(chgs))
	for _, chg := range chgs {
		if _, ok := chgsByStoreID[chg.Target.StoreID]; !ok {
			chgsByStoreID[chg.Target.StoreID] = make([]roachpb.ReplicationChange, 0, 2)
		}
		chgsByStoreID[chg.Target.StoreID] = append(chgsByStoreID[chg.Target.StoreID], chg)
	}
	return chgsByStoreID
}

func getChangesByNodeID(chgs roachpb.ReplicationChanges) changesByNodeID {
	chgsByNodeID := make(map[roachpb.NodeID][]roachpb.ReplicationChange, len(chgs))
	for _, chg := range chgs {
		if _, ok := chgsByNodeID[chg.Target.NodeID]; !ok {
			chgsByNodeID[chg.Target.NodeID] = make([]roachpb.ReplicationChange, 0, 2)
		}
		chgsByNodeID[chg.Target.NodeID] = append(chgsByNodeID[chg.Target.NodeID], chg)
	}
	return chgsByNodeID
}

// initializeRaftLearners adds etcd LearnerNodes (LEARNERs or NON_VOTERs in
// Cockroach-land) to the given replication targets and synchronously sends them
// an initial snapshot to upreplicate. Once this successfully returns, the
// callers can assume that the learners were added and have been initialized via
// that snapshot. Otherwise, if we get any errors trying to add or upreplicate
// any of these learners, this function will clean up after itself by rolling all
// of them back.
func (r *Replica) initializeRaftLearners(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	priority SnapshotRequest_Priority,
	reason kvserverpb.RangeLogEventReason,
	details string,
	targets []roachpb.ReplicationTarget,
	replicaType roachpb.ReplicaType,
) (afterDesc *roachpb.RangeDescriptor, err error) {
	var iChangeType internalChangeType
	switch replicaType {
	case roachpb.LEARNER:
		iChangeType = internalChangeTypeAddLearner
	case roachpb.NON_VOTER:
		iChangeType = internalChangeTypeAddNonVoter
	default:
		log.Fatalf(ctx, "unexpected replicaType %s", replicaType)
	}
	// Lock learner snapshots even before we run the ConfChange txn to add them
	// to prevent a race with the raft snapshot queue trying to send it first.
	//
	// Also note that the lock only prevents the raft snapshot queue from sending
	// snapshots to learners and non-voters, it will still send them to voters.
	// There are more details about this locking in
	_ = (*raftSnapshotQueue)(nil).processRaftSnapshot
	// as well as a TODO about fixing all this to be less subtle and brittle.
	releaseSnapshotLockFn := r.lockLearnerSnapshot(ctx, targets)
	defer releaseSnapshotLockFn()

	// If we fail to add or send a snapshot to the learners we're adding, roll
	// them all back.
	defer func() {
		if err != nil {
			log.Infof(
				ctx,
				"could not successfully add and upreplicate %s replica(s) on %s, rolling back: %v",
				replicaType,
				targets,
				err,
			)
			if fn := r.store.cfg.TestingKnobs.ReplicaAddSkipLearnerRollback; fn != nil && fn() {
				return
			}
			// TODO(aayush): We could probably do better here by just rolling back the
			// learners for which the initial snapshot failed. However:
			//
			// - As of the time of this writing, `AdminChangeReplicas` is only called
			// for one change at a time by its callers so it doesn't seem worth it to
			// befog its contract.
			// - If, in the future we start executing multiple changes in an
			// `AdminChangeReplicas` call: in case of voter addition, if we only
			// rolled some of the learners back but not the others, its not obvious
			// how we could still go ahead with their promotion and it also becomes
			// unclear how the error received here should be propagated to the client.
			for _, target := range targets {
				// NB: We're checking `r.Desc()` from this replica's current applied
				// state, not the `desc` we received as a parameter to this call (which
				// is stale and won't reflect the work we did in this function).
				r.tryRollbackRaftLearner(ctx, r.Desc(), target, reason, details)
			}
		}
	}()

	// TODO(tbg): we could add all learners in one go, but then we'd need to
	// do it as an atomic replication change (raft doesn't know which config
	// to apply the delta to, so we might be demoting more than one voter).
	// This isn't crazy, we just need to transition out of the joint config
	// before returning from this method, and it's unclear that it's worth
	// doing.
	for _, target := range targets {
		iChgs := []internalReplicationChange{{target: target, typ: iChangeType}}
		var err error
		desc, err = execChangeReplicasTxn(
			ctx, desc, reason, details, iChgs, changeReplicasTxnArgs{
				db:                                   r.store.DB(),
				liveAndDeadReplicas:                  r.store.allocator.storePool.liveAndDeadReplicas,
				logChange:                            r.store.logChange,
				testForceJointConfig:                 r.store.TestingKnobs().ReplicationAlwaysUseJointConfig,
				testAllowDangerousReplicationChanges: r.store.TestingKnobs().AllowDangerousReplicationChanges,
			},
		)
		if err != nil {
			return nil, err
		}
	}

	// Wait for our replica to catch up with the descriptor change. The replica is
	// expected to usually be already caught up because it's expected to usually
	// be the leaseholder - but it doesn't have to be. Being caught up is
	// important because we are sending snapshots below to newly-added replicas,
	// and those snapshots would be invalid if our stale descriptor doesn't
	// contain the respective replicas.
	// TODO(andrei): Find a better way to wait for replication. If we knew the
	// LAI of the respective command, we could use waitForApplication().
	descriptorOK := false
	start := timeutil.Now()
	retOpts := retry.Options{InitialBackoff: time.Second, MaxBackoff: time.Second, MaxRetries: 10}
	for re := retry.StartWithCtx(ctx, retOpts); re.Next(); {
		rDesc := r.Desc()
		if rDesc.Generation >= desc.Generation {
			descriptorOK = true
			break
		}
		log.VEventf(ctx, 1, "stale descriptor detected; waiting to catch up to replication. want: %s, have: %s",
			desc, rDesc)
		if _, err := r.IsDestroyed(); err != nil {
			return nil, errors.Wrapf(
				err,
				"replica destroyed while waiting desc replication",
			)
		}
	}
	if !descriptorOK {
		return nil, errors.Newf(
			"waited for %s and replication hasn't caught up with descriptor update",
			timeutil.Since(start),
		)
	}

	for _, target := range targets {
		rDesc, ok := desc.GetReplicaDescriptor(target.StoreID)
		if !ok {
			return nil, errors.Errorf("programming error: replica %v not found in %v", target, desc)
		}

		if rDesc.GetType() != replicaType {
			return nil, errors.Errorf("programming error: cannot promote replica of type %s", rDesc.Type)
		}

		if fn := r.store.cfg.TestingKnobs.ReplicaSkipInitialSnapshot; fn != nil && fn() {
			continue
		}

		// Note that raft snapshot queue will refuse to send a snapshot to a learner
		// or non-voter replica if its store is already sending a snapshot to that
		// replica. That would race with this snapshot, except that we've put a
		// (best effort) lock on it before the conf change txn was run (see call to
		// `lockLearnerSnapshot` above). This is best effort because the lock can
		// time out and the lock is local to this node, while the raft leader could
		// be on another node entirely (they're usually co-located but this is not
		// guaranteed).
		//
		// We originally tried always refusing to send snapshots from the raft
		// snapshot queue to learner replicas, but this turned out to be brittle.
		// First, if the snapshot failed, any attempt to use the learner's raft
		// group would hang until the replicate queue got around to cleaning up the
		// orphaned learner. Second, this tickled some bugs in etcd/raft around
		// switching between StateSnapshot and StateProbe. Even if we worked through
		// these, it would be susceptible to future similar issues.
		if err := r.sendSnapshot(ctx, rDesc, SnapshotRequest_INITIAL, priority); err != nil {
			return nil, err
		}
	}
	return desc, nil
}

// lockLearnerSnapshot stops the raft snapshot queue from sending snapshots to
// the soon-to-be added learner replicas to prevent duplicate snapshots from
// being sent. This lock is best effort because it times out and it is a node
// local lock while the raft snapshot queue might be running on a different
// node. An idempotent unlock function is returned.
func (r *Replica) lockLearnerSnapshot(
	ctx context.Context, additions []roachpb.ReplicationTarget,
) (unlock func()) {
	// TODO(dan): The way this works is hacky, but it was added at the last minute
	// in 19.2 to work around a commit in etcd/raft that made this race more
	// likely. It'd be nice if all learner snapshots could be sent from a single
	// place.
	var lockUUIDs []uuid.UUID
	for _, addition := range additions {
		lockUUID := uuid.MakeV4()
		lockUUIDs = append(lockUUIDs, lockUUID)
		r.addSnapshotLogTruncationConstraint(ctx, lockUUID, 1, addition.StoreID)
	}
	return func() {
		now := timeutil.Now()
		for _, lockUUID := range lockUUIDs {
			r.completeSnapshotLogTruncationConstraint(ctx, lockUUID, now)
		}
	}
}

// execReplicationChangesForVoters carries out the atomic membership change that
// finalizes the addition and/or removal of voting replicas. Any voters in the
// process of being added (as reflected by the replication changes) must have
// been added as learners already and caught up. Voter removals (from the
// replication changes) will be carried out by first demoting to a learner
// instead of outright removal (this avoids a [raft-bug] that can lead to
// unavailability). All of this occurs in one atomic raft membership change
// which is carried out across two phases. On error, it is possible that the
// range is in the intermediate ("joint") configuration in which a quorum of
// both the old and new sets of voters is required. If a range is encountered in
// this state, maybeLeaveAtomicReplicationChange can fix this, but it is the
// caller's job to do this when necessary.
//
// The atomic membership change is carried out chiefly via the construction of a
// suitable ChangeReplicasTrigger, see prepareChangeReplicasTrigger for details.
//
//  When adding/removing only a single voter, joint consensus is not used.
//  Notably, demotions must always use joint consensus, even if only a single
//  voter is being demoted, due to a (liftable) limitation in etcd/raft.
//
// [raft-bug]: https://github.com/etcd-io/etcd/issues/11284
func (r *Replica) execReplicationChangesForVoters(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	reason kvserverpb.RangeLogEventReason,
	details string,
	voterAdditions, voterRemovals []roachpb.ReplicationTarget,
) (*roachpb.RangeDescriptor, error) {
	// TODO(dan): We allow ranges with learner replicas to split, so in theory
	// this may want to detect that and retry, sending a snapshot and promoting
	// both sides.

	iChgs := make([]internalReplicationChange, 0, len(voterAdditions)+len(voterRemovals))
	for _, target := range voterAdditions {
		iChgs = append(iChgs, internalReplicationChange{target: target, typ: internalChangeTypePromoteLearner})
	}

	for _, target := range voterRemovals {
		typ := internalChangeTypeRemove
		if rDesc, ok := desc.GetReplicaDescriptor(target.StoreID); ok && rDesc.GetType() == roachpb.VOTER_FULL {
			typ = internalChangeTypeDemoteVoterToLearner
		}
		iChgs = append(iChgs, internalReplicationChange{target: target, typ: typ})
	}

	var err error
	desc, err = execChangeReplicasTxn(ctx, desc, reason, details, iChgs, changeReplicasTxnArgs{
		db:                                   r.store.DB(),
		liveAndDeadReplicas:                  r.store.allocator.storePool.liveAndDeadReplicas,
		logChange:                            r.store.logChange,
		testForceJointConfig:                 r.store.TestingKnobs().ReplicationAlwaysUseJointConfig,
		testAllowDangerousReplicationChanges: r.store.TestingKnobs().AllowDangerousReplicationChanges,
	})
	if err != nil {
		return nil, err
	}

	if fn := r.store.cfg.TestingKnobs.VoterAddStopAfterJointConfig; fn != nil && fn() {
		return desc, nil
	}

	// Leave the joint config if we entered one. Also, remove any learners we
	// might have picked up due to removal-via-demotion.
	return maybeLeaveAtomicChangeReplicasAndRemoveLearners(ctx, r.store, desc)
}

// tryRollbackRaftLearner attempts to remove a learner specified by the target.
// If no such learner is found in the descriptor (including when it is a voter
// instead), no action is taken. Otherwise, a single time-limited best-effort
// attempt at removing the learner is made.
func (r *Replica) tryRollbackRaftLearner(
	ctx context.Context,
	rangeDesc *roachpb.RangeDescriptor,
	target roachpb.ReplicationTarget,
	reason kvserverpb.RangeLogEventReason,
	details string,
) {
	repDesc, ok := rangeDesc.GetReplicaDescriptor(target.StoreID)
	isLearnerOrNonVoter := repDesc.GetType() == roachpb.LEARNER || repDesc.GetType() == roachpb.NON_VOTER
	if !ok || !isLearnerOrNonVoter {
		// There's no learner to roll back.
		log.Event(ctx, "learner to roll back not found; skipping")
		return
	}

	// If (for example) the promotion failed because of a context deadline
	// exceeded, we do still want to clean up after ourselves, so always use a new
	// context (but with the old tags and with some timeout to save this from
	// blocking the caller indefinitely).
	const rollbackTimeout = 10 * time.Second

	rollbackFn := func(ctx context.Context) error {
		_, err := execChangeReplicasTxn(
			ctx, rangeDesc, reason, details,
			[]internalReplicationChange{{target: target, typ: internalChangeTypeRemove}},
			changeReplicasTxnArgs{
				db:                                   r.store.DB(),
				liveAndDeadReplicas:                  r.store.allocator.storePool.liveAndDeadReplicas,
				logChange:                            r.store.logChange,
				testForceJointConfig:                 r.store.TestingKnobs().ReplicationAlwaysUseJointConfig,
				testAllowDangerousReplicationChanges: r.store.TestingKnobs().AllowDangerousReplicationChanges,
			})
		return err
	}
	rollbackCtx := logtags.WithTags(context.Background(), logtags.FromContext(ctx))
	if err := contextutil.RunWithTimeout(
		rollbackCtx, "learner rollback", rollbackTimeout, rollbackFn,
	); err != nil {
		log.Infof(
			ctx,
			"failed to rollback %s %s, abandoning it for the replicate queue: %v",
			repDesc.GetType(),
			target,
			err,
		)
		r.store.replicateQueue.MaybeAddAsync(ctx, r, r.store.Clock().NowAsClockTimestamp())
	} else {
		log.Infof(ctx, "rolled back %s %s in %s", repDesc.GetType(), target, rangeDesc)
	}
}

type internalChangeType byte

const (
	_ internalChangeType = iota + 1
	internalChangeTypeAddLearner
	internalChangeTypeAddNonVoter
	// NB: internalChangeTypePromote{Learner,Voter} are quite similar to each
	// other. We only chose to differentiate them in order to be able to assert on
	// the type of replica being promoted. See `prepareChangeReplicasTrigger`.
	internalChangeTypePromoteLearner
	internalChangeTypePromoteNonVoter
	// internalChangeTypeDemoteVoterToLearner changes a voter to an ephemeral
	// learner. This will necessarily go through joint consensus since it requires
	// two individual changes (only one changes the quorum, so we could allow it
	// in a simple change too, with some work here and upstream). Demotions are
	// treated like removals throughout (i.e. they show up in
	// `ChangeReplicasTrigger.Removed()`, but not in `.Added()`).
	internalChangeTypeDemoteVoterToLearner
	// internalChangeTypeDemoteVoterToNonVoter demotes a voter to a non-voter.
	// This, like the demotion to learner, will go through joint consensus.
	internalChangeTypeDemoteVoterToNonVoter
	// NB: can't remove multiple learners at once (need to remove at least one
	// voter with them), see:
	// https://github.com/cockroachdb/cockroach/pull/40268
	internalChangeTypeRemove
)

// internalReplicationChange is a replication target together with an internal
// change type. The internal change type is needed to encode in which way the
// replica is mutated (i.e. in a sense, what its predecessor looked like). We
// need this to accurately transcribe the old into the updated range descriptor.
type internalReplicationChange struct {
	target roachpb.ReplicationTarget
	typ    internalChangeType
}

type internalReplicationChanges []internalReplicationChange

func (c internalReplicationChanges) leaveJoint() bool { return len(c) == 0 }
func (c internalReplicationChanges) useJoint() bool {
	// NB: demotions require joint consensus because of limitations in etcd/raft.
	// These could be lifted, but it doesn't seem worth it.
	isDemotion := c[0].typ == internalChangeTypeDemoteVoterToNonVoter ||
		c[0].typ == internalChangeTypeDemoteVoterToLearner
	return len(c) > 1 || isDemotion
}

func prepareChangeReplicasTrigger(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	chgs internalReplicationChanges,
	testingForceJointConfig func() bool,
) (*roachpb.ChangeReplicasTrigger, error) {
	updatedDesc := *desc
	updatedDesc.SetReplicas(desc.Replicas().DeepCopy())
	updatedDesc.IncrementGeneration()

	var added, removed []roachpb.ReplicaDescriptor
	if !chgs.leaveJoint() {
		if desc.Replicas().InAtomicReplicationChange() {
			return nil, errors.Errorf("must transition out of joint config first: %s", desc)
		}

		useJoint := chgs.useJoint()
		if fn := testingForceJointConfig; fn != nil && fn() {
			useJoint = true
		}
		for _, chg := range chgs {
			switch chg.typ {
			case internalChangeTypeAddLearner:
				added = append(added,
					updatedDesc.AddReplica(chg.target.NodeID, chg.target.StoreID, roachpb.LEARNER))
			case internalChangeTypeAddNonVoter:
				added = append(added,
					updatedDesc.AddReplica(chg.target.NodeID, chg.target.StoreID, roachpb.NON_VOTER))
			case internalChangeTypePromoteLearner:
				typ := roachpb.VOTER_FULL
				if useJoint {
					typ = roachpb.VOTER_INCOMING
				}
				rDesc, prevTyp, ok := updatedDesc.SetReplicaType(chg.target.NodeID, chg.target.StoreID, typ)
				if !ok || prevTyp != roachpb.LEARNER {
					return nil, errors.Errorf("cannot promote target %v which is missing as LEARNER",
						chg.target)
				}
				added = append(added, rDesc)
			case internalChangeTypePromoteNonVoter:
				typ := roachpb.VOTER_FULL
				if useJoint {
					typ = roachpb.VOTER_INCOMING
				}
				rDesc, prevTyp, ok := updatedDesc.SetReplicaType(chg.target.NodeID, chg.target.StoreID, typ)
				if !ok || prevTyp != roachpb.NON_VOTER {
					return nil, errors.Errorf("cannot promote target %v which is missing as NON_VOTER",
						chg.target)
				}
				added = append(added, rDesc)
			case internalChangeTypeRemove:
				rDesc, ok := updatedDesc.GetReplicaDescriptor(chg.target.StoreID)
				if !ok {
					return nil, errors.Errorf("target %s not found", chg.target)
				}
				prevTyp := rDesc.GetType()
				isRaftLearner := prevTyp == roachpb.LEARNER || prevTyp == roachpb.NON_VOTER
				if !useJoint || isRaftLearner {
					rDesc, _ = updatedDesc.RemoveReplica(chg.target.NodeID, chg.target.StoreID)
				} else if prevTyp != roachpb.VOTER_FULL {
					// NB: prevTyp is already known to be VOTER_FULL because of
					// !InAtomicReplicationChange() and the learner handling
					// above. We check it anyway.
					return nil, errors.AssertionFailedf("cannot transition from %s to VOTER_OUTGOING", prevTyp)
				} else {
					rDesc, _, _ = updatedDesc.SetReplicaType(chg.target.NodeID, chg.target.StoreID, roachpb.VOTER_OUTGOING)
				}
				removed = append(removed, rDesc)
			case internalChangeTypeDemoteVoterToLearner:
				// Demotion is similar to removal, except that a demotion
				// cannot apply to a learner, and that the resulting type is
				// different when entering a joint config.
				rDesc, ok := updatedDesc.GetReplicaDescriptor(chg.target.StoreID)
				if !ok {
					return nil, errors.Errorf("target %s not found", chg.target)
				}
				if !useJoint {
					// NB: this won't fire because cc.useJoint() is always true when
					// there's a demotion. This is just a sanity check.
					return nil, errors.AssertionFailedf("demotions require joint consensus")
				}
				if prevTyp := rDesc.GetType(); prevTyp != roachpb.VOTER_FULL {
					return nil, errors.Errorf("cannot transition from %s to VOTER_DEMOTING_LEARNER", prevTyp)
				}
				rDesc, _, _ = updatedDesc.SetReplicaType(chg.target.NodeID, chg.target.StoreID, roachpb.VOTER_DEMOTING_LEARNER)
				removed = append(removed, rDesc)
			case internalChangeTypeDemoteVoterToNonVoter:
				rDesc, ok := updatedDesc.GetReplicaDescriptor(chg.target.StoreID)
				if !ok {
					return nil, errors.Errorf("target %s not found", chg.target)
				}
				if !useJoint {
					// NB: this won't fire because cc.useJoint() is always true when
					// there's a demotion. This is just a sanity check.
					return nil, errors.Errorf("demotions require joint consensus")
				}
				if prevTyp := rDesc.GetType(); prevTyp != roachpb.VOTER_FULL {
					return nil, errors.Errorf("cannot transition from %s to VOTER_DEMOTING_NON_VOTER", prevTyp)
				}
				rDesc, _, _ = updatedDesc.SetReplicaType(chg.target.NodeID, chg.target.StoreID, roachpb.VOTER_DEMOTING_NON_VOTER)
				removed = append(removed, rDesc)
			default:
				return nil, errors.Errorf("unsupported internal change type %d", chg.typ)
			}
		}
	} else {
		// Want to leave a joint config. Note that we're not populating 'added' or 'removed', this
		// is intentional; leaving the joint config corresponds to an "empty" raft conf change.
		var isJoint bool
		// NB: the DeepCopy is needed or we'll skip over an entry every time we
		// call RemoveReplica below.
		for _, rDesc := range updatedDesc.Replicas().DeepCopy().Descriptors() {
			switch rDesc.GetType() {
			case roachpb.VOTER_INCOMING:
				updatedDesc.SetReplicaType(rDesc.NodeID, rDesc.StoreID, roachpb.VOTER_FULL)
				isJoint = true
			case roachpb.VOTER_OUTGOING:
				updatedDesc.RemoveReplica(rDesc.NodeID, rDesc.StoreID)
				isJoint = true
			case roachpb.VOTER_DEMOTING_LEARNER:
				updatedDesc.SetReplicaType(rDesc.NodeID, rDesc.StoreID, roachpb.LEARNER)
				isJoint = true
			case roachpb.VOTER_DEMOTING_NON_VOTER:
				updatedDesc.SetReplicaType(rDesc.NodeID, rDesc.StoreID, roachpb.NON_VOTER)
				isJoint = true
			default:
			}
		}
		if !isJoint {
			return nil, errors.Errorf("cannot leave a joint config; desc not joint: %s", &updatedDesc)
		}
	}

	if err := updatedDesc.Validate(); err != nil {
		return nil, errors.Wrapf(err, "validating updated descriptor %s", &updatedDesc)
	}

	crt := &roachpb.ChangeReplicasTrigger{
		Desc:                    &updatedDesc,
		InternalAddedReplicas:   added,
		InternalRemovedReplicas: removed,
	}

	if _, err := crt.ConfChange(nil); err != nil {
		return nil, errors.Wrapf(err, "programming error: malformed trigger created from desc %s to %s", desc, &updatedDesc)
	}
	return crt, nil
}

func within10s(ctx context.Context, fn func() error) error {
	retOpts := retry.Options{InitialBackoff: time.Second, MaxBackoff: time.Second, MaxRetries: 10}
	var err error
	for re := retry.StartWithCtx(ctx, retOpts); re.Next(); {
		err = fn()
		if err == nil {
			return nil
		}
	}
	return ctx.Err()
}

type changeReplicasTxnArgs struct {
	db *kv.DB

	// liveAndDeadReplicas divides the provided repls slice into two slices: the
	// first for live replicas, and the second for dead replicas. Replicas for
	// which liveness or deadness cannot be ascertained are excluded from the
	// returned slices. Replicas on decommissioning node/store are considered
	// live.
	liveAndDeadReplicas func(
		repls []roachpb.ReplicaDescriptor,
	) (liveReplicas, deadReplicas []roachpb.ReplicaDescriptor)

	logChange                            logChangeFn
	testForceJointConfig                 func() bool
	testAllowDangerousReplicationChanges bool
}

// execChangeReplicasTxn runs a txn updating a range descriptor. The txn commit
// will carry a ChangeReplicasTrigger. Returns the updated descriptor. Note
// that, if the current node does not have the leaseholder for the respective
// range, then upon return the node's replica of the range (if any) might not
// reflect the updated descriptor yet until it applies the transaction.
//
// NB: Even when the method is a no-op (see the `check()` closure below), it
// will return an updated (i.e. read from KV) version of the range descriptor.
// Callers are responsible for ensuring that the returned range descriptor meets
// their expectations.
func execChangeReplicasTxn(
	ctx context.Context,
	referenceDesc *roachpb.RangeDescriptor,
	reason kvserverpb.RangeLogEventReason,
	details string,
	chgs internalReplicationChanges,
	args changeReplicasTxnArgs,
) (*roachpb.RangeDescriptor, error) {
	var returnDesc *roachpb.RangeDescriptor

	descKey := keys.RangeDescriptorKey(referenceDesc.StartKey)

	check := func(kvDesc *roachpb.RangeDescriptor) bool {
		// NB: We might fail to find the range if the range has been merged away
		// in which case we definitely want to fail the check below.
		if kvDesc != nil && kvDesc.RangeID == referenceDesc.RangeID && chgs.leaveJoint() {
			// If there are no changes, we're trying to leave a joint config,
			// so that's all we care about. But since leaving a joint config
			// is done opportunistically whenever one is encountered, this is
			// more likely to race than other operations. So we verify literally
			// nothing about the descriptor, but once we get the descriptor out
			// from conditionalGetDescValueFromDB, we'll check if it's in a
			// joint config and if not, noop.
			return true
		}
		// Otherwise, check that the descriptors are equal.
		//
		// TODO(tbg): check that the replica sets are equal only. I was going to
		// do that but then discovered #40367. Try again in the 20.1 cycle.
		return checkDescsEqual(referenceDesc)(kvDesc)
	}

	if err := args.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		log.Event(ctx, "attempting txn")
		txn.SetDebugName(replicaChangeTxnName)
		desc, dbDescValue, err := conditionalGetDescValueFromDB(
			ctx, txn, referenceDesc.StartKey, false /* forUpdate */, check)
		if err != nil {
			return err
		}
		if chgs.leaveJoint() && !desc.Replicas().InAtomicReplicationChange() {
			// Nothing to do. See comment in 'check' above for details.
			returnDesc = desc
			return nil
		}
		// Note that we are now using the descriptor from KV, not the one passed
		// into this method.
		crt, err := prepareChangeReplicasTrigger(ctx, desc, chgs, args.testForceJointConfig)
		if err != nil {
			return err
		}
		log.Infof(ctx, "change replicas (add %v remove %v): existing descriptor %s", crt.Added(), crt.Removed(), desc)

		// NB: we haven't written any intents yet, so even in the unlikely case in which
		// this is held up, we won't block anyone else.
		if err := within10s(ctx, func() error {
			if args.testAllowDangerousReplicationChanges {
				return nil
			}
			// Run (and retry for a bit) a sanity check that the configuration
			// resulting from this change is able to meet quorum. It's
			// important to do this at this low layer as there are multiple
			// entry points that are not generally too careful. For example,
			// before the below check existed, the store rebalancer could
			// carry out operations that would lead to a loss of quorum.
			//
			// See:
			// https://github.com/cockroachdb/cockroach/issues/54444#issuecomment-707706553
			replicas := crt.Desc.Replicas()
			liveReplicas, _ := args.liveAndDeadReplicas(replicas.Descriptors())
			if !replicas.CanMakeProgress(
				func(rDesc roachpb.ReplicaDescriptor) bool {
					for _, inner := range liveReplicas {
						if inner.ReplicaID == rDesc.ReplicaID {
							return true
						}
					}
					return false
				}) {
				// NB: we use newQuorumError which is recognized by the replicate queue.
				return newQuorumError("range %s cannot make progress with proposed changes add=%v del=%v "+
					"based on live replicas %v", crt.Desc, crt.Added(), crt.Removed(), liveReplicas)
			}
			return nil
		}); err != nil {
			return err
		}

		{
			b := txn.NewBatch()

			// Important: the range descriptor must be the first thing touched in the transaction
			// so the transaction record is co-located with the range being modified.
			if err := updateRangeDescriptor(ctx, b, descKey, dbDescValue, crt.Desc); err != nil {
				return err
			}

			// Run transaction up to this point to create txn record early (see #9265).
			if err := txn.Run(ctx, b); err != nil {
				return err
			}
		}

		// Log replica change into range event log.
		err = recordRangeEventsInLog(
			ctx, txn, true /* added */, crt.Added(), crt.Desc, reason, details, args.logChange,
		)
		if err != nil {
			return err
		}
		err = recordRangeEventsInLog(
			ctx, txn, false /* added */, crt.Removed(), crt.Desc, reason, details, args.logChange,
		)
		if err != nil {
			return err
		}

		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a commit trigger.
		b := txn.NewBatch()

		// Update range descriptor addressing record(s).
		if err := updateRangeAddressing(b, crt.Desc); err != nil {
			return err
		}

		b.AddRawRequest(&roachpb.EndTxnRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				ChangeReplicasTrigger: crt,
			},
		})
		if err := txn.Run(ctx, b); err != nil {
			log.Eventf(ctx, "%v", err)
			return err
		}

		returnDesc = crt.Desc
		return nil
	}); err != nil {
		log.Eventf(ctx, "%v", err)
		// NB: desc may not be the descriptor we actually compared against, but
		// either way this gives a good idea of what happened which is all it's
		// supposed to do.
		if ok, actualDesc := maybeDescriptorChangedError(referenceDesc, err); ok {
			// We do not include the original error as cause in this case -
			// the caller should not observe the cause. We still include it
			// as "secondary payload", in case the error object makes it way
			// to logs or telemetry during a crash.
			err = errors.WithSecondaryError(newDescChangedError(referenceDesc, actualDesc), err)
			err = &benignError{err}
		}
		return nil, errors.Wrapf(err, "change replicas of r%d failed", referenceDesc.RangeID)
	}
	log.Event(ctx, "txn complete")
	return returnDesc, nil
}

func getInternalChangesForExplicitPromotionsAndDemotions(
	voterDemotions, nonVoterPromotions []roachpb.ReplicationTarget,
) []internalReplicationChange {
	iChgs := make([]internalReplicationChange, len(voterDemotions)+len(nonVoterPromotions))
	for i := range voterDemotions {
		iChgs[i] = internalReplicationChange{
			target: voterDemotions[i],
			typ:    internalChangeTypeDemoteVoterToNonVoter,
		}
	}

	for j := range nonVoterPromotions {
		iChgs[j+len(voterDemotions)] = internalReplicationChange{
			target: nonVoterPromotions[j],
			typ:    internalChangeTypePromoteNonVoter,
		}
	}

	return iChgs
}

type logChangeFn func(
	ctx context.Context,
	txn *kv.Txn,
	changeType roachpb.ReplicaChangeType,
	replica roachpb.ReplicaDescriptor,
	desc roachpb.RangeDescriptor,
	reason kvserverpb.RangeLogEventReason,
	details string,
) error

func recordRangeEventsInLog(
	ctx context.Context,
	txn *kv.Txn,
	added bool,
	repDescs []roachpb.ReplicaDescriptor,
	rangeDesc *roachpb.RangeDescriptor,
	reason kvserverpb.RangeLogEventReason,
	details string,
	logChange logChangeFn,
) error {
	for _, repDesc := range repDescs {
		isNonVoter := repDesc.GetType() == roachpb.NON_VOTER
		var typ roachpb.ReplicaChangeType
		if added {
			typ = roachpb.ADD_VOTER
			if isNonVoter {
				typ = roachpb.ADD_NON_VOTER
			}
		} else {
			typ = roachpb.REMOVE_VOTER
			if isNonVoter {
				typ = roachpb.REMOVE_NON_VOTER
			}
		}
		if err := logChange(
			ctx, txn, typ, repDesc, *rangeDesc, reason, details,
		); err != nil {
			return err
		}
	}
	return nil
}

// sendSnapshot sends a snapshot of the replica state to the specified replica.
// Currently only invoked from replicateQueue and raftSnapshotQueue. Be careful
// about adding additional calls as generating a snapshot is moderately
// expensive.
//
// A snapshot is a bulk transfer of all data in a range. It consists of a
// consistent view of all the state needed to run some replica of a range as of
// some applied index (not as of some mvcc-time). Snapshots are used by Raft
// when a follower is far enough behind the leader that it can no longer be
// caught up using incremental diffs (because the leader has already garbage
// collected the diffs, in this case because it truncated the Raft log past
// where the follower is).
//
// We also proactively send a snapshot when adding a new replica to bootstrap it
// (this is called a "learner" snapshot and is a special case of a Raft
// snapshot, we just speed the process along). It's called a learner snapshot
// because it's sent to what Raft terms a learner replica. As of 19.2, when we
// add a new replica, it's first added as a learner using a Raft ConfChange,
// which means it accepts Raft traffic but doesn't vote or affect quorum. Then
// we immediately send it a snapshot to catch it up. After the snapshot
// successfully applies, we turn it into a normal voting replica using another
// ConfChange. It then uses the normal mechanisms to catch up with whatever got
// committed to the Raft log during the snapshot transfer. In contrast to adding
// the voting replica directly, this avoids a period of fragility when the
// replica would be a full member, but very far behind.
//
// Snapshots are expensive and mostly unexpected (except learner snapshots
// during rebalancing). The quota pool is responsible for keeping a leader from
// getting too far ahead of any of the followers, so ideally they'd never be far
// enough behind to need a snapshot.
//
// The snapshot process itself is broken into 3 parts: generating the snapshot,
// transmitting it, and applying it.
//
// Generating the snapshot: The data contained in a snapshot is a full copy of
// the replicated data plus everything the replica needs to be a healthy member
// of a Raft group. The former is large, so we send it via streaming rpc
// instead of keeping it all in memory at once. The `(Replica).GetSnapshot`
// method does the necessary locking and gathers the various Raft state needed
// to run a replica. It also creates an iterator for the range's data as it
// looked under those locks (this is powered by a RocksDB snapshot, which is a
// different thing but a similar idea). Notably, GetSnapshot does not do the
// data iteration.
//
// Transmitting the snapshot: The transfer itself happens over the grpc
// `RaftSnapshot` method, which is a bi-directional stream of `SnapshotRequest`s
// and `SnapshotResponse`s. The two sides are orchestrated by the
// `(RaftTransport).SendSnapshot` and `(Store).receiveSnapshot` methods.
//
// `SendSnapshot` starts up the streaming rpc and first sends a header message
// with everything but the range data and then blocks, waiting on the first
// streaming response from the recipient. This lets us short-circuit sending the
// range data if the recipient can't be contacted or if it can't use the
// snapshot (which is usually the result of a race). The recipient's grpc
// handler for RaftSnapshot sanity checks a few things and ends up calling down
// into `receiveSnapshot`, which does the bulk of the work. `receiveSnapshot`
// starts by waiting for a reservation in the snapshot rate limiter. It then
// reads the header message and hands it to `shouldAcceptSnapshotData` to
// determine if it can use the snapshot [1]. `shouldAcceptSnapshotData` is
// advisory and can return false positives. If `shouldAcceptSnapshotData`
// returns true, this is communicated back to the sender, which then proceeds to
// call `kvBatchSnapshotStrategy.Send`. This uses the iterator captured earlier
// to send the data in chunks, each chunk a streaming grpc message. The sender
// then sends a final message with an indicaton that it's done and blocks again,
// waiting for a second and final response from the recipient which indicates if
// the snapshot was a success.
//
// `receiveSnapshot` takes the key-value pairs sent and incrementally creates
// three to five SSTs from them for direct ingestion: one for the replicated
// range-ID local keys, one for the range local keys, optionally two for the
// lock table keys, and one for the user keys. The reason it creates these as
// separate SSTs is to prevent overlaps with the memtable and existing SSTs in
// RocksDB. Each of the SSTs also has a range deletion tombstone to delete the
// existing data in the range.
//

// Applying the snapshot: After the recipient has received the message
// indicating it has all the data, it hands it all to
// `(Store).processRaftSnapshotRequest` to be applied. First, this re-checks
// the same things as `shouldAcceptSnapshotData` to make sure nothing has
// changed while the snapshot was being transferred. It then guarantees that
// there is either an initialized[2] replica or a `ReplicaPlaceholder`[3] to
// accept the snapshot by creating a placeholder if necessary. Finally, a *Raft
// snapshot* message is manually handed to the replica's Raft node (by calling
// `stepRaftGroup` + `handleRaftReadyRaftMuLocked`). During the application
// process, several other SSTs may be created for direct ingestion. An SST for
// the unreplicated range-ID local keys is created for the Raft entries, hard
// state, and truncated state. An SST is created for deleting each subsumed
// replica's range-ID local keys and at most four SSTs are created for
// deleting the user keys, range local keys, and lock table keys (up to 2
// ssts) of all subsumed replicas. All in all, a maximum of 6 + 4*SR SSTs will
// be created for direct ingestion where SR is the number of subsumed
// replicas. In the case where there are no subsumed replicas, 4 to 6 SSTs
// will be created.
//
// [1]: The largest class of rejections here is if the store contains a replica
// that overlaps the snapshot but has a different id (we maintain an invariant
// that replicas on a store never overlap). This usually happens when the
// recipient has an old copy of a replica that is no longer part of a range and
// the `replicaGCQueue` hasn't gotten around to collecting it yet. So if this
// happens, `shouldAcceptSnapshotData` will queue it up for consideration.
//
// [2]: A uninitialized replica is created when a replica that's being added
// gets traffic from its new peers before it gets a snapshot. It may be possible
// to get rid of uninitialized replicas (by dropping all Raft traffic except
// votes on the floor), but this is a cleanup that hasn't happened yet.
//
// [3]: The placeholder is essentially a snapshot lock, making any future
// callers of `shouldAcceptSnapshotData` return an error so that we no longer
// have to worry about racing with a second snapshot. See the comment on
// ReplicaPlaceholder for details.
func (r *Replica) sendSnapshot(
	ctx context.Context,
	recipient roachpb.ReplicaDescriptor,
	snapType SnapshotRequest_Type,
	priority SnapshotRequest_Priority,
) (retErr error) {
	defer func() {
		// Report the snapshot status to Raft, which expects us to do this once we
		// finish sending the snapshot.
		r.reportSnapshotStatus(ctx, recipient.ReplicaID, retErr)
	}()

	snap, err := r.GetSnapshot(ctx, snapType, recipient.StoreID)
	if err != nil {
		err = errors.Wrapf(err, "%s: failed to generate %s snapshot", r, snapType)
		return errors.Mark(err, errMarkSnapshotError)
	}
	defer snap.Close()
	log.Event(ctx, "generated snapshot")

	// Check that the snapshot we generated has a descriptor that includes the
	// recipient. If it doesn't, the recipient will reject it, so it's better to
	// not send it in the first place. It's possible to hit this case if we're not
	// the leaseholder and we haven't yet applied the configuration change that's
	// adding the recipient to the range.
	if _, ok := snap.State.Desc.GetReplicaDescriptor(recipient.StoreID); !ok {
		return errors.Wrapf(errMarkSnapshotError,
			"attempting to send snapshot that does not contain the recipient as a replica; "+
				"snapshot type: %s, recipient: s%d, desc: %s", snapType, recipient, snap.State.Desc)
	}

	sender, err := r.GetReplicaDescriptor()
	if err != nil {
		return errors.Wrapf(err, "%s: change replicas failed", r)
	}

	status := r.RaftStatus()
	if status == nil {
		// This code path is sometimes hit during scatter for replicas that
		// haven't woken up yet.
		return &benignError{errors.Wrap(errMarkSnapshotError, "raft status not initialized")}
	}

	usesReplicatedTruncatedState, err := storage.MVCCGetProto(
		ctx, snap.EngineSnap, keys.RaftTruncatedStateLegacyKey(r.RangeID), hlc.Timestamp{}, nil, storage.MVCCGetOptions{},
	)
	if err != nil {
		return errors.Wrap(err, "loading legacy truncated state")
	}

	canAvoidSendingLog := !usesReplicatedTruncatedState &&
		snap.State.TruncatedState.Index < snap.State.RaftAppliedIndex

	if canAvoidSendingLog {
		// If we're not using a legacy (replicated) truncated state, we avoid
		// sending the (past) Raft log in the snapshot in the first place and
		// send only those entries that are actually useful to the follower.
		// This is done by changing the truncated state, which we're allowed
		// to do since it is not a replicated key (and thus not subject to
		// matching across replicas). The actual sending happens here:
		_ = (*kvBatchSnapshotStrategy)(nil).Send
		// and results in no log entries being sent at all. Note that
		// Metadata.Index is really the applied index of the replica.
		snap.State.TruncatedState = &roachpb.RaftTruncatedState{
			Index: snap.RaftSnap.Metadata.Index,
			Term:  snap.RaftSnap.Metadata.Term,
		}
	}

	req := SnapshotRequest_Header{
		State: snap.State,
		// Tell the recipient whether it needs to synthesize the new
		// unreplicated TruncatedState. It could tell by itself by peeking into
		// the data, but it uses a write only batch for performance which
		// doesn't support that; this is easier. Notably, this is true if the
		// snap index itself is the one at which the migration happens.
		//
		// See VersionUnreplicatedRaftTruncatedState.
		UnreplicatedTruncatedState: !usesReplicatedTruncatedState,
		RaftMessageRequest: RaftMessageRequest{
			RangeID:     r.RangeID,
			FromReplica: sender,
			ToReplica:   recipient,
			Message: raftpb.Message{
				Type:     raftpb.MsgSnap,
				To:       uint64(recipient.ReplicaID),
				From:     uint64(sender.ReplicaID),
				Term:     status.Term,
				Snapshot: snap.RaftSnap,
			},
		},
		RangeSize: r.GetMVCCStats().Total(),
		// Recipients currently cannot choose to decline any snapshots.
		// In 19.2 and earlier versions pre-emptive snapshots could be declined.
		//
		// TODO(ajwerner): Consider removing the CanDecline flag.
		CanDecline: false,
		Priority:   priority,
		Strategy:   SnapshotRequest_KV_BATCH,
		Type:       snapType,
	}
	newBatchFn := func() storage.Batch {
		return r.store.Engine().NewUnindexedBatch(true /* writeOnly */)
	}
	sent := func() {
		r.store.metrics.RangeSnapshotsGenerated.Inc(1)
	}
	if err := r.store.cfg.Transport.SendSnapshot(
		ctx,
		r.store.allocator.storePool,
		req,
		snap,
		newBatchFn,
		sent,
	); err != nil {
		if errors.Is(err, errMalformedSnapshot) {
			tag := fmt.Sprintf("r%d_%s", r.RangeID, snap.SnapUUID.Short())
			if dir, err := r.store.checkpoint(ctx, tag); err != nil {
				log.Warningf(ctx, "unable to create checkpoint %s: %+v", dir, err)
			} else {
				log.Warningf(ctx, "created checkpoint %s", dir)
			}

			log.Fatal(ctx, "malformed snapshot generated")
		}
		return errors.Mark(err, errMarkSnapshotError)
	}
	return nil
}

// replicasCollocated is used in AdminMerge to ensure that the ranges are
// all collocate on the same set of replicas.
func replicasCollocated(a, b []roachpb.ReplicaDescriptor) bool {
	if len(a) != len(b) {
		return false
	}

	set := make(map[roachpb.StoreID]int)
	for _, replica := range a {
		set[replica.StoreID]++
	}

	for _, replica := range b {
		set[replica.StoreID]--
	}

	for _, value := range set {
		if value != 0 {
			return false
		}
	}

	return true
}

func checkDescsEqual(desc *roachpb.RangeDescriptor) func(*roachpb.RangeDescriptor) bool {
	return func(desc2 *roachpb.RangeDescriptor) bool {
		return desc.Equal(desc2)
	}
}

// conditionalGetDescValueFromDB fetches an encoded RangeDescriptor from kv,
// checks that it matches the given expectation using proto Equals, and returns
// the raw fetched roachpb.Value. If the fetched value doesn't match the
// expectation, a ConditionFailedError is returned.
//
// The method allows callers to specify whether a locking read should be used or
// not. A locking read can be used to manage contention and avoid transaction
// restarts in a read-modify-write operation (which all users of this method
// are). Callers should be aware that a locking read impacts transaction record
// placement, unlike a non-locking read. Callers should also be aware that in
// mixed version clusters that contain v20.2 nodes, the locking mode of the read
// may be ignored because the leaseholder of the range may be a v20.2 node that
// is not aware of the locking option on Get requests. However, even if the
// locking mode is ignored, the impact on the transaction record placement
// remains, because that logic lives in the kv client (our process), which we
// know is a v21.1 node.
//
// TODO(nvanbenschoten): once this migration period has passed and we can rely
// on the locking mode of the read being respected, remove the forUpdate param
// and perform locking reads for all callers.
//
// This ConditionFailedError is a historical artifact. We used to pass the
// parsed RangeDescriptor directly as the expected value in a CPut, but proto
// message encodings aren't stable so this was fragile. Calling this method and
// then passing the returned bytes as the expected value in a CPut does the same
// thing, but also correctly handles proto equality. See #38308.
func conditionalGetDescValueFromDB(
	ctx context.Context,
	txn *kv.Txn,
	startKey roachpb.RKey,
	forUpdate bool,
	check func(*roachpb.RangeDescriptor) bool,
) (*roachpb.RangeDescriptor, []byte, error) {
	get := txn.Get
	if forUpdate {
		get = txn.GetForUpdate
	}
	descKey := keys.RangeDescriptorKey(startKey)
	existingDescKV, err := get(ctx, descKey)
	if err != nil {
		return nil, nil, errors.Wrap(err, "fetching current range descriptor value")
	}
	var existingDesc *roachpb.RangeDescriptor
	if existingDescKV.Value != nil {
		existingDesc = &roachpb.RangeDescriptor{}
		if err := existingDescKV.Value.GetProto(existingDesc); err != nil {
			return nil, nil, errors.Wrap(err, "decoding current range descriptor value")
		}
	}

	if !check(existingDesc) {
		return nil, nil, &roachpb.ConditionFailedError{ActualValue: existingDescKV.Value}
	}
	return existingDesc, existingDescKV.Value.TagAndDataBytes(), nil
}

// updateRangeDescriptor adds a ConditionalPut on the range descriptor. The
// conditional put verifies that changes to the range descriptor are made in a
// well-defined order, preventing a scenario where a wayward replica which is
// no longer part of the original Raft group comes back online to form a
// splinter group with a node which was also a former replica, and hijacks the
// range descriptor. This is a last line of defense; other mechanisms should
// prevent rogue replicas from getting this far (see #768).
//
// oldValue can be nil, meaning that the key is expected to not exist.
//
// Note that in addition to using this method to update the on-disk range
// descriptor, a CommitTrigger must be used to update the in-memory
// descriptor; it will not automatically be copied from newDesc.
func updateRangeDescriptor(
	ctx context.Context,
	b *kv.Batch,
	descKey roachpb.Key,
	oldValue []byte,
	newDesc *roachpb.RangeDescriptor,
) error {
	// This is subtle: []byte(nil) != interface{}(nil). A []byte(nil) refers to
	// an empty value. An interface{}(nil) refers to a non-existent value. So
	// we're careful to construct interface{}(nil)s when newDesc/oldDesc are nil.
	var newValue interface{}
	if newDesc != nil {
		if err := newDesc.Validate(); err != nil {
			return errors.Wrapf(err, "validating new descriptor %+v (old descriptor is %+v)",
				newDesc, oldValue)
		}
		newBytes, err := protoutil.Marshal(newDesc)
		if err != nil {
			return err
		}
		newValue = newBytes
	}
	b.CPut(descKey, newValue, oldValue)
	return nil
}

// AdminRelocateRange relocates a given range to a given set of stores. The
// first store in the slice becomes the new leaseholder.
//
// This is best-effort; it's possible that the replicate queue on the
// leaseholder could take action at the same time, causing errors.
func (s *Store) AdminRelocateRange(
	ctx context.Context,
	rangeDesc roachpb.RangeDescriptor,
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
) error {
	if containsDuplicates(voterTargets) {
		return errors.AssertionFailedf(
			"list of desired voter targets contains duplicates: %+v",
			voterTargets,
		)
	}
	if containsDuplicates(nonVoterTargets) {
		return errors.AssertionFailedf(
			"list of desired non-voter targets contains duplicates: %+v",
			nonVoterTargets,
		)
	}
	if containsDuplicates(append(voterTargets, nonVoterTargets...)) {
		return errors.AssertionFailedf(
			"list of voter targets overlaps with the list of non-voter targets: voters: %+v, non-voters: %+v",
			voterTargets, nonVoterTargets,
		)
	}

	// Remove learners so we don't have to think about relocating them, and leave
	// the joint config if we're in one.
	newDesc, err := maybeLeaveAtomicChangeReplicasAndRemoveLearners(ctx, s, &rangeDesc)
	if err != nil {
		log.Warningf(ctx, "%v", err)
		return err
	}
	rangeDesc = *newDesc

	rangeDesc, err = s.relocateReplicas(ctx, rangeDesc, voterTargets, nonVoterTargets)
	if err != nil {
		return err
	}
	return nil
}

// relocateReplicas repeatedly adds and/or removes a replica until we reach
// the desired state. In an "atomic replication changes" world, this is
// conceptually easy: change from the old set of replicas to the new one. But
// there are two reasons that complicate this:
// 1. we can't remove the leaseholder, so if we ultimately want to do that
//    the lease has to be moved first. If we start out with *only* the
//    leaseholder, we will have to add a replica first.
// 2. this code is rewritten late in the cycle and it is both safer and
//    closer to its previous incarnation to never issue atomic changes
//    other than simple swaps.
//
// The loop below repeatedly calls relocateOne, which gives us either
// one or two ops that move the range towards the desired replication state. If
// it's one op, then a single add or remove is carried out (and it's only done
// when we can't swap instead). If it's two ops, then we're swapping (though
// this code doesn't concern itself with the details); and it's possible that we
// need to transfer the lease before we carry out the ops, determined via the
// leaseTarget variable.
//
// Transient errors returned from relocateOne are retried until things
// work out.
func (s *Store) relocateReplicas(
	ctx context.Context,
	rangeDesc roachpb.RangeDescriptor,
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
) (roachpb.RangeDescriptor, error) {
	startKey := rangeDesc.StartKey.AsRawKey()
	transferLease := func(target roachpb.ReplicationTarget) error {
		// TODO(tbg): we ignore errors here, but it seems that in practice these
		// transfers "always work". Some of them are essential (we can't remove
		// the leaseholder so we'll fail there later if this fails), so it
		// seems like a good idea to return any errors here to the caller (or
		// to retry some errors appropriately).
		if err := s.DB().AdminTransferLease(
			ctx, startKey, target.StoreID,
		); err != nil {
			log.Warningf(ctx, "while transferring lease: %+v", err)
			if s.TestingKnobs().DontIgnoreFailureToTransferLease {
				return err
			}
		}
		return nil
	}

	every := log.Every(time.Minute)
	for {
		for re := retry.StartWithCtx(ctx, retry.Options{MaxBackoff: 5 * time.Second}); re.Next(); {
			if err := ctx.Err(); err != nil {
				return rangeDesc, err
			}

			ops, leaseTarget, err := s.relocateOne(ctx, &rangeDesc, voterTargets, nonVoterTargets)
			if err != nil {
				return rangeDesc, err
			}
			if leaseTarget != nil {
				// NB: we may need to transfer even if there are no ops, to make
				// sure the attempt is made to make the first target the final
				// leaseholder.
				if err := transferLease(*leaseTarget); err != nil {
					return rangeDesc, err
				}
			}
			if len(ops) == 0 {
				// Done.
				return rangeDesc, ctx.Err()
			}

			opss := [][]roachpb.ReplicationChange{ops}
			success := true
			for _, ops := range opss {
				newDesc, err := s.DB().AdminChangeReplicas(ctx, startKey, rangeDesc, ops)
				if err != nil {
					returnErr := errors.Wrapf(err, "while carrying out changes %v", ops)
					if !isSnapshotError(err) {
						return rangeDesc, returnErr
					}
					if every.ShouldLog() {
						log.Infof(ctx, "%v", returnErr)
					}
					success = false
					break
				}
				rangeDesc = *newDesc
			}
			if success {
				if fn := s.cfg.TestingKnobs.OnRelocatedOne; fn != nil {
					fn(ops, leaseTarget)
				}

				break
			}
		}
	}
}

type relocationArgs struct {
	votersToAdd, votersToRemove             []roachpb.ReplicationTarget
	nonVotersToAdd, nonVotersToRemove       []roachpb.ReplicationTarget
	finalVoterTargets, finalNonVoterTargets []roachpb.ReplicationTarget
	targetType                              targetReplicaType
}

func (r *relocationArgs) targetsToAdd() []roachpb.ReplicationTarget {
	switch r.targetType {
	case voterTarget:
		return r.votersToAdd
	case nonVoterTarget:
		return r.nonVotersToAdd
	default:
		panic(fmt.Sprintf("unknown targetReplicaType: %s", r.targetType))
	}
}

func (r *relocationArgs) targetsToRemove() []roachpb.ReplicationTarget {
	switch r.targetType {
	case voterTarget:
		return r.votersToRemove
	case nonVoterTarget:
		return r.nonVotersToRemove
	default:
		panic(fmt.Sprintf("unknown targetReplicaType: %s", r.targetType))
	}
}

func (r *relocationArgs) finalRelocationTargets() []roachpb.ReplicationTarget {
	switch r.targetType {
	case voterTarget:
		return r.finalVoterTargets
	case nonVoterTarget:
		return r.finalNonVoterTargets
	default:
		panic(fmt.Sprintf("unknown targetReplicaType: %s", r.targetType))
	}
}

func (s *Store) relocateOne(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
) ([]roachpb.ReplicationChange, *roachpb.ReplicationTarget, error) {
	if repls := desc.Replicas(); len(repls.VoterFullAndNonVoterDescriptors()) != len(repls.Descriptors()) {
		// The caller removed all the learners and left the joint config, so there
		// shouldn't be anything but voters and non_voters.
		return nil, nil, errors.AssertionFailedf(
			`range %s was either in a joint configuration or had learner replicas: %v`, desc, desc.Replicas())
	}

	sysCfg := s.cfg.Gossip.GetSystemConfig()
	if sysCfg == nil {
		return nil, nil, fmt.Errorf("no system config available, unable to perform RelocateRange")
	}
	zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		return nil, nil, err
	}

	storeList, _, _ := s.allocator.storePool.getStoreList(storeFilterNone)
	storeMap := storeListToMap(storeList)

	// Compute which replica to add and/or remove, respectively. We then ask the
	// allocator about this because we want to respect the constraints. For
	// example, it would be unfortunate if we put two replicas into the same zone
	// despite having a locality- preserving option available.
	args := getRelocationArgs(desc, voterTargets, nonVoterTargets)
	existingVoters := desc.Replicas().VoterDescriptors()
	existingNonVoters := desc.Replicas().NonVoterDescriptors()
	existingReplicas := desc.Replicas().Descriptors()

	var additionTarget, removalTarget roachpb.ReplicationTarget
	var shouldAdd, shouldRemove, canPromoteNonVoter, canDemoteVoter bool
	if len(args.targetsToAdd()) > 0 {
		// Each iteration, pick the most desirable replica to add. However,
		// prefer the first target because it's the one that should hold the
		// lease in the end; it helps to add it early so that the lease doesn't
		// have to move too much.
		candidateTargets := args.targetsToAdd()
		if args.targetType == voterTarget &&
			storeHasReplica(args.finalRelocationTargets()[0].StoreID, candidateTargets) {
			candidateTargets = []roachpb.ReplicationTarget{args.finalRelocationTargets()[0]}
		}

		// The storeList's list of stores is used to constrain which stores the
		// allocator considers putting a new replica on. We want it to only
		// consider the stores in candidateTargets.
		candidateDescs := make([]roachpb.StoreDescriptor, 0, len(candidateTargets))
		for _, candidate := range candidateTargets {
			store, ok := storeMap[candidate.StoreID]
			if !ok {
				return nil, nil, fmt.Errorf("cannot up-replicate to s%d; missing gossiped StoreDescriptor",
					candidate.StoreID)
			}
			candidateDescs = append(candidateDescs, *store)
		}
		candidateStoreList := makeStoreList(candidateDescs)

		targetStore, _ := s.allocator.allocateTargetFromList(
			ctx,
			candidateStoreList,
			zone,
			existingVoters,
			existingNonVoters,
			s.allocator.scorerOptions(),
			// NB: Allow the allocator to return target stores that might be on the
			// same node as an existing replica. This is to ensure that relocations
			// that require "lateral" movement of replicas within a node can succeed.
			true, /* allowMultipleReplsPerNode */
			args.targetType,
		)
		if targetStore == nil {
			return nil, nil, fmt.Errorf(
				"none of the remaining %ss %v are legal additions to %v",
				args.targetType, args.targetsToAdd(), desc.Replicas(),
			)
		}

		additionTarget = roachpb.ReplicationTarget{
			NodeID:  targetStore.Node.NodeID,
			StoreID: targetStore.StoreID,
		}

		// Pretend the new replica is already there so that the removal logic below
		// will take it into account when deciding which replica to remove.
		if args.targetType == voterTarget {
			existingVoters = append(
				existingVoters, roachpb.ReplicaDescriptor{
					NodeID:    additionTarget.NodeID,
					StoreID:   additionTarget.StoreID,
					ReplicaID: desc.NextReplicaID,
					Type:      roachpb.ReplicaTypeVoterFull(),
				},
			)
			// When we're relocating voting replicas, `additionTarget` is allowed to
			// be holding a non-voter. If that is the case, we want to promote that
			// non-voter instead of removing it and then adding a new voter.
			for i, nonVoter := range existingNonVoters {
				if nonVoter.StoreID == additionTarget.StoreID {
					canPromoteNonVoter = true

					// If can perform a promotion then we want that non-voter to be gone
					// from `existingNonVoters`.
					existingNonVoters[i] = existingNonVoters[len(existingNonVoters)-1]
					existingNonVoters = existingNonVoters[:len(existingNonVoters)-1]
					break
				}
			}
		} else {
			existingNonVoters = append(
				existingNonVoters, roachpb.ReplicaDescriptor{
					NodeID:    additionTarget.NodeID,
					StoreID:   additionTarget.StoreID,
					ReplicaID: desc.NextReplicaID,
					Type:      roachpb.ReplicaTypeNonVoter(),
				},
			)
		}
		shouldAdd = true
	}

	var transferTarget *roachpb.ReplicationTarget
	if len(args.targetsToRemove()) > 0 {
		// Pick a replica to remove. Note that existingVoters/existingNonVoters may
		// already reflect a replica we're adding in the current round. This is the
		// right thing to do. For example, consider relocating from (s1,s2,s3) to
		// (s1,s2,s4) where targetsToAdd will be (s4) and targetsToRemove is (s3).
		// In this code, we'll want the allocator to see if s3 can be removed from
		// (s1,s2,s3,s4) which is a reasonable request; that replica set is
		// overreplicated. If we asked it instead to remove s3 from (s1,s2,s3) it
		// may not want to do that due to constraints.
		targetStore, _, err := s.allocator.removeTarget(
			ctx, zone, args.targetsToRemove(), existingVoters,
			existingNonVoters, args.targetType,
		)
		if err != nil {
			return nil, nil, errors.Wrapf(
				err, "unable to select removal target from %v; current replicas %v",
				args.targetsToRemove(), existingReplicas,
			)
		}
		removalTarget = roachpb.ReplicationTarget{
			NodeID:  targetStore.NodeID,
			StoreID: targetStore.StoreID,
		}
		// We can't remove the leaseholder, which really throws a wrench into
		// atomic replication changes. If we find that we're trying to do just
		// that, we need to first move the lease elsewhere. This is not possible
		// if there is no other replica available at that point, i.e. if the
		// existing descriptor is a single replica that's being replaced.
		var b kv.Batch
		liReq := &roachpb.LeaseInfoRequest{}
		liReq.Key = desc.StartKey.AsRawKey()
		b.AddRawRequest(liReq)
		if err := s.DB().Run(ctx, &b); err != nil {
			return nil, nil, errors.Wrap(err, "looking up lease")
		}
		curLeaseholder := b.RawResponse().Responses[0].GetLeaseInfo().Lease.Replica
		shouldRemove = curLeaseholder.StoreID != removalTarget.StoreID
		if args.targetType == voterTarget {
			// If the voter being removed is about to be added as a non-voter, then we
			// can just demote it.
			for _, target := range args.nonVotersToAdd {
				if target.StoreID == removalTarget.StoreID {
					canDemoteVoter = true
				}
			}
			if !shouldRemove {
				// Pick a voting replica that we can give the lease to. We sort the first
				// target to the beginning (if it's there) because that's where the lease
				// needs to be in the end. We also exclude the last voter if it was
				// added by the add branch above (in which case it doesn't exist yet).
				added := 0
				if shouldAdd {
					added++
				}
				sortedTargetReplicas := append(
					[]roachpb.ReplicaDescriptor(nil),
					existingVoters[:len(existingVoters)-added]...,
				)
				sort.Slice(
					sortedTargetReplicas, func(i, j int) bool {
						sl := sortedTargetReplicas
						// finalRelocationTargets[0] goes to the front (if it's present).
						return sl[i].StoreID == args.finalRelocationTargets()[0].StoreID
					},
				)
				for _, rDesc := range sortedTargetReplicas {
					if rDesc.StoreID != curLeaseholder.StoreID {
						transferTarget = &roachpb.ReplicationTarget{
							NodeID:  rDesc.NodeID,
							StoreID: rDesc.StoreID,
						}
						shouldRemove = true
						break
					}
				}
			}
		}
	}

	var ops []roachpb.ReplicationChange
	if shouldAdd && shouldRemove {
		ops, _, err = replicationChangesForRebalance(
			ctx, desc, len(existingVoters), additionTarget, removalTarget, args.targetType,
		)
		if err != nil {
			return nil, nil, err
		}
	} else if shouldAdd {
		if canPromoteNonVoter {
			ops = roachpb.ReplicationChangesForPromotion(additionTarget)
		} else {
			ops = roachpb.MakeReplicationChanges(args.targetType.AddChangeType(), additionTarget)
		}
	} else if shouldRemove {
		// Carry out the removal only if there was no lease problem above. If there
		// was, we're not going to do a swap in this round but just do the addition.
		// (Note that !shouldRemove implies that we're trying to remove the last
		// replica left in the descriptor which is illegal).
		if canDemoteVoter {
			ops = roachpb.ReplicationChangesForDemotion(removalTarget)
		} else {
			ops = roachpb.MakeReplicationChanges(args.targetType.RemoveChangeType(), removalTarget)
		}
	}

	if len(ops) == 0 {
		// Make sure that the first target is the final leaseholder, as
		// AdminRelocateRange specifies.
		transferTarget = &voterTargets[0]
	}
	return ops, transferTarget, nil
}

func getRelocationArgs(
	desc *roachpb.RangeDescriptor, voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
) relocationArgs {
	args := relocationArgs{
		votersToAdd: subtractTargets(
			voterTargets,
			desc.Replicas().Voters().ReplicationTargets(),
		),
		votersToRemove: subtractTargets(
			desc.Replicas().Voters().ReplicationTargets(),
			voterTargets,
		),
		nonVotersToAdd: subtractTargets(
			nonVoterTargets,
			desc.Replicas().NonVoters().ReplicationTargets(),
		),
		nonVotersToRemove: subtractTargets(
			desc.Replicas().NonVoters().ReplicationTargets(),
			nonVoterTargets,
		),
		finalVoterTargets:    voterTargets,
		finalNonVoterTargets: nonVoterTargets,
		targetType:           voterTarget,
	}

	// If there are no voters to relocate, we relocate the non-voters.
	//
	// NB: This means that non-voters are handled after all voters have been
	// relocated since relocateOne is expected to be called repeatedly until
	// there are no more replicas to relocate.
	if len(args.votersToAdd) == 0 && len(args.votersToRemove) == 0 {
		args.targetType = nonVoterTarget
	}
	return args
}

func containsDuplicates(targets []roachpb.ReplicationTarget) bool {
	for i := range targets {
		for j := i + 1; j < len(targets); j++ {
			if targets[i] == targets[j] {
				return true
			}
		}
	}
	return false
}

// subtractTargets returns the set of replica descriptors in `left` but not in
// `right` (i.e. left - right).
//
// TODO(aayush): Make this and the `intersectTargets()` method below
// (along with other utility methods operating on `ReplicationTarget`) operate
// over an interface that both `ReplicaDescriptor` and `ReplicationTarget`
// satisfy.
func subtractTargets(left, right []roachpb.ReplicationTarget) (diff []roachpb.ReplicationTarget) {
	for _, t := range left {
		found := false
		for _, replicaDesc := range right {
			if replicaDesc.StoreID == t.StoreID && replicaDesc.NodeID == t.NodeID {
				found = true
				break
			}
		}
		if !found {
			diff = append(diff, t)
		}
	}
	return diff
}

// intersectTargets returns the set of replica descriptors in `left` and
// `right`.
func intersectTargets(
	left, right []roachpb.ReplicationTarget,
) (intersection []roachpb.ReplicationTarget) {
	isInLeft := func(id roachpb.StoreID) bool {
		for _, r := range left {
			if r.StoreID == id {
				return true
			}
		}
		return false
	}
	for i := range right {
		if isInLeft(right[i].StoreID) {
			intersection = append(intersection, right[i])
		}
	}
	return intersection
}

// adminScatter moves replicas and leaseholders for a selection of ranges.
func (r *Replica) adminScatter(
	ctx context.Context, args roachpb.AdminScatterRequest,
) (roachpb.AdminScatterResponse, error) {
	rq := r.store.replicateQueue
	retryOpts := retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2,
		MaxRetries:     5,
	}

	// Loop until the replicate queue decides there is nothing left to do for the
	// range. Note that we disable lease transfers until the final step as
	// transferring the lease prevents any further action on this node.
	var allowLeaseTransfer bool
	canTransferLease := func(ctx context.Context, repl *Replica) bool { return allowLeaseTransfer }
	for re := retry.StartWithCtx(ctx, retryOpts); re.Next(); {
		requeue, err := rq.processOneChange(ctx, r, canTransferLease, false /* dryRun */)
		if err != nil {
			// TODO(tbg): can this use IsRetriableReplicationError?
			if isSnapshotError(err) {
				continue
			}
			break
		}
		if !requeue {
			if allowLeaseTransfer {
				break
			}
			allowLeaseTransfer = true
		}
		re.Reset()
	}

	// If we've been asked to randomize the leases beyond what the replicate
	// queue would do on its own (#17341), do so after the replicate queue is
	// done by transferring the lease to any of the given N replicas with
	// probability 1/N of choosing each.
	if args.RandomizeLeases && r.OwnsValidLease(ctx, r.store.Clock().NowAsClockTimestamp()) {
		desc := r.Desc()
		// Learner replicas aren't allowed to become the leaseholder or raft leader,
		// so only consider the `VoterDescriptors` replicas.
		voterReplicas := desc.Replicas().VoterDescriptors()
		newLeaseholderIdx := rand.Intn(len(voterReplicas))
		targetStoreID := voterReplicas[newLeaseholderIdx].StoreID
		if targetStoreID != r.store.StoreID() {
			if err := r.AdminTransferLease(ctx, targetStoreID); err != nil {
				log.Warningf(ctx, "failed to scatter lease to s%d: %+v", targetStoreID, err)
			}
		}
	}

	ri := r.GetRangeInfo(ctx)
	return roachpb.AdminScatterResponse{
		RangeInfos: []roachpb.RangeInfo{ri},
	}, nil
}

func (r *Replica) adminVerifyProtectedTimestamp(
	ctx context.Context, args roachpb.AdminVerifyProtectedTimestampRequest,
) (resp roachpb.AdminVerifyProtectedTimestampResponse, err error) {
	var doesNotApplyReason string
	resp.Verified, doesNotApplyReason, err = r.protectedTimestampRecordApplies(ctx, &args)
	if err != nil {
		return resp, err
	}

	// In certain cases we do not want to return an error even if we failed to
	// verify the protected ts record. This ensures that executeAdminBatch adds
	// the response to the batch, thereby allowing us to aggregate the
	// verification failures across all AdminVerifyProtectedTimestampRequests and
	// construct a more informative error to show to the user.
	if doesNotApplyReason != "" {
		if !resp.Verified {
			desc := r.Desc()
			failedRange := roachpb.AdminVerifyProtectedTimestampResponse_FailedRange{
				RangeID:  int64(desc.GetRangeID()),
				StartKey: desc.GetStartKey(),
				EndKey:   desc.EndKey,
				Reason:   doesNotApplyReason,
			}
			resp.VerificationFailedRanges = append(resp.VerificationFailedRanges, failedRange)
			// TODO(adityamaru): This is here for compatibility with 20.2, remove in
			// 21.2.
			resp.DeprecatedFailedRanges = append(resp.DeprecatedFailedRanges, *r.Desc())
		}
	}
	return resp, nil
}
