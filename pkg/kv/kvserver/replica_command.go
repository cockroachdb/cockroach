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
	"strings"
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
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/raft/tracker"
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
		} else if err := detail.ActualValue.GetProto(&actualDesc); err == nil &&
			desc.RangeID == actualDesc.RangeID && !desc.Equal(actualDesc) {
			return true, &actualDesc
		}
	}
	return false, nil
}

const (
	descChangedRangeSubsumedErrorFmt = "descriptor changed: expected %s != [actual] nil (range subsumed)"
	descChangedErrorFmt              = "descriptor changed: [expected] %s != [actual] %s"
)

func newDescChangedError(desc, actualDesc *roachpb.RangeDescriptor) error {
	if actualDesc == nil {
		return errors.Newf(descChangedRangeSubsumedErrorFmt, desc)
	}
	return errors.Newf(descChangedErrorFmt, desc, actualDesc)
}

func wrapDescChangedError(err error, desc, actualDesc *roachpb.RangeDescriptor) error {
	if actualDesc == nil {
		return errors.Wrapf(err, descChangedRangeSubsumedErrorFmt, desc)
	}
	return errors.Wrapf(err, descChangedErrorFmt, desc, actualDesc)
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
	if (expiration != hlc.Timestamp{}) {
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

	_, dbDescValue, err := conditionalGetDescValueFromDB(ctx, txn, oldDesc.StartKey, checkDescsEqual(oldDesc))
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
		if err := updateRangeDescriptor(b, leftDescKey, dbDescValue, leftDesc); err != nil {
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
	if err := updateRangeDescriptor(b, rightDescKey, nil, rightDesc); err != nil {
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
	_, dbDescValue, err := conditionalGetDescValueFromDB(ctx, txn, desc.StartKey, checkDescsEqual(desc))
	if err != nil {
		return err
	}
	newDesc := *desc
	setStickyBit(&newDesc, expiration)

	b := txn.NewBatch()
	descKey := keys.RangeDescriptorKey(desc.StartKey)
	if err := updateRangeDescriptor(b, descKey, dbDescValue, &newDesc); err != nil {
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
	if (desc.GetStickyBit() == hlc.Timestamp{}) {
		return reply, nil
	}

	if err := r.store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, dbDescValue, err := conditionalGetDescValueFromDB(ctx, txn, desc.StartKey, checkDescsEqual(desc))
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
		if err := updateRangeDescriptor(b, descKey, dbDescValue, &newDesc); err != nil {
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
	lastErr := ctx.Err()
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

		// Observe the commit timestamp to force a client-side retry. See the
		// comment on the retry loop after this closure for details.
		//
		// TODO(benesch): expose a proper API for preventing the fast path.
		_ = txn.CommitTimestamp()

		// Pipelining might send QueryIntent requests to the RHS after the RHS has
		// noticed the merge and started blocking all traffic. This causes the merge
		// transaction to deadlock. Just turn pipelining off; the structure of the
		// merge transaction means pipelining provides no performance benefit
		// anyway.
		if err := txn.DisablePipelining(); err != nil {
			return err
		}

		// NB: reads do NOT impact transaction record placement.

		origLeftDesc := r.Desc()
		if origLeftDesc.EndKey.Equal(roachpb.RKeyMax) {
			// Merging the final range doesn't make sense.
			return errors.New("cannot merge final range")
		}

		_, dbOrigLeftDescValue, err := conditionalGetDescValueFromDB(ctx, txn, origLeftDesc.StartKey, checkDescsEqual(origLeftDesc))
		if err != nil {
			return err
		}

		// Ensure that every current replica of the LHS has been initialized.
		// Otherwise there is a rare race where the replica GC queue can GC a
		// replica of the RHS too early. The comment on
		// TestStoreRangeMergeUninitializedLHSFollower explains the situation in full.
		if err := waitForReplicasInit(
			ctx, r.store.cfg.NodeDialer, origLeftDesc.RangeID, origLeftDesc.Replicas().All(),
		); err != nil {
			return errors.Wrap(err, "waiting for all left-hand replicas to initialize")
		}

		// Do a consistent read of the right hand side's range descriptor.
		var rightDesc roachpb.RangeDescriptor
		rightDescKey := keys.RangeDescriptorKey(origLeftDesc.EndKey)
		dbRightDescKV, err := txn.Get(ctx, rightDescKey)
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

		predFullVoter := func(rDesc roachpb.ReplicaDescriptor) bool {
			return rDesc.GetType() == roachpb.VOTER_FULL
		}
		if len(lReplicas.Filter(predFullVoter)) != len(lReplicas.All()) {
			return errors.Errorf("cannot merge range with non-voter replicas on lhs: %s", lReplicas)
		}
		if len(rReplicas.Filter(predFullVoter)) != len(rReplicas.All()) {
			return errors.Errorf("cannot merge range with non-voter replicas on rhs: %s", rReplicas)
		}
		if !replicaSetsEqual(lReplicas.All(), rReplicas.All()) {
			return errors.Errorf("ranges not collocated; %s != %s", lReplicas, rReplicas)
		}
		mergeReplicas := lReplicas.All()

		updatedLeftDesc := *origLeftDesc
		// lhs.Generation = max(rhs.Generation, lhs.Generation)+1.
		// See the comment on the Generation field for why generation are useful.
		if updatedLeftDesc.Generation < rightDesc.Generation {
			updatedLeftDesc.Generation = rightDesc.Generation
		}
		updatedLeftDesc.IncrementGeneration()
		updatedLeftDesc.EndKey = rightDesc.EndKey
		log.Infof(ctx, "initiating a merge of %s into this range (%s)", &rightDesc, reason)

		// Update the range descriptor for the receiving range. It is important
		// (for transaction record placement) that the first write inside the
		// transaction is this conditional put to change the left hand side's
		// descriptor end key.
		{
			b := txn.NewBatch()
			leftDescKey := keys.RangeDescriptorKey(updatedLeftDesc.StartKey)
			if err := updateRangeDescriptor(
				b, leftDescKey, dbOrigLeftDescValue, &updatedLeftDesc,
			); err != nil {
				return err
			}
			// Commit this batch on its own to ensure that the transaction record
			// is created in the right place (our triggers rely on this).
			log.Event(ctx, "updating LHS descriptor")
			if err := txn.Run(ctx, b); err != nil {
				return err
			}
		}

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

		// Remove the range descriptor for the deleted range.
		if err := updateRangeDescriptor(b, rightDescKey,
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
					LeftDesc:       updatedLeftDesc,
					RightDesc:      rightDesc,
					RightMVCCStats: rhsSnapshotRes.MVCCStats,
					FreezeStart:    rhsSnapshotRes.FreezeStart,
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
	// Note that client.DB.Txn performs retries using the same transaction, so we
	// have to use our own retry loop.
	for {
		txn := kv.NewTxn(ctx, r.store.DB(), r.NodeID())
		err := runMergeTxn(txn)
		if err != nil {
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

type snapshotError struct {
	// NB: don't implement Cause() on this type without also updating IsSnapshotError.
	cause error
}

func (s *snapshotError) Error() string {
	return fmt.Sprintf("snapshot failed: %s", s.cause.Error())
}

// IsSnapshotError returns true iff the error indicates a snapshot failed.
func IsSnapshotError(err error) bool {
	return errors.HasType(err, (*snapshotError)(nil))
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
//    If no replicas are being added, this first step is elided.
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
//    corresponding replicas of type VOTER_INCOMING and VOTER_OUTGOING.
//    Immediately after committing this change, a second transition updates the
//    descriptor with and activates the final configuration.
//
// Concretely, if the initial members of the range are s1/1, s2/2, and s3/3, and
// an atomic membership change were to adds s4/4 and s5/5 while removing s1/1 and
// s2/2, the following range descriptors would form the overall transition:
//
// 1. s1/1 s2/2 s3/3 (VOTER_FULL is implied)
// 2. s1/1 s2/2 s3/3 s4/4LEARNER
// 3. s1/1 s2/2 s3/3 s4/4LEARNER s5/5LEARNER
// 4. s1/1VOTER_DEMOTING s2/2VOTER_DEMOTING s3/3 s4/4VOTER_INCOMING s5/5VOTER_INCOMING
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

	// We execute the change serially if we're not allowed to run atomic
	// replication changes or if that was explicitly disabled.
	st := r.ClusterSettings()
	unroll := !st.Version.IsActive(ctx, clusterversion.VersionAtomicChangeReplicas) ||
		!UseAtomicReplicationChanges.Get(&st.SV)

	if unroll {
		// Legacy behavior.
		for i := range chgs {
			var err error
			desc, err = r.changeReplicasImpl(ctx, desc, priority, reason, details, chgs[i:i+1])
			if err != nil {
				return nil, err
			}
		}
		return desc, nil
	}
	// Atomic replication change.
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
		return nil, err
	}

	if adds := chgs.Additions(); len(adds) > 0 {
		// Lock learner snapshots even before we run the ConfChange txn to add them
		// to prevent a race with the raft snapshot queue trying to send it first.
		// Note that this lock needs to cover sending the snapshots which happens in
		_ = r.atomicReplicationChange
		// which also has some more details on what's going on here.
		//
		// Also note that the lock only prevents the raft snapshot queue from
		// sending snapshots to learner replicas, it will still send them to voters.
		// There are more details about this locking in
		_ = (*raftSnapshotQueue)(nil).processRaftSnapshot
		// as well as a TODO about fixing all this to be less subtle and brittle.
		releaseSnapshotLockFn := r.lockLearnerSnapshot(ctx, adds)
		defer releaseSnapshotLockFn()

		// For all newly added nodes, first add raft learner replicas. They accept raft traffic
		// (so they can catch up) but don't get to vote (so they don't affect quorum and thus
		// don't introduce fragility into the system). For details see:
		_ = roachpb.ReplicaDescriptors.Learners
		var err error
		desc, err = addLearnerReplicas(ctx, r.store, desc, reason, details, adds)
		if err != nil {
			return nil, err
		}
	}

	// Catch up any learners, then run the atomic replication change that adds the
	// final voters and removes any undesirable replicas.
	desc, err = r.atomicReplicationChange(ctx, desc, priority, reason, details, chgs)
	if err != nil {
		// If the error occurred while transitioning out of an atomic replication change,
		// try again here with a fresh descriptor; this is a noop otherwise.
		if _, err := maybeLeaveAtomicChangeReplicas(ctx, r.store, r.Desc()); err != nil {
			return nil, err
		}
		if fn := r.store.cfg.TestingKnobs.ReplicaAddSkipLearnerRollback; fn != nil && fn() {
			return nil, err
		}
		// Don't leave a learner replica lying around if we didn't succeed in
		// promoting it to a voter.
		if targets := chgs.Additions(); len(targets) > 0 {
			log.Infof(ctx, "could not promote %v to voter, rolling back: %v", targets, err)
			for _, target := range targets {
				r.tryRollBackLearnerReplica(ctx, r.Desc(), target, reason, details)
			}
		}
		return nil, err
	}
	return desc, err
}

// maybeLeaveAtomicChangeReplicas transitions out of the joint configuration if
// the descriptor indicates one. This involves running a distributed transaction
// updating said descriptor, the result of which will be returned. The
// descriptor returned from this method will contain replicas of type LEARNER
// and VOTER_FULL only.
func maybeLeaveAtomicChangeReplicas(
	ctx context.Context, store *Store, desc *roachpb.RangeDescriptor,
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
		ctx, store, desc, kvserverpb.ReasonUnknown /* unused */, "", nil, /* iChgs */
	)
}

// maybeLeaveAtomicChangeReplicasAndRemoveLearners transitions out of the joint
// config (if there is one), and then removes all learners. After this function
// returns, all remaining replicas will be of type VOTER_FULL.
func maybeLeaveAtomicChangeReplicasAndRemoveLearners(
	ctx context.Context, store *Store, desc *roachpb.RangeDescriptor,
) (*roachpb.RangeDescriptor, error) {
	desc, err := maybeLeaveAtomicChangeReplicas(ctx, store, desc)
	if err != nil {
		return nil, err
	}
	// Now the config isn't joint any more, but we may have demoted some voters
	// into learners. These learners should go as well.

	learners := desc.Replicas().Learners()
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
			ctx, store, desc, kvserverpb.ReasonAbandonedLearner, "",
			[]internalReplicationChange{{target: target, typ: internalChangeTypeRemove}},
		)
		if err != nil {
			return nil, errors.Wrapf(err, `removing learners from %s`, origDesc)
		}
	}
	return desc, nil
}

func validateReplicationChanges(
	desc *roachpb.RangeDescriptor, chgs roachpb.ReplicationChanges,
) error {
	// First make sure that the changes don't self-overlap (i.e. we're not adding
	// a replica twice, or removing and immediately re-adding it).
	byNodeID := make(map[roachpb.NodeID]roachpb.ReplicationChange, len(chgs))
	for _, chg := range chgs {
		if _, ok := byNodeID[chg.Target.NodeID]; ok {
			return fmt.Errorf("changes %+v refer to n%d twice", chgs, chg.Target.NodeID)
		}
		byNodeID[chg.Target.NodeID] = chg
	}

	// Then, check that we're not adding a second replica on nodes that already
	// have one, or "re-add" an existing replica. We delete from byNodeID so that
	// after this loop, it contains only StoreIDs that we haven't seen in desc.
	for _, rDesc := range desc.Replicas().All() {
		chg, ok := byNodeID[rDesc.NodeID]
		delete(byNodeID, rDesc.NodeID)
		if !ok || chg.ChangeType != roachpb.ADD_REPLICA {
			continue
		}
		// We're adding a replica that's already there. This isn't allowed, even
		// when the newly added one would be on a different store.
		if rDesc.StoreID != chg.Target.StoreID {
			return errors.Errorf("unable to add replica %v; node already has a replica in %s", chg.Target.StoreID, desc)
		}

		// Looks like we found a replica with the same store and node id. If the
		// replica is already a learner, then either some previous leaseholder was
		// trying to add it with the learner+snapshot+voter cycle and got
		// interrupted or else we hit a race between the replicate queue and
		// AdminChangeReplicas.
		if rDesc.GetType() == roachpb.LEARNER {
			return errors.Errorf(
				"unable to add replica %v which is already present as a learner in %s", chg.Target, desc)
		}

		// Otherwise, we already had a full voter replica. Can't add another to
		// this store.
		return errors.Errorf("unable to add replica %v which is already present in %s", chg.Target, desc)
	}

	// Any removals left in the map now refer to nonexisting replicas, and we refuse them.
	for _, chg := range byNodeID {
		if chg.ChangeType != roachpb.REMOVE_REPLICA {
			continue
		}
		return errors.Errorf("removing %v which is not in %s", chg.Target, desc)
	}
	return nil
}

// addLearnerReplicas adds learners to the given replication targets.
func addLearnerReplicas(
	ctx context.Context,
	store *Store,
	desc *roachpb.RangeDescriptor,
	reason kvserverpb.RangeLogEventReason,
	details string,
	targets []roachpb.ReplicationTarget,
) (*roachpb.RangeDescriptor, error) {
	// TODO(tbg): we could add all learners in one go, but then we'd need to
	// do it as an atomic replication change (raft doesn't know which config
	// to apply the delta to, so we might be demoting more than one voter).
	// This isn't crazy, we just need to transition out of the joint config
	// before returning from this method, and it's unclear that it's worth
	// doing.
	for _, target := range targets {
		iChgs := []internalReplicationChange{{target: target, typ: internalChangeTypeAddLearner}}
		var err error
		desc, err = execChangeReplicasTxn(
			ctx, store, desc, reason, details, iChgs,
		)
		if err != nil {
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

// atomicReplicationChange carries out the atomic membership change that
// finalizes the addition and/or removal of replicas. Any voters in the process
// of being added (as reflected by the replication changes) must have been added
// as learners already and will be caught up before being promoted to voters.
// Cluster version permitting, voter removals (from the replication changes)
// will preferably be carried out by first demoting to a learner instead of
// outright removal (this avoids a [raft-bug] that can lead to unavailability).
// All of this occurs in one atomic raft membership change which is carried out
// across two phases. On error, it is possible that the range is in the
// intermediate ("joint") configuration in which a quorum of both the old and
// new sets of voters is required. If a range is encountered in this state,
// maybeLeaveAtomicReplicationChange can fix this, but it is the caller's job to
// do this when necessary.
//
// The atomic membership change is carried out chiefly via the construction of a
// suitable ChangeReplicasTrigger, see prepareChangeReplicasTrigger for details.
//
// Contrary to the name, *all* membership changes go through this method, even
// those that add/remove only a single voter, though the simple protocol is used
// when this is opportune. Notably, demotions can never use the simple protocol,
// even if only a single voter is being demoted, due to a (liftable) limitation
// in etcd/raft.
//
// [raft-bug]: https://github.com/etcd-io/etcd/issues/11284
func (r *Replica) atomicReplicationChange(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	priority SnapshotRequest_Priority,
	reason kvserverpb.RangeLogEventReason,
	details string,
	chgs roachpb.ReplicationChanges,
) (*roachpb.RangeDescriptor, error) {
	// TODO(dan): We allow ranges with learner replicas to split, so in theory
	// this may want to detect that and retry, sending a snapshot and promoting
	// both sides.

	iChgs := make([]internalReplicationChange, 0, len(chgs))

	for _, target := range chgs.Additions() {
		iChgs = append(iChgs, internalReplicationChange{target: target, typ: internalChangeTypePromoteLearner})
		// All adds must be present as learners right now, and we send them
		// snapshots in anticipation of promoting them to voters.
		rDesc, ok := desc.GetReplicaDescriptor(target.StoreID)
		if !ok {
			return nil, errors.Errorf("programming error: replica %v not found in %v", target, desc)
		}

		if rDesc.GetType() != roachpb.LEARNER {
			return nil, errors.Errorf("programming error: cannot promote replica of type %s", rDesc.Type)
		}

		if fn := r.store.cfg.TestingKnobs.ReplicaSkipLearnerSnapshot; fn != nil && fn() {
			continue
		}

		// Note that raft snapshot queue will refuse to send a snapshot to a learner
		// replica if its store is already sending a snapshot to that replica. That
		// would race with this snapshot, except that we've put a (best effort) lock
		// on it before the conf change txn was run. This is best effort because the
		// lock can time out and the lock is local to this node, while the raft
		// leader could be on another node entirely (they're usually co-located but
		// this is not guaranteed).
		//
		// We originally tried always refusing to send snapshots from the raft
		// snapshot queue to learner replicas, but this turned out to be brittle.
		// First, if the snapshot failed, any attempt to use the learner's raft
		// group would hang until the replicate queue got around to cleaning up the
		// orphaned learner. Second, this tickled some bugs in etcd/raft around
		// switching between StateSnapshot and StateProbe. Even if we worked through
		// these, it would be susceptible to future similar issues.
		if err := r.sendSnapshot(ctx, rDesc, SnapshotRequest_LEARNER, priority); err != nil {
			return nil, err
		}
	}

	if adds := chgs.Additions(); len(adds) > 0 {
		if fn := r.store.cfg.TestingKnobs.ReplicaAddStopAfterLearnerSnapshot; fn != nil && fn(adds) {
			return desc, nil
		}
	}

	canUseDemotion := r.store.ClusterSettings().Version.IsActive(ctx, clusterversion.VersionChangeReplicasDemotion)
	for _, target := range chgs.Removals() {
		typ := internalChangeTypeRemove
		if rDesc, ok := desc.GetReplicaDescriptor(target.StoreID); ok && rDesc.GetType() == roachpb.VOTER_FULL && canUseDemotion {
			typ = internalChangeTypeDemote
		}
		iChgs = append(iChgs, internalReplicationChange{target: target, typ: typ})
	}

	var err error
	desc, err = execChangeReplicasTxn(ctx, r.store, desc, reason, details, iChgs)
	if err != nil {
		return nil, err
	}

	if fn := r.store.cfg.TestingKnobs.ReplicaAddStopAfterJointConfig; fn != nil && fn() {
		return desc, nil
	}

	// Leave the joint config if we entered one. Also, remove any learners we
	// might have picked up due to removal-via-demotion.
	return maybeLeaveAtomicChangeReplicasAndRemoveLearners(ctx, r.store, desc)
}

// tryRollbackLearnerReplica attempts to remove a learner specified by the
// target. If no such learner is found in the descriptor (including when it is a
// voter instead), no action is taken. Otherwise, a single time-limited
// best-effort attempt at removing the learner is made.
func (r *Replica) tryRollBackLearnerReplica(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	target roachpb.ReplicationTarget,
	reason kvserverpb.RangeLogEventReason,
	details string,
) {
	repDesc, ok := desc.GetReplicaDescriptor(target.StoreID)
	if !ok || repDesc.GetType() != roachpb.LEARNER {
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
			ctx, r.store, desc, reason, details,
			[]internalReplicationChange{{target: target, typ: internalChangeTypeRemove}},
		)
		return err
	}
	rollbackCtx := logtags.WithTags(context.Background(), logtags.FromContext(ctx))
	if err := contextutil.RunWithTimeout(
		rollbackCtx, "learner rollback", rollbackTimeout, rollbackFn,
	); err != nil {
		log.Infof(ctx,
			"failed to rollback learner %s, abandoning it for the replicate queue: %v", target, err)
		r.store.replicateQueue.MaybeAddAsync(ctx, r, r.store.Clock().Now())
	} else {
		log.Infof(ctx, "rolled back learner %s in %s", target, desc)
	}
}

type internalChangeType byte

const (
	_ internalChangeType = iota + 1
	internalChangeTypeAddLearner
	internalChangeTypePromoteLearner
	// internalChangeTypeDemote changes a voter to a learner. This will
	// necessarily go through joint consensus since it requires two individual
	// changes (only one changes the quorum, so we could allow it in a simple
	// change too, with some work here and upstream). Demotions are treated like
	// removals throughout (i.e. they show up in `ChangeReplicasTrigger.Removed()`,
	// but not in `.Added()`).
	internalChangeTypeDemote
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
	return len(c) > 1 || c[0].typ == internalChangeTypeDemote
}

type storeSettings interface {
	ClusterSettings() *cluster.Settings
	TestingKnobs() *StoreTestingKnobs
}

func prepareChangeReplicasTrigger(
	ctx context.Context,
	store storeSettings,
	desc *roachpb.RangeDescriptor,
	chgs internalReplicationChanges,
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
		if fn := store.TestingKnobs().ReplicationAlwaysUseJointConfig; fn != nil && fn() {
			useJoint = true
		}
		for _, chg := range chgs {
			switch chg.typ {
			case internalChangeTypeAddLearner:
				added = append(added,
					updatedDesc.AddReplica(chg.target.NodeID, chg.target.StoreID, roachpb.LEARNER))
			case internalChangeTypePromoteLearner:
				typ := roachpb.VOTER_FULL
				if useJoint {
					typ = roachpb.VOTER_INCOMING
				}
				rDesc, prevTyp, ok := updatedDesc.SetReplicaType(chg.target.NodeID, chg.target.StoreID, typ)
				if !ok || prevTyp != roachpb.LEARNER {
					return nil, errors.Errorf("cannot promote target %v which is missing as Learner", chg.target)
				}
				added = append(added, rDesc)
			case internalChangeTypeRemove:
				rDesc, ok := updatedDesc.GetReplicaDescriptor(chg.target.StoreID)
				if !ok {
					return nil, errors.Errorf("target %s not found", chg.target)
				}
				prevTyp := rDesc.GetType()
				if !useJoint || prevTyp == roachpb.LEARNER {
					rDesc, _ = updatedDesc.RemoveReplica(chg.target.NodeID, chg.target.StoreID)
				} else if prevTyp != roachpb.VOTER_FULL {
					// NB: prevTyp is already known to be VOTER_FULL because of
					// !InAtomicReplicationChange() and the learner handling
					// above. We check it anyway.
					return nil, errors.Errorf("cannot transition from %s to VOTER_OUTGOING", prevTyp)
				} else {
					rDesc, _, _ = updatedDesc.SetReplicaType(chg.target.NodeID, chg.target.StoreID, roachpb.VOTER_OUTGOING)
				}
				removed = append(removed, rDesc)
			case internalChangeTypeDemote:
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
					return nil, errors.Errorf("demotions require joint consensus")
				}
				if prevTyp := rDesc.GetType(); prevTyp != roachpb.VOTER_FULL {
					return nil, errors.Errorf("cannot transition from %s to VOTER_DEMOTING", prevTyp)
				}
				rDesc, _, _ = updatedDesc.SetReplicaType(chg.target.NodeID, chg.target.StoreID, roachpb.VOTER_DEMOTING)
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
		for _, rDesc := range updatedDesc.Replicas().DeepCopy().All() {
			switch rDesc.GetType() {
			case roachpb.VOTER_INCOMING:
				updatedDesc.SetReplicaType(rDesc.NodeID, rDesc.StoreID, roachpb.VOTER_FULL)
				isJoint = true
			case roachpb.VOTER_OUTGOING:
				updatedDesc.RemoveReplica(rDesc.NodeID, rDesc.StoreID)
				isJoint = true
			case roachpb.VOTER_DEMOTING:
				updatedDesc.SetReplicaType(rDesc.NodeID, rDesc.StoreID, roachpb.LEARNER)
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

	var crt *roachpb.ChangeReplicasTrigger
	if !store.ClusterSettings().Version.IsActive(
		ctx, clusterversion.VersionAtomicChangeReplicasTrigger,
	) {
		var deprecatedChangeType roachpb.ReplicaChangeType
		var deprecatedRepDesc roachpb.ReplicaDescriptor
		if len(added) > 0 {
			deprecatedChangeType = roachpb.ADD_REPLICA
			deprecatedRepDesc = added[0]
		} else {
			deprecatedChangeType = roachpb.REMOVE_REPLICA
			deprecatedRepDesc = removed[0]
		}
		crt = &roachpb.ChangeReplicasTrigger{
			// NB: populate Desc as well because locally we rely on it being
			// set.
			Desc:                      &updatedDesc,
			DeprecatedChangeType:      deprecatedChangeType,
			DeprecatedReplica:         deprecatedRepDesc,
			DeprecatedUpdatedReplicas: updatedDesc.Replicas().All(),
			DeprecatedNextReplicaID:   updatedDesc.NextReplicaID,
		}
	} else {
		crt = &roachpb.ChangeReplicasTrigger{
			Desc:                    &updatedDesc,
			InternalAddedReplicas:   added,
			InternalRemovedReplicas: removed,
		}
	}

	if _, err := crt.ConfChange(nil); err != nil {
		return nil, errors.Wrapf(err, "programming error: malformed trigger created from desc %s to %s", desc, &updatedDesc)
	}
	return crt, nil
}

func execChangeReplicasTxn(
	ctx context.Context,
	store *Store,
	referenceDesc *roachpb.RangeDescriptor,
	reason kvserverpb.RangeLogEventReason,
	details string,
	chgs internalReplicationChanges,
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

	if err := store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		log.Event(ctx, "attempting txn")
		txn.SetDebugName(replicaChangeTxnName)
		desc, dbDescValue, err := conditionalGetDescValueFromDB(ctx, txn, referenceDesc.StartKey, check)
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
		crt, err := prepareChangeReplicasTrigger(ctx, store, desc, chgs)
		if err != nil {
			return err
		}
		log.Infof(ctx, "change replicas (add %v remove %v): existing descriptor %s", crt.Added(), crt.Removed(), desc)

		{
			b := txn.NewBatch()

			// Important: the range descriptor must be the first thing touched in the transaction
			// so the transaction record is co-located with the range being modified.
			if err := updateRangeDescriptor(b, descKey, dbDescValue, crt.Desc); err != nil {
				return err
			}

			// Run transaction up to this point to create txn record early (see #9265).
			if err := txn.Run(ctx, b); err != nil {
				return err
			}
		}

		// Log replica change into range event log.
		for _, tup := range []struct {
			typ      roachpb.ReplicaChangeType
			repDescs []roachpb.ReplicaDescriptor
		}{
			{roachpb.ADD_REPLICA, crt.Added()},
			{roachpb.REMOVE_REPLICA, crt.Removed()},
		} {
			for _, repDesc := range tup.repDescs {
				if err := store.logChange(
					ctx, txn, tup.typ, repDesc, *crt.Desc, reason, details,
				); err != nil {
					return err
				}
			}
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
// three SSTs from them for direct ingestion: one for the replicated range-ID
// local keys, one for the range local keys, and one for the user keys. The
// reason it creates three separate SSTs is to prevent overlaps with the
// memtable and existing SSTs in RocksDB. Each of the SSTs also has a range
// deletion tombstone to delete the existing data in the range.
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
// replica's range-ID local keys and at most two SSTs are created for deleting
// the user keys and range local keys of all subsumed replicas. All in all, a
// maximum of 6 + SR SSTs will be created for direct ingestion where SR is the
// number of subsumed replicas. In the case where there are no subsumed
// replicas, 4 SSTs will be created.
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
		return errors.Wrapf(err, "%s: failed to generate %s snapshot", r, snapType)
	}
	defer snap.Close()
	log.Event(ctx, "generated snapshot")

	sender, err := r.GetReplicaDescriptor()
	if err != nil {
		return errors.Wrapf(err, "%s: change replicas failed", r)
	}

	status := r.RaftStatus()
	if status == nil {
		// This code path is sometimes hit during scatter for replicas that
		// haven't woken up yet.
		return &benignError{errors.New("raft status not initialized")}
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
	sent := func() {
		r.store.metrics.RangeSnapshotsGenerated.Inc(1)
	}
	if err := r.store.cfg.Transport.SendSnapshot(
		ctx,
		&r.store.cfg.RaftConfig,
		r.store.allocator.storePool,
		req,
		snap,
		r.store.Engine().NewBatch,
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
		return &snapshotError{err}
	}
	return nil
}

// replicaSetsEqual is used in AdminMerge to ensure that the ranges are
// all collocate on the same set of replicas.
func replicaSetsEqual(a, b []roachpb.ReplicaDescriptor) bool {
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
	// TODO(jeffreyxiao): This hacky fix ensures that we don't fail the
	// conditional get because of the ordering of InternalReplicas. Calling
	// Replicas() will sort the list of InternalReplicas as a side-effect. The
	// invariant of having InternalReplicas sorted is not maintained in 19.1.
	// Additionally, in 19.2, it's possible for the in-memory copy of
	// RangeDescriptor to become sorted from a call to Replicas() without
	// updating the copy in kv. These two factors makes it possible for the
	// in-memory copy to be out of sync from the copy in kv. The sorted invariant
	// of InternalReplicas is used by ReplicaDescriptors.Voters() and
	// ReplicaDescriptors.Learners().
	if desc != nil {
		desc.Replicas() // for sorting side-effect
	}
	return func(desc2 *roachpb.RangeDescriptor) bool {
		if desc2 != nil {
			desc2.Replicas() // for sorting side-effect
		}

		return desc.Equal(desc2)
	}
}

// conditionalGetDescValueFromDB fetches an encoded RangeDescriptor from kv,
// checks that it matches the given expectation using proto Equals, and returns
// the raw fetched roachpb.Value. If the fetched value doesn't match the
// expectation, a ConditionFailedError is returned.
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
	check func(*roachpb.RangeDescriptor) bool,
) (*roachpb.RangeDescriptor, []byte, error) {
	descKey := keys.RangeDescriptorKey(startKey)
	existingDescKV, err := txn.Get(ctx, descKey)
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
	b *kv.Batch, descKey roachpb.Key, oldValue []byte, newDesc *roachpb.RangeDescriptor,
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
	ctx context.Context, rangeDesc roachpb.RangeDescriptor, targets []roachpb.ReplicationTarget,
) error {
	// Step 0: Remove everything that's not a full voter so we don't have to think
	// about them.
	newDesc, err := maybeLeaveAtomicChangeReplicasAndRemoveLearners(ctx, s, &rangeDesc)
	if err != nil {
		log.Warningf(ctx, "%v", err)
		return err
	}
	rangeDesc = *newDesc

	canRetry := func(err error) bool {
		allowlist := []string{
			snapshotApplySemBusyMsg,
			IntersectingSnapshotMsg,
		}
		errStr := err.Error()
		for _, substr := range allowlist {
			if strings.Contains(errStr, substr) {
				return true
			}
		}
		return false
	}

	startKey := rangeDesc.StartKey.AsRawKey()
	transferLease := func(target roachpb.ReplicationTarget) {
		// TODO(tbg): we ignore errors here, but it seems that in practice these
		// transfers "always work". Some of them are essential (we can't remove
		// the leaseholder so we'll fail there later if this fails), so it
		// seems like a good idea to return any errors here to the caller (or
		// to retry some errors appropriately).
		if err := s.DB().AdminTransferLease(
			ctx, startKey, target.StoreID,
		); err != nil {
			log.Warningf(ctx, "while transferring lease: %+v", err)
		}
	}

	// Step 2: Repeatedly add and/or remove a replica until we reach the
	// desired state. In an "atomic replication changes" world, this is
	// conceptually easy: change from the old set of replicas to the new
	// one. But there are two reasons that complicate this:
	// 1. we can't remove the leaseholder, so if we ultimately want to do that
	//    the lease has to be moved first. If we start out with *only* the
	//    leaseholder, we will have to add a replica first.
	// 2. this code is rewritten late in the cycle and it is both safer and
	//    closer to its previous incarnation to never issue atomic changes
	//    other than simple swaps.
	//
	// The loop below repeatedly calls relocateOne, which gives us either one or
	// two ops that move the range towards the desired replication state. If
	// it's one op, then a single add or remove is carried out (and it's only
	// done when we can't swap instead). If it's two ops, then we're swapping
	// (though this code doesn't concern itself with the details); and it's
	// possible that we need to transfer the lease before we carry out the ops,
	// determined via the leaseTarget variable.
	//
	// Transient errors returned from relocateOne are retried until things work
	// out.
	every := log.Every(time.Minute)
	for {
		for re := retry.StartWithCtx(ctx, retry.Options{MaxBackoff: 5 * time.Second}); ; re.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}

			ops, leaseTarget, err := s.relocateOne(ctx, &rangeDesc, targets)
			if err != nil {
				return err
			}
			if leaseTarget != nil {
				// NB: we may need to transfer even if there are no ops, to make
				// sure the attempt is made to make the first target the final
				// leaseholder.
				transferLease(*leaseTarget)
			}
			if len(ops) == 0 {
				// Done.
				return ctx.Err()
			}
			if fn := s.cfg.TestingKnobs.BeforeRelocateOne; fn != nil {
				fn(ops, leaseTarget, err)
			}

			// Make sure we don't issue anything but singles and swaps before
			// this migration is gone (for it doesn't support anything else).
			if len(ops) > 2 {
				log.Fatalf(ctx, "received more than 2 ops: %+v", ops)
			}
			opss := [][]roachpb.ReplicationChange{ops}
			success := true
			for _, ops := range opss {
				newDesc, err := s.DB().AdminChangeReplicas(ctx, startKey, rangeDesc, ops)
				if err != nil {
					returnErr := errors.Wrapf(err, "while carrying out changes %v", ops)
					if !canRetry(err) {
						return returnErr
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
				break
			}
		}
	}

}

func (s *Store) relocateOne(
	ctx context.Context, desc *roachpb.RangeDescriptor, targets []roachpb.ReplicationTarget,
) ([]roachpb.ReplicationChange, *roachpb.ReplicationTarget, error) {
	rangeReplicas := desc.Replicas().All()
	if len(rangeReplicas) != len(desc.Replicas().Voters()) {
		// The caller removed all the learners, so there shouldn't be anything but
		// voters.
		return nil, nil, errors.AssertionFailedf(
			`range %s had non-voter replicas: %v`, desc, desc.Replicas())
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

	// Compute which replica to add and/or remove, respectively. We ask the allocator
	// about this because we want to respect the constraints. For example, it would be
	// unfortunate if we put two replicas into the same zone despite having a locality-
	// preserving option available.
	//
	// TODO(radu): we can't have multiple replicas on different stores on the
	// same node, and this code doesn't do anything to specifically avoid that
	// case (although the allocator will avoid even trying to send snapshots to
	// such stores), so it could cause some failures.

	var addTargets []roachpb.ReplicaDescriptor
	for _, t := range targets {
		found := false
		for _, replicaDesc := range rangeReplicas {
			if replicaDesc.StoreID == t.StoreID && replicaDesc.NodeID == t.NodeID {
				found = true
				break
			}
		}
		if !found {
			addTargets = append(addTargets, roachpb.ReplicaDescriptor{
				NodeID:  t.NodeID,
				StoreID: t.StoreID,
			})
		}
	}

	var removeTargets []roachpb.ReplicaDescriptor
	for _, replicaDesc := range rangeReplicas {
		found := false
		for _, t := range targets {
			if replicaDesc.StoreID == t.StoreID && replicaDesc.NodeID == t.NodeID {
				found = true
				break
			}
		}
		if !found {
			removeTargets = append(removeTargets, roachpb.ReplicaDescriptor{
				NodeID:  replicaDesc.NodeID,
				StoreID: replicaDesc.StoreID,
			})
		}
	}

	var ops roachpb.ReplicationChanges

	if len(addTargets) > 0 {
		// Each iteration, pick the most desirable replica to add. However,
		// prefer the first target because it's the one that should hold the
		// lease in the end; it helps to add it early so that the lease doesn't
		// have to move too much.
		candidateTargets := addTargets
		if storeHasReplica(targets[0].StoreID, candidateTargets) {
			candidateTargets = []roachpb.ReplicaDescriptor{
				{NodeID: targets[0].NodeID, StoreID: targets[0].StoreID},
			}
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
		storeList = makeStoreList(candidateDescs)

		targetStore, _ := s.allocator.allocateTargetFromList(
			ctx,
			storeList,
			zone,
			rangeReplicas,
			s.allocator.scorerOptions())
		if targetStore == nil {
			return nil, nil, fmt.Errorf("none of the remaining targets %v are legal additions to %v",
				addTargets, desc.Replicas())
		}

		target := roachpb.ReplicationTarget{
			NodeID:  targetStore.Node.NodeID,
			StoreID: targetStore.StoreID,
		}
		ops = append(ops, roachpb.MakeReplicationChanges(roachpb.ADD_REPLICA, target)...)
		// Pretend the voter is already there so that the removal logic below will
		// take it into account when deciding which replica to remove.
		rangeReplicas = append(rangeReplicas, roachpb.ReplicaDescriptor{
			NodeID:    target.NodeID,
			StoreID:   target.StoreID,
			ReplicaID: desc.NextReplicaID,
			Type:      roachpb.ReplicaTypeVoterFull(),
		})
	}

	var transferTarget *roachpb.ReplicationTarget
	if len(removeTargets) > 0 {
		// Pick a replica to remove. Note that rangeReplicas may already reflect
		// a replica we're adding in the current round. This is the right thing
		// to do. For example, consider relocating from (s1,s2,s3) to (s1,s2,s4)
		// where addTargets will be (s4) and removeTargets is (s3). In this code,
		// we'll want the allocator to see if s3 can be removed from
		// (s1,s2,s3,s4) which is a reasonable request; that replica set is
		// overreplicated. If we asked it instead to remove s3 from (s1,s2,s3)
		// it may not want to do that due to constraints.
		targetStore, _, err := s.allocator.RemoveTarget(ctx, zone, removeTargets, rangeReplicas)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "unable to select removal target from %v; current replicas %v",
				removeTargets, rangeReplicas)
		}
		removalTarget := roachpb.ReplicationTarget{
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
		ok := curLeaseholder.StoreID != removalTarget.StoreID
		if !ok {
			// Pick a replica that we can give the lease to. We sort the first
			// target to the beginning (if it's there) because that's where the
			// lease needs to be in the end. We also exclude the last replica if
			// it was added by the add branch above (in which case it doesn't
			// exist yet).
			sortedTargetReplicas := append([]roachpb.ReplicaDescriptor(nil), rangeReplicas[:len(rangeReplicas)-len(ops)]...)
			sort.Slice(sortedTargetReplicas, func(i, j int) bool {
				sl := sortedTargetReplicas
				// targets[0] goes to the front (if it's present).
				return sl[i].StoreID == targets[0].StoreID
			})
			for _, rDesc := range sortedTargetReplicas {
				if rDesc.StoreID != curLeaseholder.StoreID {
					transferTarget = &roachpb.ReplicationTarget{
						NodeID:  rDesc.NodeID,
						StoreID: rDesc.StoreID,
					}
					ok = true
					break
				}
			}
		}

		// Carry out the removal only if there was no lease problem above. If
		// there was, we're not going to do a swap in this round but just do the
		// addition. (Note that !ok implies that len(ops) is not empty, or we're
		// trying to remove the last replica left in the descriptor which is
		// illegal).
		if ok {
			ops = append(ops, roachpb.MakeReplicationChanges(
				roachpb.REMOVE_REPLICA,
				removalTarget)...)
		}
	}

	if len(ops) == 0 {
		// Make sure that the first target is the final leaseholder, as
		// AdminRelocateRange specifies.
		transferTarget = &targets[0]
	}

	return ops, transferTarget, nil
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
	canTransferLease := func() bool { return allowLeaseTransfer }
	for re := retry.StartWithCtx(ctx, retryOpts); re.Next(); {
		requeue, err := rq.processOneChange(ctx, r, canTransferLease, false /* dryRun */)
		if err != nil {
			if IsSnapshotError(err) {
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
	if args.RandomizeLeases && r.OwnsValidLease(r.store.Clock().Now()) {
		desc := r.Desc()
		// Learner replicas aren't allowed to become the leaseholder or raft leader,
		// so only consider the `Voters` replicas.
		voterReplicas := desc.Replicas().Voters()
		newLeaseholderIdx := rand.Intn(len(voterReplicas))
		targetStoreID := voterReplicas[newLeaseholderIdx].StoreID
		if targetStoreID != r.store.StoreID() {
			if err := r.AdminTransferLease(ctx, targetStoreID); err != nil {
				log.Warningf(ctx, "failed to scatter lease to s%d: %+v", targetStoreID, err)
			}
		}
	}

	desc := r.Desc()
	return roachpb.AdminScatterResponse{
		Ranges: []roachpb.AdminScatterResponse_Range{{
			Span: roachpb.Span{
				Key:    desc.StartKey.AsRawKey(),
				EndKey: desc.EndKey.AsRawKey(),
			},
		}},
	}, nil
}

func (r *Replica) adminVerifyProtectedTimestamp(
	ctx context.Context, args roachpb.AdminVerifyProtectedTimestampRequest,
) (resp roachpb.AdminVerifyProtectedTimestampResponse, err error) {
	resp.Verified, err = r.protectedTimestampRecordApplies(ctx, &args)
	if err == nil && !resp.Verified {
		resp.FailedRanges = append(resp.FailedRanges, *r.Desc())
	}
	return resp, err
}
