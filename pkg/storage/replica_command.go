// Copyright 2014 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/causer"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/logtags"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/raft/tracker"
)

// useLearnerReplicas specifies whether to use learner replicas for replica
// addition or whether to fall back to the previous method of a preemptive
// snapshot followed by going straight to a voter replica.
var useLearnerReplicas = settings.RegisterBoolSetting(
	"kv.learner_replicas.enabled",
	"use learner replicas for replica addition",
	false)

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

func maybeDescriptorChangedError(desc *roachpb.RangeDescriptor, err error) (string, bool) {
	if detail, ok := err.(*roachpb.ConditionFailedError); ok {
		// Provide a better message in the common case that the range being changed
		// was already changed by a concurrent transaction.
		var actualDesc roachpb.RangeDescriptor
		if !detail.ActualValue.IsPresent() {
			return fmt.Sprintf("descriptor changed: expected %s != [actual] nil (range subsumed)", desc), true
		} else if err := detail.ActualValue.GetProto(&actualDesc); err == nil &&
			desc.RangeID == actualDesc.RangeID && !desc.Equal(actualDesc) {
			return fmt.Sprintf("descriptor changed: [expected] %s != [actual] %s", desc, &actualDesc), true
		}
	}
	return "", false
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
			foundSplitKey, err = engine.MVCCFindSplitKey(
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
			if !storagebase.ContainsKey(*desc, args.Key) {
				return reply, roachpb.NewRangeKeyMismatchError(args.Key, args.Key, desc)
			}
			foundSplitKey = args.SplitKey
		}

		if !storagebase.ContainsKey(*desc, foundSplitKey) {
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
		if !engine.IsValidSplitKey(foundSplitKey) {
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
			newDesc := *desc
			newDesc.StickyBit = &args.ExpirationTime
			err := r.store.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				dbDescValue, err := conditionalGetDescValueFromDB(ctx, txn, desc)
				if err != nil {
					return err
				}

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
				b.AddRawRequest(&roachpb.EndTransactionRequest{
					Commit: true,
					InternalCommitTrigger: &roachpb.InternalCommitTrigger{
						StickyBitTrigger: &roachpb.StickyBitTrigger{
							StickyBit: args.ExpirationTime,
						},
					},
				})
				return txn.Run(ctx, b)
			})
			// The ConditionFailedError can occur because the descriptors acting as
			// expected values in the CPuts used to update the range descriptor are
			// picked outside the transaction. Return ConditionFailedError in the
			// error detail so that the command can be retried.
			if msg, ok := maybeDescriptorChangedError(desc, err); ok {
				// NB: we have to wrap the existing error here as consumers of this code
				// look at the root cause to sniff out the changed descriptor.
				err = &benignError{errors.Wrap(err, msg)}
			}
			return reply, err
		}
		return reply, nil
	}
	log.Event(ctx, "found split key")

	// Create right hand side range descriptor.
	rightDesc, err := r.store.NewRangeDescriptor(ctx, splitKey, desc.EndKey, desc.Replicas().Unwrap())
	if err != nil {
		return reply, errors.Errorf("unable to allocate right hand side range descriptor: %s", err)
	}

	// TODO(jeffreyxiao): Remove this check in 20.1.
	// Note that the client API for splitting has expiration time as
	// non-nullable, but the internal representation of a sticky bit is nullable
	// for backwards compatibility. If expiration time is the zero timestamp, we
	// must be sure not to set the sticky bit to the zero timestamp because the
	// byte representation of setting the stickyBit to nil is different than
	// setting it to hlc.Timestamp{}. This check ensures that CPuts would not
	// fail on older versions.
	if (args.ExpirationTime != hlc.Timestamp{}) {
		rightDesc.StickyBit = &args.ExpirationTime
	}

	// Init updated version of existing range descriptor.
	leftDesc := *desc
	leftDesc.IncrementGeneration()
	r.maybeMarkGenerationComparable(&leftDesc)
	leftDesc.EndKey = splitKey

	// Set the generation of the right hand side descriptor to match that of the
	// (updated) left hand side. See the comment on the field for an explanation
	// of why generations are useful.
	rightDesc.Generation = leftDesc.Generation
	r.maybeMarkGenerationComparable(rightDesc)

	var extra string
	if delayable {
		extra += maybeDelaySplitToAvoidSnapshot(ctx, (*splitDelayHelper)(r))
	}
	extra += splitSnapshotWarningStr(r.RangeID, r.RaftStatus())

	log.Infof(ctx, "initiating a split of this range at key %s [r%d] (%s)%s",
		splitKey.StringWithDirs(nil /* valDirs */, 50 /* maxLen */), rightDesc.RangeID, reason, extra)

	if err := r.store.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		log.Event(ctx, "split closure begins")
		defer log.Event(ctx, "split closure ends")
		txn.SetDebugName(splitTxnName)
		// Update existing range descriptor for left hand side of
		// split. Note that we mutate the descriptor for the left hand
		// side of the split first to locate the txn record there.
		{
			dbDescValue, err := conditionalGetDescValueFromDB(ctx, txn, desc)
			if err != nil {
				return err
			}

			b := txn.NewBatch()
			leftDescKey := keys.RangeDescriptorKey(leftDesc.StartKey)
			if err := updateRangeDescriptor(b, leftDescKey, dbDescValue, &leftDesc); err != nil {
				return err
			}
			// Commit this batch first to ensure that the transaction record
			// is created in the right place (split trigger relies on this),
			// but also to ensure the transaction record is created _before_
			// intents for the RHS range descriptor or addressing records.
			// Keep in mind that the BeginTransaction request is injected
			// to accompany the first write request, but if part of a batch
			// which spans ranges, the dist sender does not guarantee the
			// order which parts of the split batch arrive.
			//
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
		// TODO(spencer): event logging API should accept a batch
		// instead of a transaction; there's no reason this logging
		// shouldn't be done in parallel via the batch with the updated
		// range addressing.
		if err := r.store.logSplit(ctx, txn, leftDesc, *rightDesc); err != nil {
			return err
		}

		b := txn.NewBatch()

		// Create range descriptor for right hand side of the split.
		rightDescKey := keys.RangeDescriptorKey(rightDesc.StartKey)
		if err := updateRangeDescriptor(b, rightDescKey, nil, rightDesc); err != nil {
			return err
		}

		// Update range descriptor addressing record(s).
		if err := splitRangeAddressing(b, rightDesc, &leftDesc); err != nil {
			return err
		}

		// End the transaction manually, instead of letting RunTransaction
		// loop do it, in order to provide a split trigger.
		b.AddRawRequest(&roachpb.EndTransactionRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				SplitTrigger: &roachpb.SplitTrigger{
					LeftDesc:  leftDesc,
					RightDesc: *rightDesc,
				},
			},
		})

		// Commit txn with final batch (RHS descriptor and meta).
		log.Event(ctx, "commit txn with batch containing RHS descriptor and meta records")
		return txn.Run(ctx, b)
	}); err != nil {
		// The ConditionFailedError can occur because the descriptors acting
		// as expected values in the CPuts used to update the left or right
		// range descriptors are picked outside the transaction. Return
		// ConditionFailedError in the error detail so that the command can be
		// retried.
		if msg, ok := maybeDescriptorChangedError(desc, err); ok {
			// NB: we have to wrap the existing error here as consumers of this
			// code look at the root cause to sniff out the changed descriptor.
			err = &benignError{errors.Wrap(err, msg)}
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

	if err := r.store.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		dbDescValue, err := conditionalGetDescValueFromDB(ctx, txn, desc)
		if err != nil {
			return err
		}

		b := txn.NewBatch()
		newDesc := *desc
		newDesc.StickyBit = &hlc.Timestamp{}
		descKey := keys.RangeDescriptorKey(newDesc.StartKey)
		if err := updateRangeDescriptor(b, descKey, dbDescValue, &newDesc); err != nil {
			return err
		}
		if err := updateRangeAddressing(b, &newDesc); err != nil {
			return err
		}
		// End the transaction manually in order to provide a sticky bit trigger.
		b.AddRawRequest(&roachpb.EndTransactionRequest{
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
		if msg, ok := maybeDescriptorChangedError(desc, err); ok {
			// NB: we have to wrap the existing error here as consumers of this code
			// look at the root cause to sniff out the changed descriptor.
			err = &benignError{errors.Wrap(err, msg)}
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
	retryOpts := base.DefaultRetryOptions()
	retryOpts.MaxRetries = 10
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
		if retry := causer.Visit(lastErr, func(err error) bool {
			switch err.(type) {
			case *roachpb.ConditionFailedError:
				return true
			case *roachpb.AmbiguousResultError:
				return true
			default:
				return false
			}
		}); !retry {
			return roachpb.NewError(lastErr)
		}
	}
	// If we broke out of the loop after MaxRetries, return the last error.
	return roachpb.NewError(lastErr)
}

// AdminMerge extends this range to subsume the range that comes next
// in the key space. The merge is performed inside of a distributed
// transaction which writes the left hand side range descriptor (the
// subsuming range) and deletes the range descriptor for the right
// hand side range (the subsumed range). It also updates the range
// addressing metadata. The handover of responsibility for the
// reassigned key range is carried out seamlessly through a merge
// trigger carried out as part of the commit of that transaction.  A
// merge requires that the two ranges are collocated on the same set
// of replicas.
//
// The supplied RangeDescriptor is used as a form of optimistic lock. See the
// comment of "AdminSplit" for more information on this pattern.
func (r *Replica) AdminMerge(
	ctx context.Context, args roachpb.AdminMergeRequest, reason string,
) (roachpb.AdminMergeResponse, *roachpb.Error) {
	var reply roachpb.AdminMergeResponse

	origLeftDesc := r.Desc()
	if origLeftDesc.EndKey.Equal(roachpb.RKeyMax) {
		// Merging the final range doesn't make sense.
		return reply, roachpb.NewErrorf("cannot merge final range")
	}

	// Ensure that every current replica of the LHS has been initialized.
	// Otherwise there is a rare race where the replica GC queue can GC a
	// replica of the RHS too early. The comment on
	// TestStoreRangeMergeUninitializedLHSFollower explains the situation in full.
	if err := waitForReplicasInit(ctx, r.store.cfg.NodeDialer, *origLeftDesc); err != nil {
		return reply, roachpb.NewError(errors.Wrap(
			err, "waiting for all left-hand replicas to initialize"))
	}

	runMergeTxn := func(txn *client.Txn) error {
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

		// Do a consistent read of the right hand side's range descriptor.
		// NB: this read does NOT impact transaction record placement.
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
		// For simplicity, don't handle learner replicas, expect the caller to
		// resolve them first. (Defensively, we check that there are no non-voter
		// replicas, in case some third type is later added). This behavior can be
		// changed later if the complexity becomes worth it, but it's not right now.
		lReplicas, rReplicas := origLeftDesc.Replicas(), rightDesc.Replicas()
		if len(lReplicas.Voters()) != len(lReplicas.All()) {
			return errors.Errorf("cannot merge range with non-voter replicas on lhs: %s", lReplicas)
		}
		if len(rReplicas.Voters()) != len(rReplicas.All()) {
			return errors.Errorf("cannot merge range with non-voter replicas on rhs: %s", rReplicas)
		}
		if !replicaSetsEqual(lReplicas.All(), rReplicas.All()) {
			return errors.Errorf("ranges not collocated; %s != %s", lReplicas, rReplicas)
		}

		updatedLeftDesc := *origLeftDesc
		// lhs.Generation = max(rhs.Generation, lhs.Generation)+1.
		// See the comment on the Generation field for why generation are useful.
		if updatedLeftDesc.GetGeneration() < rightDesc.GetGeneration() {
			updatedLeftDesc.Generation = rightDesc.Generation
		}
		updatedLeftDesc.IncrementGeneration()
		r.maybeMarkGenerationComparable(&updatedLeftDesc)
		updatedLeftDesc.EndKey = rightDesc.EndKey
		log.Infof(ctx, "initiating a merge of %s into this range (%s)", &rightDesc, reason)

		// Update the range descriptor for the receiving range. It is important
		// (for transaction record placement) that the first write inside the
		// transaction is this conditional put to change the left hand side's
		// descriptor end key.
		{
			dbOrigLeftDescValue, err := conditionalGetDescValueFromDB(ctx, txn, origLeftDesc)
			if err != nil {
				return err
			}

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
		if err := updateRangeDescriptor(b, rightDescKey, dbRightDescKV.Value, nil); err != nil {
			return err
		}

		// Send off this batch, ensuring that intents are placed on both the local
		// copy and meta2's copy of the right-hand side range descriptor before we
		// send the Subsume request below. This is the precondition for sending a
		// Subsume request; see the godoc on the Subsume request for details.
		if err := txn.Run(ctx, b); err != nil {
			return err
		}

		// Intents have been placed, so the merge is now in its critical phase. Get
		// a consistent view of the data from the right-hand range. If the merge
		// commits, we'll write this data to the left-hand range in the merge
		// trigger.
		br, pErr := client.SendWrapped(ctx, r.store.DB().NonTransactionalSender(),
			&roachpb.SubsumeRequest{
				RequestHeader: roachpb.RequestHeader{Key: rightDesc.StartKey.AsRawKey()},
				LeftDesc:      *origLeftDesc,
				RightDesc:     &rightDesc,
			})
		if pErr != nil {
			return pErr.GoError()
		}
		rhsSnapshotRes := br.(*roachpb.SubsumeResponse)

		err = waitForApplication(ctx, r.store.cfg.NodeDialer, rightDesc, rhsSnapshotRes.LeaseAppliedIndex)
		if err != nil {
			return errors.Wrap(err, "waiting for all right-hand replicas to catch up")
		}

		// Successful subsume, so we're guaranteed that the right-hand range will
		// not serve another request unless this transaction aborts. End the
		// transaction manually in order to provide a merge trigger.
		b = txn.NewBatch()
		b.AddRawRequest(&roachpb.EndTransactionRequest{
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
		txn := client.NewTxn(ctx, r.store.DB(), r.NodeID(), client.RootTxn)
		err := runMergeTxn(txn)
		if err != nil {
			txn.CleanupOnError(ctx, err)
		}
		if _, canRetry := errors.Cause(err).(*roachpb.TransactionRetryWithProtoRefreshError); !canRetry {
			if err != nil {
				return reply, roachpb.NewErrorf("merge of range into %d failed: %s",
					origLeftDesc.RangeID, err)
			}
			return reply, nil
		}
	}
}

func waitForApplication(
	ctx context.Context, dialer *nodedialer.Dialer, desc roachpb.RangeDescriptor, leaseIndex uint64,
) error {
	return contextutil.RunWithTimeout(ctx, "wait for application", 5*time.Second, func(ctx context.Context) error {
		g := ctxgroup.WithContext(ctx)
		for _, repl := range desc.Replicas().Unwrap() {
			repl := repl // copy for goroutine
			g.GoCtx(func(ctx context.Context) error {
				conn, err := dialer.Dial(ctx, repl.NodeID)
				if err != nil {
					return errors.Wrapf(err, "could not dial n%d", repl.NodeID)
				}
				_, err = NewPerReplicaClient(conn).WaitForApplication(ctx, &WaitForApplicationRequest{
					StoreRequestHeader: StoreRequestHeader{NodeID: repl.NodeID, StoreID: repl.StoreID},
					RangeID:            desc.RangeID,
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
	ctx context.Context, dialer *nodedialer.Dialer, desc roachpb.RangeDescriptor,
) error {
	return contextutil.RunWithTimeout(ctx, "wait for replicas init", 5*time.Second, func(ctx context.Context) error {
		g := ctxgroup.WithContext(ctx)
		for _, repl := range desc.Replicas().Unwrap() {
			repl := repl // copy for goroutine
			g.GoCtx(func(ctx context.Context) error {
				conn, err := dialer.Dial(ctx, repl.NodeID)
				if err != nil {
					return errors.Wrapf(err, "could not dial n%d", repl.NodeID)
				}
				_, err = NewPerReplicaClient(conn).WaitForReplicaInit(ctx, &WaitForReplicaInitRequest{
					StoreRequestHeader: StoreRequestHeader{NodeID: repl.NodeID, StoreID: repl.StoreID},
					RangeID:            desc.RangeID,
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

// IsSnapshotError returns true iff the error indicates a preemptive
// snapshot failed.
func IsSnapshotError(err error) bool {
	return causer.Visit(err, func(err error) bool {
		_, ok := errors.Cause(err).(*snapshotError)
		return ok
	})
}

// ChangeReplicas adds or removes a replica of a range. The change is performed
// in a distributed transaction and takes effect when that transaction is committed.
// When removing a replica, only the NodeID and StoreID fields of the Replica are used.
//
// The supplied RangeDescriptor is used as a form of optimistic lock. See the
// comment of "adminSplitWithDescriptor" for more information on this pattern.
// The returned RangeDescriptor is the new value of the range's descriptor
// following the successful commit of the transaction. It can be used when
// making a series of changes to detect and prevent races between concurrent
// actors.
//
// Changing the replicas for a range is complicated. A change is initiated by
// the "replicate" queue when it encounters a range which has too many
// replicas, too few replicas or requires rebalancing. Addition and removal of
// a replica is divided into four phases. The first phase, which occurs in
// Replica.ChangeReplicas, is performed via a distributed transaction which
// updates the range descriptor and the meta range addressing information. This
// transaction includes a special ChangeReplicasTrigger on the EndTransaction
// request. A ConditionalPut of the RangeDescriptor implements the optimistic
// lock on the RangeDescriptor mentioned previously. Like all transactions, the
// requests within the transaction are replicated via Raft, including the
// EndTransaction request.
//
// The second phase of processing occurs when the batch containing the
// EndTransaction is proposed to raft. This proposing occurs on whatever
// replica received the batch, usually, but not always the range lease
// holder. defaultProposeRaftCommandLocked notices that the EndTransaction
// contains a ChangeReplicasTrigger and proposes a ConfChange to Raft (via
// raft.RawNode.ProposeConfChange).
//
// The ConfChange is propagated to all of the replicas similar to a normal Raft
// command, though additional processing is done inside of Raft. A Replica
// encounters the ConfChange in Replica.handleRaftReady and executes it using
// raft.RawNode.ApplyConfChange. If a new replica was added the Raft leader
// will start sending it heartbeat messages and attempting to bring it up to
// date. If a replica was removed, it is at this point that the Raft leader
// will stop communicating with it.
//
// The fourth phase of change replicas occurs when each replica for the range
// encounters the ChangeReplicasTrigger when applying the EndTransaction
// request. The replica will update its local range descriptor so as to contain
// the new set of replicas. If the replica is the one that is being removed, it
// will queue itself for removal with replicaGCQueue.
//
// Note that a removed replica may not see the EndTransaction containing the
// ChangeReplicasTrigger. The ConfChange operation will be applied as soon as a
// quorum of nodes have committed it. If the removed replica is down or the
// message is dropped for some reason the removed replica will not be
// notified. The replica GC queue will eventually discover and cleanup this
// state.
//
// When a new replica is added, it will have to catch up to the state of the
// other replicas. The Raft leader automatically handles this by either sending
// the new replica Raft log entries to apply, or by generating and sending a
// snapshot. See Replica.Snapshot and Replica.Entries.
//
// Note that Replica.ChangeReplicas returns when the distributed transaction
// has been committed to a quorum of replicas in the range. The actual
// replication of data occurs asynchronously via a snapshot or application of
// Raft log entries. This is important for the replicate queue to be aware
// of. A node can process hundreds or thousands of ChangeReplicas operations
// per second even though the actual replication of data proceeds at a much
// slower base. In order to avoid having this background replication and
// overwhelming the system, replication is throttled via a reservation system.
// When allocating a new replica for a range, the replicate queue reserves space
// for that replica on the target store via a ReservationRequest. (See
// StorePool.reserve). The reservation is fulfilled when the snapshot is
// applied.
//
// TODO(peter): There is a rare scenario in which a replica can be brought up
// to date via Raft log replay. In this scenario, the reservation will be left
// dangling until it expires. See #7849.
//
// TODO(peter): Describe preemptive snapshots. Preemptive snapshots are needed
// for the replicate queue to function properly. Currently the replicate queue
// will fire off as many replica additions as possible until it starts getting
// reservations denied at which point it will ignore the replica until the next
// scanner cycle.
func (r *Replica) ChangeReplicas(
	ctx context.Context,
	changeType roachpb.ReplicaChangeType,
	target roachpb.ReplicationTarget,
	desc *roachpb.RangeDescriptor,
	reason storagepb.RangeLogEventReason,
	details string,
) (updatedDesc *roachpb.RangeDescriptor, _ error) {
	if desc == nil {
		return nil, errors.Errorf("%s: the current RangeDescriptor must not be nil", r)
	}

	switch changeType {
	case roachpb.ADD_REPLICA:
		return r.addReplica(ctx, target, desc, SnapshotRequest_REBALANCE, reason, details)
	case roachpb.REMOVE_REPLICA:
		return r.removeReplica(ctx, target, desc, SnapshotRequest_REBALANCE, reason, details)
	default:
		return nil, errors.Errorf(`unknown change type: %s`, changeType)
	}
}

func (r *Replica) addReplica(
	ctx context.Context,
	target roachpb.ReplicationTarget,
	desc *roachpb.RangeDescriptor,
	priority SnapshotRequest_Priority,
	reason storagepb.RangeLogEventReason,
	details string,
) (*roachpb.RangeDescriptor, error) {
	for _, rDesc := range desc.Replicas().All() {
		if rDesc.NodeID == target.NodeID {
			// Two replicas from the same range are not allowed on the same node, even
			// in different stores.
			if rDesc.StoreID != target.StoreID {
				return nil, errors.Errorf("%s: unable to add replica %v; node already has a replica", r, target)
			}

			// Looks like we found a replica with the same store and node id. If the
			// replica is already a learner, then either some previous leaseholder was
			// trying to add it with the learner+snapshot+voter cycle and got
			// interrupted or else we hit a race between the replicate queue and
			// AdminChangeReplicas.
			if rDesc.GetType() == roachpb.ReplicaType_LEARNER {
				return nil, errors.Errorf(
					"%s: unable to add replica %v which is already present as a learner", r, target)
			}

			// Otherwise, we already had a full voter replica. Can't add another to
			// this store.
			return nil, errors.Errorf("%s: unable to add replica %v which is already present", r, target)
		}
	}

	settings := r.ClusterSettings()
	useLearners := useLearnerReplicas.Get(&settings.SV)
	useLearners = useLearners && settings.Version.IsActive(cluster.VersionLearnerReplicas)
	if !useLearners {
		return r.addReplicaLegacyPreemptiveSnapshot(ctx, target, desc, priority, reason, details)
	}

	// First add the replica as a raft learner. This means it accepts raft traffic
	// (so it can catch up) but doesn't vote (so it doesn't affect quorum and thus
	// doesn't introduce fragility into the system). For details see
	_ = roachpb.ReplicaDescriptors.Learners
	learnerDesc, err := addLearnerReplica(ctx, r.store, desc, target, reason, details)
	if err != nil {
		return nil, err
	}

	// Now move it to be a full voter (waiting on it to get a raft snapshot first,
	// so it's not immediately way behind).
	voterDesc, err := r.promoteLearnerReplicaToVoter(ctx, learnerDesc, target, priority, reason, details)
	if err != nil {
		// Don't leave a learner replica lying around if we didn't succeed in
		// promoting it to a voter.
		r.rollbackLearnerReplica(ctx, learnerDesc, target, reason, details)
		return nil, err
	}
	return voterDesc, nil
}

func addLearnerReplica(
	ctx context.Context,
	store *Store,
	desc *roachpb.RangeDescriptor,
	target roachpb.ReplicationTarget,
	reason storagepb.RangeLogEventReason,
	details string,
) (*roachpb.RangeDescriptor, error) {
	newDesc := *desc
	newDesc.SetReplicas(desc.Replicas().DeepCopy())
	replDesc := roachpb.ReplicaDescriptor{
		NodeID:    target.NodeID,
		StoreID:   target.StoreID,
		ReplicaID: desc.NextReplicaID,
		Type:      roachpb.ReplicaTypeLearner(),
	}
	newDesc.NextReplicaID++
	newDesc.AddReplica(replDesc)
	err := execChangeReplicasTxn(
		ctx, store, roachpb.ADD_REPLICA, desc, replDesc, &newDesc, reason, details,
	)
	return &newDesc, err
}

func (r *Replica) promoteLearnerReplicaToVoter(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	target roachpb.ReplicationTarget,
	priority SnapshotRequest_Priority,
	reason storagepb.RangeLogEventReason,
	details string,
) (*roachpb.RangeDescriptor, error) {
	// TODO(dan): We allow ranges with learner replicas to split, so in theory
	// this may want to detect that and retry, sending a snapshot and promoting
	// both sides.

	newReplicas := desc.Replicas().DeepCopy().All()
	for i, rDesc := range newReplicas {
		if rDesc.NodeID != target.NodeID || rDesc.StoreID != target.StoreID {
			continue
		}
		if rDesc.GetType() != roachpb.ReplicaType_LEARNER {
			return nil, errors.Errorf(`%s: cannot promote replica of type %s`, r, rDesc.Type)
		}
		rDesc.Type = roachpb.ReplicaTypeVoter()
		newReplicas[i] = rDesc

		// Note that raft snapshot queue refuses to send snapshots, so this is the
		// only one a learner can get.
		if err := r.sendSnapshot(ctx, rDesc, SnapshotRequest_LEARNER, priority); err != nil {
			return nil, err
		}

		if fn := r.store.cfg.TestingKnobs.ReplicaAddStopAfterLearnerSnapshot; fn != nil {
			if fn() {
				return desc, nil
			}
		}

		updatedDesc := *desc
		updatedDesc.SetReplicas(roachpb.MakeReplicaDescriptors(&newReplicas))
		err := execChangeReplicasTxn(ctx, r.store, roachpb.ADD_REPLICA, desc, rDesc, &updatedDesc, reason, details)
		return &updatedDesc, err
	}
	return nil, errors.Errorf(`%s: could not find replica to promote %s`, r, target)
}

func (r *Replica) rollbackLearnerReplica(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	target roachpb.ReplicationTarget,
	reason storagepb.RangeLogEventReason,
	details string,
) {
	newDesc := *desc
	newDesc.SetReplicas(desc.Replicas().DeepCopy())
	replDesc, ok := newDesc.RemoveReplica(target.NodeID, target.StoreID)
	if !ok {
		// This is a programming error if it happens. Why are we rolling back
		// something that's not present?
		log.Warningf(ctx, "failed to rollback learner %s, missing from descriptor %s", target, desc)
		return
	}

	// If (for example) the promotion failed because of a context deadline
	// exceeded, we do still want to clean up after ourselves, so always use a new
	// context (but with the old tags and with some timeout to save this from
	// blocking the caller indefinitely).
	const rollbackTimeout = 10 * time.Second
	rollbackFn := func(ctx context.Context) error {
		return execChangeReplicasTxn(
			ctx, r.store, roachpb.REMOVE_REPLICA, desc, replDesc, &newDesc, reason, details,
		)
	}
	rollbackCtx := logtags.WithTags(context.Background(), logtags.FromContext(ctx))
	if err := contextutil.RunWithTimeout(
		rollbackCtx, "learner rollback", rollbackTimeout, rollbackFn,
	); err != nil {
		log.Infof(ctx,
			"failed to rollback learner %s, abandoning it for the replicate queue %v", target, err)
		r.store.replicateQueue.MaybeAddAsync(ctx, r, r.store.Clock().Now())
	} else {
		log.Infof(ctx, "rolled back learner %s to %s", replDesc, &newDesc)
	}
}

func (r *Replica) addReplicaLegacyPreemptiveSnapshot(
	ctx context.Context,
	target roachpb.ReplicationTarget,
	desc *roachpb.RangeDescriptor,
	priority SnapshotRequest_Priority,
	reason storagepb.RangeLogEventReason,
	details string,
) (*roachpb.RangeDescriptor, error) {
	if desc == nil {
		return nil, errors.Errorf("%s: the current RangeDescriptor must not be nil", r)
	}

	repDesc := roachpb.ReplicaDescriptor{
		NodeID:  target.NodeID,
		StoreID: target.StoreID,
	}
	repDescIdx := -1  // tracks NodeID && StoreID
	nodeUsed := false // tracks NodeID only
	for i, existingRep := range desc.Replicas().All() {
		nodeUsedByExistingRep := existingRep.NodeID == repDesc.NodeID
		nodeUsed = nodeUsed || nodeUsedByExistingRep

		if nodeUsedByExistingRep && existingRep.StoreID == repDesc.StoreID {
			repDescIdx = i
			repDesc = existingRep
			break
		}
	}

	updatedDesc := *desc
	updatedDesc.SetReplicas(desc.Replicas().DeepCopy())

	// If the replica exists on the remote node, no matter in which store,
	// abort the replica add.
	if nodeUsed {
		if repDescIdx != -1 {
			return nil, errors.Errorf("%s: unable to add replica %v which is already present", r, repDesc)
		}
		return nil, errors.Errorf("%s: unable to add replica %v; node already has a replica", r, repDesc)
	}

	// Send a pre-emptive snapshot. Note that the replica to which this
	// snapshot is addressed has not yet had its replica ID initialized; this
	// is intentional, and serves to avoid the following race with the replica
	// GC queue:
	//
	// - snapshot received, a replica is lazily created with the "real" replica ID
	// - the replica is eligible for GC because it is not yet a member of the range
	// - GC queue runs, creating a raft tombstone with the replica's ID
	// - the replica is added to the range
	// - lazy creation of the replica fails due to the raft tombstone
	//
	// Instead, the replica GC queue will create a tombstone with replica ID
	// zero, which is never legitimately used, and thus never interferes with
	// raft operations. Racing with the replica GC queue can still partially
	// negate the benefits of pre-emptive snapshots, but that is a recoverable
	// degradation, not a catastrophic failure.
	//
	// NB: A closure is used here so that we can release the snapshot as soon
	// as it has been applied on the remote and before the ChangeReplica
	// operation is processed. This is important to allow other ranges to make
	// progress which might be required for this ChangeReplicas operation to
	// complete. See #10409.
	if err := r.sendSnapshot(ctx, repDesc, SnapshotRequest_PREEMPTIVE, priority); err != nil {
		return nil, err
	}

	repDesc.ReplicaID = updatedDesc.NextReplicaID
	updatedDesc.NextReplicaID++
	updatedDesc.AddReplica(repDesc)

	err := execChangeReplicasTxn(ctx, r.store, roachpb.ADD_REPLICA, desc, repDesc, &updatedDesc, reason, details)
	return &updatedDesc, err
}

func (r *Replica) removeReplica(
	ctx context.Context,
	target roachpb.ReplicationTarget,
	desc *roachpb.RangeDescriptor,
	priority SnapshotRequest_Priority,
	reason storagepb.RangeLogEventReason,
	details string,
) (*roachpb.RangeDescriptor, error) {
	if desc == nil {
		return nil, errors.Errorf("%s: the current RangeDescriptor must not be nil", r)
	}
	updatedDesc := *desc
	updatedDesc.SetReplicas(desc.Replicas().DeepCopy())
	// If that exact node-store combination does not have the replica,
	// abort the removal.
	removed, ok := updatedDesc.RemoveReplica(target.NodeID, target.StoreID)
	if !ok {
		return nil, errors.Errorf("%s: unable to remove replica %v which is not present", r, target)
	}
	err := execChangeReplicasTxn(ctx, r.store, roachpb.REMOVE_REPLICA, desc, removed, &updatedDesc, reason, details)
	return &updatedDesc, err
}

func execChangeReplicasTxn(
	ctx context.Context,
	store *Store,
	changeType roachpb.ReplicaChangeType,
	desc *roachpb.RangeDescriptor,
	repDesc roachpb.ReplicaDescriptor,
	updatedDesc *roachpb.RangeDescriptor,
	reason storagepb.RangeLogEventReason,
	details string,
) error {
	generationComparableEnabled := store.ClusterSettings().Version.IsActive(cluster.VersionGenerationComparable)
	if generationComparableEnabled {
		updatedDesc.IncrementGeneration()
		updatedDesc.GenerationComparable = proto.Bool(true)
	}

	descKey := keys.RangeDescriptorKey(desc.StartKey)
	if err := store.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		log.Event(ctx, "attempting txn")
		txn.SetDebugName(replicaChangeTxnName)
		dbDescValue, err := conditionalGetDescValueFromDB(ctx, txn, desc)
		if err != nil {
			return err
		}
		log.Infof(ctx, "change replicas (%v %s): existing descriptor %s", changeType, repDesc, desc)

		{
			b := txn.NewBatch()

			// Important: the range descriptor must be the first thing touched in the transaction
			// so the transaction record is co-located with the range being modified.
			if err := updateRangeDescriptor(b, descKey, dbDescValue, updatedDesc); err != nil {
				return err
			}

			// Run transaction up to this point to create txn record early (see #9265).
			if err := txn.Run(ctx, b); err != nil {
				return err
			}
		}

		// Log replica change into range event log.
		if err := store.logChange(
			ctx, txn, changeType, repDesc, *updatedDesc, reason, details,
		); err != nil {
			return err
		}

		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a commit trigger.
		b := txn.NewBatch()

		// Update range descriptor addressing record(s).
		if err := updateRangeAddressing(b, updatedDesc); err != nil {
			return err
		}

		b.AddRawRequest(&roachpb.EndTransactionRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				ChangeReplicasTrigger: &roachpb.ChangeReplicasTrigger{
					ChangeType: changeType,
					Replica:    repDesc,
					Desc:       updatedDesc,
				},
			},
		})
		if err := txn.Run(ctx, b); err != nil {
			log.Event(ctx, err.Error())
			return err
		}

		return nil
	}); err != nil {
		log.Event(ctx, err.Error())
		if msg, ok := maybeDescriptorChangedError(desc, err); ok {
			err = &benignError{errors.New(msg)}
		}
		return errors.Wrapf(err, "change replicas of r%d failed", desc.RangeID)
	}
	log.Event(ctx, "txn complete")
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
// replica would be a full member, but very far behind. [1]
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
// of a Raft group. The former is large, so we send it via streaming rpc instead
// of keeping it all in memory at once. (Well, at least on the sender side. On
// the recipient side, we do still buffer it, but we'll fix that at some point).
// The `(Replica).GetSnapshot` method does the necessary locking and gathers the
// various Raft state needed to run a replica. It also creates an iterator for
// the range's data as it looked under those locks (this is powered by a RocksDB
// snapshot, which is a different thing but a similar idea). Notably,
// GetSnapshot does not do the data iteration.
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
// determine if it can use the snapshot [2]. `shouldAcceptSnapshotData` is
// advisory and can return false positives. If `shouldAcceptSnapshotData`
// returns true, this is communicated back to the sender, which then proceeds to
// call `kvBatchSnapshotStrategy.Send`. This uses the iterator captured earlier
// to send the data in chunks, each chunk a streaming grpc message. The sender
// then sends a final message with an indicaton that it's done and blocks again,
// waiting for a second and final response from the recipient which indicates if
// the snapshot was a success.
//
// Applying the snapshot: After the recipient has received the message
// indicating it has all the data, it hands it all to
// `(Store).processRaftSnapshotRequest` to be applied. First, this re-checks the
// same things as `shouldAcceptSnapshotData` to make sure nothing has changed
// while the snapshot was being transferred. It then guarantees that there is
// either an initialized[3] replica or a `ReplicaPlaceholder`[4] to accept the
// snapshot by creating a placeholder if necessary. Finally, a *Raft snapshot*
// message is manually handed to the replica's Raft node (by calling
// `stepRaftGroup` + `handleRaftReadyRaftMuLocked`), at which point the snapshot
// has been applied.
//
// [1]: There is a third kind of snapshot, called "preemptive", which is how we
// avoided the above fragility before learner replicas were introduced in the
// 19.2 cycle. It's essentially a snapshot that we made very fast by staging it
// on a remote node right before we added a replica on that node. However,
// preemptive snapshots came with all sorts of complexity that we're delighted
// to be rid of. They have to stay around for clusters with mixed 19.1 and 19.2
// nodes, but after 19.2, we can remove them entirely.
//
// [2]: The largest class of rejections here is if the store contains a replica
// that overlaps the snapshot but has a different id (we maintain an invariant
// that replicas on a store never overlap). This usually happens when the
// recipient has an old copy of a replica that is no longer part of a range and
// the `replicaGCQueue` hasn't gotten around to collecting it yet. So if this
// happens, `shouldAcceptSnapshotData` will queue it up for consideration.
//
// [3]: A uninitialized replica is created when a replica that's being added
// gets traffic from its new peers before it gets a snapshot. It may be possible
// to get rid of uninitialized replicas (by dropping all Raft traffic except
// votes on the floor), but this is a cleanup that hasn't happened yet.
//
// [4]: The placeholder is essentially a snapshot lock, making any future
// callers of `shouldAcceptSnapshotData` return an error so that we no longer
// have to worry about racing with a second snapshot. See the comment on
// ReplicaPlaceholder for details.
func (r *Replica) sendSnapshot(
	ctx context.Context,
	recipient roachpb.ReplicaDescriptor,
	snapType SnapshotRequest_Type,
	priority SnapshotRequest_Priority,
) error {
	snap, err := r.GetSnapshot(ctx, snapType)
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

	usesReplicatedTruncatedState, err := engine.MVCCGetProto(
		ctx, snap.EngineSnap, keys.RaftTruncatedStateLegacyKey(r.RangeID), hlc.Timestamp{}, nil, engine.MVCCGetOptions{},
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
		// Recipients can choose to decline preemptive snapshots.
		CanDecline: snapType == SnapshotRequest_PREEMPTIVE,
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

// conditionalGetDescValueFromDB fetches an encoded RangeDescriptor from kv,
// checks that it matches the given expectation using proto Equals, and returns
// the raw fetched roachpb.Value. If the fetched value doesn't match the
// expectation, a ConditionFailedError is returned.
//
// This ConditionFailedError is a historical artifact. We used to pass the
// parsed RangeDescriptor directly as the expected value in a CPut, but proto
// message encodings aren't stable so this was fragile. Calling this method and
// then passing the returned *roachpb.Value as the expected value in a CPut does
// the same thing, but also correctly handles proto equality. See #38308.
func conditionalGetDescValueFromDB(
	ctx context.Context, txn *client.Txn, expectation *roachpb.RangeDescriptor,
) (*roachpb.Value, error) {
	descKey := keys.RangeDescriptorKey(expectation.StartKey)
	existingDescKV, err := txn.Get(ctx, descKey)
	if err != nil {
		return nil, errors.Wrap(err, "fetching current range descriptor value")
	}
	var existingDesc *roachpb.RangeDescriptor
	if existingDescKV.Value != nil {
		existingDesc = &roachpb.RangeDescriptor{}
		if err := existingDescKV.Value.GetProto(existingDesc); err != nil {
			return nil, errors.Wrap(err, "decoding current range descriptor value")
		}
	}
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
	if existingDesc != nil {
		existingDesc.Replicas() // for sorting side-effect
	}
	if expectation != nil {
		expectation.Replicas() // for sorting side-effect
	}
	if !existingDesc.Equal(expectation) {
		return nil, &roachpb.ConditionFailedError{ActualValue: existingDescKV.Value}
	}
	return existingDescKV.Value, nil
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
	b *client.Batch, descKey roachpb.Key, oldValue *roachpb.Value, newDesc *roachpb.RangeDescriptor,
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
	var ov interface{}
	if oldValue != nil {
		// If the old value was fetched from kv, it may have a checksum set. This
		// panics CPut, so clear it.
		oldValue.ClearChecksum()
		ov = oldValue
	}
	b.CPut(descKey, newValue, ov)
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

	// Deep-copy the Replicas slice (in our shallow copy of the RangeDescriptor)
	// since we'll mutate it in the loop below.
	rangeDesc.SetReplicas(rangeDesc.Replicas().DeepCopy())
	startKey := rangeDesc.StartKey.AsRawKey()

	// Step 0: Remove all learners so we don't have to think about them. We could
	// do something smarter here and try to promote them, but it doesn't seem
	// worth the complexity right now. Revisit if this is an issue in practice.
	//
	// Note that we can't just add the learners to removeTargets. The below logic
	// always does add then remove and if the learner was in the requested
	// targets, we might try to add it before removing it.
	newDesc, err := removeLearners(ctx, s.DB(), &rangeDesc)
	if err != nil {
		log.Warning(ctx, err)
		return err
	}
	rangeDesc = *newDesc

	// Step 1: Compute which replicas are to be added and which are to be removed.
	//
	// TODO(radu): we can't have multiple replicas on different stores on the
	// same node, and this code doesn't do anything to specifically avoid that
	// case (although the allocator will avoid even trying to send snapshots to
	// such stores), so it could cause some failures.

	var addTargets []roachpb.ReplicaDescriptor
	for _, t := range targets {
		found := false
		for _, replicaDesc := range rangeDesc.Replicas().All() {
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
	for _, replicaDesc := range rangeDesc.Replicas().All() {
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

	canRetry := func(err error) bool {
		whitelist := []string{
			snapshotApplySemBusyMsg,
			IntersectingSnapshotMsg,
		}
		for _, substr := range whitelist {
			if strings.Contains(err.Error(), substr) {
				return true
			}
		}
		return false
	}

	transferLease := func() {
		if err := s.DB().AdminTransferLease(
			ctx, startKey, targets[0].StoreID,
		); err != nil {
			log.Warningf(ctx, "while transferring lease: %+v", err)
		}
	}

	sysCfg := s.cfg.Gossip.GetSystemConfig()
	if sysCfg == nil {
		return fmt.Errorf("no system config available, unable to perform RelocateRange")
	}
	zone, err := sysCfg.GetZoneConfigForKey(rangeDesc.StartKey)
	if err != nil {
		return err
	}

	storeList, _, _ := s.allocator.storePool.getStoreList(rangeDesc.RangeID, storeFilterNone)
	storeMap := storeListToMap(storeList)

	rangeInfo := RangeInfo{Desc: &rangeDesc}

	// Step 2: Repeatedly add a replica then remove a replica until we reach the
	// desired state.
	every := log.Every(time.Minute)
	re := retry.StartWithCtx(ctx, retry.Options{MaxBackoff: 5 * time.Second})
	for len(addTargets) > 0 || len(removeTargets) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		if len(addTargets) > 0 && len(addTargets) >= len(removeTargets) {
			// Each iteration, pick the most desirable replica to add. However,
			// prefer the first target if it doesn't yet have a replica so that we
			// can always transfer the lease to it before removing a replica below.
			// This makes it easier to avoid removing a replica that's still
			// leaseholder without needing to bounce the lease around a bunch.
			candidates := addTargets
			if storeHasReplica(targets[0].StoreID, candidates) {
				candidates = []roachpb.ReplicaDescriptor{
					{NodeID: targets[0].NodeID, StoreID: targets[0].StoreID},
				}
			}

			// The storeList's list of stores is used to constrain which stores the
			// allocator considers putting a new replica on. We want it to only
			// consider the stores in candidates.
			candidateDescs := make([]roachpb.StoreDescriptor, 0, len(candidates))
			for _, candidate := range candidates {
				store, ok := storeMap[candidate.StoreID]
				if !ok {
					return fmt.Errorf("cannot up-replicate to s%d; missing gossiped StoreDescriptor", candidate.StoreID)
				}
				candidateDescs = append(candidateDescs, *store)
			}
			storeList = makeStoreList(candidateDescs)

			targetStore, _ := s.allocator.allocateTargetFromList(
				ctx,
				storeList,
				zone,
				rangeInfo.Desc.Replicas().All(),
				rangeInfo,
				s.allocator.scorerOptions())
			if targetStore == nil {
				return fmt.Errorf("none of the remaining targets %v are legal additions to %v",
					addTargets, rangeInfo.Desc.Replicas())
			}

			target := roachpb.ReplicationTarget{
				NodeID:  targetStore.Node.NodeID,
				StoreID: targetStore.StoreID,
			}
			newDesc, err := s.DB().AdminChangeReplicas(ctx, startKey, roachpb.ADD_REPLICA,
				[]roachpb.ReplicationTarget{target}, *rangeInfo.Desc)
			if err != nil {
				returnErr := errors.Wrapf(err, "while adding target %v", target)
				if !canRetry(err) {
					return returnErr
				}
				if every.ShouldLog() {
					log.Warning(ctx, returnErr)
				}
				re.Next()
				continue
			}

			// Upon success, remove the target from our to-do list and add it to our
			// local copy of the range descriptor such that future allocator
			// decisions take it into account.
			addTargets = removeTargetFromSlice(addTargets, target)
			rangeInfo.Desc = newDesc
		}

		if len(removeTargets) > 0 && len(removeTargets) > len(addTargets) {
			targetStore, _, err := s.allocator.RemoveTarget(ctx, zone, removeTargets, rangeInfo)
			if err != nil {
				return errors.Wrapf(err, "unable to select removal target from %v; current replicas %v",
					removeTargets, rangeInfo.Desc.Replicas())
			}
			target := roachpb.ReplicationTarget{
				NodeID:  targetStore.NodeID,
				StoreID: targetStore.StoreID,
			}
			// Note that attempting to remove the leaseholder won't work, so transfer
			// the lease first in such scenarios. The first specified target should be
			// the leaseholder now, so we can always transfer the lease there.
			transferLease()
			newDesc, err := s.DB().AdminChangeReplicas(ctx, startKey, roachpb.REMOVE_REPLICA,
				[]roachpb.ReplicationTarget{target}, *rangeInfo.Desc)
			if err != nil {
				log.Warningf(ctx, "while removing target %v: %+v", target, err)
				if !canRetry(err) {
					return err
				}
				re.Next()
				continue
			}

			// Upon success, remove the target from our to-do list and from our local
			// copy of the range descriptor such that future allocator decisions take
			// its absence into account.
			removeTargets = removeTargetFromSlice(removeTargets, target)
			rangeInfo.Desc = newDesc
		}
	}

	// Step 3: Transfer the lease to the first listed target, as the API specifies.
	transferLease()

	return ctx.Err()
}

// Modifies the underlying storage of the slice rather than copying.
// Don't use on a shared slice where the order matters.
func removeTargetFromSlice(
	targets []roachpb.ReplicaDescriptor, target roachpb.ReplicationTarget,
) []roachpb.ReplicaDescriptor {
	for i, t := range targets {
		if t.NodeID == target.NodeID && t.StoreID == target.StoreID {
			// Swap the removed target with the last element in the slice and return
			// a slice that's 1 element shorter than before.
			targets[i], targets[len(targets)-1] = targets[len(targets)-1], targets[i]
			return targets[:len(targets)-1]
		}
	}
	return targets
}

func removeLearners(
	ctx context.Context, db *client.DB, desc *roachpb.RangeDescriptor,
) (*roachpb.RangeDescriptor, error) {
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
	newDesc, err := db.AdminChangeReplicas(ctx, desc.StartKey, roachpb.REMOVE_REPLICA, targets, *desc)
	if err != nil {
		return nil, errors.Wrapf(err, `removing learners from %s`, desc)
	}
	return newDesc, nil
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

// maybeMarkGenerationComparable sets GenerationComparable if the cluster is at
// a high enough version such that GenerationComparable won't be lost.
func (r *Replica) maybeMarkGenerationComparable(desc *roachpb.RangeDescriptor) {
	if r.store.ClusterSettings().Version.IsActive(cluster.VersionGenerationComparable) {
		desc.GenerationComparable = proto.Bool(true)
	}
}
