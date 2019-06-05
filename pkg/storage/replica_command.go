// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// AdminSplit divides the range into into two ranges using args.SplitKey.
func (r *Replica) AdminSplit(
	ctx context.Context, args roachpb.AdminSplitRequest, reason string,
) (reply roachpb.AdminSplitResponse, _ *roachpb.Error) {
	if len(args.SplitKey) == 0 {
		return roachpb.AdminSplitResponse{}, roachpb.NewErrorf("cannot split range with no key provided")
	}

	var lastErr error
	retryOpts := base.DefaultRetryOptions()
	retryOpts.MaxRetries = 10
	for retryable := retry.StartWithCtx(ctx, retryOpts); retryable.Next(); {
		// The replica may have been destroyed since the start of the retry loop. We
		// need to explicitly check this condition. Having a valid lease, as we
		// verify below, does not imply that the range still exists: even after a
		// range has been merged into its left-hand neighbor, its final lease (i.e.,
		// the lease we have in r.mu.state.Lease) can remain valid indefinitely.
		if _, err := r.IsDestroyed(); err != nil {
			return reply, roachpb.NewError(err)
		}

		// Admin commands always require the range lease to begin (see
		// executeAdminBatch), but we may have lost it while in this retry loop.
		// Without the lease, a replica's local descriptor can be arbitrarily
		// stale, which will result in a ConditionFailedError. To avoid this,
		// we make sure that we still have the lease before each attempt.
		if _, pErr := r.redirectOnOrAcquireLease(ctx); pErr != nil {
			return roachpb.AdminSplitResponse{}, pErr
		}

		reply, lastErr = r.adminSplitWithDescriptor(ctx, args, r.Desc(), true /* delayable */, reason)
		// On seeing a ConditionFailedError or an AmbiguousResultError, retry
		// the command with the updated descriptor.
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
			return reply, roachpb.NewError(lastErr)
		}
	}
	// If we broke out of the loop after MaxRetries, return the last error.
	return roachpb.AdminSplitResponse{}, roachpb.NewError(lastErr)
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
			return fmt.Sprintf("descriptor changed: [expected] %s != [actual] %s", desc, actualDesc), true
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
			if pr.State == raft.ProgressStateReplicate {
				// This follower is in good working order.
				continue
			}
			s += fmt.Sprintf("; r%d/%d is ", rangeID, replicaID)
			switch pr.State {
			case raft.ProgressStateSnapshot:
				// If the Raft snapshot queue is backed up, replicas can spend
				// minutes or worse until they are caught up.
				s += "waiting for a Raft snapshot"
			case raft.ProgressStateProbe:
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
		if desc.StickyBit.Less(args.ExpirationTime) {
			newDesc := *desc
			newDesc.StickyBit = args.ExpirationTime
			err := r.store.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				b := txn.NewBatch()
				descKey := keys.RangeDescriptorKey(desc.StartKey)
				if err := updateRangeDescriptor(b, descKey, desc, &newDesc); err != nil {
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

	// Set the range descriptor's sticky bit.
	rightDesc.StickyBit = args.ExpirationTime

	// Init updated version of existing range descriptor.
	leftDesc := *desc
	leftDesc.IncrementGeneration()
	leftDesc.EndKey = splitKey

	// Set the generation of the right hand side descriptor to match that of the
	// (updated) left hand side. See the comment on the field for an explanation
	// of why generations are useful.
	rightDesc.Generation = leftDesc.Generation

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
			b := txn.NewBatch()
			leftDescKey := keys.RangeDescriptorKey(leftDesc.StartKey)
			if err := updateRangeDescriptor(b, leftDescKey, desc, &leftDesc); err != nil {
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
	// TODO(jeffreyxiao): Have a retry loop for ConditionalFailed errors similar
	// to AdminSplit
	var reply roachpb.AdminUnsplitResponse

	desc := *r.Desc()
	if !bytes.Equal(desc.StartKey.AsRawKey(), args.Header().Key) {
		return reply, roachpb.NewErrorf("key %s is not the start of a range", args.Header().Key)
	}

	// If the range's sticky bit is already hlc.Timestamp{}, we treat the
	// unsplit command as a no-op and return success instead of throwing an
	// error.
	if (desc.StickyBit == hlc.Timestamp{}) {
		return reply, nil
	}

	if err := r.store.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		newDesc := desc
		newDesc.StickyBit = hlc.Timestamp{}
		descKey := keys.RangeDescriptorKey(newDesc.StartKey)

		if err := updateRangeDescriptor(b, descKey, &desc, &newDesc); err != nil {
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
		return reply, roachpb.NewErrorf("unsplit at key %s failed: %s", args.Header().Key, err)
	}

	return reply, nil
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
		if err := txn.GetProto(ctx, rightDescKey, &rightDesc); err != nil {
			return err
		}

		// Verify that the two ranges are mergeable.
		if !bytes.Equal(origLeftDesc.EndKey, rightDesc.StartKey) {
			// Should never happen, but just in case.
			return errors.Errorf("ranges are not adjacent; %s != %s", origLeftDesc.EndKey, rightDesc.StartKey)
		}
		if l, r := origLeftDesc.Replicas(), rightDesc.Replicas(); !replicaSetsEqual(l.Unwrap(), r.Unwrap()) {
			return errors.Errorf("ranges not collocated; %s != %s", l, r)
		}

		updatedLeftDesc := *origLeftDesc
		// lhs.Generation = max(rhs.Generation, lhs.Generation)+1.
		// See the comment on the Generation field for why generation are useful.
		if updatedLeftDesc.GetGeneration() < rightDesc.GetGeneration() {
			updatedLeftDesc.Generation = rightDesc.Generation
		}
		updatedLeftDesc.IncrementGeneration()
		updatedLeftDesc.EndKey = rightDesc.EndKey
		log.Infof(ctx, "initiating a merge of %s into this range (%s)", rightDesc, reason)

		// Update the range descriptor for the receiving range. It is important
		// (for transaction record placement) that the first write inside the
		// transaction is this conditional put to change the left hand side's
		// descriptor end key.
		{
			b := txn.NewBatch()
			leftDescKey := keys.RangeDescriptorKey(updatedLeftDesc.StartKey)
			if err := updateRangeDescriptor(b, leftDescKey, origLeftDesc, &updatedLeftDesc); err != nil {
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
		if err := updateRangeDescriptor(b, rightDescKey, &rightDesc, nil); err != nil {
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

		err := waitForApplication(ctx, r.store.cfg.NodeDialer, rightDesc, rhsSnapshotRes.LeaseAppliedIndex)
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
	return r.changeReplicas(ctx, changeType, target, desc, SnapshotRequest_REBALANCE, reason, details)
}

func (r *Replica) changeReplicas(
	ctx context.Context,
	changeType roachpb.ReplicaChangeType,
	target roachpb.ReplicationTarget,
	desc *roachpb.RangeDescriptor,
	priority SnapshotRequest_Priority,
	reason storagepb.RangeLogEventReason,
	details string,
) (_ *roachpb.RangeDescriptor, _ error) {
	if desc == nil {
		return nil, errors.Errorf("%s: the current RangeDescriptor must not be nil", r)
	}
	repDesc := roachpb.ReplicaDescriptor{
		NodeID:  target.NodeID,
		StoreID: target.StoreID,
	}
	repDescIdx := -1  // tracks NodeID && StoreID
	nodeUsed := false // tracks NodeID only
	for i, existingRep := range desc.Replicas().Unwrap() {
		nodeUsedByExistingRep := existingRep.NodeID == repDesc.NodeID
		nodeUsed = nodeUsed || nodeUsedByExistingRep

		if nodeUsedByExistingRep && existingRep.StoreID == repDesc.StoreID {
			repDescIdx = i
			repDesc = existingRep
			break
		}
	}

	rangeID := desc.RangeID
	updatedDesc := *desc
	updatedDesc.SetReplicas(desc.Replicas().DeepCopy())

	switch changeType {
	case roachpb.ADD_REPLICA:
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
		if err := r.sendSnapshot(ctx, repDesc, snapTypePreemptive, priority); err != nil {
			return nil, err
		}

		repDesc.ReplicaID = updatedDesc.NextReplicaID
		updatedDesc.NextReplicaID++
		updatedDesc.AddReplica(repDesc)

	case roachpb.REMOVE_REPLICA:
		// If that exact node-store combination does not have the replica,
		// abort the removal.
		if repDescIdx == -1 {
			return nil, errors.Errorf("%s: unable to remove replica %v which is not present", r, repDesc)
		}
		if !updatedDesc.RemoveReplica(repDesc) {
			return nil, errors.Errorf("%s: unable to remove replica %v which is not present", r, repDesc)
		}
	}

	descKey := keys.RangeDescriptorKey(desc.StartKey)

	if err := r.store.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		log.Event(ctx, "attempting txn")
		txn.SetDebugName(replicaChangeTxnName)
		// TODO(tschottdorf): oldDesc is used for sanity checks related to #7224.
		// Remove when that has been solved. The failure mode is likely based on
		// prior divergence of the Replica (in which case the check below does not
		// fire because everything reads from the local, diverged, set of data),
		// so we don't expect to see this fail in practice ever.
		oldDesc := new(roachpb.RangeDescriptor)
		if err := txn.GetProto(ctx, descKey, oldDesc); err != nil {
			return err
		}
		log.Infof(ctx, "change replicas (%v %s): read existing descriptor %s",
			changeType, repDesc, oldDesc)

		{
			b := txn.NewBatch()

			// Important: the range descriptor must be the first thing touched in the transaction
			// so the transaction record is co-located with the range being modified.
			if err := updateRangeDescriptor(b, descKey, desc, &updatedDesc); err != nil {
				return err
			}

			// Run transaction up to this point to create txn record early (see #9265).
			if err := txn.Run(ctx, b); err != nil {
				return err
			}
		}

		// Log replica change into range event log.
		if err := r.store.logChange(
			ctx, txn, changeType, repDesc, updatedDesc, reason, details,
		); err != nil {
			return err
		}

		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a commit trigger.
		b := txn.NewBatch()

		// Update range descriptor addressing record(s).
		if err := updateRangeAddressing(b, &updatedDesc); err != nil {
			return err
		}

		b.AddRawRequest(&roachpb.EndTransactionRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				// TODO(benesch): this trigger should just specify the updated
				// descriptor, like the split and merge triggers, so that the receiver
				// doesn't need to reconstruct the range descriptor update.
				ChangeReplicasTrigger: &roachpb.ChangeReplicasTrigger{
					ChangeType:      changeType,
					Replica:         repDesc,
					UpdatedReplicas: updatedDesc.Replicas().Unwrap(),
					NextReplicaID:   updatedDesc.NextReplicaID,
				},
			},
		})
		if err := txn.Run(ctx, b); err != nil {
			log.Event(ctx, err.Error())
			return err
		}

		if oldDesc.RangeID != 0 && !oldDesc.Equal(desc) {
			// We read the previous value, it wasn't what we supposedly used in
			// the CPut, but we still overwrote in the CPut above.
			panic(fmt.Sprintf("committed replica change, but oldDesc != assumedOldDesc:\n%+v\n%+v\nnew desc:\n%+v",
				oldDesc, desc, updatedDesc))
		}
		return nil
	}); err != nil {
		log.Event(ctx, err.Error())
		if msg, ok := maybeDescriptorChangedError(desc, err); ok {
			err = &benignError{errors.New(msg)}
		}
		return nil, errors.Wrapf(err, "change replicas of r%d failed", rangeID)
	}
	log.Event(ctx, "txn complete")
	return &updatedDesc, nil
}

// sendSnapshot sends a snapshot of the replica state to the specified
// replica. This is used for both preemptive snapshots that are performed
// before adding a replica to a range, and for Raft-initiated snapshots that
// are used to bring a replica up to date that has fallen too far
// behind. Currently only invoked from replicateQueue and raftSnapshotQueue. Be
// careful about adding additional calls as generating a snapshot is moderately
// expensive.
func (r *Replica) sendSnapshot(
	ctx context.Context,
	repDesc roachpb.ReplicaDescriptor,
	snapType string,
	priority SnapshotRequest_Priority,
) error {
	snap, err := r.GetSnapshot(ctx, snapType)
	if err != nil {
		return errors.Wrapf(err, "%s: failed to generate %s snapshot", r, snapType)
	}
	defer snap.Close()
	log.Event(ctx, "generated snapshot")

	fromRepDesc, err := r.GetReplicaDescriptor()
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
		snap.State.TruncatedState.Index < snap.State.RaftAppliedIndex &&
		r.store.ClusterSettings().Version.IsActive(cluster.VersionSnapshotsWithoutLog)

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
			FromReplica: fromRepDesc,
			ToReplica:   repDesc,
			Message: raftpb.Message{
				Type:     raftpb.MsgSnap,
				To:       uint64(repDesc.ReplicaID),
				From:     uint64(fromRepDesc.ReplicaID),
				Term:     status.Term,
				Snapshot: snap.RaftSnap,
			},
		},
		RangeSize: r.GetMVCCStats().Total(),
		// Recipients can choose to decline preemptive snapshots.
		CanDecline: snapType == snapTypePreemptive,
		Priority:   priority,
		Strategy:   SnapshotRequest_KV_BATCH,
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

// updateRangeDescriptor adds a ConditionalPut on the range descriptor. The
// conditional put verifies that changes to the range descriptor are made in a
// well-defined order, preventing a scenario where a wayward replica which is
// no longer part of the original Raft group comes back online to form a
// splinter group with a node which was also a former replica, and hijacks the
// range descriptor. This is a last line of defense; other mechanisms should
// prevent rogue replicas from getting this far (see #768).
//
// oldDesc can be nil, meaning that the key is expected to not exist.
//
// Note that in addition to using this method to update the on-disk range
// descriptor, a CommitTrigger must be used to update the in-memory
// descriptor; it will not automatically be copied from newDesc.
func updateRangeDescriptor(
	b *client.Batch,
	descKey roachpb.Key,
	oldDesc *roachpb.RangeDescriptor,
	newDesc *roachpb.RangeDescriptor,
) error {
	// This is subtle: []byte(nil) != interface{}(nil). A []byte(nil) refers to
	// an empty value. An interface{}(nil) refers to a non-existent value. So
	// we're careful to construct interface{}(nil)s when newDesc/oldDesc are nil.
	var newValue interface{}
	if newDesc != nil {
		if err := newDesc.Validate(); err != nil {
			return errors.Wrapf(err, "validating new descriptor %+v (old descriptor is %+v)", newDesc, oldDesc)
		}
		newBytes, err := protoutil.Marshal(newDesc)
		if err != nil {
			return err
		}
		newValue = newBytes
	}
	var oldValue interface{}
	if oldDesc != nil {
		oldBytes, err := protoutil.Marshal(oldDesc)
		if err != nil {
			return err
		}
		oldValue = oldBytes
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

	// Deep-copy the Replicas slice (in our shallow copy of the RangeDescriptor)
	// since we'll mutate it in the loop below.
	rangeDesc.SetReplicas(rangeDesc.Replicas().DeepCopy())
	startKey := rangeDesc.StartKey.AsRawKey()

	// Step 1: Compute which replicas are to be added and which are to be removed.
	//
	// TODO(radu): we can't have multiple replicas on different stores on the
	// same node, and this code doesn't do anything to specifically avoid that
	// case (although the allocator will avoid even trying to send snapshots to
	// such stores), so it could cause some failures.

	var addTargets []roachpb.ReplicaDescriptor
	for _, t := range targets {
		found := false
		for _, replicaDesc := range rangeDesc.Replicas().Unwrap() {
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
	for _, replicaDesc := range rangeDesc.Replicas().Unwrap() {
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
			log.Warningf(ctx, "while transferring lease: %s", err)
		}
	}

	// updateRangeDesc updates the passed RangeDescriptor following the successful
	// completion of an AdminChangeReplicasRequest with the single provided target
	// and changeType.
	// TODO(ajwerner): Remove this for 19.2 after AdminChangeReplicas always
	// returns a non-nil Desc.
	updateRangeDesc := func(
		desc *roachpb.RangeDescriptor,
		changeType roachpb.ReplicaChangeType,
		target roachpb.ReplicationTarget,
	) {
		switch changeType {
		case roachpb.ADD_REPLICA:
			desc.AddReplica(roachpb.ReplicaDescriptor{
				NodeID:    target.NodeID,
				StoreID:   target.StoreID,
				ReplicaID: desc.NextReplicaID,
			})
			desc.NextReplicaID++
		case roachpb.REMOVE_REPLICA:
			newReplicas := removeTargetFromSlice(desc.Replicas().Unwrap(), target)
			desc.SetReplicas(roachpb.MakeReplicaDescriptors(newReplicas))
		default:
			panic(errors.Errorf("unknown ReplicaChangeType %v", changeType))
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
				rangeInfo.Desc.Replicas().Unwrap(),
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
			if newDesc != nil {
				rangeInfo.Desc = newDesc
			} else {
				// TODO(ajwerner): Remove this case for 19.2.
				updateRangeDesc(rangeInfo.Desc, roachpb.ADD_REPLICA, target)
			}
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
				log.Warningf(ctx, "while removing target %v: %s", target, err)
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
			if newDesc != nil {
				rangeInfo.Desc = newDesc
			} else {
				// TODO(ajwerner): Remove this case for 19.2.
				updateRangeDesc(rangeInfo.Desc, roachpb.REMOVE_REPLICA, target)
			}
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
		newLeaseholderIdx := rand.Intn(len(desc.Replicas().Unwrap()))
		targetStoreID := desc.Replicas().Unwrap()[newLeaseholderIdx].StoreID
		if targetStoreID != r.store.StoreID() {
			if err := r.AdminTransferLease(ctx, targetStoreID); err != nil {
				log.Warningf(ctx, "failed to scatter lease to s%d: %s", targetStoreID, err)
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
