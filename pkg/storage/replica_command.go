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

package storage

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// evaluateCommand delegates to the eval method for the given
// roachpb.Request. The returned Result may be partially valid
// even if an error is returned. maxKeys is the number of scan results
// remaining for this batch (MaxInt64 for no limit).
func evaluateCommand(
	ctx context.Context,
	raftCmdID storagebase.CmdIDKey,
	index int,
	batch engine.ReadWriter,
	rec batcheval.EvalContext,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	maxKeys int64,
	args roachpb.Request,
	reply roachpb.Response,
) (result.Result, *roachpb.Error) {

	// If a unittest filter was installed, check for an injected error; otherwise, continue.
	if filter := rec.EvalKnobs().TestingEvalFilter; filter != nil {
		filterArgs := storagebase.FilterArgs{
			Ctx:   ctx,
			CmdID: raftCmdID,
			Index: index,
			Sid:   rec.StoreID(),
			Req:   args,
			Hdr:   h,
		}
		if pErr := filter(filterArgs); pErr != nil {
			log.Infof(ctx, "test injecting error: %s", pErr)
			return result.Result{}, pErr
		}
	}

	var err error
	var pd result.Result

	if cmd, ok := batcheval.LookupCommand(args.Method()); ok {
		cArgs := batcheval.CommandArgs{
			EvalCtx: rec,
			Header:  h,
			// Some commands mutate their arguments, so give each invocation
			// its own copy (shallow to mimic earlier versions of this code
			// in which args were passed by value instead of pointer).
			Args:    args.ShallowCopy(),
			MaxKeys: maxKeys,
			Stats:   ms,
		}
		pd, err = cmd.Eval(ctx, batch, cArgs, reply)
	} else {
		err = errors.Errorf("unrecognized command %s", args.Method())
	}

	if h.ReturnRangeInfo {
		returnRangeInfo(reply, rec)
	}

	// TODO(peter): We'd like to assert that the hlc clock is always updated
	// correctly, but various tests insert versioned data without going through
	// the proper channels. See TestPushTxnUpgradeExistingTxn for an example.
	//
	// if header.Txn != nil && !header.Txn.Timestamp.Less(h.Timestamp) {
	// 	if now := r.store.Clock().Now(); now.Less(header.Txn.Timestamp) {
	// 		log.Fatalf(ctx, "hlc clock not updated: %s < %s", now, header.Txn.Timestamp)
	// 	}
	// }

	if log.V(2) {
		log.Infof(ctx, "evaluated %s command %+v: %+v, err=%v", args.Method(), args, reply, err)
	}

	// Create a roachpb.Error by initializing txn from the request/response header.
	var pErr *roachpb.Error
	if err != nil {
		txn := reply.Header().Txn
		if txn == nil {
			txn = h.Txn
		}
		pErr = roachpb.NewErrorWithTxn(err, txn)
	}

	return pd, pErr
}

func returnRangeInfo(reply roachpb.Response, rec batcheval.EvalContext) {
	header := reply.Header()
	lease, _ := rec.GetLease()
	desc := rec.Desc()
	header.RangeInfos = []roachpb.RangeInfo{
		{
			Desc:  *desc,
			Lease: lease,
		},
	}
	reply.SetHeader(header)
}

// AdminSplit divides the range into into two ranges using args.SplitKey.
func (r *Replica) AdminSplit(
	ctx context.Context, args roachpb.AdminSplitRequest,
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

		reply, lastErr = r.adminSplitWithDescriptor(ctx, args, r.Desc())
		// On seeing a ConditionFailedError or an AmbiguousResultError, retry the
		// command with the updated descriptor.
		switch errors.Cause(lastErr).(type) {
		case *roachpb.ConditionFailedError:
		case *roachpb.AmbiguousResultError:
		default:
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
		if err := detail.ActualValue.GetProto(&actualDesc); err == nil {
			if desc.RangeID == actualDesc.RangeID && !desc.Equal(actualDesc) {
				return fmt.Sprintf("descriptor changed: [expected] %s != [actual] %s",
					desc, actualDesc), true
			}
		}
	}
	return "", false
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
	ctx context.Context, args roachpb.AdminSplitRequest, desc *roachpb.RangeDescriptor,
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
		return reply, nil
	}
	log.Event(ctx, "found split key")

	// Create right hand side range descriptor with the newly-allocated Range ID.
	rightDesc, err := r.store.NewRangeDescriptor(ctx, splitKey, desc.EndKey, desc.Replicas)
	if err != nil {
		return reply, errors.Errorf("unable to allocate right hand side range descriptor: %s", err)
	}

	// Init updated version of existing range descriptor.
	leftDesc := *desc
	if r.ClusterSettings().Version.IsMinSupported(cluster.VersionRangeMerges) {
		// To be safe, don't increment the generation counter unless all nodes are
		// known to be aware of its existence. Since encoded range descriptors are
		// used in CPut operations, it is theorized that non-nil generation counters
		// in mixed-version clusters could cause subtle breakage.
		leftDesc.IncrementGeneration()
	}
	leftDesc.EndKey = splitKey

	log.Infof(ctx, "initiating a split of this range at key %s [r%d]",
		splitKey, rightDesc.RangeID)

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
			err = errors.Wrap(err, msg)
		}
		return reply, errors.Wrapf(err, "split at key %s failed", splitKey)
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
	ctx context.Context, args roachpb.AdminMergeRequest,
) (roachpb.AdminMergeResponse, *roachpb.Error) {
	var reply roachpb.AdminMergeResponse

	if err := r.ClusterSettings().Version.CheckVersion(cluster.VersionRangeMerges, "range merges"); err != nil {
		return reply, roachpb.NewError(err)
	}

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

	updatedLeftDesc := *origLeftDesc
	rightDescKey := keys.RangeDescriptorKey(origLeftDesc.EndKey)

	// Lookup right hand side range (subsumed). This really belongs
	// inside the transaction for consistency, but it is important (for
	// transaction record placement) that the first action inside the
	// transaction is the conditional put to change the left hand side's
	// descriptor end key. We look up the descriptor here only to get
	// the new end key and then repeat the lookup inside the
	// transaction.
	{
		var rightDesc roachpb.RangeDescriptor
		if err := r.store.DB().GetProto(ctx, rightDescKey, &rightDesc); err != nil {
			return reply, roachpb.NewError(err)
		}

		updatedLeftDesc.IncrementGeneration()
		updatedLeftDesc.EndKey = rightDesc.EndKey
		log.Infof(ctx, "initiating a merge of %s into this range", rightDesc)
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

		// Update the range descriptor for the receiving range.
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

		// Do a consistent read of the right hand side's range descriptor.
		var rightDesc roachpb.RangeDescriptor
		if err := txn.GetProto(ctx, rightDescKey, &rightDesc); err != nil {
			return err
		}

		// Verify that the two ranges are mergeable.
		if !bytes.Equal(origLeftDesc.EndKey, rightDesc.StartKey) {
			// Should never happen, but just in case.
			return errors.Errorf("ranges are not adjacent; %s != %s", origLeftDesc.EndKey, rightDesc.StartKey)
		}
		if !bytes.Equal(rightDesc.EndKey, updatedLeftDesc.EndKey) {
			// This merge raced with a split of the right-hand range.
			// TODO(bdarnell): needs a test.
			return errors.Errorf("range changed during merge; %s != %s", rightDesc.EndKey, updatedLeftDesc.EndKey)
		}
		if !replicaSetsEqual(origLeftDesc.Replicas, rightDesc.Replicas) {
			return errors.Errorf("ranges not collocated; %s != %s", origLeftDesc.Replicas, rightDesc.Replicas)
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
				LeftRange:     *origLeftDesc,
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
		if _, canRetry := errors.Cause(err).(*roachpb.HandledRetryableTxnError); !canRetry {
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
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	g := ctxgroup.WithContext(ctx)
	for _, repl := range desc.Replicas {
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
}

// waitForReplicasInit blocks until it has proof that the replicas listed in
// desc are initialized on their respective stores. It may return a false
// negative, i.e., claim that a replica is uninitialized when it is, in fact,
// initialized, but it will never return a false positive.
func waitForReplicasInit(
	ctx context.Context, dialer *nodedialer.Dialer, desc roachpb.RangeDescriptor,
) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	g := ctxgroup.WithContext(ctx)
	for _, repl := range desc.Replicas {
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
}

type snapshotError struct {
	cause error
}

func (s *snapshotError) Error() string {
	return fmt.Sprintf("snapshot failed: %s", s.cause.Error())
}

// IsSnapshotError returns true iff the error indicates a preemptive
// snapshot failed.
func IsSnapshotError(err error) bool {
	_, ok := err.(*snapshotError)
	return ok
}

// ChangeReplicas adds or removes a replica of a range. The change is performed
// in a distributed transaction and takes effect when that transaction is committed.
// When removing a replica, only the NodeID and StoreID fields of the Replica are used.
//
// The supplied RangeDescriptor is used as a form of optimistic lock. See the
// comment of "adminSplitWithDescriptor" for more information on this pattern.
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
// slower base. In order to avoid having this background replication overwhelm
// the system, replication is throttled via a reservation system. When
// allocating a new replica for a range, the replicate queue reserves space for
// that replica on the target store via a ReservationRequest. (See
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
) error {
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
) error {
	repDesc := roachpb.ReplicaDescriptor{
		NodeID:  target.NodeID,
		StoreID: target.StoreID,
	}
	repDescIdx := -1  // tracks NodeID && StoreID
	nodeUsed := false // tracks NodeID only
	for i, existingRep := range desc.Replicas {
		nodeUsedByExistingRep := existingRep.NodeID == repDesc.NodeID
		nodeUsed = nodeUsed || nodeUsedByExistingRep

		if nodeUsedByExistingRep && existingRep.StoreID == repDesc.StoreID {
			repDescIdx = i
			repDesc.ReplicaID = existingRep.ReplicaID
			break
		}
	}

	rangeID := desc.RangeID
	updatedDesc := *desc
	updatedDesc.Replicas = append([]roachpb.ReplicaDescriptor(nil), desc.Replicas...)

	switch changeType {
	case roachpb.ADD_REPLICA:
		// If the replica exists on the remote node, no matter in which store,
		// abort the replica add.
		if nodeUsed {
			if repDescIdx != -1 {
				return errors.Errorf("%s: unable to add replica %v which is already present", r, repDesc)
			}
			return errors.Errorf("%s: unable to add replica %v; node already has a replica", r, repDesc)
		}

		// Prohibit premature raft log truncation. We set the pending index to 1
		// here until we determine what it is below. This removes a small window of
		// opportunity for the raft log to get truncated after the snapshot is
		// generated.
		if err := r.setPendingSnapshotIndex(1); err != nil {
			return err
		}
		defer r.clearPendingSnapshotIndex()

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
			return err
		}

		repDesc.ReplicaID = updatedDesc.NextReplicaID
		updatedDesc.NextReplicaID++
		updatedDesc.Replicas = append(updatedDesc.Replicas, repDesc)

	case roachpb.REMOVE_REPLICA:
		// If that exact node-store combination does not have the replica,
		// abort the removal.
		if repDescIdx == -1 {
			return errors.Errorf("%s: unable to remove replica %v which is not present", r, repDesc)
		}
		updatedDesc.Replicas[repDescIdx] = updatedDesc.Replicas[len(updatedDesc.Replicas)-1]
		updatedDesc.Replicas = updatedDesc.Replicas[:len(updatedDesc.Replicas)-1]
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
					UpdatedReplicas: updatedDesc.Replicas,
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
			return errors.Wrapf(err, "change replicas of r%d failed: %s", rangeID, msg)
		}
		return errors.Wrapf(err, "change replicas of r%d failed", rangeID)
	}
	log.Event(ctx, "txn complete")
	return nil
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

	if snapType == snapTypePreemptive {
		if err := r.setPendingSnapshotIndex(snap.RaftSnap.Metadata.Index); err != nil {
			return err
		}
	}

	status := r.RaftStatus()
	if status == nil {
		return errors.New("raft status not initialized")
	}

	req := SnapshotRequest_Header{
		State: snap.State,
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
			return err
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
	// Step 1: Compute which replicas are to be added and which are to be removed.
	//
	// TODO(radu): we can't have multiple replicas on different stores on the
	// same node, and this code doesn't do anything to specifically avoid that
	// case (although the allocator will avoid even trying to send snapshots to
	// such stores), so it could cause some failures.

	var addTargets []roachpb.ReplicaDescriptor
	for _, t := range targets {
		found := false
		for _, replicaDesc := range rangeDesc.Replicas {
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
	for _, replicaDesc := range rangeDesc.Replicas {
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
			ctx, rangeDesc.StartKey.AsRawKey(), targets[0].StoreID,
		); err != nil {
			log.Warningf(ctx, "while transferring lease: %s", err)
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

	// Deep-copy the Replicas slice (in our shallow copy of the RangeDescriptor)
	// since we'll mutate it in the loop below.
	desc := rangeDesc
	desc.Replicas = append([]roachpb.ReplicaDescriptor(nil), desc.Replicas...)
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
				rangeInfo.Desc.Replicas,
				rangeInfo,
				s.allocator.scorerOptions())
			if targetStore == nil {
				return fmt.Errorf("none of the remaining targets %v are legal additions to %v",
					addTargets, rangeInfo.Desc.Replicas)
			}

			target := roachpb.ReplicationTarget{
				NodeID:  targetStore.Node.NodeID,
				StoreID: targetStore.StoreID,
			}
			if err := s.DB().AdminChangeReplicas(
				ctx, rangeDesc.StartKey.AsRawKey(), roachpb.ADD_REPLICA, []roachpb.ReplicationTarget{target},
			); err != nil {
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
			rangeInfo.Desc.Replicas = append(rangeInfo.Desc.Replicas, roachpb.ReplicaDescriptor{
				NodeID:  target.NodeID,
				StoreID: target.StoreID,
			})
		}

		if len(removeTargets) > 0 && len(removeTargets) > len(addTargets) {
			targetStore, _, err := s.allocator.RemoveTarget(ctx, zone, removeTargets, rangeInfo)
			if err != nil {
				return errors.Wrapf(err, "unable to select removal target from %v; current replicas %v",
					removeTargets, rangeInfo.Desc.Replicas)
			}
			target := roachpb.ReplicationTarget{
				NodeID:  targetStore.NodeID,
				StoreID: targetStore.StoreID,
			}
			// Note that attempting to remove the leaseholder won't work, so transfer
			// the lease first in such scenarios. The first specified target should be
			// the leaseholder now, so we can always transfer the lease there.
			transferLease()
			if err := s.DB().AdminChangeReplicas(
				ctx, rangeDesc.StartKey.AsRawKey(), roachpb.REMOVE_REPLICA, []roachpb.ReplicationTarget{target},
			); err != nil {
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
			rangeInfo.Desc.Replicas = removeTargetFromSlice(rangeInfo.Desc.Replicas, target)
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
	sysCfg := r.store.cfg.Gossip.GetSystemConfig()
	if sysCfg == nil {
		log.Infof(ctx, "scatter failed (system config not yet available)")
		return roachpb.AdminScatterResponse{}, errors.New("system config not yet available")
	}

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
		requeue, err := rq.processOneChange(ctx, r, sysCfg, canTransferLease, false /* dryRun */)
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
		newLeaseholderIdx := rand.Intn(len(desc.Replicas))
		targetStoreID := desc.Replicas[newLeaseholderIdx].StoreID
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
