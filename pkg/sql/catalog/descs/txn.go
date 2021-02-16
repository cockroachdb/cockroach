// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

var errTwoVersionInvariantViolated = errors.Errorf("two version invariant violated")

// Txn enables callers to run transactions with a *Collection such that all
// retrieved immutable descriptors are properly leased and all mutable
// descriptors are handled. The function deals with verifying the two version
// invariant and retrying when it is violated. Callers need not worry that they
// write mutable descriptors multiple times.
//
// The passed transaction is pre-emptively anchored to the system config key on
// the system tenant.
func Txn(
	ctx context.Context,
	settings *cluster.Settings,
	leaseMgr *lease.Manager,
	ie sqlutil.InternalExecutor,
	db *kv.DB,
	f func(ctx context.Context, txn *kv.Txn, descriptors *Collection) error,
) error {
	descsCol := NewCollection(settings, leaseMgr, nil /* hydratedTables */)
	for {
		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			defer descsCol.ReleaseAll(ctx)
			if err := txn.SetSystemConfigTrigger(leaseMgr.Codec().ForSystemTenant()); err != nil {
				return err
			}
			if err := f(ctx, txn, descsCol); err != nil {
				return err
			}
			if err := descsCol.ValidateUncommittedDescriptors(ctx, txn); err != nil {
				return err
			}
			retryErr, err := CheckTwoVersionInvariant(
				ctx, db.Clock(), ie, descsCol, txn, nil /* onRetryBackoff */)
			if retryErr {
				return errTwoVersionInvariantViolated
			}
			return err
		}); errors.Is(err, errTwoVersionInvariantViolated) {
			continue
		} else {
			return err
		}
	}
}

// CheckTwoVersionInvariant checks whether any new schema being modified written
// at a version V has only valid leases at version = V - 1. A transaction retry
// error as well as a boolean is returned whenever the invariant is violated.
// Before returning the retry error the current transaction is rolled-back and
// the function waits until there are only outstanding leases on the current
// version. This affords the retry to succeed in the event that there are no
// other schema changes simultaneously contending with this txn.
//
// checkDescriptorTwoVersionInvariant blocks until it's legal for the modified
// descriptors (if any) to be committed.
//
// Reminder: a descriptor version v can only be written at a timestamp
// that's not covered by a lease on version v-2. So, if the current
// txn wants to write some updated descriptors, it needs
// to wait until all incompatible leases are revoked or expire. If
// incompatible leases exist, we'll block waiting for these leases to
// go away. Then, the transaction is restarted by generating a retriable error.
// Note that we're relying on the fact that the number of conflicting
// leases will only go down over time: no new conflicting leases can be
// created as of the time of this call because v-2 can't be leased once
// v-1 exists.
//
// If this method succeeds it is the caller's responsibility to release the
// executor's leases after the txn commits so that schema changes can
// proceed.
func CheckTwoVersionInvariant(
	ctx context.Context,
	clock *hlc.Clock,
	ie sqlutil.InternalExecutor,
	descsCol *Collection,
	txn *kv.Txn,
	onRetryBackoff func(),
) (retryDueToViolation bool, _ error) {
	descs := descsCol.GetDescriptorsWithNewVersion()
	if descs == nil {
		return false, nil
	}
	if txn.IsCommitted() {
		panic("transaction has already committed")
	}

	// We potentially hold leases for descriptors which we've modified which
	// we need to drop. Say we're updating descriptors at version V. All leases
	// for version V-2 need to be dropped immediately, otherwise the check
	// below that nobody holds leases for version V-2 will fail. Worse yet,
	// the code below loops waiting for nobody to hold leases on V-2. We also
	// may hold leases for version V-1 of modified descriptors that are good to drop
	// but not as vital for correctness. It's good to drop them because as soon
	// as this transaction commits jobs may start and will need to wait until
	// the lease expires. It is safe because V-1 must remain valid until this
	// transaction commits; if we commit then nobody else could have written
	// a new V beneath us because we've already laid down an intent.
	//
	// All this being said, we must retain our leases on descriptors which we have
	// not modified to ensure that our writes to those other descriptors in this
	// transaction remain valid.
	descsCol.ReleaseSpecifiedLeases(ctx, descs)

	// We know that so long as there are no leases on the updated descriptors as of
	// the current provisional commit timestamp for this transaction then if this
	// transaction ends up committing then there won't have been any created
	// in the meantime.
	count, err := lease.CountLeases(ctx, ie, descs, txn.ProvisionalCommitTimestamp())
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}

	// Restart the transaction so that it is able to replay itself at a newer timestamp
	// with the hope that the next time around there will be leases only at the current
	// version.
	retryErr := txn.PrepareRetryableError(ctx,
		fmt.Sprintf(
			`cannot publish new versions for descriptors: %v, old versions still in use`,
			descs))
	// We cleanup the transaction and create a new transaction after
	// waiting for the invariant to be satisfied because the wait time
	// might be extensive and intents can block out leases being created
	// on a descriptor.
	//
	// TODO(vivek): Change this to restart a txn while fixing #20526 . All the
	// descriptor intents can be laid down here after the invariant
	// has been checked.
	txn.CleanupOnError(ctx, retryErr)
	// Release the rest of our leases on unmodified descriptors so we don't hold
	// up schema changes there and potentially create a deadlock.
	descsCol.ReleaseLeases(ctx)

	// Wait until all older version leases have been released or expired.
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		// Use the current clock time.
		now := clock.Now()
		count, err := lease.CountLeases(ctx, ie, descs, now)
		if err != nil {
			return false, err
		}
		if count == 0 {
			break
		}
		if onRetryBackoff != nil {
			onRetryBackoff()
		}
	}
	return true, retryErr
}
