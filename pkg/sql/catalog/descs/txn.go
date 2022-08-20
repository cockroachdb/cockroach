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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// Txn enables callers to run transactions with a *Collection such that all
// retrieved immutable descriptors are properly leased and all mutable
// descriptors are handled. The function deals with verifying the two version
// invariant and retrying when it is violated. Callers need not worry that they
// write mutable descriptors multiple times. The call will explicitly wait for
// the leases to drain on old versions of descriptors modified or deleted in the
// transaction; callers do not need to call lease.WaitForOneVersion.
//
// The passed transaction is pre-emptively anchored to the system config key on
// the system tenant.
// Deprecated: Use cf.TxnWithExecutor().
func (cf *CollectionFactory) Txn(
	ctx context.Context,
	db *kv.DB,
	f func(ctx context.Context, txn *kv.Txn, descriptors *Collection) error,
	opts ...TxnOption,
) error {
	return cf.TxnWithExecutor(ctx, db, nil /* sessionData */, func(
		ctx context.Context, txn *kv.Txn, descriptors *Collection, _ sqlutil.InternalExecutor,
	) error {
		return f(ctx, txn, descriptors)
	}, opts...)
}

// TxnOption is used to configure a Txn or TxnWithExecutor.
type TxnOption interface {
	apply(*txnConfig)
}

type txnConfig struct {
	steppingEnabled bool
}

type txnOptionFn func(options *txnConfig)

func (f txnOptionFn) apply(options *txnConfig) { f(options) }

var steppingEnabled = txnOptionFn(func(o *txnConfig) {
	o.steppingEnabled = true
})

// SteppingEnabled creates a TxnOption to determine whether the underlying
// transaction should have stepping enabled. If stepping is enabled, the
// transaction will implicitly use lower admission priority. However, the
// user will need to remember to Step the Txn to make writes visible. The
// InternalExecutor will automatically (for better or for worse) step the
// transaction when executing each statement.
func SteppingEnabled() TxnOption {
	return steppingEnabled
}

// TxnWithExecutorFunc is used to run a transaction in the context of a
// Collection and an InternalExecutor.
type TxnWithExecutorFunc = func(
	ctx context.Context,
	txn *kv.Txn,
	descriptors *Collection,
	ie sqlutil.InternalExecutor,
) error

// TxnWithExecutor enables callers to run transactions with a *Collection such that all
// retrieved immutable descriptors are properly leased and all mutable
// descriptors are handled. The function deals with verifying the two version
// invariant and retrying when it is violated. Callers need not worry that they
// write mutable descriptors multiple times. The call will explicitly wait for
// the leases to drain on old versions of descriptors modified or deleted in the
// transaction; callers do not need to call lease.WaitForOneVersion.
// It also enables using internal executor to run sql queries in a txn manner.
//
// The passed transaction is pre-emptively anchored to the system config key on
// the system tenant.
func (cf *CollectionFactory) TxnWithExecutor(
	ctx context.Context,
	db *kv.DB,
	sd *sessiondata.SessionData,
	f TxnWithExecutorFunc,
	opts ...TxnOption,
) error {
	var config txnConfig
	for _, opt := range opts {
		opt.apply(&config)
	}
	run := db.Txn
	if config.steppingEnabled {
		type kvTxnFunc = func(context.Context, *kv.Txn) error
		run = func(ctx context.Context, f kvTxnFunc) error {
			return db.TxnWithSteppingEnabled(ctx, sessiondatapb.Normal, f)
		}
	}

	// Waits for descriptors that were modified, skipping
	// over ones that had their descriptor wiped.
	waitForDescriptors := func(modifiedDescriptors []lease.IDVersion, deletedDescs catalog.DescriptorIDSet) error {
		// Wait for a single version on leased descriptors.
		for _, ld := range modifiedDescriptors {
			waitForNoVersion := deletedDescs.Contains(ld.ID)
			retryOpts := retry.Options{
				InitialBackoff: time.Millisecond,
				Multiplier:     1.5,
				MaxBackoff:     time.Second,
			}
			// Detect unpublished ones.
			if waitForNoVersion {
				err := cf.leaseMgr.WaitForNoVersion(ctx, ld.ID, retryOpts)
				if err != nil {
					return err
				}
			} else {
				_, err := cf.leaseMgr.WaitForOneVersion(ctx, ld.ID, retryOpts)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
	for {
		var modifiedDescriptors []lease.IDVersion
		var deletedDescs catalog.DescriptorIDSet
		if err := run(ctx, func(ctx context.Context, txn *kv.Txn) error {
			modifiedDescriptors, deletedDescs = nil, catalog.DescriptorIDSet{}
			descsCol := cf.NewCollection(
				ctx, nil, /* temporarySchemaProvider */
				cf.ieFactoryWithTxn.MemoryMonitor(),
			)
			defer descsCol.ReleaseAll(ctx)
			ie, commitTxnFn := cf.ieFactoryWithTxn.NewInternalExecutorWithTxn(sd, &cf.settings.SV, txn, descsCol)
			if err := f(ctx, txn, descsCol, ie); err != nil {
				return err
			}
			deletedDescs = descsCol.deletedDescs
			modifiedDescriptors = descsCol.GetDescriptorsWithNewVersion()
			return commitTxnFn(ctx)
		}); IsTwoVersionInvariantViolationError(err) {
			continue
		} else {
			if err == nil {
				err = waitForDescriptors(modifiedDescriptors, deletedDescs)
			}
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
) error {
	withNewVersion := descsCol.GetDescriptorsWithNewVersion()
	if withNewVersion == nil {
		return nil
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
	descsCol.ReleaseSpecifiedLeases(ctx, withNewVersion)

	// We know that so long as there are no leases on the updated descriptors as of
	// the current provisional commit timestamp for this transaction then if this
	// transaction ends up committing then there won't have been any created
	// in the meantime.
	count, err := lease.CountLeases(
		ctx, ie, withNewVersion, txn.ProvisionalCommitTimestamp(),
	)
	if err != nil {
		return err
	}
	if count == 0 {
		// This is the last step before committing a transaction which modifies
		// descriptors. This is a perfect time to refresh the deadline prior to
		// committing.
		return descsCol.MaybeUpdateDeadline(ctx, txn)
	}

	// We abort the transaction to not hold on to locks while we wait
	// for the excess version leases to be dropped. We'll return a
	// sentinel error to the client to indicate that it should restart.
	if err := txn.Rollback(ctx); err != nil {
		return errors.Wrap(err, "rolling back due to two-version invariant violation")
	}

	// Release the rest of our leases on unmodified descriptors so we don't hold
	// up schema changes there and potentially create a deadlock.
	descsCol.ReleaseLeases(ctx)

	// Wait until all older version leases have been released or expired.
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		// Use the current clock time.
		now := clock.Now()
		count, err := lease.CountLeases(ctx, ie, withNewVersion, now)
		if err != nil {
			return err
		}
		if count == 0 {
			break
		}
		if onRetryBackoff != nil {
			onRetryBackoff()
		}
	}
	return &twoVersionInvariantViolationError{
		ids: withNewVersion,
	}
}

// CheckSpanCountLimit checks whether committing the set of uncommitted tables
// would exceed the span count limit we're allowed (applicable only to secondary
// tenants).
func CheckSpanCountLimit(
	ctx context.Context,
	descsCol *Collection,
	splitter spanconfig.Splitter,
	limiter spanconfig.Limiter,
	txn *kv.Txn,
) error {
	if !descsCol.codec().ForSystemTenant() {
		var totalSpanCountDelta int
		for _, ut := range descsCol.GetUncommittedTables() {
			uncommittedMutTable, err := descsCol.GetUncommittedMutableTableByID(ut.GetID())
			if err != nil {
				return err
			}

			var originalTableDesc catalog.TableDescriptor
			if originalDesc := uncommittedMutTable.OriginalDescriptor(); originalDesc != nil {
				originalTableDesc = originalDesc.(catalog.TableDescriptor)
			}
			delta, err := spanconfig.Delta(ctx, splitter, originalTableDesc, uncommittedMutTable)
			if err != nil {
				return err
			}
			totalSpanCountDelta += delta
		}

		shouldLimit, err := limiter.ShouldLimit(ctx, txn, totalSpanCountDelta)
		if err != nil {
			return err
		}
		if shouldLimit {
			return pgerror.New(pgcode.ConfigurationLimitExceeded, "exceeded limit for number of table spans")
		}
	}

	return nil
}
