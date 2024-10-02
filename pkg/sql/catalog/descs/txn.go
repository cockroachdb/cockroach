// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/regionliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

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
	db isql.DB,
	codec keys.SQLCodec,
	descsCol *Collection,
	regions regionliveness.CachedDatabaseRegions,
	settings *clustersettings.Settings,
	txn *kv.Txn,
	onRetryBackoff func(),
) error {
	withNewVersion, err := descsCol.GetOriginalPreviousIDVersionsForUncommitted()
	if err != nil || withNewVersion == nil {
		return err
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
		ctx, db, codec, regions, settings, withNewVersion, txn.ProvisionalCommitTimestamp(), false, /*forAnyVersion*/
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

	// Increment the long wait gauge for two version invariant violations, if this
	// function takes longer than the lease duration.
	decAfterWait := descsCol.leased.lm.IncGaugeAfterLeaseDuration(lease.GaugeWaitForTwoVersionViolation)
	defer decAfterWait()
	// Wait until all older version leases have been released or expired.
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		// Use the current clock time.
		now := clock.Now()
		count, err := lease.CountLeases(ctx, db, codec, regions, settings, withNewVersion, now, false /*forAnyVersion*/)
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
			originalTableDesc, uncommittedMutTable, err := descsCol.GetUncommittedMutableTableByID(ut.GetID())
			if err != nil {
				return err
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
