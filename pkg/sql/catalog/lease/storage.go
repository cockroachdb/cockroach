// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lease

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// storage implements the operations for acquiring and releasing leases and
// publishing a new version of a descriptor.
// TODO (lucy,ajwerner): This could go in its own package and expose an API for
// the manager. Some of these fields belong on the manager, in any case, since
// they're only used by the manager and not by the store itself.
type storage struct {
	nodeIDContainer  *base.SQLIDContainer
	instance         sqlliveness.Instance
	db               *kv.DB
	clock            *hlc.Clock
	internalExecutor sqlutil.InternalExecutor
	settings         *cluster.Settings
	codec            keys.SQLCodec

	// group is used for all calls made to acquireNodeLease to prevent
	// concurrent lease acquisitions from the store.
	group *singleflight.Group

	outstandingLeases *metric.Gauge
	testingKnobs      StorageTestingKnobs
}

var sqllivenessRetryError = errors.Errorf("sqlliveness session invalid for transaction")

// acquire a lease on the most recent version of a descriptor. If the lease
// cannot be obtained because the descriptor is in the process of being dropped
// or offline (currently only applicable to tables), the error will be of type
// inactiveTableError. The expiration time set for the lease > minExpiration.
func (s storage) acquire(
	ctx context.Context, id descpb.ID,
) (desc catalog.Descriptor, session sqlliveness.Session, _ error) {
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	acquireInTxn := func(ctx context.Context, txn *kv.Txn) (err error) {

		// Run the descriptor read as high-priority, thereby pushing any intents out
		// of its way. We don't want schema changes to prevent lease acquisitions;
		// we'd rather force them to refresh. Also this prevents deadlocks in cases
		// where the name resolution is triggered by the transaction doing the
		// schema change itself.
		if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
			return err
		}

		instanceID := s.nodeIDContainer.SQLInstanceID()
		if instanceID == 0 {
			panic("SQL instance ID not set")
		}

		session, err = s.instance.Session(ctx)
		if err != nil {
			return err
		}
		if txn.ProvisionalCommitTimestamp().Less(session.Start()) {
			return sqllivenessRetryError
		}
		if err = txn.UpdateDeadline(ctx, session.Expiration()); err != nil {
			return sqllivenessRetryError
		}
		version := s.settings.Version.ActiveVersion(ctx)
		desc, err = catkv.MustGetDescriptorByID(ctx, version, s.codec, txn, nil /* vd */, id, catalog.Any)
		if err != nil {
			return err
		}
		if err := catalog.FilterDescriptorState(
			desc, tree.CommonLookupFlags{IncludeOffline: true}, // filter dropped only
		); err != nil {
			return err
		}
		log.VEventf(ctx, 2, "storage attempting to acquire lease %v@%v", desc, session.Expiration())

		// We use string interpolation here, instead of passing the arguments to
		// InternalExecutor.Exec() because we don't want to pay for preparing the
		// statement (which would happen if we'd pass arguments). Besides the
		// general cost of preparing, preparing this statement always requires a
		// read from the database for the special descriptor of a system table
		// (#23937).
		ts := tree.DTimestamp{Time: timeutil.Unix(0, 0)}
		insertLease := fmt.Sprintf(
			`UPSERT INTO system.public.lease ("descID", version, "nodeID", expiration, "sessionID") VALUES (%d, %d, %d, %s, '\x%s')`,
			desc.GetID(), desc.GetVersion(), instanceID, &ts, session.ID(),
		)
		count, err := s.internalExecutor.Exec(ctx, "lease-insert", txn, insertLease)
		if err != nil {
			return err
		}
		if count != 1 {
			return errors.Errorf("%s: expected 1 result, found %d", insertLease, count)
		}
		return nil
	}

	// Run a retry loop to deal with AmbiguousResultErrors. All other error types
	// are propagated up to the caller.
	for r := retry.StartWithCtx(ctx, retry.Options{}); r.Next(); {
		err := s.db.Txn(ctx, acquireInTxn)
		var pErr *roachpb.AmbiguousResultError
		switch {
		case errors.As(err, &pErr):
			log.Infof(ctx, "ambiguous error occurred during lease acquisition for %v, retrying: %v", id, err)
			continue
		case errors.Is(err, sqllivenessRetryError):
			continue
		case err != nil:
			return nil, nil, err
		}
		log.VEventf(ctx, 2, "storage acquired lease %v@%v", desc, session.Expiration())
		if s.testingKnobs.LeaseAcquiredEvent != nil {
			s.testingKnobs.LeaseAcquiredEvent(desc, err)
		}
		s.outstandingLeases.Inc(1)
		return desc, session, nil
	}
	return nil, nil, ctx.Err()
}

// Release a previously acquired descriptor. Never let this method
// read a descriptor because it can be called while modifying a
// descriptor through a schema change before the schema change has committed
// that can result in a deadlock.
func (s storage) release(ctx context.Context, stopper *stop.Stopper, lease *storedLease) {
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	retryOptions := base.DefaultRetryOptions()
	retryOptions.Closer = stopper.ShouldQuiesce()
	firstAttempt := true
	// This transaction is idempotent; the retry was put in place because of
	// NodeUnavailableErrors.
	for r := retry.StartWithCtx(ctx, retryOptions); r.Next(); {
		log.VEventf(ctx, 2, "storage releasing lease %+v", lease)
		instanceID := s.nodeIDContainer.SQLInstanceID()
		if instanceID == 0 {
			panic("SQL instance ID not set")
		}
		const deleteLease = `DELETE FROM system.public.lease ` +
			`WHERE ("descID", version, "nodeID", "sessionID") = ($1, $2, $3, $4)`
		count, err := s.internalExecutor.Exec(
			ctx,
			"lease-release",
			nil, /* txn */
			deleteLease,
			lease.id, lease.version, instanceID, lease.sessionID.UnsafeBytes(),
		)
		if err != nil {
			log.Warningf(ctx, "error releasing lease %q: %s", lease, err)
			if grpcutil.IsConnectionRejected(err) {
				return
			}
			firstAttempt = false
			continue
		}
		// We allow count == 0 after the first attempt.
		if count > 1 || (count == 0 && firstAttempt) {
			log.Warningf(ctx, "unexpected results while deleting lease %+v: "+
				"expected 1 result, found %d", lease, count)
		}

		s.outstandingLeases.Dec(1)
		if s.testingKnobs.LeaseReleasedEvent != nil {
			s.testingKnobs.LeaseReleasedEvent(
				lease.id, descpb.DescriptorVersion(lease.version), err)
		}
		break
	}
}

// Get the descriptor valid for the expiration time from the store.
// We use a timestamp that is just less than the expiration time to read
// a version of the descriptor. A descriptorVersionState with the
// expiration time set to expiration is returned.
//
// This returns an error when Replica.checkTSAboveGCThresholdRLocked()
// returns an error when the expiration timestamp is less than the storage
// layer GC threshold.
func (s storage) getForExpiration(
	ctx context.Context, expiration hlc.Timestamp, id descpb.ID,
) (catalog.Descriptor, error) {
	var desc catalog.Descriptor
	err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		prevTimestamp := expiration.Prev()
		err := txn.SetFixedTimestamp(ctx, prevTimestamp)
		if err != nil {
			return err
		}
		version := s.settings.Version.ActiveVersion(ctx)
		desc, err = catkv.MustGetDescriptorByID(ctx, version, s.codec, txn, nil /* vd */, id, catalog.Any)
		if err != nil {
			return err
		}
		if prevTimestamp.LessEq(desc.GetModificationTime()) {
			return errors.AssertionFailedf("unable to read descriptor"+
				" (%d, %s) found descriptor with modificationTime %s",
				id, expiration, desc.GetModificationTime())
		}
		// Create a descriptorVersionState with the descriptor and without a lease.
		return nil
	})
	return desc, err
}
