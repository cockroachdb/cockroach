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
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/errors"
)

// storage implements the operations for acquiring and releasing leases and
// publishing a new version of a descriptor.
// TODO (lucy,ajwerner): This could go in its own package and expose an API for
// the manager. Some of these fields belong on the manager, in any case, since
// they're only used by the manager and not by the store itself.
type storage struct {
	nodeIDContainer  *base.SQLIDContainer
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

// LeaseRenewalDuration controls the default time before a lease expires when
// acquisition to renew the lease begins.
var LeaseRenewalDuration = settings.RegisterDurationSetting(
	"sql.catalog.descriptor_lease_renewal_fraction",
	"controls the default time before a lease expires when acquisition to renew the lease begins",
	base.DefaultDescriptorLeaseRenewalTimeout)

func (s storage) leaseRenewalTimeout() time.Duration {
	return LeaseRenewalDuration.Get(&s.settings.SV)
}

// jitteredLeaseDuration returns a randomly jittered duration from the interval
// [(1-leaseJitterFraction) * leaseDuration, (1+leaseJitterFraction) * leaseDuration].
func (s storage) jitteredLeaseDuration() time.Duration {
	leaseDuration := LeaseDuration.Get(&s.settings.SV)
	jitterFraction := LeaseJitterFraction.Get(&s.settings.SV)
	return time.Duration(float64(leaseDuration) * (1 - jitterFraction +
		2*jitterFraction*rand.Float64()))
}

// acquire a lease on the most recent version of a descriptor. If the lease
// cannot be obtained because the descriptor is in the process of being dropped
// or offline (currently only applicable to tables), the error will be of type
// inactiveTableError. The expiration time set for the lease > minExpiration.
func (s storage) acquire(
	ctx context.Context, minExpiration hlc.Timestamp, id descpb.ID,
) (desc catalog.Descriptor, expiration hlc.Timestamp, _ error) {
	err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		// Run the descriptor read as high-priority, thereby pushing any intents out
		// of its way. We don't want schema changes to prevent lease acquisitions;
		// we'd rather force them to refresh. Also this prevents deadlocks in cases
		// where the name resolution is triggered by the transaction doing the
		// schema change itself.
		if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
			return err
		}
		expiration = txn.ReadTimestamp().Add(int64(s.jitteredLeaseDuration()), 0)
		if expiration.LessEq(minExpiration) {
			// In the rare circumstances where expiration <= minExpiration
			// use an expiration based on the minExpiration to guarantee
			// a monotonically increasing expiration.
			expiration = minExpiration.Add(int64(time.Millisecond), 0)
		}

		// TODO (lucy): Previously this called getTableDescFromID followed by a call
		// to ValidateSelf() instead of Validate(), to avoid the cross-table
		// checks. Does this actually matter? We already potentially do cross-table
		// checks when populating pre-19.2 foreign keys.
		desc, err = catalogkv.MustGetDescriptorByID(ctx, txn, s.codec, id)
		if err != nil {
			return err
		}
		if err := catalog.FilterDescriptorState(
			desc, tree.CommonLookupFlags{IncludeOffline: true}, // filter dropped only
		); err != nil {
			return err
		}
		log.VEventf(ctx, 2, "storage acquired lease %v@%v", desc, expiration)
		nodeID := s.nodeIDContainer.SQLInstanceID()
		if nodeID == 0 {
			panic("zero nodeID")
		}

		// We use string interpolation here, instead of passing the arguments to
		// InternalExecutor.Exec() because we don't want to pay for preparing the
		// statement (which would happen if we'd pass arguments). Besides the
		// general cost of preparing, preparing this statement always requires a
		// read from the database for the special descriptor of a system table
		// (#23937).
		ts := storedLeaseExpiration(expiration)
		insertLease := fmt.Sprintf(
			`INSERT INTO system.public.lease ("descID", version, "nodeID", expiration) VALUES (%d, %d, %d, %s)`,
			desc.GetID(), desc.GetVersion(), nodeID, &ts,
		)
		count, err := s.internalExecutor.Exec(ctx, "lease-insert", txn, insertLease)
		if err != nil {
			return err
		}
		if count != 1 {
			return errors.Errorf("%s: expected 1 result, found %d", insertLease, count)
		}
		s.outstandingLeases.Inc(1)
		return nil
	})
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	if s.testingKnobs.LeaseAcquiredEvent != nil {
		s.testingKnobs.LeaseAcquiredEvent(desc, err)
	}
	return desc, expiration, nil
}

// Release a previously acquired descriptor. Never let this method
// read a descriptor because it can be called while modifying a
// descriptor through a schema change before the schema change has committed
// that can result in a deadlock.
func (s storage) release(ctx context.Context, stopper *stop.Stopper, lease *storedLease) {
	retryOptions := base.DefaultRetryOptions()
	retryOptions.Closer = stopper.ShouldQuiesce()
	firstAttempt := true
	// This transaction is idempotent; the retry was put in place because of
	// NodeUnavailableErrors.
	for r := retry.Start(retryOptions); r.Next(); {
		log.VEventf(ctx, 2, "storage releasing lease %+v", lease)
		nodeID := s.nodeIDContainer.SQLInstanceID()
		if nodeID == 0 {
			panic("zero nodeID")
		}
		const deleteLease = `DELETE FROM system.public.lease ` +
			`WHERE ("descID", version, "nodeID", expiration) = ($1, $2, $3, $4)`
		count, err := s.internalExecutor.Exec(
			ctx,
			"lease-release",
			nil, /* txn */
			deleteLease,
			lease.id, lease.version, nodeID, &lease.expiration,
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
	err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		prevTimestamp := expiration.Prev()
		txn.SetFixedTimestamp(ctx, prevTimestamp)
		desc, err = catalogkv.MustGetDescriptorByID(ctx, txn, s.codec, id)
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
