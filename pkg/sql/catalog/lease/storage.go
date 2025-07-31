// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

import (
	"context"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slbase"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
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
	db               isql.DB
	clock            *hlc.Clock
	settings         *cluster.Settings
	codec            keys.SQLCodec
	regionPrefix     *atomic.Value
	sysDBCache       *catkv.SystemDatabaseCache
	livenessProvider sqlliveness.Provider

	// group is used for all calls made to acquireNodeLease to prevent
	// concurrent lease acquisitions from the store.
	group *singleflight.Group

	leasingMetrics
	testingKnobs StorageTestingKnobs
	writer       writer
}

type leasingMetrics struct {
	outstandingLeases                          *metric.Gauge
	sessionBasedLeasesWaitingToExpire          *metric.Gauge
	sessionBasedLeasesExpired                  *metric.Gauge
	longWaitForOneVersionsActive               *metric.Gauge
	longWaitForNoVersionsActive                *metric.Gauge
	longTwoVersionInvariantViolationWaitActive *metric.Gauge
	longWaitForInitialVersionActive            *metric.Gauge
}

type leaseFields struct {
	regionPrefix []byte
	descID       descpb.ID
	version      descpb.DescriptorVersion
	instanceID   base.SQLInstanceID
	expiration   tree.DTimestamp
	sessionID    []byte
}

type writer interface {
	deleteLease(context.Context, *kv.Txn, leaseFields) error
	insertLease(context.Context, *kv.Txn, leaseFields) error
}

// LeaseRenewalDuration controls the default time before a lease expires when
// acquisition to renew the lease begins.
var LeaseRenewalDuration = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.catalog.descriptor_lease_renewal_fraction",
	"controls the default time before a lease expires when acquisition to renew the lease begins",
	base.DefaultDescriptorLeaseRenewalTimeout)

// LeaseRenewalCrossValidate controls if cross validation should be done during
// lease renewal.
var LeaseRenewalCrossValidate = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.catalog.descriptor_lease_renewal_cross_validation.enabled",
	"controls if cross validation should be done during lease renewal",
	base.DefaultLeaseRenewalCrossValidate)

// jitteredLeaseDuration returns a randomly jittered duration from the interval
// [(1-leaseJitterFraction) * leaseDuration, (1+leaseJitterFraction) * leaseDuration].
func (s storage) jitteredLeaseDuration() time.Duration {
	leaseDuration := LeaseDuration.Get(&s.settings.SV)
	jitterFraction := LeaseJitterFraction.Get(&s.settings.SV)
	return time.Duration(float64(leaseDuration) * (1 - jitterFraction +
		2*jitterFraction*rand.Float64()))
}

func (s storage) crossValidateDuringRenewal() bool {
	return LeaseRenewalCrossValidate.Get(&s.settings.SV)
}

// acquire a lease on the most recent version of a descriptor. The lease is tied
// to the provided sqlliveness.Session. If a newer version (then lastVersion) of
// the descriptor exists, this function will attempt to acquire a lease on it.
// If no newer version exists, it returns a nil descriptor. If the lease cannot
// be obtained because the descriptor is being dropped or is offline (currently
// only applicable to tables), an inactiveTableError is returned.
func (s storage) acquire(
	ctx context.Context,
	session sqlliveness.Session,
	id descpb.ID,
	lastVersion descpb.DescriptorVersion,
	lastSessionID sqlliveness.SessionID,
) (desc catalog.Descriptor, prefix []byte, _ error) {
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	prefix = s.getRegionPrefix()
	var sessionID []byte
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
		// If there was a previous iteration of the loop, we'd know because the
		// desc and expiration is non-empty. In this case, we may have successfully
		// written a value to the database, which we'd leak if we did not delete it.
		// Note that the expiration is part of the primary key in the table, so we
		// would not overwrite the old entry if we just were to do another insert.
		// repeatIteration = desc != nil
		if sessionID != nil && desc != nil {
			if err := s.writer.deleteLease(ctx, txn, leaseFields{
				regionPrefix: prefix,
				descID:       desc.GetID(),
				version:      desc.GetVersion(),
				instanceID:   instanceID,
				sessionID:    sessionID,
			}); err != nil {
				return errors.Wrap(err, "deleting ambiguously created lease")
			}
		}

		// Read into a temporary variable in case our read runs into
		// any retryable error. If we run into an error then the delete
		// above may need to be executed again.
		latestDesc, err := s.mustGetDescriptorByID(ctx, txn, id)

		if err != nil {
			return err
		}
		// If the descriptor version hasn't changed, then no new lease to be
		// inserted unless the session ID has changed on us. No descriptor will
		// be set indicating to the caller that no new version exists.
		if latestDesc.GetVersion() == lastVersion &&
			lastSessionID == session.ID() {
			return nil
		}
		desc = latestDesc
		if err := catalog.FilterAddingDescriptor(desc); err != nil {
			return err
		}
		if err := catalog.FilterDroppedDescriptor(desc); err != nil {
			return err
		}
		log.VEventf(ctx, 2, "storage attempting to acquire lease %v", desc)

		sessionID = session.ID().UnsafeBytes()
		lf := leaseFields{
			regionPrefix: prefix,
			descID:       desc.GetID(),
			version:      desc.GetVersion(),
			instanceID:   s.nodeIDContainer.SQLInstanceID(),
			sessionID:    sessionID,
		}
		return s.writer.insertLease(ctx, txn, lf)
	}

	// Compute the maximum time we will retry ambiguous replica errors before
	// disabling the SQL liveness heartbeat. The time chosen will guarantee that
	// the sqlliveness TTL expires once the lease duration has surpassed.
	maxTimeToDisableLiveness := s.jitteredLeaseDuration()
	defaultTTLForLiveness := slbase.DefaultTTL.Get(&s.settings.SV)
	if maxTimeToDisableLiveness > defaultTTLForLiveness {
		maxTimeToDisableLiveness -= defaultTTLForLiveness
	} else {
		// If the TTL time is somehow bigger than the lease duration, then immediately
		// after the first retry sqlliveness will renewals will be disabled.
		maxTimeToDisableLiveness = 0
	}
	acquireStart := timeutil.Now()
	extensionsBlocked := false
	defer func() {
		if extensionsBlocked {
			s.livenessProvider.UnpauseLivenessHeartbeat(ctx)
		}
	}()
	// Run a retry loop to deal with AmbiguousResultErrors. All other error types
	// are propagated up to the caller.
	for r := retry.StartWithCtx(ctx, retry.Options{}); r.Next(); {
		err := s.db.KV().Txn(ctx, acquireInTxn)
		switch {
		case startup.IsRetryableReplicaError(err):
			// If we keep encountering the retryable replica error for more then
			// the lease expiry duration we can no longer keep updating the liveness.
			// i.e. This node is no longer productive at this point since it can't
			// acquire or release releases potentially blocking schema changes.
			if !extensionsBlocked && timeutil.Since(acquireStart) > maxTimeToDisableLiveness {
				s.livenessProvider.PauseLivenessHeartbeat(ctx)
				extensionsBlocked = true
			}
			log.Infof(ctx, "retryable replica error occurred during lease acquisition for %v, retrying: %v", id, err)
			continue
		case pgerror.GetPGCode(err) == pgcode.UniqueViolation:
			log.Infof(ctx, "uniqueness violation occurred due to concurrent lease"+
				" removal for %v, retrying: %v", id, err)
			continue
		case err != nil:
			return nil, nil, err
		}
		// If desc is nil then no new descriptor was available to be leased.
		// i.e. the last version we leased is the latest and still held
		if desc == nil {
			return nil, nil, nil
		}
		log.VEventf(ctx, 2, "storage acquired lease %v", desc)
		if s.testingKnobs.LeaseAcquiredEvent != nil {
			s.testingKnobs.LeaseAcquiredEvent(desc, err)
		}
		s.outstandingLeases.Inc(1)
		return desc, prefix, nil
	}
	return nil, nil, ctx.Err()
}

// Release a previously acquired descriptor. Never let this method
// read a descriptor because it can be called while modifying a
// descriptor through a schema change before the schema change has committed
// that can result in a deadlock.
func (s storage) release(
	ctx context.Context, stopper *stop.Stopper, lease *storedLease,
) (released bool) {
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	retryOptions := base.DefaultRetryOptions()
	retryOptions.Closer = stopper.ShouldQuiesce()

	// Compute the maximum time we will retry ambiguous replica errors before
	// disabling the SQL liveness heartbeat. The time chosen will guarantee that
	// the sqlliveness TTL expires once the lease duration has surpassed.
	maxTimeToDisableLiveness := s.jitteredLeaseDuration()
	defaultTTLForLiveness := slbase.DefaultTTL.Get(&s.settings.SV)
	if maxTimeToDisableLiveness > defaultTTLForLiveness {
		maxTimeToDisableLiveness -= defaultTTLForLiveness
	} else {
		// If the TTL time is somehow bigger than the lease duration, then immediately
		// after the first retry sqlliveness will renewals will be disabled.
		maxTimeToDisableLiveness = 0
	}
	acquireStart := timeutil.Now()
	extensionsBlocked := false
	defer func() {
		if extensionsBlocked {
			s.livenessProvider.UnpauseLivenessHeartbeat(ctx)
		}
	}()
	// This transaction is idempotent; the retry was put in place because of
	// NodeUnavailableErrors.
	for r := retry.StartWithCtx(ctx, retryOptions); r.Next(); {
		log.VEventf(ctx, 2, "storage releasing lease %+v", lease)
		instanceID := s.nodeIDContainer.SQLInstanceID()
		if instanceID == 0 {
			panic("SQL instance ID not set")
		}
		lf := leaseFields{
			regionPrefix: lease.prefix,
			descID:       lease.id,
			version:      descpb.DescriptorVersion(lease.version),
			instanceID:   instanceID,
			expiration:   lease.expiration,
			sessionID:    lease.sessionID,
		}
		err := s.writer.deleteLease(ctx, nil /* txn */, lf)
		if err != nil {
			log.Warningf(ctx, "error releasing lease %q: %s", lease, err)
			if grpcutil.IsConnectionRejected(err) {
				return
			}
			if startup.IsRetryableReplicaError(err) {
				// If we keep encountering the retryable replica error for more then
				// the lease expiry duration we can no longer keep updating the liveness.
				// i.e. This node is no longer productive at this point since it can't
				// acquire or release releases potentially blocking schema changes across
				// a cluster.
				if !extensionsBlocked && timeutil.Since(acquireStart) > maxTimeToDisableLiveness {
					s.livenessProvider.PauseLivenessHeartbeat(ctx)
					extensionsBlocked = true
				}
			}
			continue
		}
		released = true
		s.outstandingLeases.Dec(1)
		if s.testingKnobs.LeaseReleasedEvent != nil {
			s.testingKnobs.LeaseReleasedEvent(
				lease.id, descpb.DescriptorVersion(lease.version), err)
		}
		break
	}
	return released
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
	err := s.db.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		prevTimestamp := expiration.Prev()
		err := txn.SetFixedTimestamp(ctx, prevTimestamp)
		if err != nil {
			return err
		}
		desc, err = s.mustGetDescriptorByID(ctx, txn, id)
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

func (s storage) newCatalogReader(ctx context.Context) catkv.CatalogReader {
	return catkv.NewCatalogReader(
		s.codec,
		s.settings.Version.ActiveVersion(ctx),
		s.sysDBCache,
		nil, /* maybeMonitor */
	)
}

func (s storage) mustGetDescriptorByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.Descriptor, error) {
	cr := s.newCatalogReader(ctx)
	const isDescriptorRequired = true
	c, err := cr.GetByIDs(ctx, txn, []descpb.ID{id}, isDescriptorRequired, catalog.Any)
	if err != nil {
		return nil, err
	}
	desc := c.LookupDescriptor(id)
	validationLevel := catalog.ValidationLevelSelfOnly
	if s.crossValidateDuringRenewal() {
		validationLevel = validate.ImmutableRead
	}
	vd := catkv.NewCatalogReaderBackedValidationDereferencer(cr, txn, nil /* dvmpMaybe */)
	ve := validate.Validate(
		ctx,
		s.settings.Version.ActiveVersion(ctx),
		vd,
		catalog.ValidationReadTelemetry,
		validationLevel,
		desc,
	)
	if err := ve.CombinedError(); err != nil {
		return nil, err
	}
	return desc, nil
}

func (s storage) getRegionPrefix() []byte {
	return s.regionPrefix.Load().([]byte)
}
