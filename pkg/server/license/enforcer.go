// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package license

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	defaultMaxOpenTransactions = 5
	defaultMaxTelemetryDelay   = 7 * 24 * time.Hour
)

// Enforcer is responsible for enforcing license policies.
type Enforcer struct {
	// DB is used to access into the KV. This is set via Start. It must have
	// access to the system tenant since it read/writes KV keys in the system
	// keyspace.
	db isql.DB

	// TestingKnobs are used to control the behavior of the enforcer for testing.
	TestingKnobs *TestingKnobs

	// telemetryStatusReporter is an interface for getting the timestamp of the
	// last successful ping to the telemetry server. For some licenses, sending
	// telemetry data is required to avoid throttling.
	telemetryStatusReporter TelemetryStatusReporter

	// gracePeriodInitTS is the timestamp of when the cluster first ran on a
	// version that requires a license. It is stored as the number of seconds
	// since the unix epoch. This is read/written to the KV.
	gracePeriodInitTS atomic.Int64

	// startTime is the time when the enforcer was created. This is used to seed
	// the grace period init timestamp if it's not set in the KV layer.
	startTime time.Time

	// licenseRequiresTelemetry will be true if the license requires that we send
	// periodic telemetry data.
	licenseRequiresTelemetry atomic.Bool

	// gracePeriodEndTS tracks when the grace period ends and throttling begins.
	// For licenses without throttling, this value will be 0. The value stored
	// is the number of seconds since the unix epoch.
	gracePeriodEndTS atomic.Int64

	// hasLicense is true if any license is installed.
	hasLicense atomic.Bool

	// lastLicenseThrottlingLogTime keeps track of the last time we logged a
	// message because we had to throttle due to a license issue. The value
	// stored is the number of seconds since the unix epoch.
	lastLicenseThrottlingLogTime atomic.Int64

	// lastTelemetryThrottlingLogTime keeps track of the last time we logged a
	// message because we had to throttle due to a telemetry issue. The value
	// stored is the number of seconds since the unix epoch.
	lastTelemetryThrottlingLogTime atomic.Int64
}

type TestingKnobs struct {
	// OverrideStartTime if set, overrides the time that's used to seed the
	// grace period init timestamp.
	OverrideStartTime *time.Time

	// OverrideThrottleCheckTime if set, overrides the timestamp used when
	// checking if throttling is active.
	OverrideThrottleCheckTime *time.Time
}

// TelemetryStatusReporter is the interface we use to find the last ping
// time for telemetry reporting.
type TelemetryStatusReporter interface {
	// GetLastSuccessfulTelemetryPing returns the time of the last time the
	// telemetry data got back an acknowledgement from Cockroach Labs.
	GetLastSuccessfulTelemetryPing() time.Time
}

var instance *Enforcer
var once sync.Once

// GetEnforcerInstance returns the singleton instance of the Enforcer. The
// Enforcer is responsible for license enforcement policies.
func GetEnforcerInstance() *Enforcer {
	if instance == nil {
		once.Do(
			func() {
				instance = newEnforcer()
			})
	}
	return instance
}

// newEnforcer creates a new Enforcer object.
func newEnforcer() *Enforcer {
	return &Enforcer{
		startTime: timeutil.Now(),
	}
}

// SetTelemetryStatusReporter will set the pointer to the telemetry status reporter.
func (e *Enforcer) SetTelemetryStatusReporter(reporter TelemetryStatusReporter) {
	e.telemetryStatusReporter = reporter
}

// Start will load the necessary metadata for the enforcer. It reads from the
// KV license metadata and will populate any missing data as needed. The DB
// passed in must have access to the system tenant.
func (e *Enforcer) Start(ctx context.Context, st *cluster.Settings, db isql.DB) error {
	e.maybeLogActiveOverrides(ctx)

	e.db = db

	// Add a hook into the license setting so that we refresh our state whenever
	// the license changes.
	RegisterCallbackOnLicenseChange(ctx, st)

	return e.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// We could use a conditional put for this logic. However, we want to read
		// and cache the value, and the common case is that the value will be read.
		// Only during the initialization of the first node in the cluster will we
		// need to write a new timestamp. So, we optimize for the case where the
		// timestamp already exists.
		val, err := txn.KV().Get(ctx, keys.GracePeriodInitTimestamp)
		if err != nil {
			return err
		}
		if val.Value == nil {
			log.Infof(ctx, "generated new grace period init time: %s", e.startTime.UTC().String())
			e.gracePeriodInitTS.Store(e.getStartTime().Unix())
			return txn.KV().Put(ctx, keys.GracePeriodInitTimestamp, e.gracePeriodInitTS.Load())
		}
		e.gracePeriodInitTS.Store(val.ValueInt())
		log.Infof(ctx, "fetched existing grace period init time: %s", e.GetGracePeriodInitTS().String())
		return nil
	})
}

// GetGracePeriodInitTS will return the timestamp of when the cluster first ran
// on a version that requires a license.
func (e *Enforcer) GetGracePeriodInitTS() time.Time {
	// In the rare case that the grace period init timestamp has not been cached yet,
	// we will return an approximate value, the start time of the server. This
	// should only happen if we are in the process of caching the grace period init
	// timestamp, or we failed to cache it. This is preferable to returning an
	// error or a zero value.
	if e.gracePeriodInitTS.Load() == 0 {
		return e.getStartTime()
	}
	return timeutil.Unix(e.gracePeriodInitTS.Load(), 0)
}

// GetHasLicense returns true if a license is installed.
func (e *Enforcer) GetHasLicense() bool {
	return e.hasLicense.Load()
}

// GetGracePeriodEndTS returns the timestamp indicating the end of the grace period.
// Some licenses provide a grace period after expiration or when no license is present.
// If no grace period is defined, the second return value will be false.
func (e *Enforcer) GetGracePeriodEndTS() (time.Time, bool) {
	if e.gracePeriodEndTS.Load() == 0 {
		return time.Time{}, false
	}
	ts := timeutil.Unix(e.gracePeriodEndTS.Load(), 0)
	return ts, true
}

// GetAllowedTelemetryDelayTS returns a timestamp of when telemetry
// data needs to be received before we start to throttle. If the license doesn't
// require telemetry, then false is returned for second return value.
func (e *Enforcer) GetAllowedTelemetryDelayTS() (time.Time, bool) {
	if !e.licenseRequiresTelemetry.Load() || e.telemetryStatusReporter == nil {
		return time.Time{}, false
	}

	lastTelemetryDataReceived := e.telemetryStatusReporter.GetLastSuccessfulTelemetryPing()
	throttleTS := lastTelemetryDataReceived.Add(e.getMaxTelemetryDelay())
	return throttleTS, true
}

// MaybeFailIfThrottled evaluates the current transaction count and license state,
// returning an error if throttling conditions are met. Throttling may be triggered
// if the maximum number of open transactions is exceeded and the grace period has
// ended or if required diagnostic reporting has not been received.
func (e *Enforcer) MaybeFailIfThrottled(ctx context.Context, txnsOpened int64) (err error) {
	// Early out if the number of transactions is below the max allowed.
	if txnsOpened < e.getMaxOpenTransactions() {
		return
	}

	now := e.getThrottleCheckTS()
	if gracePeriodEnd, ok := e.GetGracePeriodEndTS(); ok && now.After(gracePeriodEnd) {
		if e.GetHasLicense() {
			err = errors.WithHintf(pgerror.Newf(pgcode.CCLValidLicenseRequired,
				"License expired. The maximum number of open transactions has been reached."),
				"Obtain and install a new license to continue.")
		} else {
			err = errors.WithHintf(pgerror.Newf(pgcode.CCLValidLicenseRequired,
				"No license installed. The maximum number of open transactions has been reached."),
				"Obtain and install a valid license to continue.")
		}
		e.maybeLogError(ctx, err, &e.lastLicenseThrottlingLogTime)
		return
	}

	if ts, ok := e.GetAllowedTelemetryDelayTS(); ok && now.After(ts) {
		err = errors.WithHintf(pgerror.Newf(pgcode.CCLValidLicenseRequired,
			"The maximum number of open transactions has been reached because the license requires "+
				"diagnostic reporting, but none has been received by Cockroach Labs."),
			"Ensure diagnostic reporting is enabled and verify that nothing is blocking network access to the "+
				"Cockroach Labs reporting server. You can also consider changing your license to one that doesn't "+
				"require diagnostic reporting to be emitted.")
		e.maybeLogError(ctx, err, &e.lastTelemetryThrottlingLogTime)
		return
	}
	return
}

// RefreshForLicenseChange resets the state when the license changes. We cache certain
// information to optimize enforcement. Instead of reading the license from the
// settings, unmarshaling it, and checking its type and expiry each time,
// caching the information improves efficiency since licenses change infrequently.
func (e *Enforcer) RefreshForLicenseChange(licType LicType, licenseExpiry time.Time) {
	e.hasLicense.Store(licType != LicTypeNone)

	switch licType {
	case LicTypeNone:
		e.storeNewGracePeriodEndDate(e.GetGracePeriodInitTS(), e.getGracePeriodDuration(7*24*time.Hour))
		e.licenseRequiresTelemetry.Store(false)
	case LicTypeFree:
		e.storeNewGracePeriodEndDate(licenseExpiry, e.getGracePeriodDuration(30*24*time.Hour))
		e.licenseRequiresTelemetry.Store(true)
	case LicTypeTrial:
		e.storeNewGracePeriodEndDate(licenseExpiry, e.getGracePeriodDuration(7*24*time.Hour))
		e.licenseRequiresTelemetry.Store(true)
	case LicTypeEvaluation:
		e.storeNewGracePeriodEndDate(licenseExpiry, e.getGracePeriodDuration(30*24*time.Hour))
		e.licenseRequiresTelemetry.Store(false)
	default:
		e.storeNewGracePeriodEndDate(timeutil.UnixEpoch, 0)
		e.licenseRequiresTelemetry.Store(false)
	}
}

// getStartTime returns the time when the enforcer was created. This accounts
// for testing knobs that may override the time.
func (e *Enforcer) getStartTime() time.Time {
	if e.TestingKnobs != nil && e.TestingKnobs.OverrideStartTime != nil {
		return *e.TestingKnobs.OverrideStartTime
	}
	return e.startTime
}

// getThrottleCheckTS returns the time to use when checking if we should
// throttle the new transaction.
func (e *Enforcer) getThrottleCheckTS() time.Time {
	if e.TestingKnobs != nil && e.TestingKnobs.OverrideThrottleCheckTime != nil {
		return *e.TestingKnobs.OverrideThrottleCheckTime
	}
	return timeutil.Now()
}

func (e *Enforcer) storeNewGracePeriodEndDate(start time.Time, duration time.Duration) {
	ts := start.Add(duration)
	e.gracePeriodEndTS.Store(ts.Unix())
}

// getGracePeriodDuration is a helper to pick the grace period length, while
// accounting for testing knobs and/or environment variables.
func (e *Enforcer) getGracePeriodDuration(defaultAndMaxLength time.Duration) time.Duration {
	newLength := envutil.EnvOrDefaultDuration("COCKROACH_LICENSE_GRACE_PERIOD_DURATION", defaultAndMaxLength)
	// We only allow shortening of the grace period for testing purposes. Ensure
	// it can never increase.
	if defaultAndMaxLength < newLength {
		return defaultAndMaxLength
	}
	return newLength
}

// getMaxOpenTransactions returns the number of open transactions allowed before
// throttling takes affect.
func (e *Enforcer) getMaxOpenTransactions() int64 {
	newLimit := envutil.EnvOrDefaultInt64("COCKROACH_MAX_OPEN_TXNS_DURING_THROTTLE", defaultMaxOpenTransactions)
	// Ensure we can never increase the number of open transactions allowed.
	if newLimit > defaultMaxOpenTransactions {
		return defaultMaxOpenTransactions
	}
	return newLimit
}

// getMaxTelemetryDelay returns the maximum duration allowed before telemetry
// data must be sent to comply with license policies that require telemetry.
func (e *Enforcer) getMaxTelemetryDelay() time.Duration {
	newTimeframe := envutil.EnvOrDefaultDuration("COCKROACH_MAX_TELEMETRY_DELAY", defaultMaxTelemetryDelay)
	// Ensure we can never extend beyond the default delay.
	if newTimeframe > defaultMaxTelemetryDelay {
		return defaultMaxTelemetryDelay
	}
	return newTimeframe
}

// maybeLogError logs an error message about throttling if one hasn’t been
// logged recently. The purpose is to alert the user about throttling without
// overwhelming the cockroach log.
func (e *Enforcer) maybeLogError(ctx context.Context, err error, lastLogTimestamp *atomic.Int64) {
	nextLogMessage := timeutil.Unix(lastLogTimestamp.Load(), 0)
	nextLogMessage.Add(5 * time.Minute)

	now := timeutil.Now()
	if now.After(nextLogMessage) {
		lastLogTimestamp.Store(now.Unix())
		log.Infof(ctx, "throttling for license enforcement is active: %s", err.Error())
	}
}

// maybeLogActiveOverrides is a debug tool to indicate any env var overrides.
func (e *Enforcer) maybeLogActiveOverrides(ctx context.Context) {
	maxOpenTxns := e.getMaxOpenTransactions()
	if maxOpenTxns != defaultMaxOpenTransactions {
		log.Infof(ctx, "max open transactions before throttling overridden to %d", maxOpenTxns)
	}

	// The grace period may vary depending on the license type. We'll select the
	// maximum grace period across all licenses and compare it with the value
	// from the getter to determine if an override is applied.
	maxGracePeriod := 30 * 7 * time.Hour
	curGracePeriod := e.getGracePeriodDuration(maxGracePeriod)
	if curGracePeriod != maxGracePeriod {
		log.Infof(ctx, "grace period has changed to %v", curGracePeriod)
	}

	curTelemetryDelay := e.getMaxTelemetryDelay()
	if curTelemetryDelay != defaultMaxTelemetryDelay {
		log.Infof(ctx, "max telemetry delay has changed to %v", curTelemetryDelay)
	}
}
