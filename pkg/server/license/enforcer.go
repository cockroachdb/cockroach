// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package license

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	defaultMaxOpenTransactions  = 5
	defaultMaxTelemetryInterval = 7 * 24 * time.Hour
)

// Enforcer is responsible for enforcing license policies.
type Enforcer struct {
	mu struct {
		syncutil.Mutex

		// setupComplete ensures that Start() is called only once.
		setupComplete bool
	}

	// testingKnobs are used to control the behavior of the enforcer for testing.
	testingKnobs *TestingKnobs

	// telemetryStatusReporter is an interface for getting the timestamp of the
	// last successful ping to the telemetry server. For some licenses, sending
	// telemetry data is required to avoid throttling.
	telemetryStatusReporter atomic.Pointer[TelemetryStatusReporter]

	// clusterInitGracePeriodEndTS marks the end of the grace period when a
	// license is required. It is set during the cluster's initial startup. The
	// timestamp is stored as seconds since the Unix epoch and is read/written to
	// the KV.
	clusterInitGracePeriodEndTS atomic.Int64

	// startTime is the time when the enforcer was created. This is used to seed
	// the clusterInitGracePeriodEndTS if it's not set in the KV layer.
	startTime time.Time

	// licenseRequiresTelemetry will be true if the license requires that we send
	// periodic telemetry data.
	licenseRequiresTelemetry atomic.Bool

	// licenseExpiryTS tracks the license expiration time, valid only if a license
	// is installed. The value represents the number of seconds since the Unix epoch.
	licenseExpiryTS atomic.Int64

	// gracePeriodEndTS tracks when the grace period ends and throttling begins.
	// For licenses without throttling, this value will be 0. The value stored
	// is the number of seconds since the unix epoch.
	gracePeriodEndTS atomic.Int64

	// hasLicense is true if any license is installed.
	hasLicense atomic.Bool

	// throttleLogger is a logger for throttle-related messages. It is used to
	// emit logs only every few minutes to avoid spamming the logs.
	throttleLogger log.EveryN

	// isDisabled is a global override that completely disables license enforcement.
	// When enabled, all checks, including telemetry and expired license validation,
	// are bypassed. This is typically used to disable enforcement for single-node deployments.
	isDisabled atomic.Bool
}

type TestingKnobs struct {
	// Enable controls whether the enforcer writes the grace period end time to KV
	// and performs throttle checks. This is currently opt-in to allow for a gradual
	// rollout of these changes. It will be removed or changed to opt-out as we near
	// the final stages of the CockroachDB core licensing deprecation.
	// TODO(spilchen): Update or remove this knob closer to the completion of the
	// core licensing deprecation work (CRDB-41758).
	Enable bool

	// SkipDisable makes the Disable() function a no-op. This is separate from Enable
	// because we perform additional checks during server startup that may automatically
	// disable enforcement based on configuration (e.g., for single-node instances).
	SkipDisable bool

	// OverrideStartTime if set, overrides the time that's used to seed the
	// grace period init timestamp.
	OverrideStartTime *time.Time

	// OverrideThrottleCheckTime if set, overrides the timestamp used when
	// checking if throttling is active.
	OverrideThrottleCheckTime *time.Time

	// OverrideMaxOpenTransactions if set, overrides the maximum open transactions
	// when checking if active throttling.
	OverrideMaxOpenTransactions *int64

	// OverwriteClusterInitGracePeriodTS, if true, forces the enforcer to
	// overwrite the existing cluster initialization grace period timestamp,
	// even if one is already set.
	OverwriteClusterInitGracePeriodTS bool
}

// TelemetryStatusReporter is the interface we use to find the last ping
// time for telemetry reporting.
type TelemetryStatusReporter interface {
	// GetLastSuccessfulTelemetryPing returns the time of the last time the
	// telemetry data got back an acknowledgement from Cockroach Labs.
	GetLastSuccessfulTelemetryPing() time.Time
}

// NewEnforcer creates a new Enforcer object.
func NewEnforcer(tk *TestingKnobs) *Enforcer {
	e := &Enforcer{
		startTime:      timeutil.Now(),
		throttleLogger: log.Every(5 * time.Minute),
		testingKnobs:   tk,
	}
	// Start is disabled by default unless overridden by testing knobs.
	if tk == nil || !tk.Enable {
		e.isDisabled.Store(true)
	}
	return e
}

// SetTelemetryStatusReporter will set the pointer to the telemetry status reporter.
func (e *Enforcer) SetTelemetryStatusReporter(reporter TelemetryStatusReporter) {
	e.telemetryStatusReporter.Store(&reporter)
}

func (e *Enforcer) GetTestingKnobs() *TestingKnobs {
	return e.testingKnobs
}

// Start will load the necessary metadata for the enforcer. If called for the
// system tenant, it reads from the KV license metadata and will populate any
// missing data as needed.
func (e *Enforcer) Start(ctx context.Context, st *cluster.Settings, opts ...Option) error {
	options := options{}
	for _, o := range opts {
		o.apply(&options)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.mu.setupComplete {
		return nil
	}

	if options.testingKnobs != nil {
		e.testingKnobs = options.testingKnobs
	}
	e.telemetryStatusReporter.Store(&options.telemetryStatusReporter)

	// We always start disabled. If an error occurs, the enforcer setup will be
	// incomplete, but the server will continue to start. To ensure stability in
	// that case, we leave throttling disabled.
	e.isDisabled.Store(true)
	startDisabled := e.getInitialIsDisabledValue()

	e.maybeLogActiveOverrides(ctx)

	if !startDisabled {
		if err := e.maybeWriteClusterInitGracePeriodTS(ctx, options); err != nil {
			return err
		}
	}

	// Initialize assuming there is no license. This seeds necessary values. It
	// must be done after setting the cluster init grace period timestamp. And it
	// is needed for testing that may be running this in isolation to the license
	// ccl package.
	e.RefreshForLicenseChange(ctx, LicTypeNone, time.Time{})

	// Add a hook into the license setting so that we refresh our state whenever
	// the license changes. This will also update the state for the current
	// license if not in test.
	RegisterCallbackOnLicenseChange(ctx, st, e)

	// This should be the final step after all error checks are completed.
	e.isDisabled.Store(startDisabled)
	e.mu.setupComplete = true

	return nil
}

// maybeWriteClusterInitGracePeriodTS checks if the cluster init grace period
// timestamp needs to be written to the KV layer and writes it if needed.
func (e *Enforcer) maybeWriteClusterInitGracePeriodTS(ctx context.Context, options options) error {
	// Secondary tenants do not have access to the system keyspace where
	// the cluster init grace period is stored. As a fallback, we apply a 7-day
	// grace period from the tenant's start time, which is used only when no
	// license is installed. This logic applies specifically when secondary
	// tenants are started in a separate process from the system tenant. If they
	// are not, a shared enforcer will have access to the system keyspace and
	// handle the grace period.
	// TODO(spilchen): Change to give the secondary tenant read access to the
	// system keyspace KV.
	if !options.isSystemTenant {
		gracePeriodLength := e.getGracePeriodDuration(7 * 24 * time.Hour)
		end := e.getStartTime().Add(gracePeriodLength)
		e.clusterInitGracePeriodEndTS.Store(end.Unix())
		return nil
	}

	return options.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// We could use a conditional put for this logic. However, we want to read
		// and cache the value, and the common case is that the value will be read.
		// Only during the initialization of the first node in the cluster will we
		// need to write a new timestamp. So, we optimize for the case where the
		// timestamp already exists.
		val, err := txn.KV().Get(ctx, keys.ClusterInitGracePeriodTimestamp)
		if err != nil {
			return err
		}
		tk := e.GetTestingKnobs()
		if val.Value == nil || (tk != nil && tk.OverwriteClusterInitGracePeriodTS) {
			initialStart, err := e.getIsNewClusterEstimate(ctx, txn)
			if err != nil {
				return err
			}

			// The length of the grace period without a license varies based on the
			// cluster's creation time. Older databases built when we had a
			// CockroachDB core license are given more time.
			gracePeriodLength := 30 * 24 * time.Hour
			if initialStart {
				gracePeriodLength = 7 * 24 * time.Hour
			}
			gracePeriodLength = e.getGracePeriodDuration(gracePeriodLength) // Allow the value to be shortened by env var
			end := e.getStartTime().Add(gracePeriodLength)
			log.Infof(ctx, "generated new cluster init grace period end time: %s", end.UTC().String())
			e.clusterInitGracePeriodEndTS.Store(end.Unix())
			return txn.KV().Put(ctx, keys.ClusterInitGracePeriodTimestamp, e.clusterInitGracePeriodEndTS.Load())
		}
		e.clusterInitGracePeriodEndTS.Store(val.ValueInt())
		log.Infof(ctx, "fetched existing cluster init grace period end time: %s", e.GetClusterInitGracePeriodEndTS().String())
		return nil
	})
}

// GetClusterInitGracePeriodEndTS will return the ending time of the grace
// period as stored during cluster init. This is intended to be used a grace
// period for when no license is installed.
func (e *Enforcer) GetClusterInitGracePeriodEndTS() time.Time {
	// In the rare case that the timestamp has not been cached yet, we will return
	// an approximate value, the start time of the server plus 7 days. This should
	// only happen if we are in the process of caching the grace period init
	// timestamp, or we failed to cache it. This is preferable to returning an
	// error or a zero value.
	if e.clusterInitGracePeriodEndTS.Load() == 0 {
		return e.getStartTime().Add(7 * 24 * time.Hour)
	}
	return timeutil.Unix(e.clusterInitGracePeriodEndTS.Load(), 0)
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

// GetTelemetryDeadline returns a timestamp of when telemetry
// data needs to be received before we start to throttle. If the license doesn't
// require telemetry, then false is returned for second return value.
func (e *Enforcer) GetTelemetryDeadline() (deadline, lastPing time.Time, ok bool) {
	if !e.licenseRequiresTelemetry.Load() || e.telemetryStatusReporter.Load() == nil {
		return time.Time{}, time.Time{}, false
	}

	ptr := e.telemetryStatusReporter.Load()
	lastTelemetryDataReceived := (*ptr).GetLastSuccessfulTelemetryPing()
	throttleTS := lastTelemetryDataReceived.Add(e.getMaxTelemetryInterval())
	return throttleTS, lastTelemetryDataReceived, true
}

// MaybeFailIfThrottled evaluates the current transaction count and license state,
// returning an error if throttling conditions are met. Throttling may be triggered
// if the maximum number of open transactions is exceeded and the grace period has
// ended or if required diagnostic reporting has not been received.
//
// If throttling hasn't occurred yet but may soon, a notice is returned for the client.
// The notice is only returned if no throttling is detected. Callers should always
// check the error first.
func (e *Enforcer) MaybeFailIfThrottled(
	ctx context.Context, txnsOpened int64,
) (notice pgnotice.Notice, err error) {
	// Early out if the number of transactions is below the max allowed or
	// everything has been disabled.
	if txnsOpened <= e.getMaxOpenTransactions() || e.isDisabled.Load() {
		return
	}

	now := e.getThrottleCheckTS()
	expiryTS, hasExpiry := e.getLicenseExpiryTS()

	// If the license doesn't require telemetry and hasn't expired,
	// we can exit without any further checks.
	if !e.licenseRequiresTelemetry.Load() && hasExpiry && expiryTS.After(now) {
		return
	}

	// Throttle if the license has expired, is missing, and the grace period has ended.
	gracePeriodEnd, hasGracePeriod := e.GetGracePeriodEndTS()
	if hasGracePeriod && now.After(gracePeriodEnd) {
		if e.GetHasLicense() {
			err = errors.WithHintf(pgerror.Newf(pgcode.CCLValidLicenseRequired,
				"License expired on %s. The maximum number of concurrently open transactions has been reached.",
				timeutil.Unix(e.licenseExpiryTS.Load(), 0)),
				"Obtain and install a new license to continue.")
		} else {
			err = errors.WithHintf(pgerror.Newf(pgcode.CCLValidLicenseRequired,
				"No license installed. The maximum number of concurrently open transactions has been reached."),
				"Obtain and install a valid license to continue.")
		}
		if e.throttleLogger.ShouldLog() {
			log.Infof(ctx, "throttling for license enforcement is active, license expired with a grace period "+
				"that ended at %s", gracePeriodEnd)
		}
		return
	}

	// For cases where the license requires telemetry, throttle if we are past the deadline
	deadlineTS, lastPingTS, requiresTelemetry := e.GetTelemetryDeadline()
	if requiresTelemetry && now.After(deadlineTS) {
		err = errors.WithHintf(pgerror.Newf(pgcode.CCLValidLicenseRequired,
			"The maximum number of concurrently open transactions has been reached because the license requires "+
				"diagnostic reporting, but none has been received by Cockroach Labs."),
			"Ensure diagnostic reporting is enabled and verify that nothing is blocking network access to the "+
				"Cockroach Labs reporting server. You can also consider changing your license to one that doesn't "+
				"require diagnostic reporting to be emitted.")
		if e.throttleLogger.ShouldLog() {
			log.Infof(ctx, "throttling for license enforcement is active, due to no telemetry data received, "+
				"last received at %s", lastPingTS)
		}
		return
	}

	// Emit a warning if throttling is imminent. This could mean the license is expired but within
	// the grace period, or that required telemetry data hasn't been sent recently.
	if hasGracePeriod && now.After(expiryTS) {
		if e.GetHasLicense() {
			notice = pgnotice.Newf(
				"Your license expired on %s. Throttling will begin after %s. Please install a new license to prevent this.",
				expiryTS, gracePeriodEnd)
		} else {
			notice = pgnotice.Newf(
				"No license is installed. Throttling will begin after %s unless a license is installed before then.",
				gracePeriodEnd)
		}
	} else if requiresTelemetry && e.addThrottleWarningDelayForNoTelemetry(lastPingTS).Before(now) {
		notice = pgnotice.Newf(
			"Your license requires diagnostic reporting, but no data has been sent since %s. Throttling will begin "+
				"after %s unless the data is sent.",
			lastPingTS, deadlineTS)
	}
	if notice != nil {
		if e.throttleLogger.ShouldLog() {
			log.Infof(ctx, "throttling will happen soon: %s", notice.Error())
		}
	}

	return
}

// RefreshForLicenseChange resets the state when the license changes. We cache certain
// information to optimize enforcement. Instead of reading the license from the
// settings, unmarshaling it, and checking its type and expiry each time,
// caching the information improves efficiency since licenses change infrequently.
func (e *Enforcer) RefreshForLicenseChange(
	ctx context.Context, licType LicType, licenseExpiry time.Time,
) {
	e.hasLicense.Store(licType != LicTypeNone)
	e.licenseExpiryTS.Store(licenseExpiry.Unix())

	switch licType {
	case LicTypeNone:
		e.storeNewGracePeriodEndDate(e.GetClusterInitGracePeriodEndTS(), 0)
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
	case LicTypeEnterprise:
		e.storeNewGracePeriodEndDate(timeutil.UnixEpoch, 0)
		e.licenseRequiresTelemetry.Store(false)
	}

	var sb strings.Builder
	sb.WriteString("enforcer license updated: ")
	gpEnd, _ := e.GetGracePeriodEndTS()
	if !gpEnd.IsZero() {
		sb.WriteString(fmt.Sprintf("grace period ends at %q, ", gpEnd))
	}
	expiry := timeutil.Unix(e.licenseExpiryTS.Load(), 0)
	if !expiry.IsZero() {
		sb.WriteString(fmt.Sprintf("expiration at %q, ", expiry))
	}
	sb.WriteString(fmt.Sprintf("telemetry required: %t", e.licenseRequiresTelemetry.Load()))
	log.Infof(ctx, "%s", sb.String())
}

// Disable turns off all license enforcement for the lifetime of this object.
func (e *Enforcer) Disable(ctx context.Context) {
	// We provide an override so that we can continue to test license enforcement
	// policies in single-node clusters.
	skipDisable := envutil.EnvOrDefaultBool("COCKROACH_SKIP_LICENSE_ENFORCEMENT_DISABLE", false)
	tk := e.GetTestingKnobs()
	if skipDisable || (tk != nil && tk.SkipDisable) {
		return
	}
	log.Infof(ctx, "disable all license enforcement")
	e.isDisabled.Store(true)
}

// getStartTime returns the time when the enforcer was created. This accounts
// for testing knobs that may override the time.
func (e *Enforcer) getStartTime() time.Time {
	if tk := e.GetTestingKnobs(); tk != nil && tk.OverrideStartTime != nil {
		return *tk.OverrideStartTime
	}
	return e.startTime
}

// getThrottleCheckTS returns the time to use when checking if we should
// throttle the new transaction.
func (e *Enforcer) getThrottleCheckTS() time.Time {
	if tk := e.GetTestingKnobs(); tk != nil && tk.OverrideThrottleCheckTime != nil {
		return *tk.OverrideThrottleCheckTime
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
	if tk := e.GetTestingKnobs(); tk != nil && tk.OverrideMaxOpenTransactions != nil {
		newLimit = *tk.OverrideMaxOpenTransactions
	}
	// Ensure we can never increase the number of open transactions allowed.
	if newLimit > defaultMaxOpenTransactions {
		return defaultMaxOpenTransactions
	}
	return newLimit
}

// getMaxTelemetryInterval returns the maximum duration allowed before telemetry
// data must be sent to comply with license policies that require telemetry.
func (e *Enforcer) getMaxTelemetryInterval() time.Duration {
	newTimeframe := envutil.EnvOrDefaultDuration("COCKROACH_MAX_TELEMETRY_INTERVAL", defaultMaxTelemetryInterval)
	// Ensure we can never extend beyond the default interval.
	if newTimeframe > defaultMaxTelemetryInterval {
		return defaultMaxTelemetryInterval
	}
	return newTimeframe
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

	curTelemetryInterval := e.getMaxTelemetryInterval()
	if curTelemetryInterval != defaultMaxTelemetryInterval {
		log.Infof(ctx, "max telemetry interval has changed to %v", curTelemetryInterval)
	}
}

// getInitialIsDisabledValue returns bool indicating what the initial value
// should be for e.isDisabled
func (e *Enforcer) getInitialIsDisabledValue() bool {
	// The enforcer is currently opt-in. This will change as we approach the
	// final stages of CockroachDB core license deprecation.
	// TODO(spilchen): Enable the enforcer by default in CRDB-41758.
	tk := e.GetTestingKnobs()
	if tk == nil {
		// TODO(spilchen): In CRDB-41758, remove the use of an environment variable
		// as we want to avoid providing an easy way to disable the enforcer.
		return !envutil.EnvOrDefaultBool("COCKROACH_ENABLE_LICENSE_ENFORCER", false)
	}
	return !tk.Enable
}

// getIsNewClusterEstimate is a helper to determine if the cluster is a newly
// created one. This is used in Start processing to help determine the length
// of the grace period when no license is installed.
func (e *Enforcer) getIsNewClusterEstimate(ctx context.Context, txn isql.Txn) (bool, error) {
	data, err := txn.QueryRow(ctx, "check if enforcer start is near cluster init time", txn.KV(),
		`SELECT min(completed_at) FROM system.migrations`)
	if err != nil {
		return false, err
	}
	if len(data) == 0 {
		return false, errors.New("no rows found in system.migrations")
	}
	var ts time.Time
	switch t := data[0].(type) {
	case *tree.DTimestampTZ:
		ts = t.Time
	default:
		return false, errors.Newf("unexpected data type: %v", t)
	}

	// We are going to lean on system.migrations to determine if the cluster is
	// new or not. If the cluster is new, the minimum value for the completed_at
	// column should roughly match the start time of this enforcer. We will query
	// that value and see if it's within 2 hours of it. If it's within that range
	// we treat it as if it's a new cluster.
	st := e.getStartTime()
	if st.After(ts.Add(-1*time.Hour)) && st.Before(ts.Add(1*time.Hour)) {
		return true, nil
	}
	log.Infof(ctx, "cluster init is not within the bounds of the enforcer start time: %v", ts)
	return false, nil
}

// getLicenseExpiryTS returns the license expiration timestamp.
// If no license is installed, it returns false for the second parameter.
func (e *Enforcer) getLicenseExpiryTS() (ts time.Time, ok bool) {
	if !e.hasLicense.Load() {
		return
	}
	ts = timeutil.Unix(e.licenseExpiryTS.Load(), 0)
	ok = true
	return
}

// addThrottleWarningDelayForNoTelemetry will return the time when we should start emitting
// a notice that we haven't received telemetry and we will throttle soon.
func (e *Enforcer) addThrottleWarningDelayForNoTelemetry(t time.Time) time.Time {
	return t.Add(3 * 24 * time.Hour)
}
