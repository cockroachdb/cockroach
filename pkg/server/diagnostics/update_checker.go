// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package diagnostics

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	updateCheckFrequency = time.Hour * 24
	// TODO(dt): switch to settings.
	updateCheckPostStartup    = time.Minute * 5
	updateCheckRetryFrequency = time.Hour
	updateMaxVersionsToReport = 3
)

type versionInfo struct {
	Version string `json:"version"`
	Details string `json:"details"`
}

// UpdateChecker is a helper struct that phones home to check for updates.
type UpdateChecker struct {
	StartTime  time.Time
	AmbientCtx *log.AmbientContext
	Config     *base.Config
	Settings   *cluster.Settings

	// ClusterID is not yet available at the time the updater is created, so
	// instead initialize with a function to get it.
	ClusterID func() uuid.UUID

	// NodeID is not yet available at the time the updater is created, so
	// instead initialize with a function to get it.
	NodeID func() roachpb.NodeID

	// SQLInstanceID is not yet available at the time the reporter is created,
	// so instead initialize with a function that gets it dynamically.
	SQLInstanceID func() base.SQLInstanceID

	// TestingKnobs is used for internal test controls only.
	TestingKnobs *TestingKnobs
}

// PeriodicallyCheckForUpdates starts a background worker that periodically
// phones home to check for updates.
func (u *UpdateChecker) PeriodicallyCheckForUpdates(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "update-checker", func(ctx context.Context) {
		defer logcrash.RecoverAndReportNonfatalPanic(ctx, &u.Settings.SV)
		nextUpdateCheck := u.StartTime

		var timer timeutil.Timer
		defer timer.Stop()
		for {
			now := timeutil.Now()
			runningTime := now.Sub(u.StartTime)

			nextUpdateCheck = u.maybeCheckForUpdates(ctx, now, nextUpdateCheck, runningTime)

			timer.Reset(addJitter(nextUpdateCheck.Sub(timeutil.Now())))
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				timer.Read = true
			}
		}
	})
}

// CheckForUpdates calls home to check for new versions for the current platform
// and logs messages if it finds them, as well as if it encounters any errors.
// The returned boolean indicates if the check succeeded (and thus does not need
// to be re-attempted by the scheduler after a retry-interval).
func (u *UpdateChecker) CheckForUpdates(ctx context.Context) bool {
	ctx, span := u.AmbientCtx.AnnotateCtxWithSpan(ctx, "usageReport")
	defer span.Finish()

	url := u.buildUpdatesURL(ctx)
	if url == nil {
		return true // don't bother with asking for retry -- we'll never succeed.
	}

	res, err := httputil.Get(ctx, url.String())
	if err != nil {
		// This is probably going to be relatively common in production
		// environments where network access is usually curtailed.
		return false
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(res.Body)
		log.Infof(ctx, "failed to check for updates: status: %s, body: %s, error: %v",
			res.Status, b, err)
		return false
	}

	decoder := json.NewDecoder(res.Body)
	r := struct {
		Details []versionInfo `json:"details"`
	}{}

	err = decoder.Decode(&r)
	if err != nil && err != io.EOF {
		log.Warningf(ctx, "error decoding updates info: %v", err)
		return false
	}

	// Ideally the updates server only returns the most relevant updates for us,
	// but if it replied with an excessive number of updates, limit log spam by
	// only printing the last few.
	if len(r.Details) > updateMaxVersionsToReport {
		r.Details = r.Details[len(r.Details)-updateMaxVersionsToReport:]
	}
	for _, v := range r.Details {
		log.Infof(ctx, "a new version is available: %s, details: %s", v.Version, v.Details)
	}
	return true
}

// maybeCheckForUpdates determines if it is time to check for updates and does
// so if it is, before returning the time at which the next check be done.
func (u *UpdateChecker) maybeCheckForUpdates(
	ctx context.Context, now, scheduled time.Time, runningTime time.Duration,
) time.Time {
	if scheduled.After(now) {
		return scheduled
	}

	// If diagnostics reporting is disabled, we should assume that means that the
	// user doesn't want us phoning home for new-version checks either.
	if !logcrash.DiagnosticsReportingEnabled.Get(&u.Settings.SV) {
		return now.Add(updateCheckFrequency)
	}

	// checkForUpdates handles its own errors, but it returns a bool indicating if
	// it succeeded, so we can schedule a re-attempt if it did not.
	if succeeded := u.CheckForUpdates(ctx); !succeeded {
		return now.Add(updateCheckRetryFrequency)
	}

	// If we've just started up, we want to check again shortly after.
	// During startup is when a message is most likely to be actually seen by a
	// human operator so we check as early as possible, but this makes it hard to
	// differentiate real deployments vs short-lived instances for tests.
	if runningTime < updateCheckPostStartup {
		return now.Add(time.Hour - runningTime)
	}

	return now.Add(updateCheckFrequency)
}

// BuildUpdatesURL creates a URL to check for version updates.
// If an empty updates URL is set (via empty environment variable), returns nil.
func (u *UpdateChecker) buildUpdatesURL(ctx context.Context) *url.URL {
	clusterInfo := ClusterInfo{
		ClusterID:  u.ClusterID(),
		TenantID:   roachpb.SystemTenantID,
		IsInsecure: u.Config.Insecure,
		IsInternal: sql.ClusterIsInternal(&u.Settings.SV),
	}

	var env diagnosticspb.Environment
	env.Build = build.GetInfo()
	env.LicenseType = getLicenseType(ctx, u.Settings)
	populateHardwareInfo(ctx, &env)

	sqlInfo := diagnosticspb.SQLInstanceInfo{
		SQLInstanceID: u.SQLInstanceID(),
		Uptime:        int64(timeutil.Now().Sub(u.StartTime).Seconds()),
	}

	url := updatesURL
	if u.TestingKnobs != nil && u.TestingKnobs.OverrideUpdatesURL != nil {
		url = *u.TestingKnobs.OverrideUpdatesURL
	}
	return addInfoToURL(url, &clusterInfo, &env, u.NodeID(), &sqlInfo)
}
