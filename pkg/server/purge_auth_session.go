// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	math_rand "math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	webSessionPurgeTTL = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.web_session.purge.ttl",
		"if nonzero, entries in system.web_sessions older than this duration are periodically purged",
		time.Hour,
	).WithPublic()

	webSessionAutoLogoutTimeout = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.web_session.auto_logout.timeout",
		"the duration that web sessions will survive before being periodically purged, since they were last used",
		7*24*time.Hour,
		settings.NonNegativeDuration,
	).WithPublic()

	webSessionPurgePeriod = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.web_session.purge.period",
		"the time until old sessions are deleted",
		time.Hour,
		settings.NonNegativeDuration,
	).WithPublic()

	webSessionPurgeLimit = settings.RegisterIntSetting(
		settings.TenantWritable,
		"server.web_session.purge.max_deletions_per_cycle",
		"the maximum number of old sessions to delete for each purge",
		10,
	).WithPublic()
)

// startPurgeOldSessions runs an infinite loop in a goroutine
// which regularly deletes old rows in the system.web_sessions table.
func startPurgeOldSessions(ctx context.Context, s *authenticationServer) error {
	return s.sqlServer.stopper.RunAsyncTask(ctx, "purge-old-sessions", func(context.Context) {
		settingsValues := &s.sqlServer.execCfg.Settings.SV
		period := webSessionPurgePeriod.Get(settingsValues)

		timer := timeutil.NewTimer()
		defer timer.Stop()
		timer.Reset(jitteredInterval(period))

		for ; ; timer.Reset(webSessionPurgePeriod.Get(settingsValues)) {
			select {
			case <-timer.C:
				timer.Read = true
				s.purgeOldSessions(ctx)
			case <-s.sqlServer.stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
	},
	)
}

// purgeOldSessions deletes old web session records.
// Performs three purges: (1) one for sessions with expiration
// older than the purge TTL, (2) one for sessions with revocation
// older than the purge TTL, and (3) one for sessions that have
// timed out since they were last used.
func (s *authenticationServer) purgeOldSessions(ctx context.Context) {
	var (
		deleteOldExpiredSessionsStmt = `
DELETE FROM system.web_sessions
WHERE "expiresAt" < $1
ORDER BY random()
LIMIT $2
RETURNING 1
`
		deleteOldRevokedSessionsStmt = `
DELETE FROM system.web_sessions
WHERE "revokedAt" < $1
ORDER BY random()
LIMIT $2
RETURNING 1
`
		deleteSessionsAutoLogoutStmt = `
DELETE FROM system.web_sessions
WHERE "lastUsedAt" < $1
ORDER BY random()
LIMIT $2
RETURNING 1
`
		settingsValues   = &s.sqlServer.execCfg.Settings.SV
		internalExecutor = s.sqlServer.internalExecutor
		currTime         = s.sqlServer.execCfg.Clock.PhysicalTime()

		purgeTTL          = webSessionPurgeTTL.Get(settingsValues)
		autoLogoutTimeout = webSessionAutoLogoutTimeout.Get(settingsValues)
		limit             = webSessionPurgeLimit.Get(settingsValues)

		purgeTime      = currTime.Add(purgeTTL * time.Duration(-1))
		autoLogoutTime = currTime.Add(autoLogoutTimeout * time.Duration(-1))
	)

	if _, err := internalExecutor.ExecEx(
		ctx,
		"delete-old-expired-sessions",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		deleteOldExpiredSessionsStmt,
		purgeTime,
		limit,
	); err != nil {
		log.Errorf(ctx, "error while deleting old expired web sessions: %+v", err)
	}

	if _, err := internalExecutor.ExecEx(
		ctx,
		"delete-old-revoked-sessions",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		deleteOldRevokedSessionsStmt,
		purgeTime,
		limit,
	); err != nil {
		log.Errorf(ctx, "error while deleting old revoked web sessions: %+v", err)
	}

	if _, err := internalExecutor.ExecEx(
		ctx,
		"delete-sessions-timeout",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		deleteSessionsAutoLogoutStmt,
		autoLogoutTime,
		limit,
	); err != nil {
		log.Errorf(ctx, "error while deleting web sessions older than auto-logout timeout: %+v", err)
	}
}

// jitteredInterval returns a randomly jittered (+/-25%) duration
// from the interval.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.75 + 0.5*math_rand.Float64()))
}
