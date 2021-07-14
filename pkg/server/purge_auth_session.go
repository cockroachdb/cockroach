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
		"server.web_session.purge.ttl",
		"if nonzero, entries in system.web_sessions older than this duration are periodically purged",
		time.Hour,
	).WithPublic()

	webSessionAutoLogoutTimeout = settings.RegisterDurationSetting(
		"server.web_session.auto_logout.timeout",
		"the duration that web sessions will survive before being periodically purged, since they were last used",
		7*24*time.Hour,
		settings.NonNegativeDuration,
	).WithPublic()

	webSessionPurgePeriod = settings.RegisterDurationSetting(
		"server.web_session.purge.period",
		"the time until old sessions are deleted",
		time.Hour,
		settings.NonNegativeDuration,
	).WithPublic()

	webSessionPurgeLimit = settings.RegisterIntSetting(
		"server.web_session.purge_limit",
		"the maximum number of sessions to delete per purge",
		10,
	).WithPublic()
)

// runDaemon runs an infinite loop in a goroutine which regularly deletes
// old expired web session records.
func runDaemon(ctx context.Context, s *authenticationServer) error {
	return s.server.stopper.RunAsyncTask(ctx, "purge-old-sessions", func(context.Context) {
		settingsValues := &s.server.st.SV
		period := webSessionPurgePeriod.Get(settingsValues)

		timer := timeutil.NewTimer()
		defer timer.Stop()
		timer.Reset(jitteredInterval(period))

		for ; ; timer.Reset(webSessionPurgePeriod.Get(settingsValues)) {
			select {
			case <-timer.C:
				timer.Read = true
				s.purgeOldSessions(ctx)
			case <-s.server.stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
	},
	)
}

// purgeOldSessions deletes old expired web session records.
func (s *authenticationServer) purgeOldSessions(ctx context.Context) {
	settingsValues := &s.server.st.SV

	// TODO(cameron): LIMIT is used here to avoid a very large initial transaction.
	// However, this may prevent deletion of a large backlog of entries.
	// This needs to be addressed in a later iteration.
	// May be useful to look at startupmigrations/migrations for where to implement this.
	// See: https://github.com/cockroachdb/cockroach/issues/67933
	deleteSessionsStmt := `
WITH
	deleted_sessions_from_expired_purge_ttl AS (
		DELETE FROM system.web_sessions@"web_sessions_expiresAt_idx"
		WHERE "expiresAt" < $1
    ORDER BY random()
    LIMIT $3
    RETURNING 1
	),
	deleted_sessions_from_revoked_purge_ttl AS (
		DELETE FROM system.web_sessions@"web_sessions_revokedAt_idx"
		WHERE "revokedAt" < $1
    ORDER BY random()
    LIMIT $3
    RETURNING 1
	),
	deleted_sessions_from_auto_logout AS (
		DELETE FROM system.web_sessions@"web_sessions_lastUsedAt_idx"
		WHERE "lastUsedAt" < $2
    ORDER BY random()
    LIMIT $3
    RETURNING 1
	)
SELECT 1
`
	currTime := s.server.clock.PhysicalTime()

	purgeTTL := webSessionPurgeTTL.Get(settingsValues)
	autoLogoutTimeout := webSessionAutoLogoutTimeout.Get(settingsValues)
	limit := webSessionPurgeLimit.Get(settingsValues)

	purgeTime := currTime.Add(purgeTTL * time.Duration(-1))
	autoLogoutTime := currTime.Add(autoLogoutTimeout * time.Duration(-1))

	if _, err := s.server.sqlServer.internalExecutor.QueryRowEx(
		ctx,
		"delete-old-sessions",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		deleteSessionsStmt,
		purgeTime,
		autoLogoutTime,
		limit,
	); err != nil {
		log.Errorf(ctx, "error while deleting old web sessions: %+v", err)
	}
}

// jitteredInterval returns a randomly jittered (+/-25%) duration
// from the interval.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.75 + 0.5*math_rand.Float64()))
}
