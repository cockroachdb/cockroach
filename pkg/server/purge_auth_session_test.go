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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestPurgeSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})

	defer s.Stopper().Stop(ctx)

	ts := s.(*TestServer)
	username := security.TestUserName()

	_, hashedSecret, err := CreateAuthSecret()
	if err != nil {
		t.Fatal(err)
	}

	// Customize the cluster settings to lower values than the defaults.
	if _, err := db.Exec(`SET CLUSTER SETTING server.web_session.purge.ttl = '5s'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`SET CLUSTER SETTING server.web_session.auto_logout.timeout = '10s'`); err != nil {
		t.Fatal(err)
	}

	settingsValues := &ts.st.SV
	var (
		purgeTTL          = webSessionPurgeTTL.Get(settingsValues)
		autoLogoutTimeout = webSessionAutoLogoutTimeout.Get(settingsValues)
	)

	insertSessionStmt := `
INSERT INTO system.web_sessions ("hashedSecret", username, "expiresAt", "revokedAt", "lastUsedAt") 
VALUES($1, $2, $3, $4, $5)
`
	// Inserts three seemingly-old sessions.
	// Each iteration of the loop inserts a session, rewinding the age of
	// the given timestamp column with each iteration.
	insertOldSessions := func(column string) {
		currTime := ts.clock.PhysicalTime()

		// Initialize each timestamp column at the current time.
		expiresAt, revokedAt, lastUsedAt := currTime, currTime, currTime

		// A few extra seconds of margin to be added to a timestamp.
		// This helps avoid having deletion checks at the boundary of valid
		// deletion.
		margin := 5 * time.Second

		// Create three seemingly-old sessions, rewinding the age of the
		// desired column with each iteration.
		for i := 0; i < 3; i++ {
			// Rewind the age of the timestamp column such that it's older
			// than the configured cluster setting values.
			switch column {
			case "expiresAt":
				durationSinceExpiration := purgeTTL + margin
				expiresAt = expiresAt.Add(durationSinceExpiration * time.Duration(-1))
			case "revokedAt":
				durationSinceRevocation := purgeTTL + margin
				revokedAt = revokedAt.Add(durationSinceRevocation * time.Duration(-1))
			case "lastUsedAt":
				durationSinceLastUsed := autoLogoutTimeout + margin
				lastUsedAt = lastUsedAt.Add(durationSinceLastUsed * time.Duration(-1))
			}
			if _, err = ts.sqlServer.internalExecutor.QueryRowEx(
				ctx,
				"add-session",
				nil, /* txn */
				sessiondata.InternalExecutorOverride{User: security.RootUserName()},
				insertSessionStmt,
				hashedSecret,
				username.Normalized(),
				expiresAt,
				revokedAt,
				lastUsedAt,
			); err != nil {
				t.Fatal(err)
			}
		}
	}

	webSessionCount := func() int {
		var count int
		if err := db.QueryRow("SELECT count(*) FROM system.web_sessions").Scan(&count); err != nil {
			t.Fatalf("failed to get web sessions count: %v", err)
		}
		return count
	}

	// Check deletion for old expired sessions.
	insertOldSessions("expiresAt")

	ts.authentication.purgeOldSessions(ctx)

	if webSessionCount() != 0 {
		t.Fatal("failed to delete sessions with expiration older than the purge TTL")
	}

	// Check deletion for old revoked sessions.
	insertOldSessions("revokedAt")

	ts.authentication.purgeOldSessions(ctx)

	if webSessionCount() != 0 {
		t.Fatal("failed to delete sessions with revocation older than the purge TTL")
	}

	// Check deletion for sessions that have timed out since they
	// were last used.
	insertOldSessions("lastUsedAt")

	ts.authentication.purgeOldSessions(ctx)

	if webSessionCount() != 0 {
		t.Fatal("failed to delete sessions after the auto-logout timeout")
	}
}
