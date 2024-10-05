// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// Test that if a user is dropped while a session is active, the session can
// no longer access objects in the cluster.
func TestDropRoleConcurrentSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, rootDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	_, err := rootDB.Exec(`CREATE USER testuser`)
	require.NoError(t, err)
	_, err = rootDB.Exec(`CREATE ROLE testrole`)
	require.NoError(t, err)
	_, err = rootDB.Exec(`GRANT testrole TO testuser`)
	require.NoError(t, err)
	_, err = rootDB.Exec(`CREATE TABLE inherit_from_public (a INT PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = rootDB.Exec(`INSERT INTO inherit_from_public VALUES (1)`)
	require.NoError(t, err)
	_, err = rootDB.Exec(`GRANT SELECT ON inherit_from_public TO public`)
	require.NoError(t, err)
	_, err = rootDB.Exec(`CREATE TABLE inherit_from_role (a INT PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = rootDB.Exec(`INSERT INTO inherit_from_role VALUES (2)`)
	require.NoError(t, err)
	_, err = rootDB.Exec(`GRANT SELECT ON inherit_from_role TO testrole`)
	require.NoError(t, err)

	// Start a session as testuser.
	testuserConn, err := s.ApplicationLayer().SQLConnE(serverutils.User("testuser"), serverutils.DBName(""))
	require.NoError(t, err)
	defer func() { _ = testuserConn.Close() }()

	// Also create a web session for testuser.
	_, err = rootDB.Exec(`INSERT INTO system.web_sessions ("hashedSecret", username, "expiresAt", user_id)
  VALUES ($1, $2, $3, (SELECT user_id FROM system.users WHERE username = $2))`,
		"secret", "testuser", timeutil.Now().Add(24*time.Hour))
	require.NoError(t, err)

	// Verify that testuser can access the cluster.
	var i int
	err = testuserConn.QueryRow(`SELECT * FROM inherit_from_public`).Scan(&i)
	require.NoError(t, err)
	require.Equal(t, 1, i)
	err = testuserConn.QueryRow(`SELECT * FROM inherit_from_role`).Scan(&i)
	require.NoError(t, err)
	require.Equal(t, 2, i)
	_, err = testuserConn.Exec("CREATE TABLE new1(a INT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = testuserConn.Exec("DROP TABLE new1")
	require.NoError(t, err)

	// Drop testuser, and verify that it no longer has access.
	_, err = rootDB.Exec(`DROP USER testuser`)
	require.NoError(t, err)
	err = testuserConn.QueryRow(`SELECT * FROM inherit_from_public`).Scan(&i)
	require.ErrorContains(t, err, "role testuser was concurrently dropped")
	err = testuserConn.QueryRow(`SELECT * FROM inherit_from_role`).Scan(&i)
	require.ErrorContains(t, err, "role testuser was concurrently dropped")
	_, err = testuserConn.Exec("CREATE TABLE new2(a INT PRIMARY KEY)")
	require.ErrorContains(t, err, "role testuser was concurrently dropped")

	var revokedAt time.Time
	err = rootDB.QueryRow(`SELECT "revokedAt" FROM system.web_sessions WHERE username = $1`, "testuser").Scan(&revokedAt)
	require.NoError(t, err)
	require.Less(t, revokedAt, timeutil.Now())
}
