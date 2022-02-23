// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func TestAuthenticateWithSessionRevivalToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()
	s, mainDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	defer mainDB.Close()
	tenant, tenantDB := serverutils.StartTenant(t, s, tests.CreateTestTenantParams(serverutils.TestTenantID()))
	defer tenant.Stopper().Stop(ctx)
	defer tenantDB.Close()

	_, err := tenantDB.Exec("CREATE USER testuser WITH PASSWORD 'hunter2'")
	require.NoError(t, err)
	_, err = tenantDB.Exec("SET CLUSTER SETTING server.user_login.session_revival_token.enabled = true")
	require.NoError(t, err)

	var token string
	t.Run("generate token", func(t *testing.T) {
		pgURL, cleanup := sqlutils.PGUrl(
			t,
			tenant.SQLAddr(),
			"TestToken1",
			url.UserPassword(security.TestUser, "hunter2"),
		)
		defer cleanup()

		conn, err := pgx.Connect(ctx, pgURL.String())
		require.NoError(t, err)
		err = conn.QueryRow(ctx, "SELECT encode(crdb_internal.create_session_revival_token(), 'base64')").Scan(&token)
		require.NoError(t, err)
	})

	t.Run("authenticate with token", func(t *testing.T) {
		pgURL, cleanup := sqlutils.PGUrl(
			t,
			tenant.SQLAddr(),
			"TestToken2",
			url.User(security.TestUser),
		)
		defer cleanup()

		q := pgURL.Query()
		q.Add("crdb:session_revival_token_base64", token)
		pgURL.RawQuery = q.Encode()
		connWithToken, err := pgx.Connect(ctx, pgURL.String())
		require.NoError(t, err)

		var b bool
		err = connWithToken.QueryRow(
			ctx,
			"SELECT crdb_internal.validate_session_revival_token(decode($1, 'base64'))",
			token,
		).Scan(&b)
		require.NoError(t, err)
		require.True(t, b)
	})

	t.Run("use a token with invalid signature", func(t *testing.T) {
		pgURL, cleanup := sqlutils.PGUrl(
			t,
			tenant.SQLAddr(),
			"TestToken",
			url.User(security.TestUser),
		)
		defer cleanup()

		timestampProto := func(ts time.Time) *pbtypes.Timestamp {
			p, err := pbtypes.TimestampProto(ts)
			require.NoError(t, err)
			return p
		}
		payload := &sessiondatapb.SessionRevivalToken_Payload{
			User:      security.TestUser,
			Algorithm: x509.Ed25519.String(),
			ExpiresAt: timestampProto(timeutil.Now().Add(5 * time.Minute)),
			IssuedAt:  timestampProto(timeutil.Now().Add(-3 * time.Minute)),
		}
		payloadBytes, err := protoutil.Marshal(payload)
		require.NoError(t, err)
		badToken := &sessiondatapb.SessionRevivalToken{
			Payload:   payloadBytes,
			Signature: []byte("fake signature"),
		}
		badTokenBytes, err := protoutil.Marshal(badToken)
		require.NoError(t, err)

		q := pgURL.Query()
		q.Add("crdb:session_revival_token_base64", base64.StdEncoding.EncodeToString(badTokenBytes))
		pgURL.RawQuery = q.Encode()
		_, err = pgx.Connect(ctx, pgURL.String())
		require.Contains(t, err.Error(), "invalid session revival token: invalid signature")
	})

	t.Run("use a token that is not in base64 format", func(t *testing.T) {
		pgURL, cleanup := sqlutils.PGUrl(
			t,
			tenant.SQLAddr(),
			"TestToken",
			url.User(security.TestUser),
		)
		defer cleanup()

		q := pgURL.Query()
		q.Add("crdb:session_revival_token_base64", "ABC!@#")
		pgURL.RawQuery = q.Encode()
		_, err = pgx.Connect(ctx, pgURL.String())
		require.Contains(t, err.Error(), "crdb:session_revival_token_base64: illegal base64 data")
	})
}
