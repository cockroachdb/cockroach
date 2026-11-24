// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlccl

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestAuthenticateWithSessionRevivalToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, mainDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)
	defer mainDB.Close()
	tenant, tenantDB := serverutils.StartTenant(t, s, base.TestTenantArgs{
		TenantID: serverutils.TestTenantID(),
	})
	defer tenant.AppStopper().Stop(ctx)
	defer tenantDB.Close()

	_, err := tenantDB.Exec("CREATE USER testuser WITH PASSWORD 'hunter2'")
	require.NoError(t, err)
	sql.AllowSessionRevival.Override(ctx, &tenant.ClusterSettings().SV, true)

	var token string
	t.Run("generate token", func(t *testing.T) {
		conn := tenant.SQLConn(t, serverutils.User(username.TestUser))
		err := conn.QueryRowContext(ctx, "SELECT encode(crdb_internal.create_session_revival_token(), 'base64')").Scan(&token)
		require.NoError(t, err)
	})

	t.Run("authenticate with token", func(t *testing.T) {
		pgURL, cleanup := pgurlutils.PGUrl(
			t,
			tenant.SQLAddr(),
			"TestToken2",
			url.User(username.TestUser),
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
		pgURL, cleanup := pgurlutils.PGUrl(
			t,
			tenant.SQLAddr(),
			"TestToken",
			url.User(username.TestUser),
		)
		defer cleanup()

		timestampProto := func(ts time.Time) *pbtypes.Timestamp {
			p, err := pbtypes.TimestampProto(ts)
			require.NoError(t, err)
			return p
		}
		payload := &sessiondatapb.SessionRevivalToken_Payload{
			User:      username.TestUser,
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
		pgURL, cleanup := pgurlutils.PGUrl(
			t,
			tenant.SQLAddr(),
			"TestToken",
			url.User(username.TestUser),
		)
		defer cleanup()

		q := pgURL.Query()
		q.Add("crdb:session_revival_token_base64", "ABC!@#")
		pgURL.RawQuery = q.Encode()
		_, err = pgx.Connect(ctx, pgURL.String())
		require.Contains(t, err.Error(), "crdb:session_revival_token_base64: illegal base64 data")
	})
}
