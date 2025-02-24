// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestEmptyQuery is a regression test to ensure that sending an empty
// query to the database as the first query is safe.
func TestEmptyQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{Insecure: false, UseDatabase: "defaultdb"})
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanupFunc := pgurlutils.PGUrl(
		t, s.ApplicationLayer().AdvSQLAddr(), "testConnClose" /* prefix */, url.User(username.RootUser),
	)
	defer cleanupFunc()

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(t, err)

	err = conn.QueryRow(ctx, "").Scan()
	require.Error(t, err)
	require.Regexp(t, "no rows in result set", err)
}
