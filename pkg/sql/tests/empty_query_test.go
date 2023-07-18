// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

// TestEmptyQuery is a regression test to ensure that sending an empty
// query to the database as the first query is safe.
func TestEmptyQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false, UseDatabase: "defaultdb"})
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, s.ServingSQLAddr(), "testConnClose" /* prefix */, url.User(username.RootUser),
	)
	defer cleanupFunc()

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(t, err)

	err = conn.QueryRow(ctx, "").Scan()
	require.Error(t, err)
	require.Regexp(t, "no rows in result set", err)
}
