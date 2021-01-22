// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/jackc/pgx"
	"github.com/stretchr/testify/require"
)

// TestPGXWorksWithTestCluster was written to demonstrate some problem with
// using the testcluster with PGX.
func TestPGXWorksWithTestCluster(t *testing.T) {
	defer leaktest.AfterTest(t)
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, "CREATE USER testuser")
	pgURL, cleanup := sqlutils.PGUrl(t, tc.Server(0).ServingSQLAddr(), "", url.User("testuser"))
	defer cleanup()
	cfg, err := pgx.ParseConnectionString(pgURL.String())
	require.NoError(t, err)
	cp, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig:     cfg,
		MaxConnections: 1,
	})
	require.NoError(t, err)

	{
		var one int
		err := cp.QueryRow(`SELECT 1`).Scan(&one)
		require.NoError(t, err)
		require.Equal(t, 1, one)
	}
}
