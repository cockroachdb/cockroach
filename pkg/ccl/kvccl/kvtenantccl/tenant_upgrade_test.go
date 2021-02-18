// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvtenantccl_test

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTenantUpgrade(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	var err error
	tenantArgs := base.TestTenantArgs{
		TenantID:     roachpb.MakeTenantID(10),
		TestingKnobs: base.TestingKnobs{},
	}

	// Prevent a logging assertion that the server ID is initialized multiple times.
	log.TestingClearServerIdentifiers()

	tenant, err := tc.Server(0).StartTenant(tenantArgs)
	require.NoError(t, err)

	pgURL, cleanup := sqlutils.PGUrl(t, tenant.SQLAddr(), "Tenant", url.User(security.RootUser))
	defer cleanup()

	db, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("SET CLUSTER SETTING version TO $1",
		clusterversion.TestingBinaryVersion.String())
	require.NoError(t, err)
}
