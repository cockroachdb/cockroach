// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestPublicSchemaMigrationWithCreateChangefeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	knobs := base.TestingKnobs{
		DistSQL:          &execinfra.TestingKnobs{Changefeed: &TestingKnobs{}},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		Server: &server.TestingKnobs{
			DisableAutomaticVersionUpgrade: make(chan struct{}),
			BinaryVersionOverride:          clusterversion.ByKey(clusterversion.PublicSchemasWithDescriptors - 1),
		},
	}

	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Cannot set version cluster setting from tenants.
			DisableDefaultTestTenant: true,
			Knobs:                    knobs,
		},
	}

	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	s := tc.Server(0)

	sink, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()
	f := makeTableFeedFactory(s, db, sink)

	tdb := sqlutils.MakeSQLRunner(db)
	tdb.Exec(t, `CREATE TABLE defaultdb.public.t();`)

	// Save timestamp pre public schema migration to use for cursor time.
	var tsLogical string
	tdb.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsLogical)

	// Kick off public schema migration.
	{
		_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
			clusterversion.ByKey(clusterversion.PublicSchemasWithDescriptors).String())
		require.NoError(t, err)
	}

	tdb.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`)

	// Test passes if we can create and close the feed.
	out := feed(t, f, `CREATE CHANGEFEED FOR defaultdb.public.t WITH cursor=$1`, tsLogical)
	defer closeFeed(t, out)
}
