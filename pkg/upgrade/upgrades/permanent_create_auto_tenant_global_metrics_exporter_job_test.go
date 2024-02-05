// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCreateAutoTenantGlobalMetricsExporterJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.MinSupported.Version(),
				},
			},
		},
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	sqlDB := tc.ServerConn(0)
	defer tc.Stopper().Stop(ctx)

	row := sqlDB.QueryRow("SELECT count(*) FROM system.public.jobs WHERE id = 105")
	var count int
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 0, count)

	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.Permanent_V24_1_CreateAutoTenantGlobalMetricsExporterJob,
		nil,
		false,
	)

	row = sqlDB.QueryRow("SELECT count(*) FROM system.public.jobs WHERE id = 105")
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 1, count)
}
