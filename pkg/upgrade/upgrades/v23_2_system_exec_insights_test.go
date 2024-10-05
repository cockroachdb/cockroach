// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestExecSystemInsights(t *testing.T) {
	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, 23, 2)

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

	var (
		ctx = context.Background()

		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		s     = tc.Server(0)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)
	require.True(t, s.ExecutorConfig().(sql.ExecutorConfig).Codec.ForSystemTenant())

	// Introduce the sequence.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V23_2_AddSystemExecInsightsTable,
		nil,
		false,
	)

	_, err := sqlDB.Exec("SELECT * FROM system.public.statement_execution_insights")
	require.NoError(t, err, "system.public.statement_execution_insights exists")

	_, err = sqlDB.Exec("SELECT * FROM system.public.transaction_execution_insights")
	require.NoError(t, err, "system.public.transaction_execution_insights exists")

}
