// Copyright 2022 The Cockroach Authors.
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateAutoConfigRunnerJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.V23_1_CreateAutoConfigRunnerJob - 1),
				},
			},
		},
	}

	var (
		ctx   = context.Background()
		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)

	var count int
	row := sqlDB.QueryRow("SELECT count(*) FROM crdb_internal.jobs WHERE job_type = 'AUTO CONFIG RUNNER'")
	err := row.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, count, 0)

	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V23_1_CreateAutoConfigRunnerJob,
		nil,   /* done */
		false, /* expectError */
	)

	row = sqlDB.QueryRow("SELECT count(*) FROM crdb_internal.jobs WHERE job_type = 'AUTO CONFIG RUNNER'")
	err = row.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, count, 1)
}
