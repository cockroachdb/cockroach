// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestCreateActivityUpdateJobMigration(t *testing.T) {
	skip.UnderStressRace(t)
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		false,
	)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.TestingBinaryMinSupportedVersion,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()

	// NB: this isn't actually doing anything, since the table is baked into the
	// bootstrap schema, so this is really just showing the upgrade is idempotent,
	// but this is in line with the other tests of createSystemTable upgrades.
	upgrades.Upgrade(
		t,
		db,
		clusterversion.V23_1CreateSystemActivityUpdateJob,
		nil,
		false,
	)

	row := db.QueryRow("SELECT count(*) FROM system.public.jobs WHERE id = 103")
	assert.NotNil(t, row)
	assert.NoError(t, row.Err())
	var count int
	err := row.Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}
