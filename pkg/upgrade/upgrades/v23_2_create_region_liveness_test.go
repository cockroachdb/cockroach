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
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestRegionLivenessTableMigration(t *testing.T) {
	skip.UnderRace(t)
	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, 23, 2)

	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          (clusterversion.V23_2_RegionaLivenessTable - 1).Version(),
				},
			},
		},
	}

	tc := testcluster.StartTestCluster(t, 1, clusterArgs)

	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	defer db.Close()

	upgrades.Upgrade(
		t,
		db,
		clusterversion.V23_2_RegionaLivenessTable,
		nil,
		false,
	)

	_, err := db.Exec("SELECT * FROM system.region_liveness")
	assert.NoError(t, err, "system.region_liveness exists")

	upgrades.ValidateSystemDatabaseSchemaVersionBumped(t, db, clusterversion.V23_2_RegionaLivenessTable)
}
