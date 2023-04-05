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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestSystemJobInfoMigration(t *testing.T) {
	skip.UnderStressRace(t)
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.BinaryMinSupportedVersionKey),
					BootstrapVersionKeyOverride:    clusterversion.BinaryMinSupportedVersionKey,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()

	// Advance to the version immediately before the one that
	// interests us.
	upgrades.Upgrade(
		t,
		db,
		clusterversion.V23_1CreateSystemJobInfoTable-1,
		nil,
		false,
	)

	// We verify that the jobs table gets its version upgraded through
	// the upgrade, to ensure the creation of job_info synchronizes with
	// concurrent accesses to the jobs table.
	kvDB := tc.Server(0).(*server.TestServer).DB()
	tblBefore := desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "system", "public", "jobs")

	upgrades.Upgrade(
		t,
		db,
		clusterversion.V23_1CreateSystemJobInfoTable,
		nil,
		false,
	)

	tblAfter := desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "system", "public", "jobs")
	require.Greater(t, tblAfter.GetVersion(), tblBefore.GetVersion())
}
