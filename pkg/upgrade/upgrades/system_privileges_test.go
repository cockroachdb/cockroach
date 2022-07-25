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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestSystemPrivilegesMigration(t *testing.T) {
	skip.UnderStressRace(t)
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.ByKey(clusterversion.SystemPrivilegesTable-1),
		false,
	)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.SystemPrivilegesTable - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	tdb := sqlutils.MakeSQLRunner(db)

	// Delete system.role_id_seq.
	tdb.Exec(t, `INSERT INTO system.users VALUES ('node', '', false)`)
	tdb.Exec(t, `GRANT node TO root`)
	tdb.Exec(t, `DROP TABLE system.privileges`)
	tdb.Exec(t, `REVOKE node FROM root`)

	upgrades.Upgrade(
		t,
		db,
		clusterversion.SystemPrivilegesTable,
		nil,
		false,
	)

	tdb.Exec(t, `INSERT INTO system.privileges VALUES 
('user1', 'path/1/user', ARRAY['10'], ARRAY['10']), 
('user1', 'path/2/user', ARRAY['0', '0'], ARRAY['1', '0']),
('user2', 'path/1/user', ARRAY['1', '0'], ARRAY['0']),
('user2', 'path/2/user', ARRAY['10'], ARRAY['10'])`)

	tdb.CheckQueryResults(t, `SELECT * FROM system.privileges ORDER BY 1, 2`, [][]string{
		{"user1", "path/1/user", "{10}", "{10}"},
		{"user1", "path/2/user", "{0,0}", "{1,0}"},
		{"user2", "path/1/user", "{1,0}", "{0}"},
		{"user2", "path/2/user", "{10}", "{10}"},
	})
}
