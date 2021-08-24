// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestOnUpdateVersionGating(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.OnUpdateExpressions - 1,
					),
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(db)

	tdb.ExpectErr(t,
		"pq: version 21.1-152 must be finalized to use ON UPDATE",
		"CREATE TABLE test (p INT, j INT ON UPDATE 5)")

	tdb.Exec(t, "CREATE TABLE test (p INT, j INT);")

	tdb.ExpectErr(t,
		"pq: version 21.1-152 must be finalized to use ON UPDATE",
		"ALTER TABLE test ALTER COLUMN j SET ON UPDATE 5")

	tdb.ExpectErr(t,
		"pq: version 21.1-152 must be finalized to use ON UPDATE",
		"ALTER TABLE test ADD COLUMN k INT ON UPDATE 5")

	tdb.Exec(t,
		"SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.OnUpdateExpressions).String(),
	)

	tdb.Exec(t, "CREATE TABLE test_create (p INT, j INT ON UPDATE 5)")

	tdb.Exec(t, "ALTER TABLE test ALTER COLUMN j SET ON UPDATE 5")

	tdb.Exec(t, "ALTER TABLE test ADD COLUMN k INT ON UPDATE 5")
}
