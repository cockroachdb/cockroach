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
		"pq: version 21.1-150 must be finalized to use ON UPDATE",
		"CREATE TABLE test (p INT, j INT ON UPDATE 5)")

	tdb.ExecSucceedsSoon(t, "CREATE TABLE test (p INT, j INT);")

	tdb.ExpectErr(t,
		"pq: version 21.1-150 must be finalized to use ON UPDATE",
		"ALTER TABLE test ALTER COLUMN j SET ON UPDATE 5")

	tdb.ExpectErr(t,
		"pq: version 21.1-150 must be finalized to use ON UPDATE",
		"ALTER TABLE test ADD COLUMN j INT ON UPDATE 5")
}
