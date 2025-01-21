// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCreateTenantFromReplicationUsingID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	serverA, aDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer serverA.Stopper().Stop(ctx)
	serverB, bDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer serverB.Stopper().Stop(ctx)

	sqlA := sqlutils.MakeSQLRunner(aDB)
	sqlB := sqlutils.MakeSQLRunner(bDB)

	serverAURL := replicationtestutils.GetReplicationURI(t, serverA, serverB, serverutils.User(username.RootUser))

	verifyCreatedTenant := func(t *testing.T, db *sqlutils.SQLRunner, id int64, fn func()) {
		const query = "SELECT count(*), count(CASE WHEN id = $1 THEN 1 END) FROM system.tenants"
		var rowCountPrev, hasTenant int64
		db.QueryRow(t, query, id).Scan(&rowCountPrev, &hasTenant)
		require.Zero(t, hasTenant)
		fn()
		var rowCountNext int64
		db.QueryRow(t, query, id).Scan(&rowCountNext, &hasTenant)
		require.Equal(t, rowCountPrev+1, rowCountNext)
		require.Equal(t, int64(1), hasTenant)
	}

	verifyCreatedTenant(t, sqlA, 50, func() {
		t.Logf("creating tenant [50]")
		sqlA.Exec(t, "CREATE VIRTUAL CLUSTER [50]")
		sqlA.Exec(t, "ALTER VIRTUAL CLUSTER [50] START SERVICE SHARED")
	})

	verifyCreatedTenant(t, sqlB, 51, func() {
		t.Logf("starting replication [50]->[51]")
		sqlB.Exec(t, "CREATE VIRTUAL CLUSTER [51] FROM REPLICATION OF 'cluster-50' ON $1", serverAURL.String())
	})
}
