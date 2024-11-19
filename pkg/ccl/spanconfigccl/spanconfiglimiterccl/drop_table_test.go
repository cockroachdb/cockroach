// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfiglimiterccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDropTableLowersSpanCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer gcjob.SetSmallMaxGCIntervalForTest()()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	}})

	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)

	tenantID := roachpb.MustMakeTenantID(10)
	tenant, err := ts.TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantID: tenantID,
		TestingKnobs: base.TestingKnobs{
			GCJob: &sql.GCJobTestingKnobs{
				SkipWaitingForMVCCGC: true,
			},
		},
	})
	require.NoError(t, err)

	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.GC.TTLSeconds = 1
	config.TestingSetupZoneConfigHook(tc.Stopper())

	tenantSQLDB := tenant.SQLConn(t)
	tenantDB := sqlutils.MakeSQLRunner(tenantSQLDB)

	tenantDB.Exec(t, `CREATE TABLE t(k INT PRIMARY KEY)`)
	id := sqlutils.QueryTableID(t, tenantSQLDB, "defaultdb", "public", "t")
	config.TestingSetZoneConfig(config.ObjectID(id), zoneConfig)

	var spanCount int
	tenantDB.QueryRow(t, `SELECT span_count FROM system.span_count LIMIT 1`).Scan(&spanCount)
	require.Equal(t, 3, spanCount)

	tenantDB.Exec(t, `DROP TABLE t`)

	testutils.SucceedsSoon(t, func() error {
		tenantDB.QueryRow(t, `SELECT span_count FROM system.span_count LIMIT 1`).Scan(&spanCount)
		if spanCount != 0 {
			return errors.Newf("expected zero span count, found %d", spanCount)
		}
		return nil
	})
}
