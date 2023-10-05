// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	sql2 "github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/regionliveness"
	regions2 "github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRegionLivenessProber(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStressRace(t)
	skip.UnderStress(t)

	ctx := context.Background()

	// Enable settings required for configuring a tenant's system database as multi-region.
	makeSettings := func() *cluster.Settings {
		cs := cluster.MakeTestingClusterSettings()
		instancestorage.ReclaimLoopInterval.Override(ctx, &cs.SV, 150*time.Millisecond)
		return cs
	}

	expectedRegions := []string{
		"us-east",
		"us-west",
		"us-south",
	}
	cluster, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionClusterWithRegionList(t,
		expectedRegions,
		1,
		base.TestingKnobs{},
		multiregionccltestutils.WithSettings(makeSettings()))
	defer cleanup()

	id, err := roachpb.MakeTenantID(11)
	require.NoError(t, err)

	var tenants []serverutils.ApplicationLayerInterface
	var tenantSQL []*gosql.DB
	blockProbeQuery := atomic.Bool{}

	for _, s := range cluster.Servers {
		tenantArgs := base.TestTenantArgs{
			Settings: makeSettings(),
			TenantID: id,
			Locality: s.Locality(),
			TestingKnobs: base.TestingKnobs{
				SQLExecutor: &sql2.ExecutorTestingKnobs{
					BeforeExecute: func(ctx context.Context, stmt string, descriptors *descs.Collection) {
						const probeQuery = "SELECT * FROM system.sql_instances WHERE crdb_region = $1::system.crdb_internal_region"
						if strings.Contains(stmt, probeQuery) &&
							blockProbeQuery.Swap(false) {
							// Timeout this query intentionally.
							time.Sleep(15 * time.Second)
						}
					},
				},
			},
		}
		ts, sql := serverutils.StartTenant(t, s, tenantArgs)
		tenants = append(tenants, ts)
		tenantSQL = append(tenantSQL, sql)
	}
	// Enable region liveness for testing.
	_, err = tenantSQL[0].Exec("SET CLUSTER SETTING sql.region_liveness.enabled=true")
	require.NoError(t, err)
	// Convert into a multi-region DB.
	_, err = tenantSQL[0].Exec(fmt.Sprintf("ALTER DATABASE system SET PRIMARY REGION '%s'", cluster.Servers[0].Locality().Tiers[0].Value))
	require.NoError(t, err)
	for i := 1; i < len(expectedRegions); i++ {
		_, err = tenantSQL[0].Exec(fmt.Sprintf("ALTER DATABASE system ADD REGION '%s'", expectedRegions[i]))
		require.NoError(t, err)
	}
	idb := tenants[0].InternalDB().(isql.DB)
	cf := tenants[0].CollectionFactory().(*descs.CollectionFactory)
	statusServer := tenants[0].SQLServer().(*sql2.Server).GetExecutorConfig().TenantStatusServer
	providerFactory := func(txn *kv.Txn) regionliveness.RegionProvider {
		return regions2.NewProvider(tenants[0].Codec(), statusServer, txn, cf.NewCollection(ctx))
	}
	regionProber := regionliveness.NewLivenessProber(idb, providerFactory, tenants[0].ClusterSettings())
	// Validates the expected regions versus the region liveness set.
	checkExpectedRegions := func(expectedRegions []string, regions regionliveness.LiveRegions) {
		require.Equalf(t, len(regions), len(expectedRegions),
			"Expected regions and prober differ: [%v] [%v]", expectedRegions, regions)
		for _, region := range expectedRegions {
			_, found := regions[region]
			require.True(t, found,
				"Expected regions and prober differ: [%v] [%v]", expectedRegions, regions)
		}
	}

	// Validate all regions in the cluster are correctly reported as live.
	testTxn := tenants[0].DB().NewTxn(ctx, "test-txn")
	regions, err := regionProber.QueryLiveness(ctx, testTxn)
	require.NoError(t, err)
	checkExpectedRegions(expectedRegions, regions)
	// Attempt to probe all regions, they should be all up still.
	for _, region := range expectedRegions {
		require.NoError(t, regionProber.ProbeLiveness(ctx, region))
	}
	// Validate all regions in the cluster are still reported as live.
	testTxn = tenants[0].DB().NewTxn(ctx, "test-txn")
	regions, err = regionProber.QueryLiveness(ctx, testTxn)
	require.NoError(t, err)
	checkExpectedRegions(expectedRegions, regions)
	// Probe the liveness of the region, but timeout the query
	// intentionally to make it seem dead.
	blockProbeQuery.Store(true)
	require.NoError(t, regionProber.ProbeLiveness(ctx, expectedRegions[1]))
	// Validate it was marked.
	row := tenantSQL[0].QueryRow("SELECT count(*) FROM system.region_liveness")
	rowCount := 0
	require.NoError(t, row.Scan(&rowCount))
	require.Equal(t, 1, rowCount, "one region should be dead")
	// Next validate the problematic region will be removed from the
	// list of live regions.
	testutils.SucceedsSoon(t, func() error {
		return tenants[0].DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			regions, err := regionProber.QueryLiveness(ctx, txn)
			if err != nil {
				return err
			}
			if len(regions) != 2 {
				return errors.AssertionFailedf("dead region was not removed in time.")
			}
			for idx, region := range expectedRegions {
				if idx == 1 {
					continue
				}
				if _, ok := regions[region]; !ok {
					return errors.AssertionFailedf("extra region detected %s", region)
				}
			}
			if _, ok := regions[expectedRegions[1]]; ok {
				return errors.AssertionFailedf("removed region detected %s", expectedRegions[1])
			}
			return nil
		})
	})
}
