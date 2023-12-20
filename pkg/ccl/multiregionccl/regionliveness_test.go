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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/regionliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const (
	testingRegionLivenessProbeTimeout = time.Second * 2
)

func TestRegionLivenessProber(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test forces the SQL liveness TTL be a small number,
	// which makes the heartbeats even more critical. Under stress and
	// race environments this test becomes even more sensitive, if
	// we can't send heartbeats within 10 seconds.
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	ctx := context.Background()

	// Enable settings required for configuring a tenant's system database as
	// multi-region. and enable region liveness for testing.
	makeSettings := func() *cluster.Settings {
		cs := cluster.MakeTestingClusterSettings()
		instancestorage.ReclaimLoopInterval.Override(ctx, &cs.SV, 150*time.Millisecond)
		slinstance.DefaultTTL.Override(ctx, &cs.SV, 10*time.Second)
		regionliveness.RegionLivenessEnabled.Override(ctx, &cs.SV, true)
		return cs
	}

	expectedRegions := []string{
		"us-east",
		"us-west",
		"us-south",
	}
	testCluster, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionClusterWithRegionList(t,
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

	for _, s := range testCluster.Servers {
		tenantArgs := base.TestTenantArgs{
			Settings: makeSettings(),
			TenantID: id,
			Locality: s.Locality(),
			TestingKnobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					BeforeExecute: func(ctx context.Context, stmt string, descriptors *descs.Collection) {
						const probeQuery = "SELECT count(*) FROM system.sql_instances WHERE crdb_region = $1::system.crdb_internal_region"
						if strings.Contains(stmt, probeQuery) && blockProbeQuery.Swap(false) {
							// Timeout this query intentionally.
							time.Sleep(testingRegionLivenessProbeTimeout + time.Second)
						}
					},
				},
			},
		}
		ts, tenantDB := serverutils.StartTenant(t, s, tenantArgs)
		tenants = append(tenants, ts)
		tenantSQL = append(tenantSQL, tenantDB)
	}
	// Convert into a multi-region DB.
	_, err = tenantSQL[0].Exec(fmt.Sprintf("ALTER DATABASE system SET PRIMARY REGION '%s'", testCluster.Servers[0].Locality().Tiers[0].Value))
	require.NoError(t, err)
	for i := 1; i < len(expectedRegions); i++ {
		_, err = tenantSQL[0].Exec(fmt.Sprintf("ALTER DATABASE system ADD REGION '%s'", expectedRegions[i]))
		require.NoError(t, err)
	}
	// Override the table timeout probe for testing.
	for _, ts := range tenants {
		regionliveness.RegionLivenessProbeTimeout.Override(ctx, &ts.ClusterSettings().SV, testingRegionLivenessProbeTimeout)
	}

	var cachedRegionProvider *regions.CachedDatabaseRegions
	cachedRegionProvider, err = regions.NewCachedDatabaseRegions(ctx, tenants[0].DB(), tenants[0].LeaseManager().(*lease.Manager))
	require.NoError(t, err)
	idb := tenants[0].InternalDB().(isql.DB)
	regionProber := regionliveness.NewLivenessProber(idb, cachedRegionProvider, tenants[0].ClusterSettings())
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
	liveRegions, err := regionProber.QueryLiveness(ctx, testTxn)
	require.NoError(t, err)
	checkExpectedRegions(expectedRegions, liveRegions)
	// Attempt to probe all regions, they should be all up still.
	for _, region := range expectedRegions {
		require.NoError(t, regionProber.ProbeLiveness(ctx, region))
	}
	// Validate all regions in the cluster are still reported as live.
	testTxn = tenants[0].DB().NewTxn(ctx, "test-txn")
	liveRegions, err = regionProber.QueryLiveness(ctx, testTxn)
	require.NoError(t, err)
	checkExpectedRegions(expectedRegions, liveRegions)
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
			// Attempt to keep on pushing out the unavailable_at time,
			// these calls should be no-ops.
			blockProbeQuery.Store(true)
			require.NoError(t, regionProber.ProbeLiveness(ctx, expectedRegions[1]))
			// Check if the time has expired yet.
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

func TestRegionLivenessProberForLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test forces the SQL liveness TTL be a small number,
	// which makes the heartbeats even more critical. Under stress and
	// race environments this test becomes even more sensitive, if
	// we can't send heartbeats within 10 seconds.
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	ctx := context.Background()

	// Enable settings required for configuring a tenant's system database as
	// multi-region. and enable region liveness for testing.
	makeSettings := func() *cluster.Settings {
		cs := cluster.MakeTestingClusterSettings()
		instancestorage.ReclaimLoopInterval.Override(ctx, &cs.SV, 150*time.Millisecond)
		slinstance.DefaultTTL.Override(ctx, &cs.SV, 10*time.Second)
		regionliveness.RegionLivenessEnabled.Override(ctx, &cs.SV, true)
		return cs
	}

	expectedRegions := []string{
		"us-east",
		"us-south",
		"us-west",
	}
	detectLeaseWait := atomic.Bool{}
	testCluster, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionClusterWithRegionList(t,
		expectedRegions,
		1,
		base.TestingKnobs{},
		multiregionccltestutils.WithSettings(makeSettings()))
	defer cleanup()

	id, err := roachpb.MakeTenantID(11)
	require.NoError(t, err)

	var tenants []serverutils.ApplicationLayerInterface
	var tenantSQL []*gosql.DB
	targetCount := atomic.Int64{}

	for _, s := range testCluster.Servers {
		tenantArgs := base.TestTenantArgs{
			Settings: makeSettings(),
			TenantID: id,
			Locality: s.Locality(),
			TestingKnobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					BeforeExecute: func(ctx context.Context, stmt string, descriptors *descs.Collection) {
						if !detectLeaseWait.Load() {
							return
						}
						const probeQuery = "SELECT count(*) FROM system.sql_instances WHERE crdb_region = $1::system.public.crdb_internal_region"
						const leaseQuery = "SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME"
						// Fail intentionally, when we go to probe the first region.
						if strings.Contains(stmt, leaseQuery) {
							if targetCount.Add(1) != 1 {
								return
							}
							time.Sleep(testingRegionLivenessProbeTimeout + time.Second)
						} else if strings.Contains(stmt, probeQuery) {
							time.Sleep(testingRegionLivenessProbeTimeout + time.Second)
							targetCount.Swap(0)
							detectLeaseWait.Swap(false)
						}
					},
				},
			},
		}
		tenant, tenantDB := serverutils.StartTenant(t, s, tenantArgs)
		tenantSQL = append(tenantSQL, tenantDB)
		tenants = append(tenants, tenant)
	}
	// Convert into a multi-region DB.
	_, err = tenantSQL[0].Exec(fmt.Sprintf("ALTER DATABASE system SET PRIMARY REGION '%s'", testCluster.Servers[0].Locality().Tiers[0].Value))
	require.NoError(t, err)
	for i := 1; i < len(expectedRegions); i++ {
		_, err = tenantSQL[0].Exec(fmt.Sprintf("ALTER DATABASE system ADD REGION '%s'", expectedRegions[i]))
		require.NoError(t, err)
	}
	_, err = tenantSQL[0].Exec("ALTER DATABASE system SURVIVE ZONE FAILURE")
	require.NoError(t, err)
	// Override the table timeout probe for testing.
	for _, ts := range tenants {
		regionliveness.RegionLivenessProbeTimeout.Override(ctx, &ts.ClusterSettings().SV, testingRegionLivenessProbeTimeout)
	}
	// Create a new table and have it used on all nodes.
	_, err = tenantSQL[0].Exec("CREATE TABLE t1(j int)")
	require.NoError(t, err)
	for _, c := range tenantSQL {
		_, err = c.Exec("SELECT * FROM t1")
		require.NoError(t, err)
	}
	row := tenantSQL[0].QueryRow("SELECT 't1'::REGCLASS::OID")
	var tableID int
	require.NoError(t, row.Scan(&tableID))
	// Issue a schema change which should mark this region as dead, and fail
	// out because our probe query will time out.
	detectLeaseWait.Swap(true)
	_, err = tenantSQL[1].Exec("ALTER TABLE t1 ADD COLUMN i INT")
	require.ErrorContainsf(t, err, "count-lease timed out reading from a region", "failed to timeout")
	// Keep an active lease on node 1, but it will be seen as ignored eventually
	// because the region will start to get quarantined.
	tx, err := tenantSQL[0].Begin()
	require.NoError(t, err)
	_, err = tx.Exec("SELECT * FROM t1")
	require.NoError(t, err)
	// Second attempt should skip the dead region for both
	// CountLeases and WaitForNoVersion.
	testutils.SucceedsSoon(t, func() error {
		if _, err := tenantSQL[1].Exec("ALTER TABLE t1 ADD COLUMN IF NOT EXISTS i INT"); err != nil {
			return err
		}
		if _, err := tenantSQL[1].Exec("DROP TABLE t1"); err != nil {
			return err
		}
		return nil
	})

	require.NoError(t, tx.Rollback())

	// Validate we can have a "droped" region and the query won't fail.
	lm := tenants[0].LeaseManager().(*lease.Manager)
	cachedDatabaseRegions, err := regions.NewCachedDatabaseRegions(ctx, tenants[0].DB(), lm)
	require.NoError(t, err)
	regions.TestingModifyRegionEnum(cachedDatabaseRegions, func(descriptor catalog.TypeDescriptor) catalog.TypeDescriptor {
		builder := typedesc.NewBuilder(descriptor.TypeDesc())
		mut := builder.BuildCreatedMutableType()
		mut.EnumMembers = append(mut.EnumMembers, descpb.TypeDescriptor_EnumMember{
			PhysicalRepresentation: nil,
			LogicalRepresentation:  "FakeRegion",
		})
		return mut
	})
	require.NoError(t, lm.WaitForNoVersion(ctx, descpb.ID(tableID), cachedDatabaseRegions, retry.Options{}))
}
