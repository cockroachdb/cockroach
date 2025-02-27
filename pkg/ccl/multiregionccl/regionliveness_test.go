// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionccl_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/regionliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const (
	// Use a low probe timeout number, but intentionally only manipulate to
	// this value when *failures* are expected.
	testingRegionLivenessProbeTimeout = time.Second * 1
	// Use a long probe timeout by default when running this test (since CI
	// can easily hit query timeouts).
	testingRegionLivenessProbeTimeoutLong = time.Minute
	// Use a reduced liveness TTL for region liveness infrastructure.
	testingRegionLivenessTTL = time.Second * 10
)

func TestRegionLivenessProber(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Force extra logging to diagnose flakes.
	require.NoError(t, log.SetVModule("prober=2"))
	// This test forces the SQL liveness TTL be a small number,
	// which makes the heartbeats even more critical. Under stress and
	// race environments this test becomes even more sensitive, if
	// we can't send heartbeats within 20 seconds.
	skip.UnderDuress(t)

	ctx := context.Background()

	// Enable settings required for configuring a tenant's system database as
	// multi-region. and enable region liveness for testing.
	makeSettings := func() *cluster.Settings {
		cs := cluster.MakeTestingClusterSettings()
		instancestorage.ReclaimLoopInterval.Override(ctx, &cs.SV, 150*time.Millisecond)
		regionliveness.RegionLivenessEnabled.Override(ctx, &cs.SV, true)
		return cs
	}
	defer regionliveness.TestingSetUnavailableAtTTLOverride(testingRegionLivenessTTL)()

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
	defer regionliveness.TestingSetBeforeProbeLivenessHook(
		func() {
			// Timeout attempts to probe intentionally.
			if blockProbeQuery.Swap(false) {
				time.Sleep(2 * testingRegionLivenessProbeTimeout)
			}
		},
	)()

	for _, s := range testCluster.Servers {
		tenantArgs := base.TestTenantArgs{
			Settings: makeSettings(),
			TenantID: id,
			Locality: s.Locality(),
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
	var cachedRegionProvider *regions.CachedDatabaseRegions
	cachedRegionProvider, err = regions.NewCachedDatabaseRegions(ctx, tenants[0].DB(), tenants[0].LeaseManager().(*lease.Manager))
	require.NoError(t, err)
	idb := tenants[0].InternalDB().(isql.DB)
	regionProber := regionliveness.NewLivenessProber(idb.KV(), tenants[0].Codec(), cachedRegionProvider, tenants[0].ClusterSettings())
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
	// Override the table timeout probe for testing to ensure timeout failures
	// happen now.
	for _, ts := range tenants {
		regionliveness.RegionLivenessProbeTimeout.Override(ctx, &ts.ClusterSettings().SV, testingRegionLivenessProbeTimeout)
	}
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
			if err := regionProber.ProbeLiveness(ctx, expectedRegions[1]); err != nil {
				return err
			}
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
			// Similarly query the unavailable physical regions
			unavailablePhysicalRegions, err := regionProber.QueryUnavailablePhysicalRegions(ctx, txn, true /*filterAvailable*/)
			if err != nil {
				return err
			}
			if len(unavailablePhysicalRegions) != 1 {
				return errors.AssertionFailedf("physical region was not marked as unavailable")
			}
			// Validate the physical region marked as unavailable is correct
			regionTypeDesc := cachedRegionProvider.GetRegionEnumTypeDesc()
			for i := 0; i < regionTypeDesc.NumEnumMembers(); i++ {
				if regionTypeDesc.GetMemberLogicalRepresentation(i) != expectedRegions[1] {
					continue
				}
				if !unavailablePhysicalRegions.ContainsPhysicalRepresentation(string(regionTypeDesc.GetMemberPhysicalRepresentation(i))) {
					return errors.AssertionFailedf("incorrect physical region was marked as unavailable %v", unavailablePhysicalRegions)
				}
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
	// we can't send heartbeats within 20 seconds.
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	ctx := context.Background()

	// Enable settings required for configuring a tenant's system database as
	// multi-region. and enable region liveness for testing.
	makeSettings := func() *cluster.Settings {
		cs := cluster.MakeTestingClusterSettings()
		instancestorage.ReclaimLoopInterval.Override(ctx, &cs.SV, 150*time.Millisecond)
		regionliveness.RegionLivenessEnabled.Override(ctx, &cs.SV, true)
		return cs
	}
	defer regionliveness.TestingSetUnavailableAtTTLOverride(testingRegionLivenessTTL)()

	expectedRegions := []string{
		"us-east",
		"us-south",
		"us-west",
	}
	detectLeaseWait := atomic.Bool{}
	targetCount := atomic.Int64{}
	var tenants []serverutils.ApplicationLayerInterface
	var tenantSQL []*gosql.DB
	defer regionliveness.TestingSetBeforeProbeLivenessHook(
		func() {
			if !detectLeaseWait.Load() {
				return
			}
			time.Sleep(2 * testingRegionLivenessProbeTimeout)
			targetCount.Swap(0)
			detectLeaseWait.Swap(false)
		},
	)()

	var keyToBlockMu syncutil.Mutex
	var keyToBlock roachpb.Key
	recoveryBlock := make(chan struct{})
	recoveryStart := make(chan struct{})
	clusterKnobs := base.TestingKnobs{
		Store: &kvserver.StoreTestingKnobs{
			TestingRequestFilter: func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error {
				// Slow any deletes to the regionliveness table, so that the recovery
				// protocol on heartbeat never succeeds.
				deleteRequest := request.Requests[0].GetDelete()
				if deleteRequest == nil {
					return nil
				}
				keyToBlockMu.Lock()
				keyPrefix := keyToBlock
				keyToBlockMu.Unlock()
				isPrefixToDelReq := len(deleteRequest.Key) >= len(keyPrefix) &&
					!deleteRequest.Key[:len(keyPrefix)].Equal(keyPrefix)
				if keyPrefix == nil || isPrefixToDelReq {
					return nil
				}
				recoveryStart <- struct{}{}
				<-recoveryBlock
				return nil
			},
		},
	}
	testCluster, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionClusterWithRegionList(t,
		expectedRegions,
		1,
		clusterKnobs,
		multiregionccltestutils.WithSettings(makeSettings()))
	defer cleanup()

	id, err := roachpb.MakeTenantID(11)
	require.NoError(t, err)
	for i, s := range testCluster.Servers {
		var tenant serverutils.ApplicationLayerInterface
		var tenantDB *gosql.DB
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
						const leaseQueryRegex = "SELECT .* FROM system.public.lease AS OF SYSTEM TIME"
						re := regexp.MustCompile(leaseQueryRegex)
						// Fail intentionally, when we go to probe the first region.
						if re.MatchString(stmt) {
							if targetCount.Add(1) != 1 {
								return
							}
							keyToBlockMu.Lock()
							keyToBlock = tenant.Codec().TablePrefix(uint32(systemschema.RegionLivenessTable.GetID()))
							keyToBlockMu.Unlock()
							time.Sleep(2 * testingRegionLivenessProbeTimeout)
						}
					},
				},
			},
		}
		tenant, tenantDB = serverutils.StartTenant(t, s, tenantArgs)
		tenantSQL = append(tenantSQL, tenantDB)
		tenants = append(tenants, tenant)
		// Before the other tenants are added we need to configure the system database,
		// otherwise they will come up in a non multi-region mode and not all subsystems
		// will be aware (i.e. session ID and SQL instance will not be MR aware).
		if i == 0 {
			// Convert into a multi-region DB.
			_, err = tenantSQL[0].Exec(fmt.Sprintf("ALTER DATABASE system SET PRIMARY REGION '%s'", testCluster.Servers[0].Locality().Tiers[0].Value))
			require.NoError(t, err)
			for i := 1; i < len(expectedRegions); i++ {
				_, err = tenantSQL[0].Exec(fmt.Sprintf("ALTER DATABASE system ADD REGION '%s'", expectedRegions[i]))
				require.NoError(t, err)
			}
			_, err = tenantSQL[0].Exec("ALTER DATABASE system SURVIVE ZONE FAILURE")
			require.NoError(t, err)
		}
	}

	// Create a new table and have it used on all nodes.
	_, err = tenantSQL[0].Exec("CREATE TABLE t1(j int)")
	require.NoError(t, err)
	_, err = tenantSQL[0].Exec("CREATE TABLE t2(j int)")
	require.NoError(t, err)
	for _, c := range tenantSQL {
		_, err = c.Exec("SELECT * FROM t1")
		require.NoError(t, err)
	}
	row := tenantSQL[0].QueryRow("SELECT 't1'::REGCLASS::OID")
	var tableID int
	require.NoError(t, row.Scan(&tableID))
	// Override the table timeout probe for testing to ensure timeout failures
	// happen now.
	for _, ts := range tenants {
		regionliveness.RegionLivenessProbeTimeout.Override(ctx, &ts.ClusterSettings().SV, testingRegionLivenessProbeTimeout)
	}
	// Issue a schema change which should mark this region as dead, and fail
	// out because our probe query will time out.
	detectLeaseWait.Swap(true)
	_, err = tenantSQL[1].Exec("ALTER TABLE t1 ADD COLUMN i INT")
	require.ErrorContainsf(t, err, "count-lease timed out reading from a region", "failed to timeout")
	// Keep an active lease on node 0, but it will be seen as ignored eventually
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
	// Validate we can have a "dropped" region and the query won't fail.
	lm := tenants[1].LeaseManager().(*lease.Manager)
	cachedDatabaseRegions, err := regions.NewCachedDatabaseRegions(ctx, tenants[0].DB(), lm)
	require.NoError(t, err)
	regions.TestingModifyRegionEnum(cachedDatabaseRegions, func(descriptor catalog.TypeDescriptor) catalog.TypeDescriptor {
		builder := typedesc.NewBuilder(descriptor.TypeDesc())
		mut := builder.BuildCreatedMutableType()
		mut.EnumMembers = append(mut.EnumMembers, descpb.TypeDescriptor_EnumMember{
			PhysicalRepresentation: nil,
			LogicalRepresentation:  "FakeRegion",
		})
		return builder.BuildExistingMutableType()
	})
	// Restore the override to reduce the risk of failing on overloaded systems.
	for _, ts := range tenants {
		regionliveness.RegionLivenessProbeTimeout.Override(ctx, &ts.ClusterSettings().SV, testingRegionLivenessProbeTimeoutLong)
	}
	require.NoError(t, lm.WaitForNoVersion(ctx, descpb.ID(tableID), cachedDatabaseRegions, retry.Options{}))

	// Use a closure so that we unconditionally wait for the recovery to finish.
	grp := ctxgroup.WithContext(ctx)
	func() {
		grp.GoCtx(func(ctx context.Context) error {
			_, err = tx.Exec("INSERT INTO t2 VALUES(5)")
			return err
		})
		defer func() {
			<-recoveryStart
			time.Sleep(slbase.DefaultTTL.Default())
			recoveryBlock <- struct{}{}
			require.NoError(t, grp.Wait())
		}()
		// Add a new region which will execute a recovery and clean up dead rows.
		keyToBlockMu.Lock()
		keyToBlock = nil
		keyToBlockMu.Unlock()
		tenantArgs := base.TestTenantArgs{
			Settings: makeSettings(),
			TenantID: id,
			Locality: testCluster.Servers[0].Locality(),
		}
		_, newRegionSQL := serverutils.StartTenant(t, testCluster.Servers[0], tenantArgs)
		tr := sqlutils.MakeSQLRunner(newRegionSQL)
		// Validate everything was cleaned bringing up a new node in the down region.
		require.Equalf(t,
			[][]string{},
			tr.QueryStr(t, "SELECT * FROM system.region_liveness"),
			"expected no unavaialble regions.")
		require.Equalf(t,
			[][]string{{"3"}},
			tr.QueryStr(t, "SELECT count(*) FROM system.sql_instances WHERE session_id IS NOT NULL"),
			"extra sql instances are being used.")
		require.Equalf(t,
			[][]string{{"3"}},
			tr.QueryStr(t, "SELECT count(*) FROM system.sqlliveness"),
			"extra sql sessions detected.")
		require.NoError(t, err)
	}()

	// Validate that the stuck query will fail once we recover.
	_, err = tx.Exec("INSERT INTO t2 VALUES(5)")
	// If the txn failed, no commit is needed.
	const expectedTxnErr = "restart transaction: TransactionRetryWithProtoRefreshError"
	if err != nil {
		require.ErrorContainsf(t,
			err,
			expectedTxnErr,
			"txn should see a retry error.")
		require.NoError(t, tx.Rollback())
	} else {
		require.ErrorContainsf(t,
			tx.Commit(),
			expectedTxnErr,
			"txn should see a retry error")
	}
}

// TestRegionLivenessProberForSQLInstances validates that regional avaibility issues
// can be also be detected when allocating rows inside the sqlinstances table.
func TestRegionLivenessProberForSQLInstances(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test forces the SQL liveness TTL be a small number,
	// which makes the heartbeats even more critical. Under stress and
	// race environments this test becomes even more sensitive, if
	// we can't send heartbeats within 20 seconds.
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	ctx := context.Background()

	// Enable settings required for configuring a tenant's system database as
	// multi-region. and enable region liveness for testing.
	makeSettings := func() *cluster.Settings {
		cs := cluster.MakeTestingClusterSettings()
		//	instancestorage.ReclaimLoopInterval.Override(ctx, &cs.SV, 150*time.Millisecond)
		regionliveness.RegionLivenessEnabled.Override(ctx, &cs.SV, true)
		instancestorage.PreallocatedCount.Override(ctx, &cs.SV, 1)
		return cs
	}
	defer regionliveness.TestingSetUnavailableAtTTLOverride(testingRegionLivenessTTL)()

	expectedRegions := []string{
		"us-east",
		"us-west",
		"us-south",
	}
	keyToSlow := struct {
		syncutil.Mutex
		prefix []byte
	}{}
	testCluster, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionClusterWithRegionList(t,
		expectedRegions,
		1,
		base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error {
					scan := request.Requests[0].GetScan()
					keyToSlow.Lock()
					prefixToCheck := keyToSlow.prefix
					keyToSlow.Unlock()
					if scan != nil && len(prefixToCheck) > 0 {
						if len(scan.Key) >= len(prefixToCheck) && scan.Key[:len(prefixToCheck)].Equal(prefixToCheck) {
							time.Sleep(time.Second * 30)
						}
					}
					return nil
				},
			},
		},
		multiregionccltestutils.WithSettings(makeSettings()))
	defer cleanup()

	id, err := roachpb.MakeTenantID(11)
	require.NoError(t, err)

	var tenants []serverutils.ApplicationLayerInterface
	var tenantSQL []*gosql.DB
	blockProbeQuery := atomic.Bool{}
	defer regionliveness.TestingSetBeforeProbeLivenessHook(
		func() {
			// Timeout attempts to probe intentionally.
			if blockProbeQuery.Swap(false) {
				time.Sleep(2 * testingRegionLivenessProbeTimeout)
			}
		},
	)()

	for _, s := range testCluster.Servers {
		tenantArgs := base.TestTenantArgs{
			Settings: makeSettings(),
			TenantID: id,
			Locality: s.Locality(),
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
	var downRegionName []byte
	for i := 0; i < cachedRegionProvider.GetRegionEnumTypeDesc().NumEnumMembers(); i++ {
		if cachedRegionProvider.GetRegionEnumTypeDesc().GetMemberLogicalRepresentation(i) == expectedRegions[1] {
			downRegionName = cachedRegionProvider.GetRegionEnumTypeDesc().GetMemberPhysicalRepresentation(i)
			break
		}
	}
	// Probe the liveness of the region, but timeout the query
	// intentionally to make it seem dead.
	keyToSlow.Lock()
	sqlInstancePrefix := tenants[0].Codec().IndexPrefix(keys.SQLInstancesTableID, uint32(systemschema.SQLInstancesTable().GetPrimaryIndexID()))
	keyToSlow.prefix = encoding.EncodeBytesAscending(sqlInstancePrefix, downRegionName)
	keyToSlow.Unlock()
	blockProbeQuery.Store(true)
	// Attempt to start new nodes this should fail, for a bit.
	targetServer := testCluster.Servers[0]
	newTenantArgs := base.TestTenantArgs{
		Settings: makeSettings(),
		TenantID: id,
		Locality: targetServer.Locality(),
	}
	// We won't be able to scan expectedRegions[1] here, which will cause a slow
	// start because we will retry creating the instance after a first attempt.
	_, tenantDB := serverutils.StartTenant(t, targetServer, newTenantArgs)
	require.NoError(t, err)
	tdbRunner := sqlutils.MakeSQLRunner(tenantDB)
	tdbRunner.CheckQueryResults(t, "SELECT crdb_region FROM system.region_liveness",
		[][]string{{"us-west"}})
	require.NoError(t, err)
}
