// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func runAcceptanceMultitenant(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Start the storage layer.
	storageNodes := c.All()
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), storageNodes)

	// Start a virtual cluster.
	const virtualClusterName = "acceptance-tenant"
	virtualClusterNode := c.Node(1)
	c.StartServiceForVirtualCluster(
		ctx, t.L(),
		option.StartVirtualClusterOpts(virtualClusterName, virtualClusterNode),
		install.MakeClusterSettings(),
	)

	t.L().Printf("checking that a client can connect to the tenant server")
	workloadDuration := 30 * time.Second
	cmd := fmt.Sprintf(
		"./cockroach workload run tpcc --init --duration %s {pgurl:%d:%s}",
		workloadDuration, virtualClusterNode[0], virtualClusterName,
	)
	c.Run(ctx, option.WithNodes(c.Node(1)), cmd)

	// Verify that we are able to stop the virtual cluster instance.
	t.L().Printf("stopping the virtual cluster instance")
	c.StopServiceForVirtualCluster(
		ctx, t.L(),
		option.StopVirtualClusterOpts(virtualClusterName, virtualClusterNode),
	)

	db := c.Conn(
		ctx, t.L(), virtualClusterNode[0], option.VirtualClusterName(virtualClusterName),
	)
	defer db.Close()

	_, err := db.ExecContext(ctx, "CREATE TABLE bar (id INT PRIMARY KEY)")
	require.Error(t, err)
	t.L().Printf("after virtual cluster stopped, received error: %v", err)
}

// Runs a test on a multi-region multi-tenant cluster, which will be
// spread across at least two regions.
func runMultiTenantMultiRegion(ctx context.Context, t test.Test, c cluster.Cluster) {
	systemStartOpts := option.NewStartOpts(option.NoBackupSchedule)
	c.Start(ctx, t.L(), systemStartOpts, install.MakeClusterSettings())

	zones := strings.Split(c.Spec().GCE.Zones, ",")
	regions := make([]string, 0, len(zones))
	var uniqueRegions []string
	for _, z := range zones {
		parts := strings.Split(z, "-")
		region := parts[0] + "-" + parts[1]
		regions = append(regions, region)

		if slices.Index(uniqueRegions, region) == -1 {
			uniqueRegions = append(uniqueRegions, region)
		}
	}

	const (
		n1              = 1
		otherRegionNode = 7
	)

	// Verify that the constant above is valid.
	if regions[n1-1] == regions[otherRegionNode-1] {
		t.Fatal(fmt.Errorf(
			"expected n%d and n%d to be in different regions, but they are both in %s",
			n1, otherRegionNode, regions[n1-1],
		))
	}

	{
		// Intentionally, alter settings so that the system database span config
		// changes propagate faster, when we convert the system database to MR.
		conn := c.Conn(ctx, t.L(), 1)
		defer conn.Close()
		configStmts := []string{
			`SET CLUSTER SETTING sql.virtual_cluster.feature_access.multiregion.enabled='true'`,
			`SET CLUSTER SETTING kv.closed_timestamp.target_duration = '200ms'`,
			`SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '200ms'`,
			"SET CLUSTER SETTING kv.allocator.load_based_rebalancing = off",
			"SET CLUSTER SETTING kv.allocator.min_lease_transfer_interval = '10ms'",
			"SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '50 ms'",
			`SET CLUSTER SETTING kv.closed_timestamp.lead_for_global_reads_override = '1500ms'`,
			`ALTER TENANT ALL SET CLUSTER SETTING spanconfig.reconciliation_job.checkpoint_interval = '500ms'`,
			`SET CLUSTER setting kv.replication_reports.interval = '5s';`,
		}
		for _, stmt := range configStmts {
			t.L().Printf("running statement: %s", stmt)
			_, err := conn.Exec(stmt)
			require.NoError(t, err)
		}
	}

	// Start an equal number of tenants on the cluster, located in the
	// same regions.
	virtualCluster := "multiregion-tenant"
	tenantStartOpts := option.StartVirtualClusterOpts(virtualCluster, c.All(), option.NoBackupSchedule)
	c.StartServiceForVirtualCluster(ctx, t.L(), tenantStartOpts, install.MakeClusterSettings())

	tenantConn := c.Conn(ctx, t.L(), 1, option.VirtualClusterName(virtualCluster))
	defer tenantConn.Close()
	rootTenantConn := c.Conn(ctx, t.L(), 1,
		option.VirtualClusterName(virtualCluster),
		option.AuthMode(install.AuthRootCert),
	)
	defer rootTenantConn.Close()

	t.L().Printf("enabling region liveness on the tenant")
	_, err := tenantConn.Exec("SET CLUSTER SETTING sql.region_liveness.enabled='yes'")
	require.NoError(t, err)

	workloadDB := "ycsb"
	workloadType := "A"
	if rand.Float64() < 0.5 {
		workloadType = "B"
	}
	cmd := fmt.Sprintf(
		"./cockroach workload init ycsb --db %s --workload=%s {pgurl%s:%s}",
		workloadDB, workloadType, c.All(), virtualCluster,
	)
	c.Run(ctx, option.WithNodes(c.Node(1)), cmd)

	for _, r := range uniqueRegions {
		op := "ADD"
		if r == regions[0] {
			op = "SET PRIMARY"
		}

		for _, dbName := range []string{"system", "defaultdb", workloadDB} {
			stmt := fmt.Sprintf("ALTER DATABASE %s %s REGION '%s'", dbName, op, r)
			t.L().Printf("running statement: %s", stmt)

			c := tenantConn
			if dbName == "system" {
				c = rootTenantConn // only the root can modify the system database
			}
			_, err := c.Exec(stmt)
			require.NoError(t, err)
		}
	}

	t.L().Printf("running YCSB on the tenant for a few minutes")
	cmd = fmt.Sprintf(
		"./cockroach workload run ycsb --db %s --workload=%s --duration 10m {pgurl%s:%s}",
		workloadDB, workloadType, c.All(), virtualCluster,
	)
	c.Run(ctx, option.WithNodes(c.Node(1)), cmd)

	// Wait for the span configs to propagate. After we know they have
	// propagated, we'll shut down the tenant and wait for them to get
	// applied.
	t.Status("waiting for span config reconciliation...")
	tdb := sqlutils.MakeSQLRunner(tenantConn)
	sqlutils.WaitForSpanConfigReconciliation(t, tdb)
	t.Status("span config reconciliation complete, waiting for replication changes")

	checkStartTime := timeutil.Now()
	count := 0
	tableStartKeys := []string{
		"'%Table/'||'system.lease'::REGCLASS::OID||'/%'",
		"'%Table/'||'system.sqlliveness'::REGCLASS::OID||'/%'",
		"'%Table/'||'system.sql_instances'::REGCLASS::OID||'/%'",
	}
	const splitsForRBRTables = `
		SELECT
			start_key, replicas, replica_localities, voting_replicas
		FROM
			[SHOW RANGES FROM DATABASE system]
		WHERE
			start_key LIKE %s
`

	// Wait for up till 5 minutes.
	timedOut := true
	for timeutil.Since(checkStartTime) < time.Minute*5 {
		if count > 0 {
			time.Sleep(time.Second * 5)
		}
		count += 1
		validSplits := true
		// Confirm that local tables are split properly
		for _, tableStartKey := range tableStartKeys {
			func() {
				query := fmt.Sprintf(splitsForRBRTables, tableStartKey)
				rangeRows := tdb.Query(t, query)
				defer func() {
					require.NoError(t, rangeRows.Close())
				}()
				usedLocalities := make(map[string]struct{})
				for rangeRows.Next() {
					var startKey string
					var replicaLocalities []string
					var votingReplicas, replicas []int64
					err := rangeRows.Scan(&startKey, pq.Array(&replicas), pq.Array(&replicaLocalities), pq.Array(&votingReplicas))
					require.NoError(t, err)
					replicaMap := make(map[int]string)
					for idx := range replicaLocalities {
						splitLocality := strings.Split(replicaLocalities[idx], ",")
						replicaMap[int(replicas[idx])] = splitLocality[1]
					}
					targetLocality := ""
					metCriteria := true
					for _, votingReplica := range votingReplicas {
						if targetLocality == "" {
							targetLocality = replicaMap[int(votingReplica)]
						}
						if targetLocality != replicaMap[int(votingReplica)] {
							metCriteria = false
							continue
						}
					}
					if metCriteria && targetLocality != "" {
						usedLocalities[targetLocality] = struct{}{}
					}
				}
				if len(usedLocalities) != 2 {
					validSplits = false
				}
			}()
			if !validSplits {
				break
			}
		}
		if !validSplits {
			continue
		}
		timedOut = false
		t.Status("Successfully confirmed RBR and non-RBR table states")
		break
	}
	if timedOut {
		queryStr := tdb.QueryStr(t, "SELECT * FROM [SHOW RANGES FROM DATABASE system]")
		t.Status(fmt.Sprintf("%v", queryStr))
	}
	t.Status("Replication changes complete")

	// Stop all the tenants gracefully first.
	for _, node := range c.All() {
		t.L().Printf("stopping tenant on n%d", node)
		stopOpts := option.StopVirtualClusterOpts(virtualCluster, c.Node(node), option.Graceful(60))
		c.StopServiceForVirtualCluster(ctx, t.L(), stopOpts)
	}

	// Start them all up again.
	t.L().Printf("restarting virtual cluster")
	c.StartServiceForVirtualCluster(ctx, t.L(), tenantStartOpts, install.MakeClusterSettings())

	grp := t.NewErrorGroup(task.WithContext(ctx))
	startSchemaChange := make(chan struct{})
	waitForSchemaChange := make(chan struct{})
	killNodes := make(chan struct{})
	nodesKilled := make(chan struct{})
	otherRegionConn := c.Conn(ctx, t.L(), otherRegionNode, option.VirtualClusterName(virtualCluster))
	defer otherRegionConn.Close()

	// Start a connection that will hold a lease on a table that we are going
	// to schema change on. The region we are connecting to will be intentionally,
	// killed off.
	grp.Go(func(ctx context.Context, l *logger.Logger) (err error) {
		txn, err := otherRegionConn.BeginTx(ctx, nil)
		if err != nil {
			return errors.Wrap(err, "starting transaction")
		}

		defer func() {
			commitErr := txn.Commit()
			if commitErr != nil && strings.Contains(commitErr.Error(),
				"driver: bad connection") {
				commitErr = nil
			}
			err = errors.CombineErrors(err, commitErr)
			if err != nil {
				l.Printf("Committed lease holding txn with error: %#v", err)
			}
		}()
		_, err = txn.Exec(fmt.Sprintf("SELECT * FROM %s.usertable", workloadDB))
		startSchemaChange <- struct{}{}
		<-waitForSchemaChange
		return err
	})

	<-startSchemaChange

	// Start a schema change, while the lease is being held.
	grp.Go(func(ctx context.Context, l *logger.Logger) (err error) {
		defer func() {
			waitForSchemaChange <- struct{}{}
		}()
		killNodes <- struct{}{}
		<-nodesKilled
		for {
			t.Status("running schema change with lease held...")
			_, err = tenantConn.Exec(fmt.Sprintf("ALTER TABLE %s.usertable ADD COLUMN newcol int", workloadDB))
			// Confirm that we hit the expected error or no error.
			if err != nil &&
				!strings.Contains(err.Error(), "count-lease timed out reading from a region") {
				// Unrelated error, so lets kill off the test.
				return errors.NewAssertionErrorWithWrappedErrf(err, "no time out detected because of dead region")
			} else if err != nil {
				l.Printf("waiting for schema change completion, found expected error: %v", err)
				continue
			} else {
				// Schema change compleded successfully.
				t.Status("schema change was successful")
				return err
			}
		}
	})

	<-killNodes
	// Kill both tenants and storage servers in the region we want dead. The schema
	// change should just naturally unblock and succeed.
	killedRegion := c.Range(otherRegionNode, len(c.All()))
	t.Status("stopping the server ahead of checking for the tenant server")
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), killedRegion)
	nodesKilled <- struct{}{}
	require.NoErrorf(t, grp.WaitE(), "waited for go routines, expected no error.")

	// Restart the KV storage servers first.
	c.Start(ctx, t.L(), systemStartOpts, install.MakeClusterSettings(), killedRegion)
	// Re-add dead tenants back again.
	killedRegionStartOpts := option.StartVirtualClusterOpts(
		virtualCluster, killedRegion, option.NoBackupSchedule,
	)
	c.StartServiceForVirtualCluster(ctx, t.L(), killedRegionStartOpts, install.MakeClusterSettings())

	// Validate that no region is labeled as unavailable after.
	for _, node := range killedRegion {
		tenantDB := c.Conn(ctx, t.L(), node, option.VirtualClusterName(virtualCluster))
		//nolint:deferloop TODO(#137605)
		defer tenantDB.Close()

		rows, err := tenantDB.Query("SELECT crdb_region, unavailable_at FROM system.region_liveness")
		require.NoError(t, err, "error querying region liveness on n%d", node)

		var unavailableRegions []string
		for rows.Next() {
			var region []byte
			var unavailableAt time.Time

			require.NoError(t, rows.Scan(&region, unavailableAt), "reading region liveness on n%d", node)
			unavailableRegions = append(
				unavailableRegions,
				fmt.Sprintf("region: %x, unavailable_at: %s", region, unavailableAt),
			)
		}

		require.NoError(t, rows.Err(), "rows.Err() on n%d", node)
		if len(unavailableRegions) > 0 {
			t.Fatalf("unavailable regions on n%d:\n%s", node, strings.Join(unavailableRegions, "\n"))
		}
	}

	t.L().Printf("validated region liveness")
}

func registerMultiTenantMultiregion(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "multitenant-multiregion",
		Timeout: 30 * time.Minute,
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(
			9,
			spec.Geo(),
			spec.GCEZones(strings.Join([]string{
				"us-west1-b", "us-west1-b", "us-west1-b",
				"us-west1-b", "us-west1-b", "us-west1-b",
				"us-east1-b", "us-east1-b", "us-east1-b",
			}, ",")),
		),
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		CompatibleClouds:  registry.OnlyGCE,
		Suites:            registry.Suites(registry.Nightly, registry.Quick),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runMultiTenantMultiRegion(ctx, t, c)
		},
	})
}
