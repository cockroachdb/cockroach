// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

	virtualClusterURL := func() string {
		urls, err := c.ExternalPGUrl(ctx, t.L(), virtualClusterNode, roachprod.PGURLOptions{
			VirtualClusterName: virtualClusterName,
		})
		require.NoError(t, err)

		return urls[0]
	}()

	t.L().Printf("checking that a client can connect to the tenant server")
	verifySQL(t, virtualClusterURL,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

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

// Runs an acceptance test on a multi-region multi-tenant cluster, which
// will be spread across at least two regions.
func runAcceptanceMultitenantMultiRegion(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(install.SecureOption(true)), c.All())
	regions := strings.Split(c.Spec().GCE.Zones, ",")
	regionOnly := func(regionAndZone string) string {
		r := strings.Split(regionAndZone, "-")
		return r[0] + "-" + r[1]
	}

	const tenantID = 123
	{
		// Intentionally, alter settings so that the system database span config
		// changes propagate faster, when we convert the system database to MR.
		conn := c.Conn(ctx, t.L(), 1)
		defer conn.Close()
		_, err := conn.Exec(`SELECT crdb_internal.create_tenant($1::INT)`, tenantID)
		require.NoError(t, err)
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
			_, err := conn.Exec(stmt)
			require.NoError(t, err)
		}
	}

	const (
		tenantHTTPPort  = 8081
		tenantSQLPort   = 30258
		otherRegionNode = 7
	)
	// Start an equal number of tenants on the cluster, located in the same regions.
	tenants := make([]*tenantNode, 0, len(c.All()))
	for i, node := range c.All() {
		region := regions[i]
		regionInfo := fmt.Sprintf("cloud=%s,region=%s,zone=%s", c.Cloud(), regionOnly(region), region)
		tenant := deprecatedCreateTenantNode(ctx, t, c, c.All(), tenantID, node, tenantHTTPPort, tenantSQLPort, createTenantRegion(regionInfo))
		tenant.start(ctx, t, c, "./cockroach")
		tenants = append(tenants, tenant)

		// Setup the system database for multi-region, and add all the region
		// in our cluster.
		if i == 0 {
			includedRegions := make(map[string]struct{})
			verifySQL(t, tenants[0].pgURL,
				mkStmt("SET CLUSTER SETTING sql.region_liveness.enabled='yes'"),
			)
			verifySQL(t, tenants[0].pgURL,
				mkStmt(fmt.Sprintf(`ALTER DATABASE system SET PRIMARY REGION '%s'`, regionOnly(regions[0]))),
				mkStmt(fmt.Sprintf(`ALTER DATABASE defaultdb SET PRIMARY REGION '%s'`, regionOnly(regions[0]))))
			includedRegions[regions[0]] = struct{}{}
			for _, region := range regions {
				if _, ok := includedRegions[region]; ok {
					continue
				}
				includedRegions[region] = struct{}{}
				verifySQL(t, tenants[0].pgURL,
					mkStmt(fmt.Sprintf(`ALTER DATABASE system ADD REGION '%s'`, regionOnly(region))),
					mkStmt(fmt.Sprintf(`ALTER DATABASE defaultdb ADD REGION '%s'`, regionOnly(region))))
			}
		}
	}

	// Sanity: Make sure the first tenant can be connected to.
	t.Status("checking that a client can connect to the tenant server")
	verifySQL(t, tenants[0].pgURL,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	// Wait for the span configs to propagate. After we know they have
	// propagated, we'll shut down the tenant and wait for them to get
	// applied.
	tdb, tdbCloser := openDBAndMakeSQLRunner(t, tenants[0].pgURL)
	defer tdbCloser()
	t.Status("Waiting for span config reconciliation...")
	WaitForSpanConfigReconciliation(t, tdb)
	t.Status("Span config reconciliation complete")
	t.Status("Waiting for replication changes...")
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()
	//systemConn := sqlutils.MakeSQLRunner(conn)
	checkStartTime := timeutil.Now()
	count := 0
	for timeutil.Since(checkStartTime) < time.Minute*5 {
		res := tdb.QueryStr(t, `
SELECT
	locality_count
FROM
	(
		SELECT
			count(*) AS locality_count
		FROM
			(
				SELECT
					DISTINCT
					range_id,
					start_key,
					split_part(
						unnest(replica_localities),
						',',
						2
					)
				FROM
					[SHOW RANGES FROM DATABASE system]
				WHERE
					(
						start_key NOT LIKE '%Table/11/%'
						AND start_key NOT LIKE '%Table/39/%'
						AND start_key NOT LIKE '%Table/46/%'
					)
			)
		GROUP BY
			range_id
	)
WHERE
	locality_count < 2;`)
		if len(res) == 0 {
			break
		}
		count += 1
		time.Sleep(time.Second * 5)
	}
	t.Status("Replication changes complete")

	// Stop all the tenants gracefully first.
	for _, tenant := range tenants {
		tenant.stop(ctx, t, c)
	}
	// Start them all up again.
	for _, tenant := range tenants {
		tenant.start(ctx, t, c, "./cockroach")
	}

	grp := ctxgroup.WithContext(ctx)
	startSchemaChange := make(chan struct{})
	waitForSchemaChange := make(chan struct{})
	killNodes := make(chan struct{})
	nodesKilled := make(chan struct{})
	// Start a connection that will hold a lease on a table that we are going
	// to schema change on. The region we are connecting to will be intentionally,
	// killed off.
	grp.GoCtx(func(ctx context.Context) (err error) {
		db, err := gosql.Open("postgres", tenants[otherRegionNode].pgURL)
		if err != nil {
			return err
		}
		defer db.Close()
		txn, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		defer func() {
			commitErr := txn.Commit()
			if commitErr != nil && strings.Contains(commitErr.Error(),
				"driver: bad connection") {
				commitErr = nil
			}
			err = errors.CombineErrors(err, commitErr)
			t.Status("Committed lease holding txn with error: ", err)
		}()
		_, err = txn.Exec("SELECT * FROM foo")
		startSchemaChange <- struct{}{}
		<-waitForSchemaChange
		return err
	})

	<-startSchemaChange

	// Start a schema change, while the lease is being held.
	grp.GoCtx(func(ctx context.Context) error {
		defer func() {
			waitForSchemaChange <- struct{}{}
		}()
		db, err := gosql.Open("postgres", tenants[0].pgURL)
		if err != nil {
			return err
		}
		defer db.Close()
		killNodes <- struct{}{}
		<-nodesKilled
		for {
			t.Status("Running schema change with lease held...")
			_, err = db.Exec("ALTER TABLE foo ADD COLUMN newcol int")
			// Confirm that we hit the expected error or no error.
			if err != nil &&
				!strings.Contains(err.Error(), "count-lease timed out reading from a region") {
				// Unrelated error, so lets kill off the test.
				return errors.CombineErrors(errors.AssertionFailedf("no time out detect because of dead region."),
					err)
			} else if err != nil {
				t.Status("Waiting for schema change completion, hit expected error: ", err)
				continue
			} else {
				// Schema change compleded successfully.
				t.Status("Schema change was successful")
				return err
			}
		}
	})

	<-killNodes
	// Kill both tenants and storage servers in the region we want dead. The schema
	// change should just naturally unblock and succeed.
	c.Run(ctx, install.WithNodes(c.Range(otherRegionNode, len(c.All())).InstallNodes()), "killall -9 cockroach")
	nodesKilled <- struct{}{}

	require.NoErrorf(t, grp.Wait(), "Waited for go routines, expected no error.")
	t.Status("stopping the server ahead of checking for the tenant server")

	// Restart the KV storage servers first.
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(install.SecureOption(true)), c.Range(otherRegionNode, len(c.All())))
	// Re-add any dead tenants back again.
	for _, tenant := range tenants[otherRegionNode-1:] {
		tenant.start(ctx, t, c, "./cockroach")
	}
	// Validate that no region is labeled as unavailable after.
	for _, tenant := range tenants[otherRegionNode-1:] {
		verifySQL(t, tenant.pgURL,
			mkStmt("SELECT * FROM system.region_liveness").withResults([][]string{}))
	}
	// Stop the server, which also ensures that log files get flushed.
	for _, tenant := range tenants {
		tenant.stop(ctx, t, c)
	}
	// Check that the server identifiers are present in the tenant log file.
	logFile := filepath.Join(tenants[0].logDir(), "*.log")
	if err := c.RunE(ctx, install.WithNodes(c.Node(1).InstallNodes()),
		"grep", "-q", "'start\\.go.*clusterID:'", logFile); err != nil {
		t.Fatal(errors.Wrap(err, "cluster ID not found in log file"))
	}
	if err := c.RunE(ctx, install.WithNodes(c.Node(1).InstallNodes()),
		"grep", "-q", "'start\\.go.*tenantID:'", logFile); err != nil {
		t.Fatal(errors.Wrap(err, "tenant ID not found in log file"))
	}
	if err := c.RunE(ctx, install.WithNodes(c.Node(1).InstallNodes()),
		"grep", "-q", "'start\\.go.*instanceID:'", logFile); err != nil {
		t.Fatal(errors.Wrap(err, "SQL instance ID not found in log file"))
	}

	t.Status("checking log file contents")

}
