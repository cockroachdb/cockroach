// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerMultiRegionSystemDatabase(r registry.Registry) {
	clusterSpec := r.MakeClusterSpec(3, spec.Geo(), spec.GatherCores(), spec.GCEZones("us-east1-b,us-west1-b,us-central1-b"))
	r.Add(registry.TestSpec{
		Name:             "schemachange/multiregion/system-database",
		Owner:            registry.OwnerSQLFoundations,
		Timeout:          time.Hour * 1,
		Cluster:          clusterSpec,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Weekly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			nodes := c.Spec().NodeCount
			regions := strings.Split(c.Spec().GCE.Zones, ",")
			regionOnly := func(regionAndZone string) string {
				r := strings.Split(regionAndZone, "-")
				return r[0] + "-" + r[1]
			}
			t.Status("starting cluster")
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(install.SecureOption(false)))
			conn := c.Conn(ctx, t.L(), 1)
			defer conn.Close()

			require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

			_, err := conn.ExecContext(ctx, "SET CLUSTER SETTING sql.multiregion.system_database_multiregion.enabled = true")
			require.NoError(t, err)

			_, err = conn.ExecContext(ctx,
				fmt.Sprintf(`ALTER DATABASE system SET PRIMARY REGION '%s'`, regionOnly(regions[0])))
			require.NoError(t, err)

			_, err = conn.ExecContext(ctx,
				fmt.Sprintf(`ALTER DATABASE system ADD REGION '%s'`, regionOnly(regions[1])))
			require.NoError(t, err)

			_, err = conn.ExecContext(ctx,
				fmt.Sprintf(`ALTER DATABASE system ADD REGION '%s'`, regionOnly(regions[2])))
			require.NoError(t, err)

			// Perform rolling restart to propagate region information to non-primary nodes
			for i := 2; i <= nodes; i++ {
				t.WorkerStatus("stop")
				c.Stop(ctx, t.L(), option.NewStopOpts(option.Graceful(shutdownGracePeriod)), c.Node(i))
				t.WorkerStatus("start")
				startOpts := option.DefaultStartOpts()
				c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(install.SecureOption(false)), c.Node(i))
			}

			// Check system.lease table to ensure that region information for each node is correct
			rows, err := conn.Query("SELECT DISTINCT sql_instance_id, crdb_region FROM system.lease")
			require.NoError(t, err)

			nodeToRegionName := make(map[int]string)
			for rows.Next() {
				var sqlInstanceID int
				var crdbRegion string
				require.NoError(t, rows.Scan(&sqlInstanceID, &crdbRegion))
				nodeToRegionName[sqlInstanceID] = crdbRegion
			}

			for node, regionName := range nodeToRegionName {
				require.Equal(t, regionOnly(regions[node-1]), regionName)
			}

			// Intentionally tear down nodes and ensure that everything is still working
			chaosTest := func() {
				// Random operations on user-created table
				_, err := conn.Exec(`CREATE TABLE foo (key INT PRIMARY KEY)`)
				if err != nil {
					return
				}
				defer func() {
					_, err := conn.Exec(`DROP TABLE foo`)
					if err != nil {
						return
					}
				}()
				_, err = conn.Exec(`INSERT INTO foo VALUES (1), (2), (3)`)
				require.NoError(t, err)
				row := conn.QueryRow(`SELECT * FROM foo LIMIT 1`)
				var rowPK int
				require.NoError(t, row.Scan(&rowPK))
				require.Equal(t, 1, rowPK)
			}

			for i := 2; i <= nodes; i++ {
				require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

				t.WorkerStatus("stop")
				c.Run(ctx, option.WithNodes(c.Node(i)), "killall -9 cockroach")

				t.WorkerStatus("chaos testing")
				chaosTest()

				t.WorkerStatus("start")
				startOpts := option.DefaultStartOpts()
				c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(install.SecureOption(false)), c.Node(i))
			}
		},
	})
}
