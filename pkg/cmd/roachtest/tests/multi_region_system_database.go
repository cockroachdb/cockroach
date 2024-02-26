// Copyright 2024 The Cockroach Authors.
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
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerMultiRegionSystemDatabase(r registry.Registry) {
	clusterSpec := r.MakeClusterSpec(3, spec.Geo(), spec.GatherCores())
	r.Add(registry.TestSpec{
		Name:             "multi-region-system-database",
		Owner:            registry.OwnerSQLFoundations,
		Timeout:          time.Hour * 1,
		RequiresLicense:  true,
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
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()

			_, err := db.ExecContext(ctx, "SET CLUSTER SETTING sql.multiregion.preview_multiregion_system_database.enabled = true")
			require.NoError(t, err)

			_, err = db.ExecContext(ctx,
				fmt.Sprintf(`ALTER DATABASE system SET PRIMARY REGION '%s'`, regionOnly(regions[0])))
			require.NoError(t, err)

			_, err = db.ExecContext(ctx,
				fmt.Sprintf(`ALTER DATABASE system ADD REGION '%s'`, regionOnly(regions[1])))
			require.NoError(t, err)

			_, err = db.ExecContext(ctx,
				fmt.Sprintf(`ALTER DATABASE system ADD REGION '%s'`, regionOnly(regions[2])))
			require.NoError(t, err)

			//Perform rolling restart to propagate region information to non-primary nodes
			for i := 1; i < nodes; i++ {
				t.WorkerStatus("stop")
				stopOpts := option.DefaultStopOpts()
				c.Stop(ctx, t.L(), stopOpts, c.Node(i))
				t.WorkerStatus("start")
				startOpts := option.DefaultStartOpts()
				c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
			}

			//Check system.lease table to ensure that region information for each node is correct
			rows, err := db.Query("SELECT DISTINCT sql_instance_id, crdb_region FROM system.lease")
			require.NoError(t, err)

			nodeToRegionName := make(map[int]string)
			for rows.Next() {
				var sqlInstanceID int
				var crdbRegion string
				require.NoError(t, rows.Scan(&sqlInstanceID, &crdbRegion))
				nodeToRegionName[sqlInstanceID] = crdbRegion
			}

			for node, regionName := range nodeToRegionName {
				require.Equal(t, regionOnly(regions[node]), regionName)
			}

			//Intentionally tear down nodes and ensure that everything is still working
			stopOpts := option.DefaultStopOpts()
			c.Stop(ctx, t.L(), stopOpts, c.Node(2))

		},
	})
}
