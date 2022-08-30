// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package externalconn_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/externalconn/providers" // register all the concrete External Connection implementations
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/utils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(), // speeds up test
				},
			},
		})
		defer tc.Stopper().Stop(ctx)

		externalConnTestCluster := utils.NewHandle(t, tc)
		defer externalConnTestCluster.Cleanup()

		externalConnTestCluster.InitializeTenant(ctx, roachpb.SystemTenantID)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			tenantID := roachpb.SystemTenantID
			if d.HasArg("tenant") {
				var id uint64
				d.ScanArgs(t, "tenant", &id)
				tenantID = roachpb.MakeTenantID(id)
			}

			tenant, found := externalConnTestCluster.LookupTenant(tenantID)
			if d.Cmd != "initialize" {
				require.Truef(t, found, "tenant %s not found (was it initialized?)", tenantID)
			}

			switch d.Cmd {
			case "initialize":
				externalConnTestCluster.InitializeTenant(ctx, tenantID)

			case "exec-sql":
				if err := tenant.ExecWithErr(d.Input); err != nil {
					return fmt.Sprint(err.Error())
				}

			case "query-sql":
				var rows *gosql.Rows
				var err error
				if rows, err = tenant.QueryWithErr(d.Input); err != nil {
					return fmt.Sprint(err.Error())
				}
				output, err := sqlutils.RowsToDataDrivenOutput(rows)
				require.NoError(t, err)
				return output

			case "inspect-system-table":
				rows := tenant.Query(`
SELECT connection_name, connection_type, crdb_internal.pb_to_json('cockroach.cloud.externalconn.connectionpb.ConnectionDetails', connection_details) 
FROM system.external_connections;
`)
				output, err := sqlutils.RowsToDataDrivenOutput(rows)
				require.NoError(t, err)
				return output

			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			return ""
		})
	})
}
