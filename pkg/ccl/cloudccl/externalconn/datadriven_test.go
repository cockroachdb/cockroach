// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package externalconn_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/backup"
	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/externalconn/providers" // register all the concrete External Connection implementations
	ectestutils "github.com/cockroachdb/cockroach/pkg/cloud/externalconn/testutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		defer log.Scope(t).Close(t)

		dir, dirCleanupFn := testutils.TempDir(t)
		defer dirCleanupFn()

		var skipCheckExternalStorageConnection bool
		var skipCheckKMSConnection bool
		ecTestingKnobs := &externalconn.TestingKnobs{
			SkipCheckingExternalStorageConnection: func() bool {
				return skipCheckExternalStorageConnection
			},
			SkipCheckingKMSConnection: func() bool {
				return skipCheckKMSConnection
			},
		}
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
				Knobs: base.TestingKnobs{
					JobsTestingKnobs:   jobs.NewTestingKnobsWithShortIntervals(), // speeds up test
					ExternalConnection: ecTestingKnobs,
				},
				ExternalIODirConfig: base.ExternalIODirConfig{},
				ExternalIODir:       dir,
			},
		})
		defer tc.Stopper().Stop(ctx)

		externalConnTestCluster := ectestutils.NewHandle(t, tc)
		defer externalConnTestCluster.Cleanup()

		externalConnTestCluster.InitializeTenant(ctx, roachpb.SystemTenantID)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			tenantID := roachpb.SystemTenantID
			if d.HasArg("tenant") {
				var id uint64
				d.ScanArgs(t, "tenant", &id)
				tenantID = roachpb.MustMakeTenantID(id)
			}

			tenant, found := externalConnTestCluster.LookupTenant(tenantID)
			if d.Cmd != "initialize" {
				require.Truef(t, found, "tenant %s not found (was it initialized?)", tenantID)
			}

			switch d.Cmd {
			case "initialize":
				externalConnTestCluster.InitializeTenant(ctx, tenantID)

			case "enable-check-external-storage":
				skipCheckExternalStorageConnection = false

			case "disable-check-external-storage":
				skipCheckExternalStorageConnection = true

			case "enable-check-kms":
				skipCheckKMSConnection = false

			case "disable-check-kms":
				skipCheckKMSConnection = true

			case "exec-sql":
				if d.HasArg("user") {
					var user string
					d.ScanArgs(t, "user", &user)
					resetToRootUser := externalConnTestCluster.SetSQLDBForUser(tenantID, user)
					defer resetToRootUser()
				}
				if err := tenant.ExecWithErr(d.Input); err != nil {
					return fmt.Sprint(err.Error())
				}

			case "query-sql":
				if d.HasArg("user") {
					var user string
					d.ScanArgs(t, "user", &user)
					resetToRootUser := externalConnTestCluster.SetSQLDBForUser(tenantID, user)
					defer resetToRootUser()
				}
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
SELECT connection_name,
       connection_type,
       crdb_internal.pb_to_json('cockroach.cloud.externalconn.connectionpb.ConnectionDetails', connection_details),
       owner,
       owner_id
FROM system.external_connections
ORDER BY connection_name;
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
