// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfiglimiterccl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils/spanconfigtestcluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDataDriven is a data-driven test for spanconfig.Limiter. It offers the
// following commands:
//
//   - "initialize" tenant=<int>
//     Initialize a secondary tenant with the given ID.
//
//   - "exec-sql" [tenant=<int>]
//     Executes the input SQL query for the given tenant. All statements are
//     executed in a single transaction.
//
//   - "query-sql" [tenant=<int>] [retry]
//     Executes the input SQL query for the given tenant and print the results.
//     If retry is specified and the expected results do not match the actual
//     results, the query will be retried under a testutils.SucceedsSoon block.
//     If run with -rewrite, we insert a 500ms sleep before executing the query
//     once.
//
//   - override limit=<int>
//     Override the span limit each tenant is configured with.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		// TODO(irfansharif): This is a stop-gap for tenant read-only cluster
		// settings. See https://github.com/cockroachdb/cockroach/pull/76929. Once
		// that's done, this test should be updated to use:
		//   SET CLUSTER SETTING spanconfig.virtual_cluster.max_spans = <whatever>
		limitOverride := 50
		scKnobs := &spanconfig.TestingKnobs{
			// Instead of relying on the GC job to wait out TTLs and clear out
			// descriptors, let's simply exclude dropped tables to simulate
			// descriptors no longer existing.
			ExcludeDroppedDescriptorsFromLookup: true,
			LimiterLimitOverride: func() int64 {
				return int64(limitOverride)
			},
		}
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestTenantProbabilistic,
				Knobs: base.TestingKnobs{
					SpanConfig: scKnobs,
				},
			},
		})
		defer tc.Stopper().Stop(ctx)
		{
			sysDB := sqlutils.MakeSQLRunner(tc.SystemLayer(0).SQLConn(t))
			sysDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '20ms'`)
		}

		spanConfigTestCluster := spanconfigtestcluster.NewHandle(t, tc, scKnobs)
		defer spanConfigTestCluster.Cleanup()

		spanConfigTestCluster.InitializeTenant(ctx, roachpb.SystemTenantID)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			tenantID := roachpb.SystemTenantID
			if d.HasArg("tenant") {
				var id uint64
				d.ScanArgs(t, "tenant", &id)
				tenantID = roachpb.MustMakeTenantID(id)
			}

			tenant, found := spanConfigTestCluster.LookupTenant(tenantID)
			if d.Cmd != "initialize" {
				require.Truef(t, found, "tenant %s not found (was it initialized?)", tenantID)
			}

			switch d.Cmd {
			case "initialize":
				spanConfigTestCluster.InitializeTenant(ctx, tenantID)

			case "exec-sql":
				if err := tenant.ExecWithErr(d.Input); err != nil {
					return fmt.Sprintf("err: %s", err)
				}

			case "query-sql":
				query := func() string {
					rows := tenant.Query(d.Input)
					output, err := sqlutils.RowsToDataDrivenOutput(rows)
					require.NoError(t, err)
					return output
				}
				if !d.HasArg("retry") {
					return query()
				}

				if d.Rewrite {
					time.Sleep(500 * time.Millisecond)
					return query()
				}

				var output string
				testutils.SucceedsSoon(t, func() error {
					if output = query(); output != d.Expected {
						return errors.Newf("expected %q, got %q; retrying..", d.Expected, output)
					}
					return nil
				})
				return output

			case "override":
				if tc.StartedDefaultTestTenant() {
					// Tests using an override need to run from the system
					// tenant. Skip the test if running with a default test
					// tenant.
					skip.IgnoreLintf(t, "unable to run with a default test tenant")
				}
				d.ScanArgs(t, "limit", &limitOverride)

			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}

			return ""
		})
	})
}
