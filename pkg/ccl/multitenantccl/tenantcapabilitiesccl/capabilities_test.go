// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcapabilitiesccl

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitieswatcher"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDataDriven runs datadriven tests against the entire tenant capabilities
// subsystem, and in doing so, serves as an end-to-end integration test.
//
// Crucially, it keeps track of how up-to-date the in-memory Authorizer state
// is when capabilities are updated. This allows test authors to change
// capabilities and make assertions against those changes, without needing to
// worry about the asynchronous nature of capability changes applying.
//
// The test creates a secondary tenant, with tenant ID 10, that test authors can
// reference directly.
//
// The syntax is as follows:
//
// query-sql-system: runs a query against the system tenant.
//
// exec-sql-tenant: executes a query against a secondary tenant (with ID 10).
//
// exec-privileged-op-tenant: executes a privileged operation (one that requires
// capabilities) as a secondary tenant.
//
// update-capabilities: expects a SQL statement that updates capabilities for a
// tenant. Following statements are guaranteed to see the effects of the
// update.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		ctx := context.Background()

		// Setup both the system tenant and a secondary tenant.
		mu := struct {
			syncutil.Mutex
			lastFrontierTS hlc.Timestamp // ensures watcher is caught up
		}{}

		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
				Knobs: base.TestingKnobs{
					TenantCapabilitiesTestingKnobs: &tenantcapabilities.TestingKnobs{
						WatcherTestingKnobs: &tenantcapabilitieswatcher.TestingKnobs{
							WatcherRangeFeedKnobs: &rangefeedcache.TestingKnobs{
								OnTimestampAdvance: func(ts hlc.Timestamp) {
									mu.Lock()
									defer mu.Unlock()
									mu.lastFrontierTS = ts
								},
							},
						},
					},
				},
			},
		})
		defer tc.Stopper().Stop(ctx)
		systemSQLDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

		tenantArgs := base.TestTenantArgs{
			TenantID: serverutils.TestTenantID(),
		}
		testTenantInterface, err := tc.Server(0).TenantController().StartTenant(ctx, tenantArgs)
		require.NoError(t, err)

		tenantSQLDB := testTenantInterface.SQLConn(t)

		lastUpdateTS := tc.Server(0).Clock().Now() // ensure watcher isn't starting out empty
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {

			switch d.Cmd {
			case "update-capabilities":
				systemSQLDB.Exec(t, d.Input)
				lastUpdateTS = tc.Server(0).Clock().Now()

			case "exec-privileged-op-tenant":
				testutils.SucceedsSoon(t, func() error {
					mu.Lock()
					defer mu.Unlock()

					if lastUpdateTS.Less(mu.lastFrontierTS) {
						return nil
					}

					return errors.Newf("frontier timestamp (%s) lagging last update (%s)",
						mu.lastFrontierTS.String(), lastUpdateTS.String())
				})
				_, err := tenantSQLDB.Exec(d.Input)
				if err != nil {
					errStr := err.Error()
					// Redact transaction IDs from error strings, for determinism.
					errStr = regexp.MustCompile(`\[txn: [0-9a-f]+]`).ReplaceAllString(errStr, `[txn: ‹×›]`)
					return errStr
				}

			case "exec-sql-tenant":
				_, err := tenantSQLDB.Exec(d.Input)
				if err != nil {
					return err.Error()
				}

			case "query-sql-system":
				rows := systemSQLDB.Query(t, d.Input)
				output, err := sqlutils.RowsToDataDrivenOutput(rows)
				require.NoError(t, err)
				return output

			default:
				return fmt.Sprintf("unknown command %s", d.Cmd)
			}
			return "ok"
		})
	})
}
