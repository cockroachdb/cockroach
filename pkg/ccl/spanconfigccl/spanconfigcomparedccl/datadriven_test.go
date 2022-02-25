// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package spanconfigcomparedccl is a test-only package that compares the span
// configs infrastructure against the gossiped system config span subsystem it
// was designed to replace.
package spanconfigcomparedccl

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/systemconfigwatcher"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils/spanconfigtestcluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/require"
)

// TestDataDriven compares the span configs infrastructure against the gossipped
// SystemConfigSpan it was designed to replace. The test spits out a visual
// "diff" of the two subsystems, indicating exactly where the differences and
// similarities lie. It does so by having both systems run side-by-side, and
// scanning their contents to determine what configs are to be applied over what
// key spans -- data that determine what the range boundaries are or what
// actions are executed on behalf of ranges by internal queues. We're not
// interested in the secondary effects of the subsystems, just what's visible
// through their APIs.
//
// In this test we can spin up secondary tenants, create arbitrary schemas/zone
// configs, and understand what each subsystem presents as the global
// configuration state. The results are presented as a diff sorted by the start
// key of exposed span configs, compare the gossip-backed implementation
// ("legacy") with the span configs one ("current"). The following syntax is
// provided:
//
// - "initialize" tenant=<int>
//   Initialize a secondary tenant with the given ID.
//
// - "exec-sql" [tenant=<int>]
//   Executes the input SQL query for the given tenant. All statements are
//   executed in a single transaction.
//
// - "query-sql" [tenant=<int>]
//   Executes the input SQL query for the given tenant and print the results.
//
// - "reconcile" [tenant=<int>]
//   Start the reconciliation process for the given tenant.
//
// - "diff" [offset=<int>] [limit=<int>]
//   Print out the diffs between the two subsystems, skipping 'offset' rows and
//   returning up to the specified 'limit' (if any).
//
// - "configs" version=<legacy|current> [offset=<int>] [limit=<int>]
//   Print out the configs as observed, skipping 'offset' rows and returning up
//   to the specified 'limit' (if any).
//
// Commands with optional tenant IDs when unspecified default to the host
// tenant.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		scKnobs := &spanconfig.TestingKnobs{
			// Instead of relying on the GC job to wait out TTLs and clear out
			// descriptors, let's simply exclude dropped tables to simulate
			// descriptors no longer existing. See comment on
			// ExcludeDroppedDescriptorsFromLookup for more details.
			ExcludeDroppedDescriptorsFromLookup: true,
			// We run the reconciler manually in this test.
			ManagerDisableJobCreation: true,
			// Checkpoint no-ops frequently to speed up the test.
			SQLWatcherCheckpointNoopsEveryDurationOverride: 100 * time.Millisecond,
		}
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(), // speeds up test
					SpanConfig:       scKnobs,
				},
			},
		})
		defer tc.Stopper().Stop(ctx)

		{
			tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
			tdb.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
			tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '20ms'`)
			tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '20ms'`)
		}

		spanConfigTestCluster := spanconfigtestcluster.NewHandle(t, tc, scKnobs, nil /* ptsKnobs */)
		defer spanConfigTestCluster.Cleanup()

		kvSubscriber := tc.Server(0).SpanConfigKVSubscriber().(spanconfig.KVSubscriber)
		systemConfig := tc.Server(0).SystemConfigProvider().(*systemconfigwatcher.Cache)

		systemTenant := spanConfigTestCluster.InitializeTenant(ctx, roachpb.SystemTenantID)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			tenantID := roachpb.SystemTenantID
			if d.HasArg("tenant") {
				if d.Cmd == "state" {
					d.Fatalf(t, "unexpected argument 'tenant' for command %q", d.Cmd)
				}

				var id uint64
				d.ScanArgs(t, "tenant", &id)
				tenantID = roachpb.MakeTenantID(id)
			}

			tenant, found := spanConfigTestCluster.LookupTenant(tenantID)
			if d.Cmd != "initialize" {
				require.Truef(t, found, "tenant %s not found (was it initialized?)", tenantID)
			}

			if d.Cmd == "configs" || d.Cmd == "diff" {
				// To observe up-to-date KVSubscriber state, we have to:
				// - wait for all tenant checkpoints to cross their last
				//   execution timestamp (ensuring that sql changes have been
				//   reconciled)
				// - wait for the KVSubscriber to catch up to the latest
				//   configuration changes
				testutils.SucceedsSoon(t, func() error {
					for _, tenant := range spanConfigTestCluster.Tenants() {
						lastCheckpoint, lastExec := tenant.LastCheckpoint(), tenant.TimestampAfterLastSQLChange()
						if lastCheckpoint.IsEmpty() {
							continue // reconciler wasn't started
						}
						if lastCheckpoint.Less(lastExec) {
							return errors.Newf("last checkpoint timestamp (%s) lagging last sql execution (%s)",
								lastCheckpoint.GoTime(), lastExec.GoTime())
						}
					}

					return nil
				})

				// Now that all tenants have reconciled their SQL changes, read
				// the current time and wait for the kvsubscriber to advance
				// past it. This lets us observe the effects of all:
				// (i)  reconciliation processes;
				// (ii) tenant initializations (where seed span configs are
				//      installed).
				checkLastUpdated := func(t *testing.T, n string, c interface{ LastUpdated() hlc.Timestamp }) {
					now := systemTenant.Clock().Now()
					testutils.SucceedsSoon(t, func() error {
						lastUpdated := c.LastUpdated()
						if lastUpdated.Less(now) {
							return errors.Newf("%s last updated timestamp (%s) lagging barrier timestamp (%s)",
								n, lastUpdated.GoTime(), now.GoTime())
						}
						return nil
					})
				}
				checkLastUpdated(t, "kvsubscriber", kvSubscriber)
				checkLastUpdated(t, "systemconfigwatcher", systemConfig)

				// As for the gossiped system config span, because we're using a
				// single node cluster there's no additional timestamp
				// sequencing required -- any changes to data in the system
				// config span immediately updates the only node's in-memory
				// gossip as part of the commit trigger.
			}

			switch d.Cmd {
			case "initialize":
				secondaryTenant := spanConfigTestCluster.InitializeTenant(ctx, tenantID)
				secondaryTenant.Exec(`SET CLUSTER SETTING sql.zone_configs.allow_for_secondary_tenant.enabled = true`)

			case "exec-sql":
				// Run under an explicit transaction -- we rely on having a
				// single timestamp for the statements (see
				// tenant.TimestampAfterLastSQLChange) for ordering guarantees.
				tenant.Exec(fmt.Sprintf("BEGIN; %s; COMMIT;", d.Input))

			case "query-sql":
				rows := tenant.Query(d.Input)
				output, err := sqlutils.RowsToDataDrivenOutput(rows)
				require.NoError(t, err)
				return output

			case "reconcile":
				tsBeforeReconcilerStart := tenant.Clock().Now()
				go func() {
					err := tenant.Reconciler().Reconcile(ctx, hlc.Timestamp{} /* startTS */, func() error {
						tenant.RecordCheckpoint()
						return nil
					})
					require.NoError(t, err)
				}()

				testutils.SucceedsSoon(t, func() error {
					if tenant.LastCheckpoint().Less(tsBeforeReconcilerStart) {
						return errors.New("expected reconciler to have started")
					}
					return nil
				})

			case "configs":
				var version string
				d.ScanArgs(t, "version", &version)
				if version != "legacy" && version != "current" {
					d.Fatalf(t, "malformed command: expected either %q or %q to be specified", "legacy", "current")
				}

				var reader spanconfig.StoreReader
				if version == "legacy" {
					reader = systemConfig.GetSystemConfig()
				} else {
					reader = kvSubscriber
				}

				data := strings.Split(spanconfigtestutils.GetSplitPoints(ctx, t, reader).String(), "\n")
				return spanconfigtestutils.MaybeLimitAndOffset(t, d, "...", data)

			case "diff":
				var before, after spanconfig.StoreReader = systemConfig.GetSystemConfig(), kvSubscriber
				diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A:        difflib.SplitLines(spanconfigtestutils.GetSplitPoints(ctx, t, before).String()),
					B:        difflib.SplitLines(spanconfigtestutils.GetSplitPoints(ctx, t, after).String()),
					FromFile: "gossiped system config span (legacy)",
					ToFile:   "span config infrastructure (current)",
					Context:  2,
				})
				require.NoError(t, err)
				if diff == "" {
					return ""
				}

				lines := strings.Split(strings.TrimSpace(diff), "\n")
				headerLines, diffLines := lines[:2], lines[2:]

				var output strings.Builder
				for _, headerLine := range headerLines {
					output.WriteString(fmt.Sprintf("%s\n", headerLine))
				}

				output.WriteString(spanconfigtestutils.MaybeLimitAndOffset(t, d, " ...", diffLines))
				return output.String()

			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			return ""
		})
	})
}
