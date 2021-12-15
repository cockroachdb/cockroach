// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package spanconfigreconcilerccl

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	"github.com/stretchr/testify/require"
)

// TestDataDriven is a data-driven test for spanconfig.Reconciler. It lets test
// authors spin up secondary tenants, create arbitrary schema objects with
// arbitrary zone configs, and verify that the global span config state is as
// we'd expect. Only fields that differ from the static RANGE DEFAULT are
// printed in the test output for readability. The following syntax is provided:
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
// - "mutations" [tenant=<int>] [discard]
//   Print the latest set of mutations issued by the reconciler for the given
//   tenant. If 'discard' is specified, nothing is printed.
//
// - "state" [offset=<int>] [limit=<int]
//   Print out the contents of KVAccessor directly, skipping 'offset' entries,
//   returning up to the specified limit if any.
//
// TODO(irfansharif): Provide a way to stop reconcilers and/or start them back
// up again. It would let us add simulate for suspended tenants, and behavior of
// the reconciler with existing kvaccessor state (populated by an earlier
// incarnation). When tearing existing reconcilers down, we'd need to
// synchronize with "last-exec" timestamp safely seeing as how no further
// checkpoints are expected.
//
// TODO(irfansharif): Test tenant teardown/GC -- all tenant-scoped span configs
// must be cleared out.
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
			// Checkpoint noops frequently; speeds this test up.
			SQLWatcherCheckpointNoopsEveryDurationOverride: 100 * time.Millisecond,
		}
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				EnableSpanConfigs: true,
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
			tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
		}

		spanConfigTestCluster := spanconfigtestcluster.NewHandle(t, tc)
		defer spanConfigTestCluster.Cleanup()

		systemTenant := spanConfigTestCluster.InitializeTenant(ctx, roachpb.SystemTenantID)
		kvAccessor := systemTenant.SpanConfigKVAccessor().(spanconfig.KVAccessor)
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

			switch d.Cmd {
			case "initialize":
				secondaryTenant := spanConfigTestCluster.InitializeTenant(ctx, tenantID)
				secondaryTenant.Exec(`SET CLUSTER SETTING sql.zone_configs.allow_for_secondary_tenant.enabled = true`)

			case "exec-sql":
				// Run under an explicit transaction -- we rely on having a
				// single timestamp for the statements (see
				// tenant.TimestampAfterLastExec) for ordering guarantees.
				tenant.Exec(fmt.Sprintf("BEGIN; %s; COMMIT;", d.Input))

			case "query-sql":
				rows := tenant.Query(d.Input)
				output, err := sqlutils.RowsToDataDrivenOutput(rows)
				require.NoError(t, err)
				return output

			case "reconcile":
				tsBeforeReconcilerStart := tenant.Clock().Now()
				go func() {
					err := tenant.Reconciler().Reconcile(ctx, hlc.Timestamp{}, func(checkpoint hlc.Timestamp) error {
						tenant.Checkpoint(checkpoint)
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

			case "mutations":
				testutils.SucceedsSoon(t, func() error {
					lastCheckpoint, lastExec := tenant.LastCheckpoint(), tenant.TimestampAfterLastExec()
					if lastCheckpoint.Less(lastExec) {
						return errors.Newf("last checkpoint timestamp (%s) lagging last sql execution (%s)",
							lastCheckpoint.GoTime(), lastExec.GoTime())
					}
					return nil
				})

				output := tenant.KVAccessorRecorder().Recording(true /* clear */)
				if d.HasArg("discard") {
					return ""
				}
				return output

			case "state":
				testutils.SucceedsSoon(t, func() error {
					// To observe up-to-date KVAccess state, we wait for all
					// tenant checkpoints to cross their last execution
					// timestamp.
					for _, tenant := range spanConfigTestCluster.Tenants() {
						lastCheckpoint, lastExec := tenant.LastCheckpoint(), tenant.TimestampAfterLastExec()
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
				entries, err := kvAccessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{keys.EverythingSpan})
				require.NoError(t, err)
				sort.Slice(entries, func(i, j int) bool {
					return entries[i].Span.Key.Compare(entries[j].Span.Key) < 0
				})

				lines := make([]string, len(entries))
				for i, entry := range entries {
					lines[i] = fmt.Sprintf("%-42s %s", entry.Span,
						spanconfigtestutils.PrintSpanConfigDiffedAgainstDefaults(entry.Config))
				}
				return spanconfigtestutils.MaybeLimitAndOffset(t, d, "...", lines)

			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			return ""
		})
	})
}
