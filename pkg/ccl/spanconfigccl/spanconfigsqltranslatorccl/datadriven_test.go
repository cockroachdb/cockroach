// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigsqltranslatorccl

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqltranslator"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils/spanconfigtestcluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestDataDriven is a data-driven test for the spanconfig.SQLTranslator. It
// allows users to set up zone config hierarchies and validate their translation
// to SpanConfigs is as expected. Only fields that are different from the
// (default) RANGE DEFAULT are printed in the test output for readability. It
// offers the following commands:
//
//   - "exec-sql"
//     Executes the input SQL query.
//
//   - "query-sql"
//     Executes the input SQL query and prints the results.
//
//   - "translate" [database=<str>] [table=<str>] [named-zone=<str>] [id=<int>]
//     Translates the SQL zone config state to the span config state starting
//     from the referenced object (named zone, database, database + table, or
//     descriptor id) as the root.
//
//   - "full-translate"
//     Performs a full translation of the SQL zone config state to the implied
//     span config state.
//
//   - "mark-table-offline" [database=<str>] [table=<str>]
//     Marks the given table as offline for testing purposes.
//
//   - "mark-table-public" [database=<str>] [table=<str>]
//     Marks the given table as public.
//
//   - "protect" [record-id=<int>] [ts=<int>]
//     cluster                  OR
//     tenants       id1,id2... OR
//     descs         id1,id2...
//     Creates and writes a protected timestamp record with id and ts with an
//     appropriate ptpb.Target.
//
//   - "release" [record-id=<int>]
//     Releases the protected timestamp record with id.
func TestDataDriven(t *testing.T) {
	t.Cleanup(leaktest.AfterTest(t))
	scope := log.Scope(t)
	t.Cleanup(func() {
		scope.Close(t)
	})

	skip.UnderRace(t, "very long-running test under race")

	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		t.Parallel() // SAFE FOR TESTING
		gcWaiter := sync.NewCond(&syncutil.Mutex{})
		allowGC := true
		gcTestingKnobs := &sql.GCJobTestingKnobs{
			RunBeforeResume: func(_ jobspb.JobID) error {
				gcWaiter.L.Lock()
				for !allowGC {
					gcWaiter.Wait()
				}
				gcWaiter.L.Unlock()
				return nil
			},
			SkipWaitingForMVCCGC: true,
		}
		scKnobs := &spanconfig.TestingKnobs{
			// Instead of relying on the GC job to wait out TTLs and clear out
			// descriptors, let's simply exclude dropped tables to simulate
			// descriptors no longer existing. See comment on
			// ExcludeDroppedDescriptorsFromLookup for more details.
			ExcludeDroppedDescriptorsFromLookup: true,
			// We run the reconciler manually in this test (through the span config
			// test cluster).
			ManagerDisableJobCreation: true,
		}
		sqlExecutorKnobs := &sql.ExecutorTestingKnobs{
			UseTransactionalDescIDGenerator: true,
		}
		tsArgs := func(attr string) base.TestServerArgs {
			return base.TestServerArgs{
				Knobs: base.TestingKnobs{
					GCJob:       gcTestingKnobs,
					SpanConfig:  scKnobs,
					SQLExecutor: sqlExecutorKnobs,
				},
				StoreSpecs: []base.StoreSpec{
					{InMemory: true, Attributes: roachpb.Attributes{Attrs: []string{attr}}},
				},
			}
		}
		// Use 1 node by default to make tests run faster.
		nodes := 1
		if strings.Contains(path, "3node") {
			nodes = 3
		}
		tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
			},
			ServerArgsPerNode: map[int]base.TestServerArgs{
				0: tsArgs("n1"),
				1: tsArgs("n2"),
				2: tsArgs("n3"),
			},
		})
		defer tc.Stopper().Stop(ctx)

		spanConfigTestCluster := spanconfigtestcluster.NewHandle(t, tc, scKnobs)
		defer spanConfigTestCluster.Cleanup()

		var tenant *spanconfigtestcluster.Tenant
		if strings.Contains(path, "tenant") {
			tenantID := roachpb.MustMakeTenantID(10)
			tenant = spanConfigTestCluster.InitializeTenant(ctx, tenantID)
		} else {
			tenant = spanConfigTestCluster.InitializeTenant(ctx, roachpb.SystemTenantID)
		}
		execCfg := tenant.ExecCfg()
		tenant.Exec("SET autocommit_before_ddl = false")

		var f func(t *testing.T, d *datadriven.TestData) string
		f = func(t *testing.T, d *datadriven.TestData) string {
			var generateSystemSpanConfigs bool
			var descIDs []descpb.ID
			switch d.Cmd {
			case "exec-sql":
				tenant.Exec(d.Input)

			case "query-sql":
				rows := tenant.Query(d.Input)
				output, err := sqlutils.RowsToDataDrivenOutput(rows)
				require.NoError(t, err)
				return output

			case "translate":
				// Parse the args to get the descriptor ID we're looking to
				// translate.
				switch {
				case d.HasArg("named-zone"):
					var zone string
					d.ScanArgs(t, "named-zone", &zone)
					namedZoneID, found := zonepb.NamedZones[zonepb.NamedZone(zone)]
					require.Truef(t, found, "unknown named zone: %s", zone)
					descIDs = []descpb.ID{descpb.ID(namedZoneID)}
				case d.HasArg("id"):
					var scanID int
					d.ScanArgs(t, "id", &scanID)
					descIDs = []descpb.ID{descpb.ID(scanID)}
				case d.HasArg("database"):
					var dbName string
					d.ScanArgs(t, "database", &dbName)
					if d.HasArg("table") {
						var tbName string
						d.ScanArgs(t, "table", &tbName)
						descIDs = []descpb.ID{tenant.LookupTableByName(ctx, dbName, tbName).GetID()}
					} else {
						descIDs = []descpb.ID{tenant.LookupDatabaseByName(ctx, dbName).GetID()}
					}
				case d.HasArg("system-span-configurations"):
					generateSystemSpanConfigs = true
				default:
					d.Fatalf(t, "insufficient/improper args (%v) provided to translate", d.CmdArgs)
				}

				var records []spanconfig.Record
				sqlTranslatorFactory := tenant.SpanConfigSQLTranslatorFactory().(*spanconfigsqltranslator.Factory)
				err := execCfg.InternalDB.DescsTxn(ctx, func(
					ctx context.Context, txn descs.Txn,
				) error {
					sqlTranslator := sqlTranslatorFactory.NewSQLTranslator(txn)
					var err error
					records, err = sqlTranslator.Translate(ctx, descIDs, generateSystemSpanConfigs)
					require.NoError(t, err)
					return nil
				})
				require.NoError(t, err)

				sort.Slice(records, func(i, j int) bool {
					return records[i].GetTarget().Less(records[j].GetTarget())
				})
				var output strings.Builder
				for _, record := range records {
					switch {
					case record.GetTarget().IsSpanTarget():
						output.WriteString(fmt.Sprintf("%-42s %s\n", record.GetTarget().GetSpan(),
							spanconfigtestutils.PrintSpanConfigDiffedAgainstDefaults(record.GetConfig())))
					case record.GetTarget().IsSystemTarget():
						output.WriteString(fmt.Sprintf("%-42s %s\n", record.GetTarget().GetSystemTarget(),
							spanconfigtestutils.PrintSystemSpanConfigDiffedAgainstDefault(record.GetConfig())))
					default:
						panic("unsupported target type")
					}
				}
				return output.String()

			case "full-translate":
				sqlTranslatorFactory := tenant.SpanConfigSQLTranslatorFactory().(*spanconfigsqltranslator.Factory)
				var records []spanconfig.Record
				err := execCfg.InternalDB.DescsTxn(ctx, func(
					ctx context.Context, txn descs.Txn,
				) error {
					sqlTranslator := sqlTranslatorFactory.NewSQLTranslator(txn)
					var err error
					records, err = spanconfig.FullTranslate(ctx, sqlTranslator)
					require.NoError(t, err)
					return nil
				})
				require.NoError(t, err)

				sort.Slice(records, func(i, j int) bool {
					return records[i].GetTarget().Less(records[j].GetTarget())
				})
				var output strings.Builder
				for _, record := range records {
					output.WriteString(fmt.Sprintf("%-42s %s\n", record.GetTarget().GetSpan(),
						spanconfigtestutils.PrintSpanConfigDiffedAgainstDefaults(record.GetConfig())))
				}
				return output.String()

			case "mark-table-offline":
				var dbName, tbName string
				d.ScanArgs(t, "database", &dbName)
				d.ScanArgs(t, "table", &tbName)
				tenant.WithMutableTableDescriptor(ctx, dbName, tbName, func(mutable *tabledesc.Mutable) {
					mutable.SetOffline("for testing")
				})

			case "mark-table-public":
				var dbName, tbName string
				d.ScanArgs(t, "database", &dbName)
				d.ScanArgs(t, "table", &tbName)
				tenant.WithMutableTableDescriptor(ctx, dbName, tbName, func(mutable *tabledesc.Mutable) {
					mutable.SetPublic()
				})

			case "mark-database-offline":
				var dbName string
				d.ScanArgs(t, "database", &dbName)
				tenant.WithMutableDatabaseDescriptor(ctx, dbName, func(mutable *dbdesc.Mutable) {
					mutable.SetOffline("for testing")
				})

			case "mark-database-public":
				var dbName string
				d.ScanArgs(t, "database", &dbName)
				tenant.WithMutableDatabaseDescriptor(ctx, dbName, func(mutable *dbdesc.Mutable) {
					mutable.SetPublic()
				})

			case "protect":
				var recordID string
				var protectTS int
				d.ScanArgs(t, "record-id", &recordID)
				d.ScanArgs(t, "ts", &protectTS)
				target := spanconfigtestutils.ParseProtectionTarget(t, d.Input)
				target.IgnoreIfExcludedFromBackup = d.HasArg("ignore-if-excluded-from-backup")
				tenant.MakeProtectedTimestampRecordAndProtect(ctx, recordID, protectTS, target)

			case "release":
				var recordID string
				d.ScanArgs(t, "record-id", &recordID)
				tenant.ReleaseProtectedTimestampRecord(ctx, recordID)

			case "block-gc-jobs":
				gcWaiter.L.Lock()
				allowGC = false
				gcWaiter.L.Unlock()

			case "unblock-gc-jobs":
				gcWaiter.L.Lock()
				allowGC = true
				gcWaiter.Signal()
				gcWaiter.L.Unlock()

			case "repartition":
				var fromRelativePath, toRelativePath string
				d.ScanArgs(t, "from", &fromRelativePath)
				d.ScanArgs(t, "to", &toRelativePath)
				parentDir := filepath.Dir(path)

				fromAbsolutePath := filepath.Join(parentDir, fromRelativePath)
				datadriven.RunTest(t, fromAbsolutePath, f)

				toAbsolutePath := filepath.Join(parentDir, toRelativePath)
				datadriven.RunTest(t, toAbsolutePath, f)

			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}

			return ""
		}

		datadriven.RunTest(t, path, f)
	})
}
