// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package spanconfigsplitterccl

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsplitter"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils/spanconfigtestcluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestDataDriven is a data-driven test for spanconfig.Splitter. It offers
// the following commands:
//
//   - "exec-sql"
//     Executes the input SQL query.
//
//   - "query-sql"
//     Executes the input SQL query and prints the results.
//
//   - "splits" [database=<str> table=<str>] [id=<int>]
//     Prints the number splits generated the referenced object (named database +
//     table, or descriptor id). Also logs the set of internal steps the Splitter
//     takes to arrive at the number.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var steps strings.Builder
	scKnobs := &spanconfig.TestingKnobs{
		// Instead of relying on the GC job to wait out TTLs and clear out descriptors,
		// let's simply exclude dropped tables to simulate descriptors no longer existing.
		// See comment on ExcludeDroppedDescriptorsFromLookup for more details.
		ExcludeDroppedDescriptorsFromLookup: true,
		// We run the reconciler manually in this test (through the span config
		// test cluster).
		ManagerDisableJobCreation: true,
		// SplitterStepLogger captures splitter-internal steps for test output.
		SplitterStepLogger: func(step string) {
			steps.WriteString(fmt.Sprintf("%s\n", step))
		},
	}
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				// Fails with nil pointer dereference. Tracked with #76378 and #106818.
				DefaultTestTenant: base.TestDoesNotWorkWithSecondaryTenantsButWeDontKnowWhyYet(106818),
				Knobs: base.TestingKnobs{
					SpanConfig: scKnobs,
				},
			},
		})
		defer tc.Stopper().Stop(ctx)

		spanConfigTestCluster := spanconfigtestcluster.NewHandle(t, tc, scKnobs)
		defer spanConfigTestCluster.Cleanup()

		var tenant *spanconfigtestcluster.Tenant
		if strings.Contains(path, "tenant") {
			tenantID := roachpb.MustMakeTenantID(10)
			tenant = spanConfigTestCluster.InitializeTenant(ctx, tenantID)
			spanConfigTestCluster.AllowSecondaryTenantToSetZoneConfigurations(t, tenantID)
			spanConfigTestCluster.EnsureTenantCanSetZoneConfigurationsOrFatal(t, tenant)
		} else {
			tenant = spanConfigTestCluster.InitializeTenant(ctx, roachpb.SystemTenantID)
		}

		// TODO(irfansharif): Expose this through the test harness once we integrate
		// it into the schema changer.
		splitter := spanconfigsplitter.New(tenant.ExecCfg().Codec, scKnobs)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "exec-sql":
				tenant.Exec(d.Input)

			case "query-sql":
				rows := tenant.Query(d.Input)
				output, err := sqlutils.RowsToDataDrivenOutput(rows)
				require.NoError(t, err)
				return output

			case "splits":
				// Parse the args to get the object ID we're looking to split.
				var objID descpb.ID
				switch {
				case d.HasArg("id"):
					var scanID int
					d.ScanArgs(t, "id", &scanID)
					objID = descpb.ID(scanID)
				case d.HasArg("database"):
					// NB: Name resolution for dropped descriptors does not work like
					// you'd expect, i.e. it does not work at all. We have to look things
					// up by ID first.
					var dbName, tbName string
					d.ScanArgs(t, "database", &dbName)
					d.ScanArgs(t, "table", &tbName)
					objID = tenant.LookupTableByName(ctx, dbName, tbName).GetID()
				default:
					d.Fatalf(t, "insufficient/improper args (%v) provided to split", d.CmdArgs)
				}

				steps.Reset()
				splits, err := splitter.Splits(ctx, tenant.LookupTableDescriptorByID(ctx, objID))
				require.NoError(t, err)
				steps.WriteString(fmt.Sprintf("= %d", splits))
				return steps.String()

			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}

			return ""
		})
	})
}
