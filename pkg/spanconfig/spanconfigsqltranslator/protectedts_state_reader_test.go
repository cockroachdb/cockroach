// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqltranslator_test

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqltranslator"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils/spanconfigtestcluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestProtectedTimestampStateReader is a data driven test for the
// spanconfigsqltranslator.ProtectedTimestampStateReader. It allows users to
// protect and release protected timestamp records on the cluster, tenants, or
// schema objects and validate that the ProtectedTimestampStateReader maintains
// an accurate view of all protected timestamp records. It offers the following
// commands:
//
// - "exec-sql"
//   Executes the input SQL query.
//
// - "query-sql"
//   Executes the input SQL query and prints the results.
//
// - "protect" [id=<int>] [ts=<int>]
//   cluster                  OR
//   tenants       id1,id2... OR
//   schemaObject  id1,id2...
//   Creates and writes a protected timestamp record with id and ts with an
//   appropriate ptpb.Target.
//
// - "release" [id=<int>]
//   Releases the protected timestamp record with id.
//
// - "statereader" [cluster] [tenants] [schemaObject=<int>]
//    Fetches the protected timestamps that apply to the target using the
//    ProtectedTimestampStateReader.
func TestProtectedTimestampStateReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				EnableSpanConfigs: true,
				Knobs: base.TestingKnobs{
					ProtectedTS: &protectedts.TestingKnobs{EnableProtectedTimestampForMultiTenant: true},
				},
			},
		})
		defer tc.Stopper().Stop(ctx)

		spanConfigTestCluster := spanconfigtestcluster.NewHandle(t, tc, &spanconfig.TestingKnobs{})
		defer spanConfigTestCluster.Cleanup()

		var tenant *spanconfigtestcluster.Tenant
		// TODO(adityamaru): When secondary tenants have a "real"
		// protectedts.Provider we can write a test inside `/tenant/` to ensure the
		// state reader works as expected.
		if strings.Contains(path, "tenant") {
			tenant = spanConfigTestCluster.InitializeTenant(ctx, roachpb.MakeTenantID(10))
		} else {
			tenant = spanConfigTestCluster.InitializeTenant(ctx, roachpb.SystemTenantID)
		}

		execCfg := tenant.ExecutorConfig().(sql.ExecutorConfig)
		ptp := tenant.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider
		jr := tenant.JobRegistry().(*jobs.Registry)

		mkRecordAndProtect := func(recordID string, ts hlc.Timestamp, target *ptpb.Target) {
			jobID := jr.MakeJobID()
			require.NoError(t, execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
				recID, err := uuid.FromBytes([]byte(strings.Repeat(recordID, 16)))
				require.NoError(t, err)
				rec := jobsprotectedts.MakeRecord(recID, int64(jobID), ts,
					nil /* deprecatedSpans */, jobsprotectedts.Jobs, target)
				return ptp.Protect(ctx, txn, rec)
			}))
		}

		releaseRecord := func(recordID string) {
			require.NoError(t, execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				recID, err := uuid.FromBytes([]byte(strings.Repeat(recordID, 16)))
				require.NoError(t, err)
				return ptp.Release(ctx, txn, recID)
			}))
		}

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "exec-sql":
				tenant.Exec(d.Input)

			case "query-sql":
				rows := tenant.Query(d.Input)
				output, err := sqlutils.RowsToDataDrivenOutput(rows)
				require.NoError(t, err)
				return output
			case "statereader":
				var output string
				require.NoError(t, execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					ptsState, err := ptp.GetState(ctx, txn)
					require.NoError(t, err)
					stateReader, err := spanconfigsqltranslator.TestingNewProtectedTimestampStateReader(ctx, ptsState)
					if err != nil {
						return err
					}

					if d.HasArg("cluster") {
						output = outputTimestamps(stateReader.GetProtectedTimestampsForCluster())
					} else if d.HasArg("tenants") {
						output = outputTenants(stateReader.GetProtectedTimestampsForTenants())
					} else if d.HasArg("schemaObject") {
						var schemaObjectID int
						d.ScanArgs(t, "schemaObject", &schemaObjectID)
						output = outputTimestamps(stateReader.GetProtectedTimestampsForSchemaObject(
							descpb.ID(schemaObjectID)))
					}
					return nil
				}))
				return output

			case "protect":
				var recordID string
				var protectTS int
				d.ScanArgs(t, "id", &recordID)
				d.ScanArgs(t, "ts", &protectTS)
				target := parseProtectionTarget(t, d.Input)
				mkRecordAndProtect(recordID, hlc.Timestamp{WallTime: int64(protectTS)}, target)
				return ""
			case "release":
				var recordID string
				d.ScanArgs(t, "id", &recordID)
				releaseRecord(recordID)
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			return ""
		})
	})
}

// Returns the spanconfigsqltranslator.TenantProtectedTimestamps formatted as:
//
// tenant_id=<int> [ts=<int>, ts=<int>...]
func outputTenants(tenantProtections []spanconfigsqltranslator.TenantProtectedTimestamps) string {
	sort.Slice(tenantProtections, func(i, j int) bool {
		return tenantProtections[i].TenantID.ToUint64() < tenantProtections[j].TenantID.ToUint64()
	})
	var output strings.Builder
	for i, p := range tenantProtections {
		output.WriteString(fmt.Sprintf("tenant_id=%d %s", p.TenantID.ToUint64(),
			outputTimestamps(p.Protections)))
		if i != len(tenantProtections)-1 {
			output.WriteString("\n")
		}
	}
	return output.String()
}

// Returns the timestamps formatted as:
//
// [ts=<int>, ts=<int>...]
func outputTimestamps(timestamps []hlc.Timestamp) string {
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].Less(timestamps[j])
	})

	var output strings.Builder
	output.WriteString(fmt.Sprintf("[%s]", func() string {
		var s strings.Builder
		for i, pts := range timestamps {
			s.WriteString(fmt.Sprintf("ts=%d", int(pts.WallTime)))
			if i != len(timestamps)-1 {
				s.WriteString(" ")
			}
		}
		return s.String()
	}()))
	return output.String()
}

// TODO(adityamaru): move this into `spanconfigtestutils` once we write
// SQLTranslator datadriven tests.
func parseProtectionTarget(t *testing.T, input string) *ptpb.Target {
	line := strings.Split(input, "\n")
	if len(line) != 1 {
		t.Fatal("only one target must be specified per protectedts operation")
	}
	target := line[0]

	const clusterPrefix, tenantPrefix, schemaObjectPrefix = "cluster", "tenant", "schemaObject"
	switch {
	case strings.HasPrefix(target, clusterPrefix):
		return ptpb.MakeClusterTarget()
	case strings.HasPrefix(target, tenantPrefix):
		target = strings.TrimPrefix(target, target[:len(tenantPrefix)+1])
		tenantIDs := strings.Split(target, ",")
		ids := make([]roachpb.TenantID, 0, len(tenantIDs))
		for _, tenID := range tenantIDs {
			id, err := strconv.Atoi(tenID)
			require.NoError(t, err)
			ids = append(ids, roachpb.MakeTenantID(uint64(id)))
		}
		return ptpb.MakeTenantsTarget(ids)
	case strings.HasPrefix(target, schemaObjectPrefix):
		target = strings.TrimPrefix(target, target[:len(schemaObjectPrefix)+1])
		fmt.Println(target)
		schemaObjectIDs := strings.Split(target, ",")
		fmt.Println(schemaObjectIDs)
		ids := make([]descpb.ID, 0, len(schemaObjectIDs))
		for _, tenID := range schemaObjectIDs {
			id, err := strconv.Atoi(tenID)
			require.NoError(t, err)
			ids = append(ids, descpb.ID(id))
		}
		return ptpb.MakeSchemaObjectsTarget(ids)
	default:
		t.Fatalf("malformed line %q, expecetd to find prefix %q or %q", target, tenantPrefix,
			schemaObjectPrefix)
	}
	return nil
}
