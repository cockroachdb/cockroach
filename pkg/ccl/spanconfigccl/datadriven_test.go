// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package spanconfigccl_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqltranslator"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestSQLTranslatorDataDriven is a data-driven test for the
// spanconfigsqltranslator.SQLTranslator. It allows users to set up zone config
// hierarchies and validate their translation to SpanConfigs is as expected.
// Only fields that are different from the (default) RANGE DEFAULT are printed
// in the test output for readability.
//
// It offers the following commands:
//
// 	"exec-sql": executes the input SQL query.
//
// 	"query-sql": executes the input SQL query and prints the results.
//
// 	"translate [database=<string>] [table=<string>] [named-zone=<string>]
// 	[id=<int>]:
// 	translates the SQL zone config state to the span config state starting from
// 	the referenced object (named zone, database, database + table, or descriptor
//	id) as the root.
//
// 	"full-translate": performs a full translation of the SQL zone config state
// 	to the implied span config state.
//
//  "sleep" [duration=<int>]: sleep for the provided duration.
//
//  "mark-table-offline" [database=<string>] [table=<string>]: marks the given
//   table as offline for testing purposes.
//
//  "mark-table-public" [database=<string>] [table=<string>]: marks the given
//   table as public.
//
// TODO(arul): Add a secondary tenant configuration for this test as well.
func TestSQLTranslatorDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	datadriven.Walk(t, "testdata/", func(t *testing.T, path string) {
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				EnableSpanConfigs: true,
				Knobs: base.TestingKnobs{
					SpanConfig: &spanconfig.TestingKnobs{
						ManagerDisableJobCreation: true,
					},
				},
			},
		})
		defer tc.Stopper().Stop(ctx)
		sqlDB := tc.ServerConn(0 /* idx */)

		sqlTranslator := tc.Server(0).SpanConfigSQLTranslator().(spanconfig.SQLTranslator)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "exec-sql":
				_, err := sqlDB.Exec(d.Input)
				if err != nil {
					return err.Error()
				}
			case "query-sql":
				rows, err := sqlDB.Query(d.Input)
				if err != nil {
					return err.Error()
				}
				output, err := sqlutils.RowsToDataDrivenOutput(rows)
				require.NoError(t, err)
				return output
			case "translate":
				// Parse the args to get the object ID we're interested in translating.
				objID := descpb.InvalidID
				if d.HasArg(namedZone) {
					var zone string
					d.ScanArgs(t, namedZone, &zone)
					namedZoneID, found := zonepb.NamedZones[zonepb.NamedZone(zone)]
					require.Truef(t, found, "unknown named zone: %s", zone)
					objID = descpb.ID(namedZoneID)
				} else if d.HasArg(database) {
					var dbName string
					d.ScanArgs(t, database, &dbName)
					if d.HasArg(table) {
						var tbName string
						d.ScanArgs(t, table, &tbName)
						tableDesc := catalogkv.TestingGetTableDescriptor(
							tc.Server(0).DB(),
							tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).Codec,
							dbName,
							tbName,
						)
						objID = tableDesc.GetID()
					} else {
						dbDesc := catalogkv.TestingGetDatabaseDescriptor(
							tc.Server(0).DB(),
							tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).Codec,
							dbName,
						)
						objID = dbDesc.GetID()
					}
				} else if d.HasArg(id) {
					var scanID int
					d.ScanArgs(t, id, &scanID)
					objID = descpb.ID(scanID)
				} else {
					t.Fatal("insufficient args provided to translate")
				}
				entries, _, err := sqlTranslator.Translate(ctx, descpb.IDs{objID})
				require.NoError(t, err)
				return datadrivenTranslationResult(entries)
			case "full-translate":
				entries, _, err := spanconfigsqltranslator.FullTranslate(ctx, sqlTranslator)
				require.NoError(t, err)
				return datadrivenTranslationResult(entries)
			case "sleep":
				var sleepDuration int
				d.ScanArgs(t, duration, &sleepDuration)
				time.Sleep(time.Second * time.Duration(sleepDuration))
			case "mark-table-offline":
				var dbName string
				d.ScanArgs(t, database, &dbName)
				var tbName string
				d.ScanArgs(t, table, &tbName)
				err := modifyTableDescriptor(ctx, tc, dbName, tbName, func(mutable *tabledesc.Mutable) {
					mutable.SetOffline("for testing")
				})
				require.NoError(t, err)
			case "mark-table-public":
				var dbName string
				d.ScanArgs(t, database, &dbName)
				var tbName string
				d.ScanArgs(t, table, &tbName)
				err := modifyTableDescriptor(ctx, tc, dbName, tbName, func(mutable *tabledesc.Mutable) {
					mutable.SetPublic()
				})
				require.NoError(t, err)
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			return ""
		})
	})
}

// Constants for data-driven args.
const (
	id        = "id"
	namedZone = "named-zone"
	table     = "table"
	database  = "database"
	duration  = "duration"
)

func modifyTableDescriptor(
	ctx context.Context,
	tc *testcluster.TestCluster,
	dbName string,
	tbName string,
	f func(*tabledesc.Mutable),
) error {
	cfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	return sql.DescsTxn(ctx, &cfg, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		_, tableDesc, err := descsCol.GetMutableTableByName(
			ctx,
			txn,
			tree.NewTableNameWithSchema(tree.Name(dbName), "public", tree.Name(tbName)),
			tree.ObjectLookupFlags{
				CommonLookupFlags: tree.CommonLookupFlags{
					Required:       true,
					IncludeOffline: true,
				},
			},
		)
		if err != nil {
			return err
		}
		f(tableDesc)
		return descsCol.WriteDesc(ctx, false, tableDesc, txn)
	})
}

// datadrivenTranslationResult constructs the datadriven output for a given
// slice of (translated) span config entries.
func datadrivenTranslationResult(entries []roachpb.SpanConfigEntry) string {
	var output strings.Builder
	for _, entry := range entries {
		res := diffEntryAgainstRangeDefault(entry)
		output.WriteString(res)
		output.WriteByte('\n')
	}
	return output.String()
}

// diffEntryAgainstRangeDefault computes the difference between the given config
// and RANGE DEFAULT. It then constructs a (span<->mismatching field(s)) string
// and returns it. If there config is same as RANGE DEFAULT, a (span, DEFAULT)
// string is returned instead.
func diffEntryAgainstRangeDefault(entry roachpb.SpanConfigEntry) string {
	defaultSpanConfig := roachpb.TestingDefaultSpanConfig()
	var diffs []string

	if entry.Config.RangeMaxBytes != defaultSpanConfig.RangeMaxBytes {
		diffs = append(diffs, fmt.Sprintf("range_max_bytes=%d", entry.Config.RangeMaxBytes))
	}
	if entry.Config.RangeMinBytes != defaultSpanConfig.RangeMinBytes {
		diffs = append(diffs, fmt.Sprintf("range_min_bytes=%d", entry.Config.RangeMinBytes))
	}
	if entry.Config.GCPolicy.TTLSeconds != defaultSpanConfig.GCPolicy.TTLSeconds {
		diffs = append(diffs, fmt.Sprintf("ttl_seconds=%d", entry.Config.GCPolicy.TTLSeconds))
	}
	if entry.Config.GlobalReads != defaultSpanConfig.GlobalReads {
		diffs = append(diffs, fmt.Sprintf("global_reads=%v", entry.Config.GlobalReads))
	}
	if entry.Config.NumReplicas != defaultSpanConfig.NumReplicas {
		diffs = append(diffs, fmt.Sprintf("num_replicas=%d", entry.Config.NumReplicas))
	}
	if entry.Config.NumVoters != defaultSpanConfig.NumVoters {
		diffs = append(diffs, fmt.Sprintf("num_voters=%d", entry.Config.NumVoters))
	}
	if !reflect.DeepEqual(entry.Config.Constraints, defaultSpanConfig.Constraints) {
		diffs = append(diffs, fmt.Sprintf("constraints=%v", entry.Config.Constraints))
	}
	if !reflect.DeepEqual(entry.Config.VoterConstraints, defaultSpanConfig.VoterConstraints) {
		diffs = append(diffs, fmt.Sprintf("voter_constraints=%v", entry.Config.VoterConstraints))
	}
	if !reflect.DeepEqual(entry.Config.LeasePreferences, defaultSpanConfig.LeasePreferences) {
		diffs = append(diffs, fmt.Sprintf("lease_preferences=%v", entry.Config.VoterConstraints))
	}

	if len(diffs) == 0 {
		diffs = []string{"DEFAULT"}
	}
	return fmt.Sprintf("%-30s %s", entry.Span.String(), strings.Join(diffs, " "))
}
