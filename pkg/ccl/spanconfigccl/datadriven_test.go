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
	gosql "database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqltranslator"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
// "exec-sql": executes the input SQL query.
//
// "query-sql": executes the input SQL query and prints the results.
//
// "translate id=<descriptor_id>": translates the SQL zone config state to the
// span config state by starting at the given ID as the root. Also see
// spanconfig.SQLTranslator.Translate().
//
// "full-translate": performs a full translation of the SQL zone config state
// to the implied span config state. Also see
// spanconfig.SQLTranslator.FullTranslate().
func TestSQLTranslatorDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	datadriven.Walk(t, "testdata/", func(t *testing.T, path string) {

		tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					SpanConfig: &spanconfig.TestingKnobs{
						ManagerDisableJobCreation: true,
					},
					JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				},
			},
		})
		ds := newDataDrivenTestState(tc)
		defer ds.cleanup(ctx)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "exec-sql":
				_, err := ds.sqlDB.Exec(d.Input)
				if err != nil {
					return err.Error()
				}
			case "query-sql":
				rows, err := ds.sqlDB.Query(d.Input)
				if err != nil {
					return err.Error()
				}
				cols, err := rows.Columns()
				if err != nil {
					t.Fatal(err)
				}
				// Allocate a buffer of *interface{} to write results into.
				elemsI := make([]interface{}, len(cols))
				for i := range elemsI {
					elemsI[i] = new(interface{})
				}
				elems := make([]string, len(cols))

				// Build string output of the row data.
				var output strings.Builder
				for rows.Next() {
					if err := rows.Scan(elemsI...); err != nil {
						t.Fatal(err)
					}
					for i, elem := range elemsI {
						val := *(elem.(*interface{}))
						elems[i] = fmt.Sprintf("%v", val)
					}
					output.WriteString(strings.Join(elems, " "))
					output.WriteString("\n")
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				return output.String()
			case "translate":
				mustHaveArgOrFatal(t, d, id)

				var objID int
				d.ScanArgs(t, id, &objID)
				entries, err := ds.sqlTranslator.Translate(ctx, descpb.IDs{descpb.ID(objID)})
				if err != nil {
					return err.Error()
				}
				require.NoError(t, err)
				var output strings.Builder
				for _, entry := range entries {
					diffAgainstRangeDefault(entry, &output)
				}
				return output.String()
			case "full-translate":
				entries, _, err := ds.sqlTranslator.FullTranslate(ctx)
				require.NoError(t, err)
				var output strings.Builder
				for _, entry := range entries {
					diffAgainstRangeDefault(entry, &output)
				}
				return output.String()
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			return ""
		})
	})
}

const (
	id = "id"
)

// diffAgainstRangeDefault diffs the given span config entry against RANGE
// DEFAULT and adds non-matching fields to the output buffer. If there are no
// diffs "DEFAULT" is printed instead.
func diffAgainstRangeDefault(entry roachpb.SpanConfigEntry, output *strings.Builder) {
	defaultSpanConfig := zonepb.DefaultZoneConfig().AsSpanConfig()
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

	if len(diffs) != 0 {
		for i, diff := range diffs {
			span := ""
			if i == 0 {
				span = entry.Span.String()
			}
			output.WriteString(fmt.Sprintf("%-30s %s\n", span, diff))
		}

	} else {
		output.WriteString(fmt.Sprintf("%-30s %s\n",
			entry.Span.String(), "DEFAULT"))
	}
	output.WriteString("-----------------------\n")
}

func mustHaveArgOrFatal(t *testing.T, d *datadriven.TestData, arg string) {
	if !d.HasArg(arg) {
		t.Fatalf("no %q provided", arg)
	}
}

type dataDrivenTestState struct {
	tc            serverutils.TestClusterInterface
	sqlDB         *gosql.DB
	sqlTranslator *spanconfigsqltranslator.SQLTranslator
}

func newDataDrivenTestState(tc serverutils.TestClusterInterface) *dataDrivenTestState {
	ts := tc.Server(0)
	execCfg := ts.ExecutorConfig().(sql.ExecutorConfig)
	sqlTranslator := spanconfigsqltranslator.New(&execCfg, keys.SystemSQLCodec)
	return &dataDrivenTestState{
		tc:            tc,
		sqlDB:         tc.ServerConn(0),
		sqlTranslator: sqlTranslator,
	}
}

func (d *dataDrivenTestState) cleanup(ctx context.Context) {
	if d.tc != nil {
		d.tc.Stopper().Stop(ctx)
	}
	d.tc = nil
}
