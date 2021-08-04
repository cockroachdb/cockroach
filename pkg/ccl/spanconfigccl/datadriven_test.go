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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqlwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestSpanConfigsDataDriven(t *testing.T) {
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
			case "generate-span-configs":
				mustHaveArgOrFatal(t, d, tableName)
				mustHaveArgOrFatal(t, d, databaseName)

				var tbName string
				var dbName string
				d.ScanArgs(t, tableName, &tbName)
				d.ScanArgs(t, databaseName, &dbName)
				tableDesc := catalogkv.TestingGetTableDescriptor(
					ds.tc.Server(0).DB(), keys.SystemSQLCodec, dbName, tbName,
				)
				var spanConfigEntries []roachpb.SpanConfigEntry
				err := ds.tc.Server(0).DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					var err error
					spanConfigEntries, err = ds.sqlWatcher.GenerateSpanConfigurationsForTable(ctx, txn, tableDesc.GetID())
					return err
				})
				require.NoError(t, err)

				// Build string output of the row data.
				var output strings.Builder
				for _, entry := range spanConfigEntries {
					span := keys.PrettyPrintRange(entry.Span.Key, entry.Span.EndKey, 1000)
					output.WriteString("Span: ")
					output.WriteString(span)
					output.WriteString("\n")

					yamlConfig, err := yaml.Marshal(entry.Config)
					require.NoError(t, err)

					output.WriteString(string(yamlConfig))
					output.WriteString("-----------------------")
					output.WriteString("\n")
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
	tableName    = "table-name"
	databaseName = "database-name"
)

func mustHaveArgOrFatal(t *testing.T, d *datadriven.TestData, arg string) {
	if !d.HasArg(arg) {
		t.Fatalf("no %q provided", arg)
	}
}

type dataDrivenTestState struct {
	tc         serverutils.TestClusterInterface
	sqlDB      *gosql.DB
	sqlWatcher *spanconfigsqlwatcher.SQLWatcher
}

func newDataDrivenTestState(tc serverutils.TestClusterInterface) *dataDrivenTestState {
	ts := tc.Server(0)
	sqlWatcher := spanconfigsqlwatcher.New(
		keys.SystemSQLCodec,
		ts.DB(),
		ts.ClusterSettings(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		ts.Clock(),
		ts.InternalExecutor().(*sql.InternalExecutor),
		ts.LeaseManager().(*lease.Manager),
		tc.Stopper(),
		&spanconfig.TestingKnobs{
			SQLWatcherDisableInitialScan: true,
		},
	)
	return &dataDrivenTestState{
		tc:         tc,
		sqlDB:      tc.ServerConn(0),
		sqlWatcher: sqlWatcher,
	}
}

func (d *dataDrivenTestState) cleanup(ctx context.Context) {
	if d.tc != nil {
		d.tc.Stopper().Stop(ctx)
	}
	d.tc = nil
}
