// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvaccessor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestDataDriven runs datadriven tests against the kvaccessor interface.
// The syntax is as follows:
//
// 		kvaccessor-get
// 		span [a,e)
// 		span [a,b)
// 		span [b,c)
//		system-target {cluster}
//		system-target {source=1,target=20}
//		system-target {source=1,target=1}
//		system-target {source=20,target=20}
// 		system-target {source=1, all-tenant-keyspace-targets-set}
//      ----
//
// 		kvaccessor-update
// 		delete [c,e)
// 		upsert [c,d):C
// 		upsert [d,e):D
// 		delete {source=1,target=1}
// 		upsert {source=1,target=1}:A
// 		upsert {cluster}:F
//      ----
//
// They tie into GetSpanConfigRecords and UpdateSpanConfigRecords
// respectively. For kvaccessor-get, each listed target is added to the set of
// targets being read. For kvaccessor-update, the lines prefixed with "delete"
// count towards the targets being deleted, and for "upsert" they correspond to
// the span config records being upserted. See
// spanconfigtestutils.Parse{Span,Config,SpanConfigRecord} for more details.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
		defer tc.Stopper().Stop(ctx)

		const dummySpanConfigurationsFQN = "defaultdb.public.dummy_span_configurations"
		tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
		tdb.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummySpanConfigurationsFQN))
		accessor := spanconfigkvaccessor.New(
			tc.Server(0).DB(),
			tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor),
			tc.Server(0).ClusterSettings(),
			dummySpanConfigurationsFQN,
		)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "kvaccessor-get":
				targets := spanconfigtestutils.ParseKVAccessorGetArguments(t, d.Input)
				records, err := accessor.GetSpanConfigRecords(ctx, targets)
				if err != nil {
					return fmt.Sprintf("err: %s", err.Error())
				}

				var output strings.Builder
				for _, record := range records {
					output.WriteString(fmt.Sprintf(
						"%s\n", spanconfigtestutils.PrintSpanConfigRecord(t, record),
					))
				}
				return output.String()
			case "kvaccessor-update":
				toDelete, toUpsert := spanconfigtestutils.ParseKVAccessorUpdateArguments(t, d.Input)
				if err := accessor.UpdateSpanConfigRecords(ctx, toDelete, toUpsert); err != nil {
					return fmt.Sprintf("err: %s", err.Error())
				}
				return "ok"
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			return ""
		})
	})
}

func BenchmarkKVAccessorUpdate(b *testing.B) {
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	for _, batchSize := range []int{100, 1000, 10000} {
		records := make([]spanconfig.Record, 0, batchSize)
		for i := 0; i < batchSize; i++ {
			log.Infof(ctx, "generating batch: %s", fmt.Sprintf("[%06d,%06d):X", i, i+1))
			record := spanconfigtestutils.ParseSpanConfigRecord(b, fmt.Sprintf("[%06d,%06d):X", i, i+1))
			records = append(records, record)
		}

		b.Run(fmt.Sprintf("batch-size=%d", batchSize), func(b *testing.B) {
			tc := testcluster.StartTestCluster(b, 1, base.TestClusterArgs{})
			defer tc.Stopper().Stop(ctx)

			const dummySpanConfigurationsFQN = "defaultdb.public.dummy_span_configurations"
			tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
			tdb.Exec(b, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummySpanConfigurationsFQN))

			accessor := spanconfigkvaccessor.New(
				tc.Server(0).DB(),
				tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor),
				tc.Server(0).ClusterSettings(),
				dummySpanConfigurationsFQN,
			)

			start := timeutil.Now()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				require.NoError(b, accessor.UpdateSpanConfigRecords(ctx, nil, records))
			}
			duration := timeutil.Since(start)

			b.ReportMetric(0, "ns/op")
			b.ReportMetric(float64(len(records))*float64(b.N)/float64(duration.Milliseconds()), "records/ms")
			b.ReportMetric(float64(duration.Milliseconds())/float64(b.N), "ms/batch")
		})
	}
}
