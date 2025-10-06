// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigkvaccessor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestDataDriven runs datadriven tests against the kvaccessor interface.
// The syntax is as follows:
//
//			kvaccessor-get
//			span [a,e)
//			span [a,b)
//			span [b,c)
//			system-target {cluster}
//			system-target {source=1,target=20}
//			system-target {source=1,target=1}
//			system-target {source=20,target=20}
//			system-target {source=1, all-tenant-keyspace-targets-set}
//	     ----
//
//			kvaccessor-update
//			delete [c,e)
//			upsert [c,d):C
//			upsert [d,e):D
//			delete {source=1,target=1}
//			upsert {source=1,target=1}:A
//			upsert {cluster}:F
//	     ----
//
//			kvaccessor-get-all-system-span-configs-that-apply tenant-id=<tenantID>
//	     ----
//
// They tie into GetSpanConfigRecords and UpdateSpanConfigRecords
// respectively. For kvaccessor-get, each listed target is added to the set of
// targets being read. For kvaccessor-update, the lines prefixed with "delete"
// count towards the targets being deleted, and for "upsert" they correspond to
// the span config records being upserted. See
// spanconfigtestutils.Parse{Span,Config,SpanConfigRecord} for more details.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		defer log.Scope(t).Close(t)

		ctx := context.Background()
		srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{
			// Requires span_configuration table which is not visible
			// from secondary tenants.
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		})
		defer srv.Stopper().Stop(ctx)
		ts := srv.ApplicationLayer()

		const dummySpanConfigurationsFQN = "defaultdb.public.dummy_span_configurations"
		tdb := sqlutils.MakeSQLRunner(db)
		tdb.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummySpanConfigurationsFQN))
		accessor := spanconfigkvaccessor.New(
			kvDB,
			ts.InternalExecutor().(isql.Executor),
			ts.ClusterSettings(),
			ts.Clock(),
			dummySpanConfigurationsFQN,
			nil, /* knobs */
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
				if err := accessor.UpdateSpanConfigRecords(
					ctx, toDelete, toUpsert, hlc.MinTimestamp, hlc.MaxTimestamp,
				); err != nil {
					return fmt.Sprintf("err: %s", err.Error())
				}
				return "ok"
			case "kvaccessor-get-all-system-span-configs-that-apply":
				var tenID uint64
				d.ScanArgs(t, "tenant-id", &tenID)
				spanConfigs, err := accessor.GetAllSystemSpanConfigsThatApply(ctx, roachpb.MustMakeTenantID(tenID))
				if err != nil {
					return fmt.Sprintf("err: %s", err.Error())
				}
				var output strings.Builder
				for _, config := range spanConfigs {
					output.WriteString(fmt.Sprintf(
						"%s\n", spanconfigtestutils.PrintSpanConfig(config),
					))
				}
				return output.String()

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
			srv, db, kvDB := serverutils.StartServer(b, base.TestServerArgs{
				// Requires span_configuration table which is not visible
				// from secondary tenants.
				DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			})
			defer srv.Stopper().Stop(ctx)
			ts := srv.ApplicationLayer()

			const dummySpanConfigurationsFQN = "defaultdb.public.dummy_span_configurations"
			tdb := sqlutils.MakeSQLRunner(db)
			tdb.Exec(b, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummySpanConfigurationsFQN))

			accessor := spanconfigkvaccessor.New(
				kvDB,
				ts.InternalExecutor().(isql.Executor),
				ts.ClusterSettings(),
				ts.Clock(),
				dummySpanConfigurationsFQN,
				nil, /* knobs */
			)

			start := timeutil.Now()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				require.NoError(b, accessor.UpdateSpanConfigRecords(
					ctx, nil, records, hlc.MinTimestamp, hlc.MaxTimestamp,
				))
			}
			duration := timeutil.Since(start)

			b.ReportMetric(0, "ns/op")
			b.ReportMetric(float64(len(records))*float64(b.N)/float64(duration.Milliseconds()), "records/ms")
			b.ReportMetric(float64(duration.Milliseconds())/float64(b.N), "ms/batch")
		})
	}
}

func TestKVAccessorPagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	ts, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		// Requires span_configuration table which is not visible
		// from secondary tenants.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer ts.Stopper().Stop(ctx)

	const dummySpanConfigurationsFQN = "defaultdb.public.dummy_span_configurations"
	tdb := sqlutils.MakeSQLRunner(db)
	tdb.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummySpanConfigurationsFQN))

	var batches, batchSize int
	accessor := spanconfigkvaccessor.New(
		kvDB,
		ts.InternalExecutor().(isql.Executor),
		ts.ClusterSettings(),
		ts.Clock(),
		dummySpanConfigurationsFQN,
		&spanconfig.TestingKnobs{
			KVAccessorPaginationInterceptor: func() {
				batches++
			},
			KVAccessorBatchSizeOverrideFn: func() int {
				return batchSize
			},
		},
	)
	const upsert10Confs = `
upsert [a,b):X
upsert [b,c):X
upsert [c,d):X
upsert [d,e):X
upsert [e,f):X
upsert [f,g):X
upsert [g,h):X
upsert [h,i):X
upsert [i,j):X
upsert [j,k):X
`

	const delete10Confs = `
delete [a,b)
delete [b,c)
delete [c,d)
delete [d,e)
delete [e,f)
delete [f,g)
delete [g,h)
delete [h,i)
delete [i,j)
delete [j,k)
`
	const get10Confs = `
span [a,b)
span [b,c)
span [c,d)
span [d,e)
span [e,f)
span [f,g)
span [g,h)
span [h,i)
span [i,j)
span [j,k)
`
	{ // Seed the accessor with 10 entries.
		batches, batchSize = 0, 100
		toDelete, toUpsert := spanconfigtestutils.ParseKVAccessorUpdateArguments(t, upsert10Confs)
		require.NoError(t, accessor.UpdateSpanConfigRecords(
			ctx, toDelete, toUpsert, hlc.MinTimestamp, hlc.MaxTimestamp,
		))
		require.Equal(t, 1, batches)
	}

	{ // Lower the batch size, we should observe more batches.
		batches, batchSize = 0, 2
		toDelete, toUpsert := spanconfigtestutils.ParseKVAccessorUpdateArguments(t, upsert10Confs)
		require.NoError(t, accessor.UpdateSpanConfigRecords(
			ctx, toDelete, toUpsert, hlc.MinTimestamp, hlc.MaxTimestamp,
		))
		require.Equal(t, 5, batches)
	}

	{ // Try another multiple, and with deletions.
		batches, batchSize = 0, 5
		toDelete, toUpsert := spanconfigtestutils.ParseKVAccessorUpdateArguments(t, delete10Confs)
		require.NoError(t, accessor.UpdateSpanConfigRecords(
			ctx, toDelete, toUpsert, hlc.MinTimestamp, hlc.MaxTimestamp,
		))
		require.Equal(t, 2, batches)
	}

	{ // Try a multiple that doesn't factor exactly (re-inserting original entries).
		batches, batchSize = 0, 3
		toDelete, toUpsert := spanconfigtestutils.ParseKVAccessorUpdateArguments(t, upsert10Confs)
		require.NoError(t, accessor.UpdateSpanConfigRecords(
			ctx, toDelete, toUpsert, hlc.MinTimestamp, hlc.MaxTimestamp,
		))
		require.Equal(t, 4, batches)
	}

	{ // Try another multiple using both upserts and deletes.
		batches, batchSize = 0, 4
		toDelete, toUpsert := spanconfigtestutils.ParseKVAccessorUpdateArguments(t, delete10Confs+upsert10Confs)
		require.NoError(t, accessor.UpdateSpanConfigRecords(
			ctx, toDelete, toUpsert, hlc.MinTimestamp, hlc.MaxTimestamp,
		))
		require.Equal(t, 6, batches) // 3 batches for the updates, 3 more for the deletions
	}

	{ // Try another multiple but for gets.
		batches, batchSize = 0, 6
		targets := spanconfigtestutils.ParseKVAccessorGetArguments(t, get10Confs)
		_, err := accessor.GetSpanConfigRecords(ctx, targets)
		require.NoError(t, err)
		require.Equal(t, 2, batches)
	}
}

// TestKVAccessorCommitMinTSWaitRespondsToCtxCancellation ensures that
// KVAccessor updates which are waiting for their local clocks to be in advance
// of the minimum commit timestamp respond to context cancellations.
func TestKVAccessorCommitMinTSWaitRespondsToCtxCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		// Requires span_configuration table which is not visible
		// from secondary tenants.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	const dummySpanConfigurationsFQN = "defaultdb.public.dummy_span_configurations"

	ctx, cancel := context.WithCancel(context.Background())
	accessor := spanconfigkvaccessor.New(
		kvDB,
		ts.InternalExecutor().(isql.Executor),
		ts.ClusterSettings(),
		ts.Clock(),
		dummySpanConfigurationsFQN,
		&spanconfig.TestingKnobs{
			KVAccessorPreCommitMinTSWaitInterceptor: func() {
				cancel()
			},
		},
	)

	commitMinTS := ts.Clock().Now().Add(time.Second.Nanoseconds(), 0)
	err := accessor.UpdateSpanConfigRecords(
		ctx, nil /* toDelete */, nil /* toUpsert */, commitMinTS, hlc.MaxTimestamp,
	)
	require.Error(t, err)
	require.True(t, testutils.IsError(err, "waiting for clock to be in advance of minimum commit timestamp"))
}
