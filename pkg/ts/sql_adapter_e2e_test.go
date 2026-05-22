// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ts_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestSQLAdapter_EndToEnd round-trips datapoints through the full
// SQL → SQLAdapter → TSDB stack via crdb_internal.tsdb_query. The
// unit tests cover dispatch and clamping against a stub; this test
// exercises the real TSDB read path and the generator plumbing.
//
// The TSDB maintenance queue is disabled per-store: it periodically
// prunes raw samples and would race with the SELECT.
func TestSQLAdapter_EndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		// Pin to the system tenant; secondary-tenant plumbing is
		// already covered by the NewTenantSQLAdapter unit test.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	tsdb := s.TsDB().(*ts.DB)
	sqlDB := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))

	// Each subtest uses its own metric name to avoid cross-contamination.
	storeOne := func(t *testing.T, metricName, source string, sampleTime time.Time, value float64) {
		t.Helper()
		require.NoError(t, tsdb.StoreData(ctx, ts.Resolution10s, []tspb.TimeSeriesData{{
			Name:   metricName,
			Source: source,
			Datapoints: []tspb.TimeSeriesDatapoint{{
				TimestampNanos: sampleTime.UnixNano(),
				Value:          value,
			}},
		}}))
	}

	t.Run("sources filter with single source returns per-source row", func(t *testing.T) {
		const metricName = "test.tsdb.e2e.sources_single"
		const source = "1"
		const value = 42.0
		sampleTime := time.Now().Add(-30 * time.Second).Truncate(10 * time.Second)
		storeOne(t, metricName, source, sampleTime, value)

		rows := sqlDB.QueryStr(t, `
SELECT value, source
  FROM crdb_internal.tsdb_query(
    $1,
    $2::TIMESTAMPTZ - INTERVAL '5 minutes',
    now(),
    '{"sources":["1"]}'::jsonb
  )
`, metricName, sampleTime)
		require.Equal(t, [][]string{{"42", source}}, rows)
	})

	t.Run("sources filter without aggregator preserves per-source breakdown", func(t *testing.T) {
		const metricName = "test.tsdb.e2e.sources_per_source"
		sampleTime := time.Now().Add(-30 * time.Second).Truncate(10 * time.Second)
		for i, src := range []string{"1", "2", "3"} {
			storeOne(t, metricName, src, sampleTime, float64(100+i))
		}

		rows := sqlDB.QueryStr(t, `
SELECT source, value
  FROM crdb_internal.tsdb_query(
    $1,
    $2::TIMESTAMPTZ - INTERVAL '5 minutes',
    now(),
    '{"sources":["1","2","3"]}'::jsonb
  )
 ORDER BY source
`, metricName, sampleTime)
		require.Equal(t, [][]string{
			{"1", "100"},
			{"2", "101"},
			{"3", "102"},
		}, rows)
	})

	t.Run("no source filter collapses across sources via default aggregator", func(t *testing.T) {
		const metricName = "test.tsdb.e2e.no_sources_aggregate"
		sampleTime := time.Now().Add(-30 * time.Second).Truncate(10 * time.Second)
		storeOne(t, metricName, "1", sampleTime, 10.0)
		storeOne(t, metricName, "2", sampleTime, 20.0)

		// Default source_aggregator is SUM.
		rows := sqlDB.QueryStr(t, `
SELECT value, source
  FROM crdb_internal.tsdb_query(
    $1,
    $2::TIMESTAMPTZ - INTERVAL '5 minutes',
    now(),
    '{}'::jsonb
  )
`, metricName, sampleTime)
		require.Len(t, rows, 1)
		require.Equal(t, "30", rows[0][0])   // 10 + 20 = 30
		require.Equal(t, "NULL", rows[0][1]) // source column rendered as SQL NULL
	})

	t.Run("sources filter with source_aggregator collapses the named subset", func(t *testing.T) {
		const metricName = "test.tsdb.e2e.sources_with_aggregator"
		sampleTime := time.Now().Add(-30 * time.Second).Truncate(10 * time.Second)
		storeOne(t, metricName, "1", sampleTime, 5.0)
		storeOne(t, metricName, "2", sampleTime, 15.0)
		storeOne(t, metricName, "3", sampleTime, 25.0)

		// Query only sources 1 and 3 with MAX aggregator.
		rows := sqlDB.QueryStr(t, `
SELECT value, source
  FROM crdb_internal.tsdb_query(
    $1,
    $2::TIMESTAMPTZ - INTERVAL '5 minutes',
    now(),
    '{"sources":["1","3"],"source_aggregator":"MAX"}'::jsonb
  )
`, metricName, sampleTime)
		require.Len(t, rows, 1)
		require.Equal(t, "25", rows[0][0])   // max(5, 25) = 25
		require.Equal(t, "NULL", rows[0][1]) // source column rendered as SQL NULL
	})

	t.Run("time-range cap fires before TSDB I/O", func(t *testing.T) {
		// Cap is computed from the requested window before any TSDB
		// I/O, so no datapoint needs to exist. The hint must name the
		// setting so the operator can find the knob from the error.
		sqlDB.Exec(t, `SET CLUSTER SETTING sql.crdb_internal.tsdb_query.max_time_range = '1m'`)
		defer sqlDB.Exec(t, `RESET CLUSTER SETTING sql.crdb_internal.tsdb_query.max_time_range`)

		sqlDB.ExpectErrWithHint(t,
			`crdb_internal\.tsdb_query effective window of .* exceeds the configured maximum of 1m0s`,
			`sql\.crdb_internal\.tsdb_query\.max_time_range`,
			`SELECT * FROM crdb_internal.tsdb_query(
			   'cr.node.sql.query.count',
			   now() - INTERVAL '1 hour',
			   now())`,
		)
	})

	t.Run("downsampler aggregates within buckets", func(t *testing.T) {
		const metricName = "test.tsdb.e2e.downsampler"
		const source = "1"
		baseTime := time.Now().Add(-2 * time.Minute).Truncate(10 * time.Second)
		// Six 10s datapoints (10..60) inside the first 1m bucket.
		for i := 0; i < 6; i++ {
			storeOne(t, metricName, source, baseTime.Add(time.Duration(i)*10*time.Second), float64(10*(i+1)))
		}

		rows := sqlDB.QueryStr(t, `
SELECT value, source
  FROM crdb_internal.tsdb_query(
    $1,
    $2::TIMESTAMPTZ - INTERVAL '5 minutes',
    now(),
    '{"sources":["1"],"downsampler":"MAX","interval":"1m"}'::jsonb
  )
 ORDER BY value DESC
 LIMIT 1
`, metricName, baseTime)
		require.Len(t, rows, 1)
		require.Equal(t, "60", rows[0][0]) // max of [10,20,30,40,50,60]
		require.Equal(t, source, rows[0][1])
	})

	t.Run("derivative computes rate of change", func(t *testing.T) {
		const metricName = "test.tsdb.e2e.derivative"
		const source = "1"
		baseTime := time.Now().Add(-90 * time.Second).Truncate(10 * time.Second)
		// Monotonic counter incrementing by 10 every 10s.
		for i := 0; i < 5; i++ {
			storeOne(t, metricName, source, baseTime.Add(time.Duration(i)*10*time.Second), float64(i*10))
		}

		rows := sqlDB.QueryStr(t, `
SELECT value
  FROM crdb_internal.tsdb_query(
    $1,
    $2::TIMESTAMPTZ - INTERVAL '5 minutes',
    now(),
    '{"sources":["1"],"derivative":"NON_NEGATIVE_DERIVATIVE"}'::jsonb
  )
`, metricName, baseTime)
		// All derivatives ~1.0 (10 / 10s); the first sample has no
		// predecessor and is skipped.
		require.Greater(t, len(rows), 0)
		for _, row := range rows {
			require.Equal(t, "1", row[0])
		}
	})

	t.Run("row-count cap pre-check trips before I/O when per-source upper bound exceeds cap", func(t *testing.T) {
		// The per-source dispatch's upper bound is sources ×
		// samples-in-window; tighten the cap below that to trip the
		// pre-check before any I/O.
		const metricName = "test.tsdb.e2e.row_cap_per_source"
		const source = "1"
		sampleTime := time.Now().Add(-30 * time.Second).Truncate(10 * time.Second)
		require.NoError(t, tsdb.StoreData(ctx, ts.Resolution10s, []tspb.TimeSeriesData{{
			Name:   metricName,
			Source: source,
			Datapoints: []tspb.TimeSeriesDatapoint{{
				TimestampNanos: sampleTime.UnixNano(),
				Value:          1,
			}},
		}}))

		sqlDB.Exec(t, `SET CLUSTER SETTING sql.crdb_internal.tsdb_query.max_rows = 2`)
		defer sqlDB.Exec(t, `RESET CLUSTER SETTING sql.crdb_internal.tsdb_query.max_rows`)

		// 1 source × ceil(5min / 10s) = 30 samples upper bound, well
		// above the cap of 2.
		sqlDB.ExpectErrWithHint(t,
			`crdb_internal\.tsdb_query: upper-bound estimate of \d+ rows .* exceeds row cap of 2`,
			`sql\.crdb_internal\.tsdb_query\.max_rows`,
			`SELECT * FROM crdb_internal.tsdb_query(
			   $1,
			   now() - INTERVAL '5 minutes',
			   now(),
			   '{"sources":["1"]}'::jsonb)`,
			metricName,
		)
	})
}
