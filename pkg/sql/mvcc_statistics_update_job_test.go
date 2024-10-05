// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

func TestTenantGlobalAggregatedLivebytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "too slow")

	ctx := context.Background()
	jobID := jobs.MVCCStatisticsJobID
	testingKnobs := base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	settings := cluster.MakeTestingClusterSettings()
	// Ensure that KV metrics are broken down by the tenant_id label.
	status.ChildMetricsEnabled.Override(ctx, &settings.SV, true)
	// Disable time series poller to avoid a data race between a read from the
	// test instrumentation and a write from the poller. We can avoid this if
	// we scraped from the recorder in the tests instead of using the metric
	// registry directly, but the application layer doesn't currently expose a
	// way to obtain the recorder instance.
	ts.TimeseriesStorageEnabled.Override(ctx, &settings.SV, false)
	// Speed up the interval in which livebytes metrics are updated.
	sql.TenantGlobalMetricsExporterInterval.Override(ctx, &settings.SV, 50*time.Millisecond)

	// Start a cluster with 5 nodes, and 2 stores each.
	tc := testcluster.NewTestCluster(t, 5, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Settings:          settings,
			Knobs:             testingKnobs,
			StoreSpecs: []base.StoreSpec{
				base.DefaultTestStoreSpec,
				base.DefaultTestStoreSpec,
			},
		},
	})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	type testTenant struct {
		id   int
		name string
		app  serverutils.ApplicationLayerInterface
		db   *sqlutils.SQLRunner
	}

	// makeTenant starts up a tenant process, and returns its details.
	makeTenant := func(t *testing.T, tenantID int, external bool) *testTenant {
		idx := rand.Intn(tc.NumServers())
		tenantName := fmt.Sprintf("tenant-%d", tenantID)
		if external {
			app, err := tc.Server(idx).TenantController().StartTenant(
				ctx,
				base.TestTenantArgs{
					TenantID:     roachpb.MustMakeTenantID(uint64(tenantID)),
					TenantName:   roachpb.TenantName(tenantName),
					Settings:     settings,
					TestingKnobs: testingKnobs,
				},
			)
			require.NoError(t, err)
			return &testTenant{tenantID, tenantName, app, sqlutils.MakeSQLRunner(app.SQLConn(t))}
		}
		app, tenantDB, err := tc.Server(idx).TenantController().StartSharedProcessTenant(
			ctx,
			base.TestSharedProcessTenantArgs{
				TenantID:   roachpb.MustMakeTenantID(uint64(tenantID)),
				TenantName: roachpb.TenantName(tenantName),
				Settings:   settings,
				Knobs:      testingKnobs,
			},
		)
		require.NoError(t, err)
		return &testTenant{tenantID, tenantName, app, sqlutils.MakeSQLRunner(tenantDB)}
	}

	// scrapeMetric scrapes from the given metric registry, and returns the
	// value of the supplied metric.
	scrapeMetric := func(
		t *testing.T, r *metric.Registry, metricName, tenantLabel string,
	) (val int64, found bool) {
		ex := metric.MakePrometheusExporter()
		var in bytes.Buffer
		err := ex.ScrapeAndPrintAsText(
			&in,
			expfmt.FmtText,
			func(ex *metric.PrometheusExporter) {
				ex.ScrapeRegistry(r, true /* includeChildMetrics */)
			},
		)
		require.NoError(t, err)
		sc := bufio.NewScanner(&in)
		re := regexp.MustCompile(`^(\w+)\{.*(?:tenant|tenant_id)="` + tenantLabel + `".*\} (.+)$`)
		for sc.Scan() {
			matches := re.FindAllStringSubmatch(sc.Text(), 1)
			if matches == nil {
				continue
			}
			metric, valStr := matches[0][1], matches[0][2]
			if metric != metricName {
				continue
			}
			val, err := strconv.ParseFloat(valStr, 64)
			require.NoError(t, err)
			return int64(val), true
		}
		return 0, false
	}

	// scrapeLivebytes scrapes from all KV nodes, and returns the sum of
	// livebytes for a given tenant.
	scrapeLivebytes := func(t *testing.T, tenantID int) int64 {
		var val int64
		for i := 0; i < tc.NumServers(); i++ {
			sl := tc.StorageLayer(i)
			require.NoError(t, sl.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				v, _ := scrapeMetric(t, s.Registry(), "livebytes", fmt.Sprintf("%d", tenantID))
				val += v
				return nil
			}))
		}
		return val
	}

	// validateAggregatedLivebytes returns an error if the aggregated livebytes
	// metric in the app layer does not match the sum of livebytes metrics in
	// the storage layer, or is 0. Otherwise, this returns nil.
	validateAggregatedLivebytes := func(t *testing.T, tenant *testTenant, confidenceLevel float64) error {
		// livebytes from KV are tagged with `tenant_id`, whereas aggregated
		// livebytes from SQL are tagged with `tenant` (i.e. name).
		exp := scrapeLivebytes(t, tenant.id)
		r := tenant.app.SQLServerInternal().(*server.SQLServer).MetricsRegistry()
		val, _ := scrapeMetric(t, r, "sql_aggregated_livebytes", tenant.name)

		if math.Abs(float64(exp-val))/float64(exp) > confidenceLevel {
			return errors.Newf("expected within +/-%.2f of %d, but got %d", confidenceLevel, exp, val)
		}
		if val <= 0 {
			return errors.New("livebytes must be greater than 0")
		}
		return nil
	}

	validateNoAggregatedLivebytes := func(t *testing.T, r *metric.Registry, tenantName string) error {
		_, found := scrapeMetric(t, r, "sql_aggregated_livebytes", tenantName)
		if found {
			return errors.New("sql_aggregated_livebytes should not be present")
		}
		return nil
	}

	tenantFoo := makeTenant(t, 10, true /* external */)
	tenantBar := makeTenant(t, 11, true /* external */)

	// Metrics should be exported for out-of-process secondary tenants, and are
	// correct, i.e. sql_aggregated_livebytes in SQL = sum(livebytes in KV).
	t.Run("external secondary tenants", func(t *testing.T) {
		// Flaky test.
		skip.WithIssue(t, 120775)

		// Exact match for non stress tests, and allow values to differ by up to
		// 5% in stress situations.
		confidenceLevel := 0.0
		if skip.Stress() {
			confidenceLevel = 0.05
		}
		testutils.SucceedsSoon(t, func() error {
			return validateAggregatedLivebytes(t, tenantFoo, confidenceLevel)
		})
		testutils.SucceedsSoon(t, func() error {
			return validateAggregatedLivebytes(t, tenantBar, confidenceLevel)
		})
	})

	t.Run("internal secondary tenants", func(t *testing.T) {
		// The test seems to hang when trying to start an in-process tenant.
		// Skip the for now.
		skip.WithIssue(t, 120775)

		tenantInternal := makeTenant(t, 20, false /* external */)

		jobutils.WaitForJobToRun(t, tenantInternal.db, jobID)

		// We should never get the aggregated metric in the application layer.
		r := tenantInternal.app.SQLServerInternal().(*server.SQLServer).MetricsRegistry()
		require.NoError(t, validateNoAggregatedLivebytes(t, r, tenantInternal.name))
	})

	t.Run("system tenant", func(t *testing.T) {
		sysDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
		jobutils.WaitForJobToRun(t, sysDB, jobID)

		// Look for the node that runs the job.
		var instanceID int64
		sysDB.QueryRow(t, "SELECT claim_instance_id FROM system.jobs WHERE id = $1", &jobID).Scan(&instanceID)
		require.Greater(t, instanceID, int64(0))

		// We should never get the aggregated metric in the application layer.
		r := tc.ApplicationLayer(int(instanceID - 1)).SQLServerInternal().(*server.SQLServer).MetricsRegistry()
		require.NoError(t, validateNoAggregatedLivebytes(t, r, "system"))
	})

	t.Run("metric not exported when job is paused", func(t *testing.T) {
		r := tenantFoo.app.SQLServerInternal().(*server.SQLServer).MetricsRegistry()

		// Ensure that metric is exported.
		testutils.SucceedsSoon(t, func() error {
			val, found := scrapeMetric(t, r, "sql_aggregated_livebytes", tenantFoo.name)
			if !found {
				return errors.New("aggregated metric is not exported")
			}
			require.Greater(t, val, int64(0))
			return nil
		})

		// Pause the job.
		tenantFoo.db.Exec(t, "PAUSE JOB $1", &jobID)
		jobutils.WaitForJobToPause(t, tenantFoo.db, jobID)

		// Metric will be unexported eventually when job's context is cancelled.
		testutils.SucceedsSoon(t, func() error {
			_, found := scrapeMetric(t, r, "sql_aggregated_livebytes", tenantFoo.name)
			if found {
				return errors.New("aggregated metric is still being exported")
			}
			return nil
		})

		// Resume the job.
		tenantFoo.db.Exec(t, "RESUME JOB $1", &jobID)
		jobutils.WaitForJobToRun(t, tenantFoo.db, jobID)

		// Metric will be exported again, and should be > 0 the moment it is
		// exported.
		testutils.SucceedsSoon(t, func() error {
			val, found := scrapeMetric(t, r, "sql_aggregated_livebytes", tenantFoo.name)
			if !found {
				return errors.New("aggregated metric is not exported")
			}
			require.Greater(t, val, int64(0))
			return nil
		})
	})
}
