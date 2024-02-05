// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"bufio"
	"bytes"
	"context"
	gosql "database/sql"
	"math/rand"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
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

	// This test becomes too slow when it runs under stress and/or race. The
	// aggregated metric value in SQL will always be lower than the actual
	// metric in KV since by the time we query metrics from KV, livebytes would
	// have increased, leading to this loop of waiting for the exact match.
	skip.UnderStress(t, "test is too slow")
	skip.UnderRace(t, "test is too slow")

	ctx := context.Background()

	jobID := jobs.AutoTenantGlobalMetricsExporterJobID
	settings := cluster.MakeTestingClusterSettings()
	status.ChildMetricsEnabled.Override(ctx, &settings.SV, true)
	sql.TenantGlobalMetricsExporterInterval.Override(ctx, &settings.SV, 50*time.Millisecond)
	testingKnobs := base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	// Start a cluster with 5 nodes.
	tc := testcluster.NewTestCluster(t, 5, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Settings:          settings,
			Knobs:             testingKnobs,
		},
	})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	mkTenant := func(
		t *testing.T, name string,
	) (app serverutils.ApplicationLayerInterface, tenantDB *gosql.DB) {
		idx := rand.Intn(tc.NumServers())
		app, tenantDB, err := tc.Server(idx).TenantController().StartSharedProcessTenant(
			ctx,
			base.TestSharedProcessTenantArgs{
				TenantName: roachpb.TenantName(name),
				Settings:   settings,
				Knobs:      testingKnobs,
			},
		)
		require.NoError(t, err)
		return app, tenantDB
	}

	const (
		fooTenantName = "tenant-foo"
		barTenantName = "tenant-bar"
	)
	fooApp, fooConn := mkTenant(t, fooTenantName)
	barApp, barConn := mkTenant(t, barTenantName)

	// Create singleton jobs.
	sysDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sysDB.Exec(t, "SELECT crdb_internal.create_tenant_global_metrics_exporter_job()")
	fooDB := sqlutils.MakeSQLRunner(fooConn)
	fooDB.Exec(t, "SELECT crdb_internal.create_tenant_global_metrics_exporter_job()")
	barDB := sqlutils.MakeSQLRunner(barConn)
	barDB.Exec(t, "SELECT crdb_internal.create_tenant_global_metrics_exporter_job()")

	// scrapeMetric scrapes from the given metric registry, and returns the
	// value of it.
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
		re := regexp.MustCompile(`^(\w+)\{.*,(?:tenant|tenant_id)="` + tenantLabel + `"\} (.+)$`)
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

	// scrapeLivebytes scrapes from all KV nodes, and returns the sum of livebytes
	// for a given tenant.
	scrapeLivebytes := func(t *testing.T, tenantLabel string) int64 {
		var val int64
		for i := 0; i < tc.NumServers(); i++ {
			s := tc.StorageLayer(i)
			// We only have one store per node.
			store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
			require.NoError(t, err)
			v, _ := scrapeMetric(t, store.Registry(), "livebytes", tenantLabel)
			val += v
		}
		if val <= 0 {
			t.Fatalf("livebytes must be greater than 0, got %d", val)
		}
		return val
	}

	checkTenant := func(
		t *testing.T,
		app serverutils.ApplicationLayerInterface,
		tenantName string,
	) error {
		_, tenantID, err := keys.DecodeTenantPrefix(app.Codec().TenantPrefix())
		if err != nil {
			return err
		}
		require.NoError(t, err)
		tenantIDStr := tenantID.String()
		if tenantID == roachpb.SystemTenantID {
			tenantIDStr = "system"
		}
		exp := scrapeLivebytes(t, tenantIDStr)

		r := app.JobRegistry().(*jobs.Registry).MetricsRegistry()
		val, _ := scrapeMetric(t, r, "sql_aggregated_livebytes", tenantName)
		if exp != val {
			return errors.Newf("expected %d, but got %d", exp, val)
		}
		return nil
	}

	// Test for correctness: sql_aggregated_livebytes in SQL = sum(livebytes in KV).
	t.Run("secondary tenants", func(t *testing.T) {
		testutils.SucceedsSoon(t, func() error {
			return checkTenant(t, fooApp, fooTenantName)
		})
		testutils.SucceedsSoon(t, func() error {
			return checkTenant(t, barApp, barTenantName)
		})
	})

	// Test for correctness: sql_aggregated_livebytes in SQL = sum(livebytes in KV).
	t.Run("system tenant", func(t *testing.T) {
		// Look for the node that runs the job.
		jobutils.WaitForJobToRun(t, sysDB, jobID)
		var instanceID int64
		sysDB.QueryRow(t, "SELECT claim_instance_id FROM system.jobs WHERE id = $1", &jobID).Scan(&instanceID)
		require.Greater(t, instanceID, int64(0))

		// The system tenant livebytes metric fluctuates a lot even without
		// stress. Use a larger wait time here to account for them.
		testutils.SucceedsWithin(t, func() error {
			return checkTenant(t, tc.ApplicationLayer(int(instanceID-1)), "system")
		}, 3*time.Minute)
	})

	t.Run("metric not exported when job is paused", func(t *testing.T) {
		r := fooApp.JobRegistry().(*jobs.Registry).MetricsRegistry()

		// Ensure that metric is exported.
		testutils.SucceedsSoon(t, func() error {
			val, found := scrapeMetric(t, r, "sql_aggregated_livebytes", fooTenantName)
			if !found {
				return errors.New("aggregated metric is not exported")
			}
			require.Greater(t, val, int64(0))
			return nil
		})

		// Pause the job.
		fooDB.Exec(t, "PAUSE JOB $1", &jobID)
		jobutils.WaitForJobToPause(t, fooDB, jobID)

		// Metric will be unexported eventually when job's context is cancelled.
		testutils.SucceedsSoon(t, func() error {
			_, found := scrapeMetric(t, r, "sql_aggregated_livebytes", fooTenantName)
			if found {
				return errors.New("aggregated metric is still being exported")
			}
			return nil
		})

		// Resume the job.
		fooDB.Exec(t, "RESUME JOB $1", &jobID)
		jobutils.WaitForJobToRun(t, fooDB, jobID)

		// Metric will be exported again, and should be > 0 the moment it is
		// exported.
		testutils.SucceedsSoon(t, func() error {
			val, found := scrapeMetric(t, r, "sql_aggregated_livebytes", fooTenantName)
			if !found {
				return errors.New("aggregated metric is not exported")
			}
			require.Greater(t, val, int64(0))
			return nil
		})
	})
}
