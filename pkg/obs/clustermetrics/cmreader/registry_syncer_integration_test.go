// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmreader_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics"
	clustermetricutils "github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/utils"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

// TestRegistrySyncerIntegration starts a real test server, wires the registrySyncer into the
// server's cluster metric registry via cmreader.Start, inserts rows into
// system.cluster_metrics, and verifies that:
//   - metrics appear in the registry and respond to inserts, upserts, and deletes
//   - multiple labeled rows for the same GaugeVec are tracked correctly
//   - metrics are visible through the /_status/vars prometheus endpoint
//   - scalar metrics appear in TSDB time series data while labeled (vec) metrics do not
func TestRegistrySyncerIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStress(t, "test is too slow to run under stress")

	// Register test metric metadata so ToMetric() can resolve them.
	defer clustermetrics.TestingRegisterLabeledClusterMetric(
		"test.gauge_labeled", metric.Metadata{
			Name: "test.gauge_labeled",
			Help: "A test gauge",
		},
		[]string{"store"},
	)()
	defer clustermetrics.TestingRegisterClusterMetric("test.counter", metric.Metadata{
		Name: "test.counter",
		Help: "A test counter",
	})()
	defer clustermetrics.TestingRegisterClusterMetric("test.scalar", metric.Metadata{
		Name: "test.scalar",
		Help: "A scalar gauge for value verification",
	})()

	ctx := context.Background()
	startedChan := make(chan struct{})
	defer close(startedChan)
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			ClusterMetricsKnobs: &clustermetricutils.TestingKnobs{
				OnRegistrySyncerStart: func() {
					startedChan <- struct{}{}
				},
			},
		},
	})

	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()
	r := sqlutils.MakeSQLRunner(db)

	// Insert rows before starting the registrySyncer so the initial scan picks
	// them up via OnRefresh.
	r.Exec(t, `INSERT INTO system.cluster_metrics
		(id, name, labels, type, value, node_id)
		VALUES (100, 'test.gauge_labeled', '{"store": "1"}', 'gauge', 42, 1)`)
	r.Exec(t, `INSERT INTO system.cluster_metrics
		(id, name, labels, type, value, node_id)
		VALUES (200, 'test.counter', '{}', 'counter', 10, 1)`)
	r.Exec(t, `INSERT INTO system.cluster_metrics
		(id, name, labels, type, value, node_id)
		VALUES (300, 'test.scalar', '{}', 'gauge', 50, 1)`)

	<-startedChan
	execCfg := ts.ExecutorConfig().(sql.ExecutorConfig)

	// Get a handle to the cluster metric registry for direct inspection.
	reg := srv.MetricsRecorder().ClusterMetricRegistry(execCfg.Codec.TenantID)

	// After start returns, the initial scan has completed and OnRefresh has
	// fired. Verify all metrics were registered.
	requireMetricExists(t, reg, "test.gauge_labeled")
	requireMetricExists(t, reg, "test.counter")
	requireMetricExists(t, reg, "test.scalar")

	// Verify initial values for scalar metrics.
	requireCounterValue(t, reg, "test.counter", 10)
	requireScalarGaugeValue(t, reg, "test.scalar", 50)

	// ---------------------------------------------------------------
	// Upsert the scalar gauge and verify the updated value.
	// ---------------------------------------------------------------
	r.Exec(t, `UPSERT INTO system.cluster_metrics
		(id, name, labels, type, value, node_id)
		VALUES (300, 'test.scalar', '{}', 'gauge', 123, 1)`)

	testutils.SucceedsSoon(t, func() error {
		return checkScalarGaugeValue(reg, "test.scalar", 123)
	})

	// ---------------------------------------------------------------
	// Insert a second label set for the same labeled metric, verifying
	// multiple labeled rows are tracked under one vec metric.
	// ---------------------------------------------------------------
	r.Exec(t, `INSERT INTO system.cluster_metrics
		(id, name, labels, type, value, node_id)
		VALUES (101, 'test.gauge_labeled', '{"store": "2"}', 'gauge', 77, 1)`)

	testutils.SucceedsSoon(t, func() error {
		return checkGaugeVecValue(reg, "test.gauge_labeled", map[string]string{"store": "2"}, 77)
	})

	// The original label set should still be present.
	requireGaugeVecValue(t, reg, "test.gauge_labeled", map[string]string{"store": "1"}, 42)

	// Upsert the first label set with a new value.
	r.Exec(t, `UPSERT INTO system.cluster_metrics
		(id, name, labels, type, value, node_id)
		VALUES (100, 'test.gauge_labeled', '{"store": "1"}', 'gauge', 99, 1)`)

	testutils.SucceedsSoon(t, func() error {
		return checkGaugeVecValue(reg, "test.gauge_labeled", map[string]string{"store": "1"}, 99)
	})

	// ---------------------------------------------------------------
	// Insert a new metric after the initial scan (via OnUpsert).
	// ---------------------------------------------------------------
	defer clustermetrics.TestingRegisterClusterMetric("test.newgauge", metric.Metadata{
		Name: "test.newgauge",
		Help: "A new gauge added after initial scan",
	})()

	r.Exec(t, `INSERT INTO system.cluster_metrics
		(id, name, labels, type, value, node_id)
		VALUES (400, 'test.newgauge', '{}', 'gauge', 55, 1)`)

	testutils.SucceedsSoon(t, func() error {
		return checkScalarGaugeValue(reg, "test.newgauge", 55)
	})

	// ---------------------------------------------------------------
	// Verify metrics are visible via the /_status/vars endpoint.
	// This is the same prometheus endpoint that external scrapers use.
	// ---------------------------------------------------------------
	body, err := srvtestutils.GetText(ts, ts.AdminURL().WithPath(apiconstants.StatusVars).String())
	require.NoError(t, err)
	promOutput := string(body)

	require.Contains(t, promOutput, "test_gauge_labeled", "test.gauge_labeled should appear in /_status/vars")
	require.Contains(t, promOutput, "test_counter", "test.counter should appear in /_status/vars")
	require.Contains(t, promOutput, "test_scalar", "test.scalar should appear in /_status/vars")
	require.Contains(t, promOutput, "test_newgauge", "test.newgauge should appear in /_status/vars")
	// Verify labeled metrics include both label sets.
	require.Contains(t, promOutput, `store="1"`, "store=1 label should appear in /_status/vars")
	require.Contains(t, promOutput, `store="2"`, "store=2 label should appear in /_status/vars")

	// Parse the prometheus output and verify specific values.
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(strings.NewReader(promOutput))
	require.NoError(t, err)

	scalarFamily, ok := families["test_scalar"]
	require.True(t, ok, "test_scalar should be in parsed prometheus families")
	require.Len(t, scalarFamily.GetMetric(), 1)
	require.Equal(t, float64(123), scalarFamily.GetMetric()[0].GetGauge().GetValue())

	counterFamily, ok := families["test_counter"]
	require.True(t, ok, "test_counter should be in parsed prometheus families")
	require.Len(t, counterFamily.GetMetric(), 1)
	require.Equal(t, float64(10), counterFamily.GetMetric()[0].GetCounter().GetValue())

	// ---------------------------------------------------------------
	// Verify TSDB time series data. The MetricsRecorder.GetTimeSeriesData
	// produces the data that the TSDB poller stores. Scalar metrics
	// should appear in TSDB, while labeled (vec) metrics should NOT
	// appear because PrometheusVector types are excluded from TSDB
	// recording.
	// ---------------------------------------------------------------
	tsData := srv.MetricsRecorder().GetTimeSeriesData(false /* childMetrics */)
	tsNames := make(map[string]float64, len(tsData))
	for _, d := range tsData {
		if len(d.Datapoints) > 0 {
			tsNames[d.Name] = d.Datapoints[0].Value
		}
	}

	// Scalar metrics should be present. The system tenant's cluster metric
	// registry is recorded with the cr.node. prefix (same recorder as node,
	// app, log, and sys registries).
	require.Contains(t, tsNames, "cr.cluster.test.scalar")
	require.Contains(t, tsNames, "cr.cluster.test.counter")
	require.Contains(t, tsNames, "cr.cluster.test.newgauge")
	require.Equal(t, float64(123), tsNames["cr.cluster.test.scalar"])
	require.Equal(t, float64(10), tsNames["cr.cluster.test.counter"])
	require.Equal(t, float64(55), tsNames["cr.cluster.test.newgauge"])

	// Labeled (vec) metrics should NOT be in TSDB data. The recorder's
	// extractValue function returns a no-op for PrometheusVector types.
	require.NotContains(t, tsNames, "cr.cluster.test.gauge_labeled")

	// ---------------------------------------------------------------
	// DELETE a labeled metric row and verify it's removed from
	// prometheus output while the other label set remains.
	// ---------------------------------------------------------------
	r.Exec(t, `DELETE FROM system.cluster_metrics WHERE id = 100`)

	testutils.SucceedsSoon(t, func() error {
		return checkGaugeVecLabelAbsent(
			reg, "test.gauge_labeled", map[string]string{"store": "1"})
	})

	// The other label set should still be present.
	requireGaugeVecValue(t, reg, "test.gauge_labeled",
		map[string]string{"store": "2"}, 77)

	// ---------------------------------------------------------------
	// DELETE a scalar metric and verify it's removed from the
	// registry and from TSDB time series data.
	// ---------------------------------------------------------------
	r.Exec(t, `DELETE FROM system.cluster_metrics WHERE id = 200`)

	testutils.SucceedsSoon(t, func() error {
		return checkMetricAbsent(reg, "test.counter")
	})

	// Once the metric is gone from the registry, it should no longer
	// appear in TSDB time series data.
	tsData = srv.MetricsRecorder().GetTimeSeriesData(false /* childMetrics */)
	tsNames = make(map[string]float64, len(tsData))
	for _, d := range tsData {
		if len(d.Datapoints) > 0 {
			tsNames[d.Name] = d.Datapoints[0].Value
		}
	}
	require.NotContains(t, tsNames, "cr.cluster.test.counter")
	// Remaining scalar metrics should still be present.
	require.Contains(t, tsNames, "cr.cluster.test.scalar")
	require.Contains(t, tsNames, "cr.cluster.test.newgauge")
}

// requireMetricExists asserts that a metric with the given name is present
// in the registry.
func requireMetricExists(t *testing.T, reg metric.RegistryReader, name string) {
	t.Helper()
	var found bool
	reg.Each(func(n string, _ interface{}) {
		if n == name {
			found = true
		}
	})
	require.True(t, found, "metric %q not found in registry", name)
}

func checkMetricAbsent(reg metric.RegistryReader, name string) error {
	var found bool
	reg.Each(func(n string, _ interface{}) {
		if n == name {
			found = true
		}
	})
	if found {
		return fmt.Errorf("metric %q still present in registry", name)
	}
	return nil
}

// requireScalarGaugeValue asserts that a scalar *metric.Gauge has the expected
// value.
func requireScalarGaugeValue(t *testing.T, reg metric.RegistryReader, name string, expected int64) {
	t.Helper()
	err := checkScalarGaugeValue(reg, name, expected)
	require.NoError(t, err)
}

func checkScalarGaugeValue(reg metric.RegistryReader, name string, expected int64) error {
	var g *metric.Gauge
	reg.Each(func(n string, v interface{}) {
		if n == name {
			if gauge, ok := v.(*metric.Gauge); ok {
				g = gauge
			}
		}
	})
	if g == nil {
		return fmt.Errorf("scalar gauge %q not found in registry", name)
	}
	if g.Value() != expected {
		return fmt.Errorf("scalar gauge %q: expected %d, got %d", name, expected, g.Value())
	}
	return nil
}

// requireCounterValue asserts that a scalar *metric.Counter has the expected
// value.
func requireCounterValue(t *testing.T, reg metric.RegistryReader, name string, expected int64) {
	t.Helper()
	var c *metric.Counter
	reg.Each(func(n string, v interface{}) {
		if n == name {
			if counter, ok := v.(*metric.Counter); ok {
				c = counter
			}
		}
	})
	require.NotNilf(t, c, "counter %q not found in registry", name)
	require.Equal(t, expected, c.Count())
}

// requireGaugeVecValue asserts that a *metric.GaugeVec has the expected value
// for the given label set. Uses the prometheus exporter to read individual
// label values.
func requireGaugeVecValue(
	t *testing.T, reg metric.RegistryReader, name string, labels map[string]string, expected int64,
) {
	t.Helper()
	err := checkGaugeVecValue(reg, name, labels, expected)
	require.NoError(t, err)
}

func checkGaugeVecValue(
	reg metric.RegistryReader, name string, labels map[string]string, expected int64,
) error {
	var gv *metric.GaugeVec
	reg.Each(func(n string, v interface{}) {
		if n == name {
			if vec, ok := v.(*metric.GaugeVec); ok {
				gv = vec
			}
		}
	})
	if gv == nil {
		return fmt.Errorf("gauge vec %q not found in registry", name)
	}

	// Scrape the registry and parse prometheus output to read specific label values.
	pe := metric.MakePrometheusExporter()
	var buf strings.Builder
	err := pe.ScrapeAndPrintAsText(&buf, expfmt.FmtText, func(exporter *metric.PrometheusExporter) {
		exporter.ScrapeRegistry(reg)
	})
	if err != nil {
		return fmt.Errorf("failed to scrape registry: %w", err)
	}

	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(strings.NewReader(buf.String()))
	if err != nil {
		return fmt.Errorf("failed to parse prometheus output: %w", err)
	}

	// Prometheus export replaces dots with underscores.
	exportedName := strings.ReplaceAll(name, ".", "_")
	family, ok := families[exportedName]
	if !ok {
		return fmt.Errorf("metric family %q not found in prometheus output", exportedName)
	}

	for _, m := range family.GetMetric() {
		// Build a map of the metric's labels for easy lookup.
		metricLabels := make(map[string]string, len(m.GetLabel()))
		for _, lp := range m.GetLabel() {
			metricLabels[lp.GetName()] = lp.GetValue()
		}
		// Check that all expected labels are present (the metric may have
		// additional labels like node_id or tenant added by the recorder).
		match := true
		for k, v := range labels {
			if metricLabels[k] != v {
				match = false
				break
			}
		}
		if match {
			actual := int64(m.GetGauge().GetValue())
			if actual != expected {
				return fmt.Errorf("gauge vec %q labels=%v: expected %d, got %d",
					name, labels, expected, actual)
			}
			return nil
		}
	}
	return fmt.Errorf("gauge vec %q: no metric found with labels %v", name, labels)
}

// checkGaugeVecLabelAbsent returns nil if the given label set is NOT present
// in the prometheus output for the named metric. Returns an error if the label
// set is still found.
func checkGaugeVecLabelAbsent(
	reg metric.RegistryReader, name string, labels map[string]string,
) error {
	pe := metric.MakePrometheusExporter()
	var buf strings.Builder
	err := pe.ScrapeAndPrintAsText(
		&buf, expfmt.FmtText, func(exporter *metric.PrometheusExporter) {
			exporter.ScrapeRegistry(reg)
		})
	if err != nil {
		return fmt.Errorf("failed to scrape registry: %w", err)
	}

	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(
		strings.NewReader(buf.String()))
	if err != nil {
		return fmt.Errorf("failed to parse prometheus output: %w", err)
	}

	exportedName := strings.ReplaceAll(name, ".", "_")
	family, ok := families[exportedName]
	if !ok {
		return nil // metric family is gone entirely
	}

	for _, m := range family.GetMetric() {
		metricLabels := make(map[string]string, len(m.GetLabel()))
		for _, lp := range m.GetLabel() {
			metricLabels[lp.GetName()] = lp.GetValue()
		}
		match := true
		for k, v := range labels {
			if metricLabels[k] != v {
				match = false
				break
			}
		}
		if match {
			return fmt.Errorf(
				"gauge vec %q: label set %v still present", name, labels)
		}
	}
	return nil
}

// TestRegistrySyncerMultiTenant starts a system tenant and a shared-process
// secondary tenant, inserts metrics with the same name into each tenant's
// system.cluster_metrics table with different values, and verifies that each
// tenant's cluster metric registry is fully isolated. Updates and deletes
// in one tenant do not affect the other.
func TestRegistrySyncerMultiTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStress(t, "test is too slow to run under stress")

	defer clustermetrics.TestingRegisterClusterMetric("test.mt_gauge", metric.Metadata{
		Name: "test.mt_gauge",
		Help: "A gauge for multi-tenant isolation testing",
	})()

	ctx := context.Background()

	// Use buffered channels so the registrySyncer goroutines do not block if the
	// test hasn't started receiving yet.
	sysStartedChan := make(chan struct{}, 1)
	srv, sysDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			ClusterMetricsKnobs: &clustermetricutils.TestingKnobs{
				OnRegistrySyncerStart: func() {
					sysStartedChan <- struct{}{}
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	tenantStartedChan := make(chan struct{}, 1)
	tenant, tenantDB := serverutils.StartSharedProcessTenant(t, srv,
		base.TestSharedProcessTenantArgs{
			TenantName: "app",
			Knobs: base.TestingKnobs{
				ClusterMetricsKnobs: &clustermetricutils.TestingKnobs{
					OnRegistrySyncerStart: func() {
						tenantStartedChan <- struct{}{}
					},
				},
			},
		})

	sysRunner := sqlutils.MakeSQLRunner(sysDB)
	tenantRunner := sqlutils.MakeSQLRunner(tenantDB)

	// Insert a metric with the same name into each tenant's
	// system.cluster_metrics table, but with different values.
	sysRunner.Exec(t, `INSERT INTO system.cluster_metrics
		(id, name, labels, type, value, node_id)
		VALUES (100, 'test.mt_gauge', '{}', 'gauge', 42, 1)`)
	tenantRunner.Exec(t, `INSERT INTO system.cluster_metrics
		(id, name, labels, type, value, node_id)
		VALUES (100, 'test.mt_gauge', '{}', 'gauge', 99, 1)`)

	// Wait for both registry syncers to complete their initial scan.
	<-sysStartedChan
	<-tenantStartedChan

	tenantExecCfg := tenant.ExecutorConfig().(sql.ExecutorConfig)
	tenantID := tenantExecCfg.Codec.TenantID

	sysReg := srv.MetricsRecorder().ClusterMetricRegistry(roachpb.SystemTenantID)
	tenantReg := srv.MetricsRecorder().ClusterMetricRegistry(tenantID)
	require.NotNil(t, sysReg, "system tenant registry should exist")
	require.NotNil(t, tenantReg, "app tenant registry should exist")

	// ---------------------------------------------------------------
	// Verify initial isolation: same metric name, different values.
	// ---------------------------------------------------------------
	requireScalarGaugeValue(t, sysReg, "test.mt_gauge", 42)
	requireScalarGaugeValue(t, tenantReg, "test.mt_gauge", 99)

	// ---------------------------------------------------------------
	// Update the system tenant's metric and verify isolation.
	// ---------------------------------------------------------------
	sysRunner.Exec(t, `UPSERT INTO system.cluster_metrics
		(id, name, labels, type, value, node_id)
		VALUES (100, 'test.mt_gauge', '{}', 'gauge', 100, 1)`)

	testutils.SucceedsSoon(t, func() error {
		return checkScalarGaugeValue(sysReg, "test.mt_gauge", 100)
	})
	// App tenant's value should remain unchanged.
	requireScalarGaugeValue(t, tenantReg, "test.mt_gauge", 99)

	// ---------------------------------------------------------------
	// Update the app tenant's metric and verify isolation.
	// ---------------------------------------------------------------
	tenantRunner.Exec(t, `UPSERT INTO system.cluster_metrics
		(id, name, labels, type, value, node_id)
		VALUES (100, 'test.mt_gauge', '{}', 'gauge', 200, 1)`)

	testutils.SucceedsSoon(t, func() error {
		return checkScalarGaugeValue(tenantReg, "test.mt_gauge", 200)
	})
	// System tenant's value should remain unchanged.
	requireScalarGaugeValue(t, sysReg, "test.mt_gauge", 100)

	// ---------------------------------------------------------------
	// Delete from the system tenant and verify the app tenant is
	// unaffected.
	// ---------------------------------------------------------------
	sysRunner.Exec(t, `DELETE FROM system.cluster_metrics WHERE id = 100`)

	testutils.SucceedsSoon(t, func() error {
		return checkMetricAbsent(sysReg, "test.mt_gauge")
	})
	// App tenant's metric should still be present.
	requireScalarGaugeValue(t, tenantReg, "test.mt_gauge", 200)
}
