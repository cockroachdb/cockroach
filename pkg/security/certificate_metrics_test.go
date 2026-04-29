// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security_test

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func writeTestCerts(t *testing.T, certsDir string) {

	for offset, certName := range []string{
		"ca",
		"ca-client",
		"ca-client-tenant",
		"ca-ui",
		"node",
		"ui",
		"client-tenant.1",
		"client.node",
	} {

		certFile := certName + ".crt"
		expiration := offset + 2
		_, certBytes := makeTestCert(t, "", 0, nil, timeutil.Unix(int64(expiration), 0))
		if err := os.WriteFile(filepath.Join(certsDir, certFile), certBytes, 0700); err != nil {
			t.Fatalf("#%d: could not write file %s: %v", offset, certFile, err)
		}

		keyFile := certName + ".key"
		if err := os.WriteFile(filepath.Join(certsDir, keyFile), []byte(keyFile), 0700); err != nil {
			t.Fatalf("#%d: could not write file %s: %v", offset, keyFile, err)
		}
	}
}

// TestMetricsValues asserts that with the appropriate certificates on disk,
// the correct expiration and ttl values are set on the metrics vars that are
// exposed to our collectors. It uses a single key pair to approximate the
// behavior across other key pairs.
func TestMetricsValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// required to read certs from disk in tests
	securityassets.ResetLoader()
	defer ResetTest()

	// now is unix 1, each expiration is +1 after that.
	// this means an expiration is 1 + cert offset, and ttl is expiration - 1.
	now := timeutil.Unix(1, 0)

	certsDir := t.TempDir()
	writeTestCerts(t, certsDir)

	cm, err := security.NewCertificateManager(
		certsDir,
		security.CommandTLSSettings{},
		security.WithTimeSource(timeutil.NewManualTime(now)),
		security.ForTenant(1),
	)
	if err != nil {
		t.Error(err)
	}

	metrics := cm.Metrics()
	checks := []struct {
		name     string
		expected int64
		actual   int64
	}{
		{"CA Certificate Expiration", 2, metrics.CAExpiration.Value()},
		{"CA Certificate TTL", 1, metrics.CATTL.Value()},
		{"Client CA Certificate Expiration", 3, metrics.ClientCAExpiration.Value()},
		{"Client CA Certificate TTL", 2, metrics.ClientCATTL.Value()},
		{"Tenant CA Certificate Expiration", 4, metrics.TenantCAExpiration.Value()},
		{"Tenant CA Certificate TTL", 3, metrics.TenantCATTL.Value()},
		{"UI CA Certificate Expiration", 5, metrics.UICAExpiration.Value()},
		{"UI CA Certificate TTL", 4, metrics.UICATTL.Value()},
		{"Node Certificate Expiration", 6, metrics.NodeExpiration.Value()},
		{"Node Certificate TTL", 5, metrics.NodeTTL.Value()},
		{"UI Certificate Expiration", 7, metrics.UIExpiration.Value()},
		{"UI Certificate TTL", 6, metrics.UITTL.Value()},
		{"Tenant Certificate Expiration", 8, metrics.TenantExpiration.Value()},
		{"Tenant Certificate TTL", 7, metrics.TenantTTL.Value()},
		{"Node Client Certificate Expiration", 9, metrics.NodeClientExpiration.Value()},
		{"Node Client Certificate TTL", 8, metrics.NodeClientTTL.Value()},
		// placeholder client aggregate gauge
	}
	for _, check := range checks {
		if check.expected != check.actual {
			t.Fatalf("Expected %s to be %d, but instead got %d", check.name, check.expected, check.actual)
		}
	}

}

// TestExpiryDaysValues verifies that the days-until-expiry metrics report
// the correct number of days for each certificate type, with distinct
// day counts per cert to ensure each metric is wired correctly.
func TestExpiryDaysValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	securityassets.ResetLoader()
	defer ResetTest()

	const day = int64(86400)
	now := timeutil.Unix(0, 0)
	certsDir := t.TempDir()

	// Write each certificate with a distinct multi-day expiration.
	// Cert order matches writeTestCerts file names.
	certs := []struct {
		certFile string
		keyFile  string
		days     int64
	}{
		{"ca.crt", "ca.key", 1},
		{"ca-client.crt", "ca-client.key", 2},
		{"ca-client-tenant.crt", "ca-client-tenant.key", 3},
		{"ca-ui.crt", "ca-ui.key", 4},
		{"node.crt", "node.key", 5},
		{"ui.crt", "ui.key", 6},
		{"client-tenant.1.crt", "client-tenant.1.key", 7},
		{"client.node.crt", "client.node.key", 8},
	}
	for _, c := range certs {
		_, certBytes := makeTestCert(t, "", 0, nil, timeutil.Unix(c.days*day, 0))
		require.NoError(t, os.WriteFile(filepath.Join(certsDir, c.certFile), certBytes, 0700))
		require.NoError(t, os.WriteFile(filepath.Join(certsDir, c.keyFile), []byte("key"), 0700))
	}

	cm, err := security.NewCertificateManager(
		certsDir,
		security.CommandTLSSettings{},
		security.WithTimeSource(timeutil.NewManualTime(now)),
		security.ForTenant(1),
	)
	require.NoError(t, err)

	metrics := cm.Metrics()
	checks := []struct {
		name     string
		expected int64
		actual   int64
	}{
		{"CA Expiry Days", 1, metrics.CAExpiryDays.Value()},
		{"Client CA Expiry Days", 2, metrics.ClientCAExpiryDays.Value()},
		{"Tenant CA Expiry Days", 3, metrics.TenantCAExpiryDays.Value()},
		{"UI CA Expiry Days", 4, metrics.UICAExpiryDays.Value()},
		{"Node Expiry Days", 5, metrics.NodeExpiryDays.Value()},
		{"UI Expiry Days", 6, metrics.UIExpiryDays.Value()},
		{"Tenant Expiry Days", 7, metrics.TenantExpiryDays.Value()},
		{"Node Client Expiry Days", 8, metrics.NodeClientExpiryDays.Value()},
	}
	for _, check := range checks {
		require.Equal(t, check.expected, check.actual, check.name)
	}
}

// TestExpiryDaysLargerValues verifies days-until-expiry with multi-day
// expirations to ensure the calculation is correct beyond trivial values.
func TestExpiryDaysLargerValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	securityassets.ResetLoader()
	defer ResetTest()

	now := timeutil.Unix(0, 0)
	certsDir := t.TempDir()

	// Write a CA cert expiring in exactly 10 days (864000 seconds).
	_, certBytes := makeTestCert(t, "", 0, nil, timeutil.Unix(864000, 0))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "ca.crt"), certBytes, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "ca.key"), []byte("key"), 0700))

	// Write a node cert expiring in 2.5 days (216000 seconds) -> ceil = 3 days.
	_, nodeBytes := makeTestCert(t, "", 0, nil, timeutil.Unix(216000, 0))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "node.crt"), nodeBytes, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "node.key"), []byte("key"), 0700))

	cm, err := security.NewCertificateManager(
		certsDir,
		security.CommandTLSSettings{},
		security.WithTimeSource(timeutil.NewManualTime(now)),
	)
	require.NoError(t, err)

	metrics := cm.Metrics()
	require.Equal(t, int64(10), metrics.CAExpiryDays.Value(), "CA expiry days")
	require.Equal(t, int64(3), metrics.NodeExpiryDays.Value(), "Node expiry days")
}

// TestRotationTimestamps verifies that rotation timestamp metrics are zero
// after initial load and are updated when certificates actually change.
func TestRotationTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	securityassets.ResetLoader()
	defer ResetTest()

	now := timeutil.NewManualTime(timeutil.Unix(1000, 0))
	certsDir := t.TempDir()
	writeTestCerts(t, certsDir)

	cm, err := security.NewCertificateManager(
		certsDir,
		security.CommandTLSSettings{},
		security.WithTimeSource(now),
		security.ForTenant(1),
	)
	require.NoError(t, err)

	metrics := cm.Metrics()

	// After initial load, all rotation timestamps should be 0
	// (no rotation has happened yet, only the first load).
	require.Equal(t, int64(0), metrics.CALastRotation.Value(), "CA rotation before reload")
	require.Equal(t, int64(0), metrics.NodeLastRotation.Value(), "Node rotation before reload")
	require.Equal(t, int64(0), metrics.UILastRotation.Value(), "UI rotation before reload")
	require.Equal(t, int64(0), metrics.TenantLastRotation.Value(), "Tenant rotation before reload")

	// Reload without changing any certificates. Rotation timestamps should
	// remain 0 because no certificate content changed.
	now.Advance(100 * time.Second)
	require.NoError(t, cm.LoadCertificates())

	require.Equal(t, int64(0), metrics.CALastRotation.Value(), "CA rotation after no-op reload")
	require.Equal(t, int64(0), metrics.NodeLastRotation.Value(), "Node rotation after no-op reload")

	// Change the CA certificate and reload. Only the CA rotation timestamp
	// should be updated.
	now.Advance(100 * time.Second)
	_, newCACert := makeTestCert(t, "", 0, nil, timeutil.Unix(5000, 0))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "ca.crt"), newCACert, 0700))
	require.NoError(t, cm.LoadCertificates())

	require.Equal(t, int64(1200), metrics.CALastRotation.Value(), "CA rotation after CA change")
	require.Equal(t, int64(0), metrics.NodeLastRotation.Value(), "Node rotation unchanged after CA change")
	require.Equal(t, int64(0), metrics.UILastRotation.Value(), "UI rotation unchanged after CA change")

	// Now change the node cert and reload.
	now.Advance(50 * time.Second)
	_, newNodeCert := makeTestCert(t, "", 0, nil, timeutil.Unix(6000, 0))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "node.crt"), newNodeCert, 0700))
	require.NoError(t, cm.LoadCertificates())

	// CA rotation timestamp should not change; node should be updated.
	require.Equal(t, int64(1200), metrics.CALastRotation.Value(), "CA rotation stays from previous")
	require.Equal(t, int64(1250), metrics.NodeLastRotation.Value(), "Node rotation after node change")
}

// TestRotationTimestampsAllCertTypes verifies rotation timestamps for all
// certificate types after a full cert rotation.
func TestRotationTimestampsAllCertTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	securityassets.ResetLoader()
	defer ResetTest()

	now := timeutil.NewManualTime(timeutil.Unix(1000, 0))
	certsDir := t.TempDir()
	writeTestCerts(t, certsDir)

	cm, err := security.NewCertificateManager(
		certsDir,
		security.CommandTLSSettings{},
		security.WithTimeSource(now),
		security.ForTenant(1),
	)
	require.NoError(t, err)

	metrics := cm.Metrics()

	type rotationCheck struct {
		name     string
		certFile string
		gauge    *metric.Gauge
	}
	checks := []rotationCheck{
		{"CA", "ca.crt", metrics.CALastRotation},
		{"ClientCA", "ca-client.crt", metrics.ClientCALastRotation},
		{"TenantCA", "ca-client-tenant.crt", metrics.TenantCALastRotation},
		{"UICA", "ca-ui.crt", metrics.UICALastRotation},
		{"Node", "node.crt", metrics.NodeLastRotation},
		{"UI", "ui.crt", metrics.UILastRotation},
		{"Tenant", "client-tenant.1.crt", metrics.TenantLastRotation},
		{"NodeClient", "client.node.crt", metrics.NodeClientLastRotation},
	}

	// All should be 0 after initial load.
	for _, c := range checks {
		require.Equal(t, int64(0), c.gauge.Value(), "%s rotation before reload", c.name)
	}

	// Rotate each certificate at a distinct time so each gets a unique
	// rotation timestamp, verifying that each metric is wired correctly.
	for i, c := range checks {
		now.Advance(time.Duration(100*(i+1)) * time.Second)
		_, certBytes := makeTestCert(t, "", 0, nil, timeutil.Unix(9999, 0))
		require.NoError(t, os.WriteFile(filepath.Join(certsDir, c.certFile), certBytes, 0700))
		require.NoError(t, cm.LoadCertificates())
	}

	// Compute expected timestamps. Starting at 1000, each cert i advances
	// by 100*(i+1) seconds cumulatively.
	// i=0: 1000 + 100 = 1100
	// i=1: 1100 + 200 = 1300
	// i=2: 1300 + 300 = 1600
	// i=3: 1600 + 400 = 2000
	// i=4: 2000 + 500 = 2500
	// i=5: 2500 + 600 = 3100
	// i=6: 3100 + 700 = 3800
	// i=7: 3800 + 800 = 4600
	expectedTimestamps := []int64{1100, 1300, 1600, 2000, 2500, 3100, 3800, 4600}
	for i, c := range checks {
		require.Equal(t, expectedTimestamps[i], c.gauge.Value(), "%s rotation after reload", c.name)
	}
}

// TestExpiryDaysExpiredCert verifies that the expiry-days gauge returns 0
// when the certificate is already expired (exercises the sec < 0 branch
// in expiryDaysGauge).
func TestExpiryDaysExpiredCert(t *testing.T) {
	defer leaktest.AfterTest(t)()

	securityassets.ResetLoader()
	defer ResetTest()

	certsDir := t.TempDir()

	// Write certs that expired at Unix time 100.
	expiration := timeutil.Unix(100, 0)
	for _, certName := range []string{"ca", "node", "client.node"} {
		_, certBytes := makeTestCert(t, "", 0, nil, expiration)
		require.NoError(t, os.WriteFile(filepath.Join(certsDir, certName+".crt"), certBytes, 0700))
		require.NoError(t, os.WriteFile(filepath.Join(certsDir, certName+".key"), []byte("key"), 0700))
	}

	// Set "now" well past the expiration so sec < 0.
	now := timeutil.NewManualTime(timeutil.Unix(99999, 0))
	cm, err := security.NewCertificateManager(
		certsDir,
		security.CommandTLSSettings{},
		security.WithTimeSource(now),
	)
	require.NoError(t, err)

	metrics := cm.Metrics()
	require.Equal(t, int64(0), metrics.CAExpiryDays.Value(), "expired CA should report 0 days")
	require.Equal(t, int64(0), metrics.NodeExpiryDays.Value(), "expired node should report 0 days")
	require.Equal(t, int64(0), metrics.NodeClientExpiryDays.Value(), "expired node-client should report 0 days")
}

// TestExpiryDaysErroredCert verifies that the expiry-days gauge returns 0
// when a certificate file contains garbage and CertInfo.Error is set.
func TestExpiryDaysErroredCert(t *testing.T) {
	defer leaktest.AfterTest(t)()

	securityassets.ResetLoader()
	defer ResetTest()

	certsDir := t.TempDir()
	now := timeutil.NewManualTime(timeutil.Unix(0, 0))

	// Write a valid CA cert (required) and node cert, but write garbage to
	// the UI cert file so its CertInfo.Error is set.
	_, caCert := makeTestCert(t, "", 0, nil, timeutil.Unix(86400, 0))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "ca.crt"), caCert, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "ca.key"), []byte("key"), 0700))

	_, nodeCert := makeTestCert(t, "", 0, nil, timeutil.Unix(86400, 0))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "node.crt"), nodeCert, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "node.key"), []byte("key"), 0700))

	_, nodeClientCert := makeTestCert(t, "", 0, nil, timeutil.Unix(86400, 0))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "client.node.crt"), nodeClientCert, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "client.node.key"), []byte("key"), 0700))

	// Write garbage to ui.crt — the cert loader will set CertInfo.Error.
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "ui.crt"), []byte("not a cert"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "ui.key"), []byte("key"), 0700))

	cm, err := security.NewCertificateManager(
		certsDir,
		security.CommandTLSSettings{},
		security.WithTimeSource(now),
	)
	require.NoError(t, err)

	metrics := cm.Metrics()
	// Valid certs should report > 0 days.
	require.Greater(t, metrics.CAExpiryDays.Value(), int64(0), "valid CA should report > 0 days")
	require.Greater(t, metrics.NodeExpiryDays.Value(), int64(0), "valid node should report > 0 days")
	// Errored UI cert should report 0.
	require.Equal(t, int64(0), metrics.UIExpiryDays.Value(), "errored UI cert should report 0 days")
}

// TestExpiryDaysMissingCert verifies that the expiry-days gauge returns 0
// for optional certificates that are not present on disk (CertInfo is nil).
func TestExpiryDaysMissingCert(t *testing.T) {
	defer leaktest.AfterTest(t)()

	securityassets.ResetLoader()
	defer ResetTest()

	certsDir := t.TempDir()
	now := timeutil.NewManualTime(timeutil.Unix(0, 0))

	// Write only the required certs: CA + node + client.node.
	// All optional certs (UI CA, client CA, tenant CA, UI, tenant) are absent.
	_, caCert := makeTestCert(t, "", 0, nil, timeutil.Unix(86400, 0))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "ca.crt"), caCert, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "ca.key"), []byte("key"), 0700))

	_, nodeCert := makeTestCert(t, "", 0, nil, timeutil.Unix(86400, 0))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "node.crt"), nodeCert, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "node.key"), []byte("key"), 0700))

	_, nodeClientCert := makeTestCert(t, "", 0, nil, timeutil.Unix(86400, 0))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "client.node.crt"), nodeClientCert, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(certsDir, "client.node.key"), []byte("key"), 0700))

	cm, err := security.NewCertificateManager(
		certsDir,
		security.CommandTLSSettings{},
		security.WithTimeSource(now),
	)
	require.NoError(t, err)

	metrics := cm.Metrics()
	// Present certs should report > 0 days.
	require.Greater(t, metrics.CAExpiryDays.Value(), int64(0), "CA should report > 0 days")
	require.Greater(t, metrics.NodeExpiryDays.Value(), int64(0), "node should report > 0 days")
	require.Greater(t, metrics.NodeClientExpiryDays.Value(), int64(0), "node-client should report > 0 days")
	// Missing optional certs should report 0.
	require.Equal(t, int64(0), metrics.UICAExpiryDays.Value(), "missing UI CA should report 0 days")
	require.Equal(t, int64(0), metrics.ClientCAExpiryDays.Value(), "missing client CA should report 0 days")
	require.Equal(t, int64(0), metrics.TenantCAExpiryDays.Value(), "missing tenant CA should report 0 days")
	require.Equal(t, int64(0), metrics.UIExpiryDays.Value(), "missing UI cert should report 0 days")
	require.Equal(t, int64(0), metrics.TenantExpiryDays.Value(), "missing tenant cert should report 0 days")
}

// TestCertificateReload verifies that when the certificate manager's
// underlying ceritificates change, the original metrics references (which are
// the ones registered) also reflect the TTLs on the new certificates.
func TestCertificateReload(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// required to read certs from disk in tests
	securityassets.ResetLoader()
	defer ResetTest()

	// now is unix 1, each expiration is +1 after that.
	// this means an expiration is 1 + cert offset, and ttl is expiration - 1.
	now := timeutil.Unix(1, 0)

	certsDir := t.TempDir()
	// writeTestCerts writes the ca certificate with an expiration of 2.
	writeTestCerts(t, certsDir)

	cm, err := security.NewCertificateManager(
		certsDir,
		security.CommandTLSSettings{},
		security.WithTimeSource(timeutil.NewManualTime(now)),
		security.ForTenant(1),
	)
	if err != nil {
		t.Error(err)
	}

	caCertExpiration := cm.Metrics().CAExpiration
	caCertTTL := cm.Metrics().CATTL

	// Validate the starting values, with an expiration of 2 and a now of 1,
	// expiration = 2, ttl = 1.
	require.Equal(t, 2, int(caCertExpiration.Value()))
	require.Equal(t, 1, int(caCertTTL.Value()))

	// overwrite the ca certificate with an expiration of 1000.
	certFile := "ca.crt"
	expiration := 1000
	_, certBytes := makeTestCert(t, "", 0, nil, timeutil.Unix(int64(expiration), 0))
	if err := os.WriteFile(filepath.Join(certsDir, certFile), certBytes, 0700); err != nil {
		t.Fatalf("could not write file %s: %v", certFile, err)
	}

	// reload certificates after new one is written.
	err = cm.LoadCertificates()
	require.NoError(t, err)

	// Validate the values after reload, with an expiration of 1000 and a now of 1,
	// expiration = 1000, ttl = 999.
	require.Equal(t, 1000, int(caCertExpiration.Value()))
	require.Equal(t, 999, int(caCertTTL.Value()))
}

// TestCertificateMetricsReloadRace verifies that all certificate expiration and
// TTL metrics update correctly after certificate rotation, and that reading the
// metrics concurrently with LoadCertificates does not race. Before the fix,
// some metric closures (NodeClientExpiration, TenantExpiration,
// TenantCAExpiration and their TTL counterparts) read CertificateManager fields
// without holding cm.mu, which is a data race detectable with -race.
func TestCertificateMetricsReloadRace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	securityassets.ResetLoader()
	defer ResetTest()

	now := timeutil.Unix(1, 0)
	certsDir := t.TempDir()
	writeTestCerts(t, certsDir)

	cm, err := security.NewCertificateManager(
		certsDir,
		security.CommandTLSSettings{},
		security.WithTimeSource(timeutil.NewManualTime(now)),
		security.ForTenant(1),
	)
	require.NoError(t, err)

	metrics := cm.Metrics()

	// Each entry maps a cert file to its expiration/TTL metrics and the
	// initial values assigned by writeTestCerts (expiration = offset + 2,
	// TTL = expiration - now = expiration - 1).
	type metricCheck struct {
		name          string
		certFile      string
		expiration    *metric.FunctionalGauge
		ttl           *metric.FunctionalGauge
		initialExp    int64
		initialTTL    int64
		newExpiration int64
	}
	checks := []metricCheck{
		{"CA", "ca.crt", metrics.CAExpiration, metrics.CATTL, 2, 1, 1000},
		{"ClientCA", "ca-client.crt", metrics.ClientCAExpiration, metrics.ClientCATTL, 3, 2, 1001},
		{"TenantCA", "ca-client-tenant.crt", metrics.TenantCAExpiration, metrics.TenantCATTL, 4, 3, 1002},
		{"UICA", "ca-ui.crt", metrics.UICAExpiration, metrics.UICATTL, 5, 4, 1003},
		{"Node", "node.crt", metrics.NodeExpiration, metrics.NodeTTL, 6, 5, 1004},
		{"UI", "ui.crt", metrics.UIExpiration, metrics.UITTL, 7, 6, 1005},
		{"Tenant", "client-tenant.1.crt", metrics.TenantExpiration, metrics.TenantTTL, 8, 7, 1006},
		{"NodeClient", "client.node.crt", metrics.NodeClientExpiration, metrics.NodeClientTTL, 9, 8, 1007},
	}

	// Verify initial values.
	for _, c := range checks {
		require.Equal(t, c.initialExp, c.expiration.Value(), "%s expiration before reload", c.name)
		require.Equal(t, c.initialTTL, c.ttl.Value(), "%s TTL before reload", c.name)
	}

	// Overwrite every certificate with a new expiration.
	for _, c := range checks {
		_, certBytes := makeTestCert(t, "", 0, nil, timeutil.Unix(c.newExpiration, 0))
		require.NoError(t, os.WriteFile(filepath.Join(certsDir, c.certFile), certBytes, 0700))
	}

	// Read all metrics concurrently with LoadCertificates to trigger the
	// race detector on any unsynchronized field access. The stop channel
	// ensures the reader goroutine stays active throughout the entire
	// LoadCertificates call.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for i := range checks {
					checks[i].expiration.Value()
					checks[i].ttl.Value()
				}
			}
		}
	}()

	require.NoError(t, cm.LoadCertificates())
	close(stop)
	wg.Wait()

	// After reload, every metric must reflect the new certificate.
	for _, c := range checks {
		require.Equal(t, c.newExpiration, c.expiration.Value(), "%s expiration after reload", c.name)
		require.Equal(t, c.newExpiration-1, c.ttl.Value(), "%s TTL after reload", c.name)
	}
}
