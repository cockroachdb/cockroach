// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

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
