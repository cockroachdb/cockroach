// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

	now := timeutil.Unix(1, 0)
	certsDir := t.TempDir()
	_, caCert := makeTestCert(t, "", 0, nil, timeutil.Unix(2, 0))

	for fileNum, f := range []struct {
		name     string
		mode     os.FileMode
		contents []byte
	}{
		{"ca.crt", 0777, caCert},
		{"ca.key", 0777, []byte("ca.key")},
	} {

		n := f.name
		if err := os.WriteFile(filepath.Join(certsDir, n), f.contents, f.mode); err != nil {
			t.Fatalf("#%d: could not write file %s: %v", fileNum, n, err)
		}
	}

	cm, err := security.NewCertificateManager(certsDir, security.CommandTLSSettings{}, security.WithTimeSource(timeutil.NewManualTime(now)))
	if err != nil {
		t.Error(err)
	}

	exp := cm.Metrics().CAExpiration.Value()
	ttl := cm.Metrics().CATTL.Value()
	if exp != 2 {
		t.Fatalf("Expected certificate expiration to be %d, but instead got %d", 2, exp)
	}
	if ttl != 1 {
		t.Fatalf("Expected certificate ttl to be %d, but instead got %d", 1, ttl)
	}
}
