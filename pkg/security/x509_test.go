// Copyright 2017 The Cockroach Authors.
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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

const wiggle = time.Minute * 5

// Returns true if "|a-b| <= wiggle".
func timesFuzzyEqual(a, b time.Time) bool {
	diff := a.Sub(b)
	if diff < 0 {
		diff = -diff
	}
	return diff <= wiggle
}

func TestGenerateCertLifetime(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testKey, err := rsa.GenerateKey(rand.Reader, 512)
	if err != nil {
		t.Fatal(err)
	}

	// Create a CA that expires in 2 days.
	caDuration := time.Hour * 48
	now := timeutil.Now()
	caBytes, err := security.GenerateCA(testKey, caDuration)
	if err != nil {
		t.Fatal(err)
	}

	caCert, err := x509.ParseCertificate(caBytes)
	if err != nil {
		t.Fatal(err)
	}

	if a, e := caCert.NotAfter, now.Add(caDuration); !timesFuzzyEqual(a, e) {
		t.Fatalf("CA expiration differs from requested: %s vs %s", a, e)
	}

	// Create a Node certificate expiring in 4 days. Fails on shorter CA lifetime.
	nodeDuration := time.Hour * 96
	_, err = security.GenerateServerCert(caCert, testKey,
		testKey.Public(), nodeDuration, username.NodeUserName(), []string{"localhost"})
	if !testutils.IsError(err, "CA lifetime is .*, shorter than the requested .*") {
		t.Fatal(err)
	}

	// Try again, but expiring before the CA cert.
	nodeDuration = time.Hour * 24
	nodeBytes, err := security.GenerateServerCert(caCert, testKey,
		testKey.Public(), nodeDuration, username.NodeUserName(), []string{"localhost"})
	if err != nil {
		t.Fatal(err)
	}

	nodeCert, err := x509.ParseCertificate(nodeBytes)
	if err != nil {
		t.Fatal(err)
	}

	if a, e := nodeCert.NotAfter, now.Add(nodeDuration); !timesFuzzyEqual(a, e) {
		t.Fatalf("node expiration differs from requested: %s vs %s", a, e)
	}

	// Create a Client certificate expiring in 4 days. Should get reduced to the CA lifetime.
	clientDuration := time.Hour * 96
	_, err = security.GenerateClientCert(caCert, testKey, testKey.Public(), clientDuration, username.TestUserName(),
		[]roachpb.TenantID{roachpb.SystemTenantID}, nil)
	if !testutils.IsError(err, "CA lifetime is .*, shorter than the requested .*") {
		t.Fatal(err)
	}

	// Try again, but expiring before the CA cert.
	clientDuration = time.Hour * 24
	clientBytes, err := security.GenerateClientCert(caCert, testKey, testKey.Public(), clientDuration,
		username.TestUserName(), []roachpb.TenantID{roachpb.SystemTenantID}, nil)
	if err != nil {
		t.Fatal(err)
	}

	clientCert, err := x509.ParseCertificate(clientBytes)
	if err != nil {
		t.Fatal(err)
	}

	if a, e := clientCert.NotAfter, now.Add(clientDuration); !timesFuzzyEqual(a, e) {
		t.Fatalf("client expiration differs from requested: %s vs %s", a, e)
	}
}

func TestIsTenantNameSANURI(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		name     string
		sanURL   string
		expected bool
	}

	testCases := []testCase{
		{
			name:     "valid_tenant_name_prefix",
			sanURL:   "crdb://tenant-name/tenant10/user/user",
			expected: true,
		},
		{
			name:     "valid_tenant_name_prefix_upper_case",
			sanURL:   "crdb://TENANT-NAME/tenant10/user/user",
			expected: true,
		},
		{
			name:     "invalid_tenant_name_prefix",
			sanURL:   "crdb://tenant/tenant-name/user/user",
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			url, err := url.Parse(tc.sanURL)
			require.Nilf(t, err, "invalid SAN URI: %q", tc.sanURL)
			require.Equalf(t, tc.expected, security.IsTenantNameSANURI(url), "invalid tenant name SAN URI: %q", tc.sanURL)
		})
	}
}

func TestIsCRDBSANURI(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		name     string
		sanURL   string
		expected bool
	}

	testCases := []testCase{
		{
			name:     "valid_crdb_scheme",
			sanURL:   "crdb://tenant-name/tenant10/user/user",
			expected: true,
		},
		{
			name:     "valid_crdb_scheme_upper_case",
			sanURL:   "CRDB://tenant/10/user/user",
			expected: true,
		},
		{
			name:     "invalid_crdb_scheme",
			sanURL:   "crdb1://tenant/10/user/user",
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			url, err := url.Parse(tc.sanURL)
			require.Nilf(t, err, "invalid SAN URI: %q", tc.sanURL)
			require.Equalf(t, tc.expected, security.IsCRDBSANURI(url), "invalid crdb SAN URI: %q", tc.sanURL)
		})
	}
}
