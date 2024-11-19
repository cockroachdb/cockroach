// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldapccl

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestValidateLDAPDomainCACertificate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	caCertPEMBlock, err := securityassets.GetLoader().ReadFile(
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedCACert))
	require.NoError(t, err)

	tests := []struct {
		name        string
		setting     string
		wantErr     bool
		expectedErr string
	}{
		{"empty string", "", false, ""},
		{"valid CA certificate", string(caCertPEMBlock), false, ""},
		{"invalid CA certificate", "--BEGIN INVALID CERT `[]!@#%#^$&*", true,
			"LDAP initialization failed: invalid cert PEM block provided"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateCertificate(nil, tt.setting); (err != nil) != tt.wantErr {
				t.Errorf("validateLDAPDomainCACertificate() error = %v, wantErr %v", err, tt.wantErr)
			} else if err != nil {
				require.Regexp(t, tt.expectedErr, err.Error())
			}
		})
	}
}

func TestValidateLDAPClientCertificate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clientCertPEMBlock, err := securityassets.GetLoader().ReadFile(
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedRootCert))
	require.NoError(t, err)

	tests := []struct {
		name        string
		setting     string
		wantErr     bool
		expectedErr string
	}{
		{"empty string", "", false, ""},
		{"valid client certificate", string(clientCertPEMBlock), false, ""},
		{"invalid client certificate", "--BEGIN INVALID CERT `[]!@#%#^$&*", true,
			"LDAP initialization failed: invalid cert PEM block provided"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateCertificate(nil, tt.setting); (err != nil) != tt.wantErr {
				t.Errorf("validateCertificate() error = %v, wantErr %v", err, tt.wantErr)
			} else if err != nil {
				require.Regexp(t, tt.expectedErr, err.Error())
			}
		})
	}
}

func TestValidateLDAPClientPrivateKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clientPrivateKeyPEMBlock, err := securityassets.GetLoader().ReadFile(
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedRootKey))
	require.NoError(t, err)

	tests := []struct {
		name        string
		setting     string
		wantErr     bool
		expectedErr string
	}{
		{"empty string", "", false, ""},
		{"valid client private key", string(clientPrivateKeyPEMBlock), false, ""},
		{"invalid private key certificate", "---BEGIN INVALID RSA PRIVATE KEY `[]!@#%#^$&*", true,
			"LDAP initialization failed: invalid private key PEM provided"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validatePrivateKey(nil, tt.setting); (err != nil) != tt.wantErr {
				t.Errorf("validatePrivateKey() error = %v, wantErr %v", err, tt.wantErr)
			} else if err != nil {
				require.Regexp(t, tt.expectedErr, err.Error())
			}
		})
	}
}
