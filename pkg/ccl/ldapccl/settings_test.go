// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
		{"empty string",
			"", false,
			""},
		{"valid CA certificate",
			string(caCertPEMBlock), false,
			""},
		{"invalid CA certificate",
			"--BEGIN INVALID CERT `[]!@#%#^$&*", true,
			"LDAP authentication could not parse domain CA cert PEM"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateLDAPDomainCACertificate(nil, tt.setting); (err != nil) != tt.wantErr {
				t.Errorf("validateLDAPDomainCACertificate() error = %v, wantErr %v", err, tt.wantErr)
			} else if err != nil {
				require.Regexp(t, tt.expectedErr, err.Error())
			}
		})
	}
}
