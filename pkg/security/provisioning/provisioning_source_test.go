// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisioning

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestParseProvisioningSource(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name           string
		sourceStr      string
		wantErr        bool
		expectedMethod string
		expectedIDP    string
		expectedErrMsg string
	}{
		{
			name:           "valid ldap source",
			sourceStr:      "ldap:ldap.bar.com",
			wantErr:        false,
			expectedMethod: "ldap",
			expectedIDP:    "ldap.bar.com",
		},
		{
			name:           "valid ldap source with example.com",
			sourceStr:      "ldap:ldap.example.com",
			wantErr:        false,
			expectedMethod: "ldap",
			expectedIDP:    "ldap.example.com",
		},
		{
			name:           "valid ldap source with simple hostname",
			sourceStr:      "ldap:foo.bar",
			wantErr:        false,
			expectedMethod: "ldap",
			expectedIDP:    "foo.bar",
		},
		{
			name:           "missing auth method prefix",
			sourceStr:      "ldap.example.com",
			wantErr:        true,
			expectedErrMsg: `PROVISIONSRC "ldap.example.com" was not prefixed with any valid auth methods ["ldap"]`,
		},
		{
			name:           "invalid characters in IDP",
			sourceStr:      "ldap:[]!@#%#^$&*",
			wantErr:        true,
			expectedErrMsg: `provided IDP "[]!@#%#^$&*" in PROVISIONSRC is non parseable`,
		},
		{
			name:           "empty string",
			sourceStr:      "",
			wantErr:        true,
			expectedErrMsg: `PROVISIONSRC "" was not prefixed with any valid auth methods ["ldap"]`,
		},
		{
			name:           "invalid auth method",
			sourceStr:      "oauth:example.com",
			wantErr:        true,
			expectedErrMsg: `PROVISIONSRC "oauth:example.com" was not prefixed with any valid auth methods ["ldap"]`,
		},
		{
			name:           "only auth method without IDP",
			sourceStr:      "ldap:",
			wantErr:        true,
			expectedErrMsg: `PROVISIONSRC IDP cannot be empty`,
		},
		{
			name:           "IDP url with port",
			sourceStr:      "ldap:example.com:389",
			wantErr:        true,
			expectedErrMsg: "unknown PROVISIONSRC IDP url format in \"example.com:389\"",
		},
		{
			name:           "IDP url starts with double slash",
			sourceStr:      "ldap://ldap.example.com",
			wantErr:        false,
			expectedMethod: "ldap",
			expectedIDP:    "//ldap.example.com",
		},
		{
			name:           "space in IDP url",
			sourceStr:      "ldap:ldap1 ldap2",
			wantErr:        false,
			expectedMethod: "ldap",
			expectedIDP:    "ldap1%20ldap2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source, err := ParseProvisioningSource(tt.sourceStr)
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, source)
				require.Contains(t, err.Error(), tt.expectedErrMsg)
			} else {
				require.NoError(t, err)
				require.NotNil(t, source)
				require.Equal(t, tt.expectedMethod, source.authMethod)
				require.Equal(t, tt.expectedIDP, source.idp.String())
			}
		})
	}
}

func TestValidateSource(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name           string
		sourceStr      string
		wantErr        bool
		expectedErrMsg string
	}{
		{
			name:      "valid ldap source",
			sourceStr: "ldap:ldap.bar.com",
			wantErr:   false,
		},
		{
			name:      "valid ldap source with example.com",
			sourceStr: "ldap:ldap.example.com",
			wantErr:   false,
		},
		{
			name:      "valid ldap source with simple hostname",
			sourceStr: "ldap:foo.bar",
			wantErr:   false,
		},
		{
			name:           "missing auth method prefix",
			sourceStr:      "ldap.example.com",
			wantErr:        true,
			expectedErrMsg: `PROVISIONSRC "ldap.example.com" was not prefixed with any valid auth methods ["ldap"]`,
		},
		{
			name:           "invalid characters in IDP",
			sourceStr:      "ldap:[]!@#%#^$&*",
			wantErr:        true,
			expectedErrMsg: `provided IDP "[]!@#%#^$&*" in PROVISIONSRC is non parseable`,
		},
		{
			name:           "empty string",
			sourceStr:      "",
			wantErr:        true,
			expectedErrMsg: `PROVISIONSRC "" was not prefixed with any valid auth methods ["ldap"]`,
		},
		{
			name:           "invalid auth method",
			sourceStr:      "oauth:example.com",
			wantErr:        true,
			expectedErrMsg: `PROVISIONSRC "oauth:example.com" was not prefixed with any valid auth methods ["ldap"]`,
		},
		{
			name:           "only auth method without IDP",
			sourceStr:      "ldap:",
			wantErr:        true,
			expectedErrMsg: `PROVISIONSRC IDP cannot be empty`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSource(tt.sourceStr)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
