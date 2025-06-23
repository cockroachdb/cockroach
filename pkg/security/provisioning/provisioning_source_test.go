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

	// Test cases that should apply to both ldap and jwt_token
	sharedTests := []struct {
		name           string
		idpStr         string
		wantErr        bool
		expectedIDP    string
		expectedErrMsg string
	}{
		{
			name:        "valid source",
			idpStr:      "idp.example.com",
			wantErr:     false,
			expectedIDP: "idp.example.com",
		},
		{
			name:           "only auth method without IDP",
			idpStr:         "",
			wantErr:        true,
			expectedErrMsg: `PROVISIONSRC IDP cannot be empty`,
		},
		{
			name:           "IDP url with port",
			idpStr:         "example.com:389",
			wantErr:        true,
			expectedErrMsg: "unknown PROVISIONSRC IDP url format in \"example.com:389\"",
		},
		{
			name:        "space in IDP url",
			idpStr:      "idp1 idp2",
			wantErr:     false,
			expectedIDP: "idp1%20idp2",
		},
		{
			name:        "IDP url starts with double slash",
			idpStr:      "//idp.example.com",
			wantErr:     false,
			expectedIDP: "//idp.example.com",
		},
	}

	for _, method := range []string{"ldap", "jwt_token"} {
		t.Run(method, func(t *testing.T) {
			for _, tt := range sharedTests {
				t.Run(tt.name, func(t *testing.T) {
					source, err := ParseProvisioningSource(method + ":" + tt.idpStr)
					if tt.wantErr {
						require.Error(t, err)
						require.Nil(t, source)
						if tt.expectedErrMsg != "" {
							require.Contains(t, err.Error(), tt.expectedErrMsg)
						}
					} else {
						require.NoError(t, err)
						require.NotNil(t, source)
						require.Equal(t, method, source.authMethod)
						require.Equal(t, tt.expectedIDP, source.idp.String())
					}
				})
			}
		})
	}

	// Test case specific to jwt_token allowing https
	t.Run("jwt_token/valid_https_source", func(t *testing.T) {
		source, err := ParseProvisioningSource("jwt_token:https://accounts.google.com")
		require.NoError(t, err)
		require.NotNil(t, source)
		require.Equal(t, "jwt_token", source.authMethod)
		require.Equal(t, "https://accounts.google.com", source.idp.String())
	})

	// According to the current implementation of `parseIDP`, this is valid.
	// The function `url.Parse` accepts this and the checks for Port and Opaque pass.
	t.Run("ldap/https_source_is_valid_by_current_implementation", func(t *testing.T) {
		source, err := ParseProvisioningSource("ldap:https://accounts.google.com")
		require.NoError(t, err)
		require.NotNil(t, source)
		require.Equal(t, "ldap", source.authMethod)
		require.Equal(t, "https://accounts.google.com", source.idp.String())
	})

	// Test cases that are independent of any valid auth method prefix
	t.Run("global_failures", func(t *testing.T) {
		globalTests := []struct {
			name           string
			sourceStr      string
			expectedErrMsg string
		}{
			{
				name:           "missing auth method prefix",
				sourceStr:      "nocolon.example.com",
				expectedErrMsg: `PROVISIONSRC "nocolon.example.com" was not prefixed with any valid auth methods`,
			},
			{
				name:           "empty string",
				sourceStr:      "",
				expectedErrMsg: `PROVISIONSRC "" was not prefixed with any valid auth methods`,
			},
			{
				name:           "invalid auth method",
				sourceStr:      "oauth:example.com",
				expectedErrMsg: `PROVISIONSRC "oauth:example.com" was not prefixed with any valid auth methods`,
			},
		}

		for _, tt := range globalTests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := ParseProvisioningSource(tt.sourceStr)
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErrMsg)
			})
		}
	})
}

func TestValidateSource(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Shared test cases
	sharedTests := []struct {
		name           string
		idpStr         string
		wantErr        bool
		expectedErrMsg string
	}{
		{
			name:    "valid source",
			idpStr:  "idp.bar.com",
			wantErr: false,
		},
		{
			name:           "invalid characters in IDP",
			idpStr:         "[]!@#%#^$&*",
			wantErr:        true,
			expectedErrMsg: `provided IDP "[]!@#%#^$&*" in PROVISIONSRC is non parseable`,
		},
		{
			name:           "only auth method without IDP",
			idpStr:         "",
			wantErr:        true,
			expectedErrMsg: `PROVISIONSRC IDP cannot be empty`,
		},
	}

	for _, method := range []string{"ldap", "jwt_token"} {
		t.Run(method, func(t *testing.T) {
			for _, tt := range sharedTests {
				t.Run(tt.name, func(t *testing.T) {
					err := ValidateSource(method + ":" + tt.idpStr)
					if tt.wantErr {
						require.Error(t, err)
						if tt.expectedErrMsg != "" {
							require.Contains(t, err.Error(), tt.expectedErrMsg)
						}
					} else {
						require.NoError(t, err)
					}
				})
			}
		})
	}

	// Specific cases
	t.Run("jwt_token/valid_https_source", func(t *testing.T) {
		err := ValidateSource("jwt_token:https://accounts.google.com")
		require.NoError(t, err)
	})

	t.Run("ldap/https_source_is_valid_by_current_implementation", func(t *testing.T) {
		err := ValidateSource("ldap:https://accounts.google.com")
		require.NoError(t, err)
	})

	// Global failure cases
	t.Run("global_failures", func(t *testing.T) {
		globalTests := []struct {
			name           string
			sourceStr      string
			expectedErrMsg string
		}{
			{
				name:           "missing auth method prefix",
				sourceStr:      "nocolon.example.com",
				expectedErrMsg: `PROVISIONSRC "nocolon.example.com" was not prefixed with any valid auth methods`,
			},
			{
				name:           "empty string",
				sourceStr:      "",
				expectedErrMsg: `PROVISIONSRC "" was not prefixed with any valid auth methods`,
			},
			{
				name:           "invalid auth method",
				sourceStr:      "oauth:example.com",
				expectedErrMsg: `PROVISIONSRC "oauth:example.com" was not prefixed with any valid auth methods`,
			},
		}

		for _, tt := range globalTests {
			t.Run(tt.name, func(t *testing.T) {
				err := ValidateSource(tt.sourceStr)
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErrMsg)
			})
		}
	})
}
