// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestValidateExternalConnectionSinkURICallsDial tests that the validation
// function properly calls Dial() on the created sink and fails appropriately
// when the target cannot be reached.
func TestValidateExternalConnectionSinkURICallsDial(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test with a URI that would normally succeed in creation but fail on dial
	ctx := context.Background()
	
	// Create a minimal server config for testing
	serverCfg := &execinfra.ServerConfig{}
	
	env := externalconn.ExternalConnEnv{
		Username:  username.RootUserName(),
		ServerCfg: serverCfg,
	}

	// Test with a kafka URI that will fail to dial
	kafkaURI := "kafka://nonexistent-host:9092?topic_name=test"
	
	err := validateExternalConnectionSinkURI(ctx, env, kafkaURI)
	
	// We expect this to fail with a dial error, not a creation error
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to dial changefeed sink")
}

// TestExternalConnectionDialIntegration tests the end-to-end behavior 
// of external connection creation with dialing validation.
func TestExternalConnectionDialIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stopServer := makeServer(t)
	defer stopServer()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)

	// Test cases for external connection creation that should fail due to dial errors
	testCases := []struct {
		name        string
		uri         string
		expectError bool
		errorSubstr string
	}{
		{
			name:        "kafka-nonexistent-host",
			uri:         "kafka://definitely-nonexistent-host-12345:9092/?topic_name=test",
			expectError: true,
			errorSubstr: "dial", // Should contain "dial" indicating it tried to connect
		},
		{
			name:        "webhook-nonexistent-host",
			uri:         "webhook-https://definitely-nonexistent-host-12345/webhook",
			expectError: true,
			errorSubstr: "dial", // Should contain "dial" indicating it tried to connect
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sql := "CREATE EXTERNAL CONNECTION test_conn AS $1"
			if tc.expectError {
				// The error should indicate that dialing was attempted
				err := sqlDB.DB.QueryRow(sql, tc.uri).Err()
				require.Error(t, err)
				require.True(t, strings.Contains(err.Error(), tc.errorSubstr),
					"Expected error to contain '%s', got: %v", tc.errorSubstr, err)
			} else {
				sqlDB.Exec(t, sql, tc.uri)
				// Clean up
				sqlDB.Exec(t, "DROP EXTERNAL CONNECTION test_conn")
			}
		})
	}
}