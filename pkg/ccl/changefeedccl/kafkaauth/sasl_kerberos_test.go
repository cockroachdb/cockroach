// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kafkaauth

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKerberosRegistration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a temporary keytab file for testing
	tmpDir := t.TempDir()
	keytabPath := filepath.Join(tmpDir, "test.keytab")
	err := os.WriteFile(keytabPath, []byte("fake keytab content"), 0600)
	require.NoError(t, err)

	testCases := []struct {
		name        string
		rawURL      string
		expectError bool
		checkFields func(t *testing.T, mech *saslKerberos)
	}{
		{
			name:   "minimal configuration",
			rawURL: `kafka://broker:9092?sasl_enabled=true&sasl_mechanism=GSSAPI&sasl_kerberos_service_name=kafka`,
			checkFields: func(t *testing.T, mech *saslKerberos) {
				require.Equal(t, "kafka", mech.serviceName)
				require.Empty(t, mech.principal)
				require.Empty(t, mech.keytabPath)
				require.Empty(t, mech.realm)
				require.Empty(t, mech.configPath)
				require.True(t, mech.handshake) // default value
			},
		},
		{
			name:   "keytab authentication",
			rawURL: fmt.Sprintf(`kafka://broker:9092?sasl_enabled=true&sasl_mechanism=GSSAPI&sasl_kerberos_service_name=kafka&sasl_kerberos_principal=user@EXAMPLE.COM&sasl_kerberos_keytab_path=%s`, keytabPath),
			checkFields: func(t *testing.T, mech *saslKerberos) {
				require.Equal(t, "kafka", mech.serviceName)
				require.Equal(t, "user@EXAMPLE.COM", mech.principal)
				require.Equal(t, keytabPath, mech.keytabPath)
			},
		},
		{
			name:   "full configuration",
			rawURL: fmt.Sprintf(`kafka://broker:9092?sasl_enabled=true&sasl_mechanism=GSSAPI&sasl_kerberos_service_name=kafka&sasl_kerberos_principal=user@EXAMPLE.COM&sasl_kerberos_keytab_path=%s&sasl_kerberos_realm=EXAMPLE.COM&sasl_kerberos_config=/etc/krb5.conf&sasl_handshake=false`, keytabPath),
			checkFields: func(t *testing.T, mech *saslKerberos) {
				require.Equal(t, "kafka", mech.serviceName)
				require.Equal(t, "user@EXAMPLE.COM", mech.principal)
				require.Equal(t, keytabPath, mech.keytabPath)
				require.Equal(t, "EXAMPLE.COM", mech.realm)
				require.Equal(t, "/etc/krb5.conf", mech.configPath)
				require.False(t, mech.handshake)
			},
		},
		{
			name:        "missing service name",
			rawURL:      `kafka://broker:9092?sasl_enabled=true&sasl_mechanism=GSSAPI`,
			expectError: true,
		},
		{
			name:        "principal without keytab",
			rawURL:      `kafka://broker:9092?sasl_enabled=true&sasl_mechanism=GSSAPI&sasl_kerberos_service_name=kafka&sasl_kerberos_principal=user@EXAMPLE.COM`,
			expectError: true,
		},
		{
			name:        "keytab without principal",
			rawURL:      fmt.Sprintf(`kafka://broker:9092?sasl_enabled=true&sasl_mechanism=GSSAPI&sasl_kerberos_service_name=kafka&sasl_kerberos_keytab_path=%s`, keytabPath),
			expectError: true,
		},
		{
			name:        "nonexistent keytab file",
			rawURL:      `kafka://broker:9092?sasl_enabled=true&sasl_mechanism=GSSAPI&sasl_kerberos_service_name=kafka&sasl_kerberos_principal=user@EXAMPLE.COM&sasl_kerberos_keytab_path=/nonexistent/path/to/keytab`,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.Parse(tc.rawURL)
			require.NoError(t, err)
			su := &changefeedbase.SinkURL{URL: u}

			mech, ok, err := Pick(su)

			if tc.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.True(t, ok)
			require.NotNil(t, mech)

			kerbMech, ok := mech.(*saslKerberos)
			require.True(t, ok)

			// Check that all parameters were consumed
			require.Empty(t, su.RemainingQueryParams())

			if tc.checkFields != nil {
				tc.checkFields(t, kerbMech)
			}
		})
	}
}

func TestKerberosSaramaConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a temporary keytab file for testing
	tmpDir := t.TempDir()
	keytabPath := filepath.Join(tmpDir, "test.keytab")
	err := os.WriteFile(keytabPath, []byte("fake keytab content"), 0600)
	require.NoError(t, err)

	testCases := []struct {
		name        string
		kerberos    *saslKerberos
		checkConfig func(t *testing.T, cfg *sarama.Config)
	}{
		{
			name: "ticket cache authentication",
			kerberos: &saslKerberos{
				serviceName: "kafka",
				realm:       "EXAMPLE.COM",
				handshake:   true,
			},
			checkConfig: func(t *testing.T, cfg *sarama.Config) {
				assert.True(t, cfg.Net.SASL.Enable)
				assert.Equal(t, string(sarama.SASLTypeGSSAPI), string(cfg.Net.SASL.Mechanism))
				assert.True(t, cfg.Net.SASL.Handshake)
				assert.Equal(t, "kafka", cfg.Net.SASL.GSSAPI.ServiceName)
				assert.Equal(t, "EXAMPLE.COM", cfg.Net.SASL.GSSAPI.Realm)
				assert.Equal(t, sarama.KRB5_USER_AUTH, cfg.Net.SASL.GSSAPI.AuthType)
			},
		},
		{
			name: "keytab authentication",
			kerberos: &saslKerberos{
				serviceName: "kafka",
				principal:   "user@EXAMPLE.COM",
				keytabPath:  keytabPath,
				handshake:   false,
			},
			checkConfig: func(t *testing.T, cfg *sarama.Config) {
				assert.True(t, cfg.Net.SASL.Enable)
				assert.Equal(t, string(sarama.SASLTypeGSSAPI), string(cfg.Net.SASL.Mechanism))
				assert.False(t, cfg.Net.SASL.Handshake)
				assert.Equal(t, "kafka", cfg.Net.SASL.GSSAPI.ServiceName)
				assert.Equal(t, sarama.KRB5_KEYTAB_AUTH, cfg.Net.SASL.GSSAPI.AuthType)
				assert.Equal(t, "user@EXAMPLE.COM", cfg.Net.SASL.GSSAPI.Username)
				assert.Equal(t, keytabPath, cfg.Net.SASL.GSSAPI.KeyTabPath)
			},
		},
		{
			name: "custom config path",
			kerberos: &saslKerberos{
				serviceName: "kafka",
				configPath:  "/etc/krb5.conf",
				handshake:   true,
			},
			checkConfig: func(t *testing.T, cfg *sarama.Config) {
				assert.Equal(t, "/etc/krb5.conf", cfg.Net.SASL.GSSAPI.KerberosConfigPath)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := sarama.NewConfig()
			ctx := context.Background()

			err := tc.kerberos.ApplySarama(ctx, cfg)
			require.NoError(t, err)

			if tc.checkConfig != nil {
				tc.checkConfig(t, cfg)
			}
		})
	}
}

func TestKerberosKgoSupported(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	kerberos := &saslKerberos{
		serviceName: "kafka",
	}

	ctx := context.Background()
	opts, err := kerberos.KgoOpts(ctx)
	require.NoError(t, err)
	require.NotNil(t, opts)
	require.Len(t, opts, 1) // Should contain one SASL option

	// Test with keytab configuration
	tmpDir := t.TempDir()
	keytabPath := filepath.Join(tmpDir, "test.keytab")
	err = os.WriteFile(keytabPath, []byte("fake keytab content"), 0600)
	require.NoError(t, err)

	kerberosWithKeytab := &saslKerberos{
		serviceName: "kafka",
		principal:   "user@EXAMPLE.COM",
		keytabPath:  keytabPath,
	}

	opts, err = kerberosWithKeytab.KgoOpts(ctx)
	require.NoError(t, err)
	require.NotNil(t, opts)
	require.Len(t, opts, 1) // Should contain one SASL option
}
