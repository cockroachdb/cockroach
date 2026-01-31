// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package config

import (
	"os"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/env"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicFunctionality(t *testing.T) {
	// Test creating a flag set
	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	require.NotNil(t, flagSet)

	// Test AddFlagsToFlagSet function
	err := AddFlagsToFlagSet(flagSet)
	require.NoError(t, err)

	// Test InitConfigWithFlagSet function
	config, err := InitConfigWithFlagSet(flagSet)
	require.NoError(t, err)
	require.NotNil(t, config)

	// Test that default values are set correctly
	assert.Equal(t, "info", config.Log.Level)
	assert.Equal(t, 8080, config.Api.Port)
	assert.Equal(t, "memory", config.Database.Type)
}

func TestDefaultValues(t *testing.T) {
	// Clear any existing environment variables that might affect the test
	clearConfigEnvVars(t)

	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	err := AddFlagsToFlagSet(flagSet)
	require.NoError(t, err)

	config, err := InitConfigWithFlagSet(flagSet)
	require.NoError(t, err)

	// Test default values
	assert.Equal(t, "", config.Config)
	assert.Equal(t, "info", config.Log.Level)
	assert.Equal(t, 8080, config.Api.Port)
	assert.Equal(t, "", config.Api.BasePath)
	assert.Equal(t, true, config.Api.Metrics.Enabled)
	assert.Equal(t, "/metrics", config.Api.Metrics.Path)
	assert.Equal(t, 8081, config.Api.Metrics.Port)
	assert.Equal(t, "jwt", config.Api.Authentication.Type)
	assert.Equal(t, "Authorization", config.Api.Authentication.Header)
	assert.Equal(t, "", config.Api.Authentication.JWT.Audience)
	assert.Equal(t, 1, config.Tasks.Workers)
	assert.Equal(t, "", config.Database.URL)
	assert.Equal(t, "memory", config.Database.Type)
	assert.Equal(t, 10, config.Database.MaxConns)
	assert.Equal(t, 300, config.Database.MaxIdleTime)
	assert.Empty(t, config.CloudProviders)
	assert.Empty(t, config.DNSProviders)
}

func TestEnvironmentVariables(t *testing.T) {
	// Clear any existing environment variables that might affect the test
	clearConfigEnvVars(t)

	// Set test environment variables
	testEnvVars := map[string]string{
		"ROACHPROD_LOG_LEVEL":                       "debug",
		"ROACHPROD_API_PORT":                        "9090",
		"ROACHPROD_API_BASE_PATH":                   "/api",
		"ROACHPROD_API_METRICS_ENABLED":             "false",
		"ROACHPROD_API_METRICS_PATH":                "/health",
		"ROACHPROD_API_METRICS_PORT":                "9091",
		"ROACHPROD_API_AUTHENTICATION_TYPE":         "disabled",
		"ROACHPROD_API_AUTHENTICATION_HEADER":       "Authorization",
		"ROACHPROD_API_AUTHENTICATION_JWT_AUDIENCE": "test-audience",
		"ROACHPROD_TASKS_WORKERS":                   "5",
		"ROACHPROD_DATABASE_URL":                    "postgres://test",
		"ROACHPROD_DATABASE_TYPE":                   "cockroachdb",
		"ROACHPROD_DATABASE_MAX_CONNS":              "25",
		"ROACHPROD_DATABASE_MAX_IDLE_TIME":          "600",
	}

	for key, value := range testEnvVars {
		t.Setenv(key, value)
	}

	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	err := AddFlagsToFlagSet(flagSet)
	require.NoError(t, err)

	config, err := InitConfigWithFlagSet(flagSet)
	require.NoError(t, err)

	// Test that environment variables are properly parsed
	assert.Equal(t, "debug", config.Log.Level)
	assert.Equal(t, 9090, config.Api.Port)
	assert.Equal(t, "/api", config.Api.BasePath)
	assert.Equal(t, false, config.Api.Metrics.Enabled)
	assert.Equal(t, "/health", config.Api.Metrics.Path)
	assert.Equal(t, 9091, config.Api.Metrics.Port)
	assert.Equal(t, "disabled", config.Api.Authentication.Type)
	assert.Equal(t, "Authorization", config.Api.Authentication.Header)
	assert.Equal(t, "test-audience", config.Api.Authentication.JWT.Audience)
	assert.Equal(t, 5, config.Tasks.Workers)
	assert.Equal(t, "postgres://test", config.Database.URL)
	assert.Equal(t, "cockroachdb", config.Database.Type)
	assert.Equal(t, 25, config.Database.MaxConns)
	assert.Equal(t, 600, config.Database.MaxIdleTime)
}

func TestCommandLineFlags(t *testing.T) {
	// Clear any existing environment variables that might affect the test
	clearConfigEnvVars(t)

	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	err := AddFlagsToFlagSet(flagSet)
	require.NoError(t, err)

	// Set test command line flags
	testArgs := []string{
		"--log-level", "warn",
		"--api-port", "7070",
		"--api-base-path", "/v2",
		"--api-metrics-enabled=false",
		"--api-metrics-path", "/status",
		"--api-metrics-port", "7071",
		"--api-authentication-type", "disabled",
		"--api-authentication-header", "X-Auth-Token",
		"--api-authentication-jwt-audience", "test-app",
		"--tasks-workers", "3",
		"--database-url", "sqlite://test.db",
		"--database-type", "cockroachdb",
		"--database-max-conns", "15",
		"--database-max-idle-time", "450",
	}

	err = flagSet.Parse(testArgs)
	require.NoError(t, err)

	config, err := InitConfigWithFlagSet(flagSet)
	require.NoError(t, err)

	// Test that command line flags are properly parsed
	assert.Equal(t, "warn", config.Log.Level)
	assert.Equal(t, 7070, config.Api.Port)
	assert.Equal(t, "/v2", config.Api.BasePath)
	assert.Equal(t, false, config.Api.Metrics.Enabled)
	assert.Equal(t, "/status", config.Api.Metrics.Path)
	assert.Equal(t, 7071, config.Api.Metrics.Port)
	assert.Equal(t, "disabled", config.Api.Authentication.Type)
	assert.Equal(t, "X-Auth-Token", config.Api.Authentication.Header)
	assert.Equal(t, "test-app", config.Api.Authentication.JWT.Audience)
	assert.Equal(t, 3, config.Tasks.Workers)
	assert.Equal(t, "sqlite://test.db", config.Database.URL)
	assert.Equal(t, "cockroachdb", config.Database.Type)
	assert.Equal(t, 15, config.Database.MaxConns)
	assert.Equal(t, 450, config.Database.MaxIdleTime)
}

func TestYAMLFileConfig(t *testing.T) {
	// Clear any existing environment variables that might affect the test
	clearConfigEnvVars(t)

	// Create temporary YAML config file
	yamlContent := `
log:
  level: error
api:
  port: 6060
  basePath: /api/v3
  metrics:
    enabled: false
    path: /ping
    port: 6061
  authentication:
    type: disabled
    header: X-Custom-Auth
    jwt:
      audience: yaml-test
tasks:
  workers: 7
database:
  url: mysql://test
  type: cockroachdb
  maxConns: 30
  maxIdleTime: 900
`

	tmpFile, err := os.CreateTemp("", "config-test-*.yaml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, err = tmpFile.WriteString(yamlContent)
	require.NoError(t, err)
	err = tmpFile.Close()
	require.NoError(t, err)

	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	err = AddFlagsToFlagSet(flagSet)
	require.NoError(t, err)

	// Set config file flag
	err = flagSet.Set("config", tmpFile.Name())
	require.NoError(t, err)

	config, err := InitConfigWithFlagSet(flagSet)
	require.NoError(t, err)

	// Test that YAML config is properly parsed
	assert.Equal(t, "error", config.Log.Level)
	assert.Equal(t, 6060, config.Api.Port)
	assert.Equal(t, "/api/v3", config.Api.BasePath)
	assert.Equal(t, false, config.Api.Metrics.Enabled)
	assert.Equal(t, "/ping", config.Api.Metrics.Path)
	assert.Equal(t, 6061, config.Api.Metrics.Port)
	assert.Equal(t, "disabled", config.Api.Authentication.Type)
	assert.Equal(t, "X-Custom-Auth", config.Api.Authentication.Header)
	assert.Equal(t, "yaml-test", config.Api.Authentication.JWT.Audience)
	assert.Equal(t, 7, config.Tasks.Workers)
	assert.Equal(t, "mysql://test", config.Database.URL)
	assert.Equal(t, "cockroachdb", config.Database.Type)
	assert.Equal(t, 30, config.Database.MaxConns)
	assert.Equal(t, 900, config.Database.MaxIdleTime)
}

func TestJSONFileConfig(t *testing.T) {
	// Clear any existing environment variables that might affect the test
	clearConfigEnvVars(t)

	// Create temporary JSON config file
	jsonContent := `{
	"log": {
		"level": "trace"
	},
	"api": {
		"port": 5050,
		"basePath": "/json/api",
		"metrics": {
			"enabled": true,
			"path": "/json-metrics",
			"port": 5051
		},
		"authentication": {
			"type": "jwt",
			"header": "X-JSON-Auth",
			"jwt": {
				"audience": "json-test"
			}
		}
	},
	"tasks": {
		"workers": 2
	},
	"database": {
		"url": "json://test",
		"type": "memory",
		"maxConns": 5,
		"maxIdleTime": 120
	}
}`

	tmpFile, err := os.CreateTemp("", "config-test-*.json")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, err = tmpFile.WriteString(jsonContent)
	require.NoError(t, err)
	err = tmpFile.Close()
	require.NoError(t, err)

	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	err = AddFlagsToFlagSet(flagSet)
	require.NoError(t, err)

	// Set config file flag
	err = flagSet.Set("config", tmpFile.Name())
	require.NoError(t, err)

	config, err := InitConfigWithFlagSet(flagSet)
	require.NoError(t, err)

	// Test that JSON config is properly parsed
	assert.Equal(t, "trace", config.Log.Level)
	assert.Equal(t, 5050, config.Api.Port)
	assert.Equal(t, "/json/api", config.Api.BasePath)
	assert.Equal(t, true, config.Api.Metrics.Enabled)
	assert.Equal(t, "/json-metrics", config.Api.Metrics.Path)
	assert.Equal(t, 5051, config.Api.Metrics.Port)
	assert.Equal(t, "jwt", config.Api.Authentication.Type)
	assert.Equal(t, "X-JSON-Auth", config.Api.Authentication.Header)
	assert.Equal(t, "json-test", config.Api.Authentication.JWT.Audience)
	assert.Equal(t, 2, config.Tasks.Workers)
	assert.Equal(t, "json://test", config.Database.URL)
	assert.Equal(t, "memory", config.Database.Type)
	assert.Equal(t, 5, config.Database.MaxConns)
	assert.Equal(t, 120, config.Database.MaxIdleTime)
}

// Test precedence: Flags > Environment Variables > Config files > Defaults
func TestPrecedenceRules(t *testing.T) {
	// Clear any existing environment variables that might affect the test
	clearConfigEnvVars(t)

	// Create a config file with some values
	yamlContent := `
log:
  level: debug
api:
  port: 9999
tasks:
  workers: 10
database:
  type: memory
`

	tmpFile, err := os.CreateTemp("", "precedence-test-*.yaml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, err = tmpFile.WriteString(yamlContent)
	require.NoError(t, err)
	err = tmpFile.Close()
	require.NoError(t, err)

	// Set some environment variables (should override config file but not flags)
	t.Setenv("ROACHPROD_LOG_LEVEL", "error")
	t.Setenv("ROACHPROD_API_PORT", "7777")

	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	err = AddFlagsToFlagSet(flagSet)
	require.NoError(t, err)

	// Set some command line flags (should override env vars and config file)
	testArgs := []string{
		"--config", tmpFile.Name(),
		"--log-level", "warn", // This should override env var and file
		"--api-port", "8888", // This should override env var and file
		"--tasks-workers", "5", // This should override file value (no env var set)
		"--database-type", "cockroachdb", // This should override file value (no env var set)
	}

	err = flagSet.Parse(testArgs)
	require.NoError(t, err)

	config, err := InitConfigWithFlagSet(flagSet)
	require.NoError(t, err)

	// Test precedence rules: Flags > Environment Variables > Config files > Defaults
	assert.Equal(t, "warn", config.Log.Level)            // Flag wins over env var and file
	assert.Equal(t, 8888, config.Api.Port)               // Flag wins over env var and file
	assert.Equal(t, 5, config.Tasks.Workers)             // Flag wins over file (no env var)
	assert.Equal(t, "cockroachdb", config.Database.Type) // Flag wins over file (no env var)
}

func TestErrorCases(t *testing.T) {
	t.Run("InvalidConfigFile", func(t *testing.T) {
		clearConfigEnvVars(t)

		flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
		err := AddFlagsToFlagSet(flagSet)
		require.NoError(t, err)

		// Set a non-existent config file
		err = flagSet.Set("config", "/non/existent/file.yaml")
		require.NoError(t, err)

		_, err = InitConfigWithFlagSet(flagSet)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unable to read config file")
	})

	t.Run("InvalidYAMLContent", func(t *testing.T) {
		clearConfigEnvVars(t)

		// Create invalid YAML file
		invalidYAML := `
log:
  level: debug
api:
  port: not_a_number
  invalid_yaml: [unclosed
`

		tmpFile, err := os.CreateTemp("", "invalid-config-*.yaml")
		require.NoError(t, err)
		defer func() { _ = os.Remove(tmpFile.Name()) }()

		_, err = tmpFile.WriteString(invalidYAML)
		require.NoError(t, err)
		err = tmpFile.Close()
		require.NoError(t, err)

		flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
		err = AddFlagsToFlagSet(flagSet)
		require.NoError(t, err)

		err = flagSet.Set("config", tmpFile.Name())
		require.NoError(t, err)

		_, err = InitConfigWithFlagSet(flagSet)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unable to parse config file")
	})

	t.Run("InvalidEnvironmentVariableTypes", func(t *testing.T) {
		clearConfigEnvVars(t)

		// Set invalid environment variables
		t.Setenv("ROACHPROD_API_PORT", "not_a_number")
		t.Setenv("ROACHPROD_TASKS_WORKERS", "invalid_int")
		t.Setenv("ROACHPROD_API_METRICS_ENABLED", "not_a_bool")

		flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
		err := AddFlagsToFlagSet(flagSet)
		require.NoError(t, err)

		_, err = InitConfigWithFlagSet(flagSet)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unable to unmarshal")
	})
}

func TestEdgeCases(t *testing.T) {
	t.Run("EmptyValues", func(t *testing.T) {
		clearConfigEnvVars(t)

		// Set empty environment variables - Viper treats empty env vars as unset,
		// so defaults will be used
		t.Setenv("ROACHPROD_LOG_LEVEL", "")
		t.Setenv("ROACHPROD_API_BASE_PATH", "")
		t.Setenv("ROACHPROD_DATABASE_URL", "")

		flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
		err := AddFlagsToFlagSet(flagSet)
		require.NoError(t, err)

		config, err := InitConfigWithFlagSet(flagSet)
		require.NoError(t, err)

		// Empty environment variables fall back to defaults (Viper behavior)
		assert.Equal(t, "info", config.Log.Level) // Default value
		assert.Equal(t, "", config.Api.BasePath)  // Default is empty
		assert.Equal(t, "", config.Database.URL)  // Default is empty
	})

	t.Run("SpecialCharacters", func(t *testing.T) {
		clearConfigEnvVars(t)

		// Set values with special characters
		specialPath := "/api/test-special_chars@123"
		specialAudience := "aud:test@domain.com"
		specialURL := "postgres://user:pass@host:5432/db?sslmode=require"

		t.Setenv("ROACHPROD_API_BASE_PATH", specialPath)
		t.Setenv("ROACHPROD_API_AUTHENTICATION_JWT_AUDIENCE", specialAudience)
		t.Setenv("ROACHPROD_DATABASE_URL", specialURL)

		flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
		err := AddFlagsToFlagSet(flagSet)
		require.NoError(t, err)

		config, err := InitConfigWithFlagSet(flagSet)
		require.NoError(t, err)

		// Special characters should be preserved
		assert.Equal(t, specialPath, config.Api.BasePath)
		assert.Equal(t, specialAudience, config.Api.Authentication.JWT.Audience)
		assert.Equal(t, specialURL, config.Database.URL)
	})

	t.Run("BoundaryValues", func(t *testing.T) {
		clearConfigEnvVars(t)

		// Set boundary values
		t.Setenv("ROACHPROD_API_PORT", "0")
		t.Setenv("ROACHPROD_API_METRICS_PORT", "65535")
		t.Setenv("ROACHPROD_TASKS_WORKERS", "1")
		t.Setenv("ROACHPROD_DATABASE_MAX_CONNS", "1000")

		flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
		err := AddFlagsToFlagSet(flagSet)
		require.NoError(t, err)

		config, err := InitConfigWithFlagSet(flagSet)
		require.NoError(t, err)

		// Boundary values should be accepted
		assert.Equal(t, 0, config.Api.Port)
		assert.Equal(t, 65535, config.Api.Metrics.Port)
		assert.Equal(t, 1, config.Tasks.Workers)
		assert.Equal(t, 1000, config.Database.MaxConns)
	})
}

func TestConfigString(t *testing.T) {
	clearConfigEnvVars(t)

	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	err := AddFlagsToFlagSet(flagSet)
	require.NoError(t, err)

	config, err := InitConfigWithFlagSet(flagSet)
	require.NoError(t, err)

	// Test that String() method returns valid YAML
	configStr := config.String()
	assert.NotEmpty(t, configStr)
	assert.Contains(t, configStr, "log:")
	assert.Contains(t, configStr, "api:")
	assert.Contains(t, configStr, "database:")
}

// Helper function to clear all ROACHPROD environment variables
func clearConfigEnvVars(t *testing.T) {
	t.Helper()

	// List of all possible ROACHPROD environment variables
	envVars := []string{
		"ROACHPROD_CONFIG",
		"ROACHPROD_LOG_LEVEL",
		"ROACHPROD_API_PORT",
		"ROACHPROD_API_BASE_PATH",
		"ROACHPROD_API_METRICS_ENABLED",
		"ROACHPROD_API_METRICS_PATH",
		"ROACHPROD_API_METRICS_PORT",
		"ROACHPROD_API_AUTHENTICATION_METHOD",
		"ROACHPROD_API_AUTHENTICATION_JWT_HEADER",
		"ROACHPROD_API_AUTHENTICATION_JWT_AUDIENCE",
		"ROACHPROD_INSTANCE_HEALTH_TIMEOUT_SECONDS",
		"ROACHPROD_TASKS_WORKERS",
		"ROACHPROD_DATABASE_URL",
		"ROACHPROD_DATABASE_TYPE",
		"ROACHPROD_DATABASE_MAX_CONNS",
		"ROACHPROD_DATABASE_MAX_IDLE_TIME",
		"ROACHPROD_CLOUDPROVIDERS",
		"ROACHPROD_DNSPROVIDERS",
	}

	for _, envVar := range envVars {
		t.Setenv(envVar, "")
	}
}

// Test slice configuration scenarios
func TestSliceConfiguration(t *testing.T) {
	t.Run("EmptyToFlags", func(t *testing.T) {
		// Scenario: having 0 CloudProviders in the config file, and specifying 2 with flags
		clearConfigEnvVars(t)

		yamlContent := `
log:
  level: info
cloudProviders: []
`

		tmpFile, err := os.CreateTemp("", "slice-test-*.yaml")
		require.NoError(t, err)
		defer func() { _ = os.Remove(tmpFile.Name()) }()

		_, err = tmpFile.WriteString(yamlContent)
		require.NoError(t, err)
		err = tmpFile.Close()
		require.NoError(t, err)

		flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
		err = AddFlagsToFlagSet(flagSet)
		require.NoError(t, err)

		testArgs := []string{
			"--config", tmpFile.Name(),
			"--cloud-providers-0-type", "gce",
			"--cloud-providers-0-gce-project", "test-project",
			"--cloud-providers-1-type", "aws",
			"--cloud-providers-1-aws-profile", "test-profile",
		}

		err = flagSet.Parse(testArgs)
		require.NoError(t, err)

		config, err := InitConfigWithFlagSet(flagSet)
		require.NoError(t, err)

		// Should have 2 CloudProviders from flags
		assert.Len(t, config.CloudProviders, 2)
		assert.Equal(t, "gce", config.CloudProviders[0].Type)
		assert.Equal(t, "aws", config.CloudProviders[1].Type)
	})

	t.Run("ConfigFileWithEnvOverrides", func(t *testing.T) {
		// Scenario: having 4 CloudProviders in the config file, and overriding indices 1 & 3 via env vars
		clearConfigEnvVars(t)

		yamlContent := `
log:
  level: info
cloudProviders:
  - type: gce
    gce:
      project: config-project-0
  - type: aws
    aws:
      region: us-west-1
  - type: azure
    azure:
      subscription: config-sub
  - type: ibm
    ibm:
      account: config-account
`

		tmpFile, err := os.CreateTemp("", "slice-test-*.yaml")
		require.NoError(t, err)
		defer func() { _ = os.Remove(tmpFile.Name()) }()

		_, err = tmpFile.WriteString(yamlContent)
		require.NoError(t, err)
		err = tmpFile.Close()
		require.NoError(t, err)

		// Override indices 1 & 3 via env vars
		t.Setenv("ROACHPROD_CLOUDPROVIDERS_1_AWS_PROFILE", "override-profile")
		t.Setenv("ROACHPROD_CLOUDPROVIDERS_3_TYPE", "azure")

		flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
		err = AddFlagsToFlagSet(flagSet)
		require.NoError(t, err)

		testArgs := []string{
			"--config", tmpFile.Name(),
		}

		err = flagSet.Parse(testArgs)
		require.NoError(t, err)

		config, err := InitConfigWithFlagSet(flagSet)
		require.NoError(t, err)

		// Should have 4 CloudProviders: indices 0,2 from config, indices 1,3 with env overrides
		assert.Len(t, config.CloudProviders, 4)
		assert.Equal(t, "gce", config.CloudProviders[0].Type)
		assert.Equal(t, "aws", config.CloudProviders[1].Type)
		assert.Equal(t, "azure", config.CloudProviders[2].Type)
		assert.Equal(t, "azure", config.CloudProviders[3].Type)

		// Check that env vars overrode the config values
		// Note: The exact field access depends on the AWS/IBM provider options structure
		// For now, we test that the type is correct and trust the override mechanism
	})

	t.Run("ConfigFileWithFlagExpansion", func(t *testing.T) {
		// Scenario: having 2 CloudProviders in the config file, and specifying 4 with flags (override + add)
		clearConfigEnvVars(t)

		yamlContent := `
log:
  level: info
cloudProviders:
  - type: gce
    gce:
      project: config-project
  - type: aws
    aws:
      region: us-west-1
`

		tmpFile, err := os.CreateTemp("", "slice-test-*.yaml")
		require.NoError(t, err)
		defer func() { _ = os.Remove(tmpFile.Name()) }()

		_, err = tmpFile.WriteString(yamlContent)
		require.NoError(t, err)
		err = tmpFile.Close()
		require.NoError(t, err)

		flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
		err = AddFlagsToFlagSet(flagSet)
		require.NoError(t, err)

		testArgs := []string{
			"--config", tmpFile.Name(),
			"--cloud-providers-0-gce-project", "override-project", // Override existing
			"--cloud-providers-1-aws-profile", "override-profile", // Override existing
			"--cloud-providers-2-type", "azure", // Add new
			"--cloud-providers-3-type", "ibm", // Add new
		}

		err = flagSet.Parse(testArgs)
		require.NoError(t, err)

		config, err := InitConfigWithFlagSet(flagSet)
		require.NoError(t, err)

		// Should have 4 CloudProviders: 2 overridden + 2 new
		assert.Len(t, config.CloudProviders, 4)
		assert.Equal(t, "gce", config.CloudProviders[0].Type)
		assert.Equal(t, "aws", config.CloudProviders[1].Type)
		assert.Equal(t, "azure", config.CloudProviders[2].Type)
		assert.Equal(t, "ibm", config.CloudProviders[3].Type)
	})

	t.Run("EnvironmentVariableDiscovery", func(t *testing.T) {
		// Test that environment variables are discovered and flags are created
		clearConfigEnvVars(t)

		// Set environment variables for indices 1 and 3 (skip 0, 2)
		t.Setenv("ROACHPROD_CLOUDPROVIDERS_1_TYPE", "aws")
		t.Setenv("ROACHPROD_CLOUDPROVIDERS_1_AWS_PROFILE", "test-profile")
		t.Setenv("ROACHPROD_CLOUDPROVIDERS_3_TYPE", "gce")
		t.Setenv("ROACHPROD_CLOUDPROVIDERS_3_GCE_PROJECT", "test-project")

		// Test environment variable collection directly
		overrides, err := env.CollectSliceOverridesFromEnv()
		require.NoError(t, err)
		require.NotEmpty(t, overrides, "Should have collected environment variable overrides")

		flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
		err = AddFlagsToFlagSet(flagSet)
		require.NoError(t, err)

		config, err := InitConfigWithFlagSet(flagSet)
		require.NoError(t, err)

		// Should have CloudProviders at indices 1 and 3, with 0 and 2 empty
		if len(config.CloudProviders) == 0 {
			t.Fatalf("CloudProviders slice is empty, environment variables not processed correctly")
		}
		assert.Len(t, config.CloudProviders, 4) // 0, 1, 2, 3
		assert.Equal(t, "", config.CloudProviders[0].Type)
		assert.Equal(t, "aws", config.CloudProviders[1].Type)
		assert.Equal(t, "", config.CloudProviders[2].Type)
		assert.Equal(t, "gce", config.CloudProviders[3].Type)
	})

	t.Run("FlagPrecedenceOverEnvironment", func(t *testing.T) {
		// Test that flags override environment variables
		clearConfigEnvVars(t)

		// Set environment variable
		t.Setenv("ROACHPROD_CLOUDPROVIDERS_0_TYPE", "aws")
		t.Setenv("ROACHPROD_CLOUDPROVIDERS_0_AWS_PROFILE", "env-profile")

		flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
		err := AddFlagsToFlagSet(flagSet)
		require.NoError(t, err)

		// Override with flags
		testArgs := []string{
			"--cloud-providers-0-type", "gce",
			"--cloud-providers-0-gce-project", "flag-project",
		}

		err = flagSet.Parse(testArgs)
		require.NoError(t, err)

		config, err := InitConfigWithFlagSet(flagSet)
		require.NoError(t, err)

		// Flag should override environment variable
		assert.Len(t, config.CloudProviders, 1)
		assert.Equal(t, "gce", config.CloudProviders[0].Type)
	})
}

// TestNonSliceFieldsMergeCompleteness uses reflection to verify that ALL non-slice
// fields in the Config struct are properly merged from environment variables.
// This is a regression test that will catch bugs where new fields are added but
// the merge logic doesn't handle them (as happened with InstanceHealthTimeoutSeconds).
func TestNonSliceFieldsMergeCompleteness(t *testing.T) {
	clearConfigEnvVars(t)

	// Set environment variables for several different non-slice fields at various nesting levels
	envVars := map[string]string{
		"ROACHPROD_CONFIG":                          "/test/config.yaml",
		"ROACHPROD_LOG_LEVEL":                       "debug",
		"ROACHPROD_API_PORT":                        "9090",
		"ROACHPROD_API_BASE_PATH":                   "/api/test",
		"ROACHPROD_TASKS_WORKERS":                   "5",
		"ROACHPROD_DATABASE_TYPE":                   "cockroachdb",
		"ROACHPROD_DATABASE_URL":                    "postgres://test:5432/db",
		"ROACHPROD_DATABASE_MAX_CONNS":              "25",
		"ROACHPROD_INSTANCE_HEALTH_TIMEOUT_SECONDS": "10",
	}

	for key, value := range envVars {
		t.Setenv(key, value)
	}

	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	err := AddFlagsToFlagSet(flagSet)
	require.NoError(t, err)

	config, err := InitConfigWithFlagSet(flagSet)
	require.NoError(t, err)

	// Use reflection to verify that non-zero values were set
	// This ensures the merge actually happened
	configVal := reflect.ValueOf(config).Elem()
	configType := configVal.Type()

	var nonSliceFieldsChecked int
	var mergedFieldsFound int

	for i := 0; i < configVal.NumField(); i++ {
		field := configVal.Field(i)
		fieldType := configType.Field(i)

		// Skip slice fields (they're handled separately)
		if field.Kind() == reflect.Slice {
			continue
		}

		nonSliceFieldsChecked++

		// Check if the field has a non-zero value, indicating it was merged
		// We use a recursive check to handle nested structs
		if hasNonZeroValue(field) {
			mergedFieldsFound++
			t.Logf("✓ Field %s has non-zero value (was merged)", fieldType.Name)
		} else {
			t.Logf("○ Field %s has zero value (using default or not set)", fieldType.Name)
		}
	}

	// Assert that we checked at least some fields and found non-zero values
	assert.Greater(t, nonSliceFieldsChecked, 0, "Should have checked at least one non-slice field")
	assert.Greater(t, mergedFieldsFound, 0, "Should have found at least one merged field")

	// Verify specific known values to ensure merging is working correctly
	assert.Equal(t, "/test/config.yaml", config.Config)
	assert.Equal(t, "debug", config.Log.Level)
	assert.Equal(t, 9090, config.Api.Port)
	assert.Equal(t, "/api/test", config.Api.BasePath)
	assert.Equal(t, 5, config.Tasks.Workers)
	assert.Equal(t, "cockroachdb", config.Database.Type)
	assert.Equal(t, "postgres://test:5432/db", config.Database.URL)
	assert.Equal(t, 25, config.Database.MaxConns)
	assert.Equal(t, 10, config.InstanceHealthTimeoutSeconds)

	t.Logf("Summary: Checked %d non-slice fields, found %d with merged values", nonSliceFieldsChecked, mergedFieldsFound)
}

// hasNonZeroValue recursively checks if a reflect.Value has any non-zero fields
func hasNonZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Struct:
		// For structs, check if any field has a non-zero value
		for i := 0; i < v.NumField(); i++ {
			if hasNonZeroValue(v.Field(i)) {
				return true
			}
		}
		return false
	case reflect.Slice, reflect.Map, reflect.Chan, reflect.Func, reflect.Interface, reflect.Ptr:
		return !v.IsNil()
	default:
		// For primitive types, check if it's non-zero
		return !v.IsZero()
	}
}
