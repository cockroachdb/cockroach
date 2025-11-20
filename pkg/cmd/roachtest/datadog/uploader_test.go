// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package datadog

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/require"
)

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    string
		expectError bool
	}{
		{
			name:     "valid timestamp",
			input:    "2025/11/12 07:19:40",
			expected: "2025-11-12T07:19:40Z",
		},
		{
			name:     "midnight timestamp",
			input:    "2025/01/01 00:00:00",
			expected: "2025-01-01T00:00:00Z",
		},
		{
			name:     "end of day timestamp",
			input:    "2025/12/31 23:59:59",
			expected: "2025-12-31T23:59:59Z",
		},
		{
			name:        "invalid format - missing seconds",
			input:       "2025/11/12 07:19",
			expectError: true,
		},
		{
			name:        "invalid format - wrong separator",
			input:       "2025-11-12 07:19:40",
			expectError: true,
		},
		{
			name:        "invalid date",
			input:       "2025/13/32 07:19:40",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTimestamp(tt.input)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestCloudToDatadogTag(t *testing.T) {
	tests := []struct {
		name     string
		cloud    spec.Cloud
		expected string
	}{
		{"gce", spec.GCE, "gce"},
		{"aws", spec.AWS, "aws"},
		{"azure", spec.Azure, "azure"},
		{"ibm", spec.IBM, "ibm"},
		{"local", spec.Local, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cloudToDatadogTag(tt.cloud)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildPlatformTag(t *testing.T) {
	tests := []struct {
		name     string
		os       string
		arch     vm.CPUArch
		expected string
	}{
		{
			name:     "linux amd64",
			os:       "linux",
			arch:     vm.ArchAMD64,
			expected: "linux-amd64",
		},
		{
			name:     "linux arm64",
			os:       "linux",
			arch:     vm.ArchARM64,
			expected: "linux-arm64",
		},
		{
			name:     "linux fips",
			os:       "linux",
			arch:     vm.ArchFIPS,
			expected: "linux-fips",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildPlatformTag(tt.os, tt.arch)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestParseLogFile(t *testing.T) {
	// Dummy logger for parseLogFile
	l, err := logger.RootLogger(os.DevNull, logger.NoTee)
	require.NoError(t, err)
	defer l.Close()

	testLogPath := filepath.Join("testdata", "test_log_example")

	cfg := UploadConfig{
		TestName: "acceptance/event-log",
		Owner:    "test-eng",
		Cloud:    "gce",
		Platform: "linux-amd64",
		Host:     "test-host",
		Version:  "master",
		BuildID:  "12345",
		Cluster:  "test-cluster",
	}

	entries, err := parseLogFile(l, testLogPath, cfg)
	require.NoError(t, err)

	// The test log has 56 lines, but only well-formed lines should be parsed
	// Lines without timestamps: 9-10, 21-24, 28-29, 36, 42, 46
	// Total Skip Count: 11
	require.Equal(t, len(entries), 55-11, "should skip non well-formatted lines")

	// Verify the first entry
	require.Equal(t, "2025/11/12 07:19:40 test_impl.go:197: Runtime assertions enabled", entries[0].Message)

	// Check that entry fields are set correctly
	require.Equal(t, defaultDDSource, *entries[0].Ddsource)
	require.Equal(t, defaultService, *entries[0].Service)
	require.Equal(t, cfg.Host, *entries[0].Hostname)

	// Verify ddtags contains all expected tags
	ddtags := *entries[0].Ddtags
	require.Contains(t, ddtags, "env:"+defaultEnv)
	require.Contains(t, ddtags, "version:"+cfg.Version)
	require.Contains(t, ddtags, "platform:"+cfg.Platform)
	require.Contains(t, ddtags, "cloud:"+cfg.Cloud)
	require.Contains(t, ddtags, "name:"+cfg.TestName)
	require.Contains(t, ddtags, "cluster:"+cfg.Cluster)
	require.Contains(t, ddtags, "build_id:"+cfg.BuildID)
	require.Contains(t, ddtags, "owner:"+cfg.Owner)

	// Verify timestamp is set in AdditionalProperties
	require.NotNil(t, entries[0].AdditionalProperties)
	timestamp, ok := entries[0].AdditionalProperties["timestamp"]
	require.True(t, ok, "timestamp should be in AdditionalProperties")
	require.Equal(t, "2025-11-12T07:19:40Z", timestamp)
}

func TestParseLogFileEmptyFile(t *testing.T) {
	// Dummy logger for parseLogFile
	l, err := logger.RootLogger(os.DevNull, logger.NoTee)
	require.NoError(t, err)
	defer l.Close()

	// Create an empty test file
	logDir := t.TempDir()
	emptyFile := filepath.Join(logDir, "empty.log")
	err = os.WriteFile(emptyFile, []byte(""), 0644)
	require.NoError(t, err)

	cfg := UploadConfig{
		TestName: "test",
		Cloud:    "gce",
		Platform: "linux-amd64",
		Host:     "test-host",
		Version:  "master",
	}

	_, err = parseLogFile(l, emptyFile, cfg)
	require.Error(t, err)
}

func TestParseLogFileNoValidTimestamps(t *testing.T) {
	// Dummy logger for parseLogFile
	l, err := logger.RootLogger(os.DevNull, logger.NoTee)
	require.NoError(t, err)
	defer l.Close()

	// Create a test file with no valid timestamps
	logDir := t.TempDir()
	invalidFile := filepath.Join(logDir, "invalid.log")
	content := `this is a log line without timestamp
another line without timestamp
yet another line`
	err = os.WriteFile(invalidFile, []byte(content), 0644)
	require.NoError(t, err)

	cfg := UploadConfig{
		TestName: "test",
		Cloud:    "gce",
		Platform: "linux-amd64",
		Host:     "test-host",
		Version:  "master",
	}

	_, err = parseLogFile(l, invalidFile, cfg)
	require.Error(t, err)
}

func TestParseLogFileNonexistent(t *testing.T) {
	// Dummy logger for parseLogFile
	l, err := logger.RootLogger(os.DevNull, logger.NoTee)
	require.NoError(t, err)
	defer l.Close()

	cfg := UploadConfig{
		TestName: "test",
		Cloud:    "gce",
		Platform: "linux-amd64",
		Host:     "test-host",
		Version:  "master",
	}

	_, err = parseLogFile(l, "/nonexistent/path/test.log", cfg)
	require.Error(t, err)
}

func TestNewDatadogContextMissingCredentials(t *testing.T) {
	// Save and restore env vars
	originalAPIKey := os.Getenv("DD_API_KEY")
	originalDatadogAPIKey := os.Getenv("DATADOG_API_KEY")
	originalDatadogSite := os.Getenv("DD_SITE")
	defer func() {
		_ = os.Setenv("DD_API_KEY", originalAPIKey)
		_ = os.Setenv("DATADOG_API_KEY", originalDatadogAPIKey)
		_ = os.Setenv("DD_SITE", originalDatadogSite)
	}()
	_ = os.Unsetenv("DD_API_KEY")
	_ = os.Unsetenv("DATADOG_API_KEY")
	_ = os.Unsetenv("DD_SITE")

	ctx := context.Background()
	_, err := newDatadogContext(ctx)
	require.Error(t, err)
}

func TestNewDatadogContextDDKeySet(t *testing.T) {
	// Save and restore env vars
	originalAPIKey := os.Getenv("DD_API_KEY")
	originalDatadogAPIKey := os.Getenv("DATADOG_API_KEY")
	originalDatadogSite := os.Getenv("DD_SITE")
	defer func() {
		_ = os.Setenv("DD_API_KEY", originalAPIKey)
		_ = os.Setenv("DATADOG_API_KEY", originalDatadogAPIKey)
		_ = os.Setenv("DD_SITE", originalDatadogSite)
	}()
	_ = os.Setenv("DD_API_KEY", "1234")
	_ = os.Unsetenv("DATADOG_API_KEY")
	_ = os.Unsetenv("DD_SITE")
	ctx := context.Background()
	datadogCtx, err := newDatadogContext(ctx)
	require.NoError(t, err)
	apiKeyMap, ok := datadogCtx.Value(datadog.ContextAPIKeys).(map[string]datadog.APIKey)
	require.True(t, ok)
	require.Equal(t, "1234", apiKeyMap["apiKeyAuth"].Key)
}

func TestNewDatadogContextDatadogFlagSet(t *testing.T) {
	// Save and restore env vars
	originalAPIKey := os.Getenv("DD_API_KEY")
	originalDatadogAPIKey := os.Getenv("DATADOG_API_KEY")
	originalDatadogSite := os.Getenv("DD_SITE")
	originalFlag := roachtestflags.DatadogAPIKey
	defer func() {
		_ = os.Setenv("DD_API_KEY", originalAPIKey)
		_ = os.Setenv("DATADOG_API_KEY", originalDatadogAPIKey)
		_ = os.Setenv("DD_SITE", originalDatadogSite)
		roachtestflags.DatadogAPIKey = originalFlag
	}()
	_ = os.Unsetenv("DD_API_KEY")
	_ = os.Unsetenv("DATADOG_API_KEY")
	_ = os.Unsetenv("DD_SITE")
	roachtestflags.DatadogAPIKey = "1234"
	ctx := context.Background()
	datadogCtx, err := newDatadogContext(ctx)
	require.NoError(t, err)
	apiKeyMap, ok := datadogCtx.Value(datadog.ContextAPIKeys).(map[string]datadog.APIKey)
	require.True(t, ok)
	require.Equal(t, "1234", apiKeyMap["apiKeyAuth"].Key)
}

func TestNewDatadogContextDatadogKeySet(t *testing.T) {
	// Save and restore env vars
	originalAPIKey := os.Getenv("DD_API_KEY")
	originalDatadogAPIKey := os.Getenv("DATADOG_API_KEY")
	originalDatadogSite := os.Getenv("DD_SITE")
	defer func() {
		_ = os.Setenv("DD_API_KEY", originalAPIKey)
		_ = os.Setenv("DATADOG_API_KEY", originalDatadogAPIKey)
		_ = os.Setenv("DD_SITE", originalDatadogSite)
	}()
	_ = os.Unsetenv("DD_API_KEY")
	_ = os.Setenv("DATADOG_API_KEY", "1234")
	_ = os.Unsetenv("DD_SITE")
	ctx := context.Background()
	datadogCtx, err := newDatadogContext(ctx)
	require.NoError(t, err)
	apiKeyMap, ok := datadogCtx.Value(datadog.ContextAPIKeys).(map[string]datadog.APIKey)
	require.True(t, ok)
	require.Equal(t, "1234", apiKeyMap["apiKeyAuth"].Key)
}

func TestNewDatadogContextDatadogSiteSet(t *testing.T) {
	// Save and restore env vars
	originalAPIKey := os.Getenv("DD_API_KEY")
	originalDatadogAPIKey := os.Getenv("DATADOG_API_KEY")
	originalDatadogSite := os.Getenv("DD_SITE")
	defer func() {
		_ = os.Setenv("DD_API_KEY", originalAPIKey)
		_ = os.Setenv("DATADOG_API_KEY", originalDatadogAPIKey)
		_ = os.Setenv("DD_SITE", originalDatadogSite)
	}()
	_ = os.Unsetenv("DD_API_KEY")
	_ = os.Setenv("DATADOG_API_KEY", "1234")
	_ = os.Setenv("DD_SITE", "test-site")
	ctx := context.Background()
	datadogCtx, err := newDatadogContext(ctx)
	require.NoError(t, err)
	serverVarMap, ok := datadogCtx.Value(datadog.ContextServerVariables).(map[string]string)
	require.True(t, ok)
	require.Equal(t, "test-site", serverVarMap["site"])
	apiKeyMap, ok := datadogCtx.Value(datadog.ContextAPIKeys).(map[string]datadog.APIKey)
	require.True(t, ok)
	require.Equal(t, "1234", apiKeyMap["apiKeyAuth"].Key)
}
