// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package datadog

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/require"
)

// To be used throughout unit tests
var unittestLogMetadata = LogMetadata{
	TestName:        "acceptance/event-log",
	Result:          "PASS",
	Duration:        "6.67s",
	Owner:           "test-eng",
	Cloud:           "gce",
	Platform:        "linux-amd64",
	Version:         "master",
	Cluster:         "test-cluster",
	TCHost:          "test-host",
	TCBuildConfName: "Roachtest Nightly - GCE (Bazel)",
	TCBuildNumber:   "12345",
	LogName:         "test.log",
}

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

func TestMakePlatform(t *testing.T) {
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
			result := makePlatform(tt.os, tt.arch)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestMakeResult(t *testing.T) {
	require.Equal(t, testResultPass, makeResult(true))
	require.Equal(t, testResultFail, makeResult(false))
}

func TestParseLogFile(t *testing.T) {
	originalLogMetadata := unittestLogMetadata
	defer func() {
		unittestLogMetadata = originalLogMetadata
		unittestLogMetadata.Tags = map[string]string{} // reset tags explicitly because the above is a shallow copy
	}()
	// Dummy logger for parseLogFile
	l, err := logger.RootLogger(os.DevNull, logger.NoTee)
	require.NoError(t, err)
	defer l.Close()

	unittestLogMetadata.Tags = unittestLogMetadata.makeTags()

	testLogPath := filepath.Join("testdata", "test_log_example")
	file, err := os.Open(testLogPath)
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	validEntries := make([]datadogV2.HTTPLogItem, 0)
	skippedEntries := make([]string, 0)
	for scanner.Scan() {
		line := scanner.Text()
		entry, err := parseLogLine(line, unittestLogMetadata)
		if err != nil {
			skippedEntries = append(skippedEntries, line)
		} else {
			validEntries = append(validEntries, *entry)
		}
	}

	// The test log has 56 lines, but only well-formed lines should be parsed
	// Lines without timestamps: 9-10, 21-24, 28-29, 36, 42, 46
	// Total Skip Count: 11
	require.Equal(t, len(validEntries), 55-11, "should skip non well-formatted lines")

	// Verify the first entry
	require.Equal(t, "2025/11/12 07:19:40 test_impl.go:197: Runtime assertions enabled", validEntries[0].Message)

	// Verify skipped log lines
	require.Contains(t, skippedEntries, "teamcity-20723220-1762931616-18-n4cpu4: initializing certs")
	require.Contains(t, skippedEntries, "SET CLUSTER SETTING")
	require.Len(t, skippedEntries, 11)

	// Check that entry fields are set correctly
	require.Equal(t, defaultDDSource, *validEntries[0].Ddsource)
	require.Equal(t, defaultService, *validEntries[0].Service)
	require.Equal(t, unittestLogMetadata.TCHost, *validEntries[0].Hostname)

	// Verify ddtags contains all expected tags
	ddTags := validEntries[0].Ddtags
	require.Contains(t, *ddTags, "env:"+defaultEnv)
	require.Contains(t, *ddTags, "version:"+unittestLogMetadata.Version)
	require.Contains(t, *ddTags, "platform:"+unittestLogMetadata.Platform)
	require.Contains(t, *ddTags, "cloud:"+unittestLogMetadata.Cloud)
	require.Contains(t, *ddTags, "name:"+unittestLogMetadata.TestName)
	require.Contains(t, *ddTags, "owner:"+unittestLogMetadata.Owner)

	// Verify timestamp is set in AdditionalProperties
	require.NotNil(t, validEntries[0].AdditionalProperties)
	timestamp, ok := validEntries[0].AdditionalProperties["timestamp"]
	require.True(t, ok, "timestamp should be in AdditionalProperties")
	require.Equal(t, "2025-11-12T07:19:40Z", timestamp)
}

func TestMakeTags(t *testing.T) {
	tags := unittestLogMetadata.makeTags()
	require.Equal(t, defaultEnv, tags[envTagName])
	require.Equal(t, unittestLogMetadata.TestName, tags[testNameTagName])
	require.Equal(t, unittestLogMetadata.Owner, tags[ownerTagName])
	require.Equal(t, unittestLogMetadata.Cloud, tags[cloudTagName])
	require.Equal(t, unittestLogMetadata.Platform, tags[platformTagName])
	require.Equal(t, unittestLogMetadata.Version, tags[versionTagName])
	require.Equal(t, unittestLogMetadata.LogName, tags[logFileTagName])
}

func TestFormatTags(t *testing.T) {
	tests := []struct {
		name          string
		tags          map[string]string
		checkContains []string
	}{
		{
			name:          "empty map",
			tags:          map[string]string{},
			checkContains: []string{},
		},
		{
			name:          "single tag",
			tags:          map[string]string{"env": "test"},
			checkContains: []string{"env:test"},
		},
		{
			name: "multiple tags",
			tags: map[string]string{
				"env":     "ci",
				"version": "master",
				"cloud":   "gce",
			},
			checkContains: []string{"env:ci", "version:master", "cloud:gce"},
		},
		{
			name: "tag with empty value",
			tags: map[string]string{
				"env":   "test",
				"empty": "",
			},
			checkContains: []string{"env:test", "empty:"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatTags(tt.tags)
			if len(tt.tags) == 0 {
				require.Equal(t, "", result) // edge case
			} else {
				// Check that all expected tags are present
				for _, part := range tt.checkContains {
					require.Contains(t, result, part)
				}
				// Verify tag string format
				expectedCommas := len(tt.tags) - 1
				actualCommas := strings.Count(result, ",")
				require.Equal(t, expectedCommas, actualCommas, "should have correct number of commas")
				require.Len(t, strings.Split(result, ","), len(tt.tags))
			}
		})
	}
}

func TestNewDatadogContextMissingCredentials(t *testing.T) {
	// Save and restore env vars
	originalAPIKey := os.Getenv(envDatadogAPIKey)
	originalDatadogSite := os.Getenv(envDatadogSite)
	defer func() {
		_ = os.Setenv(envDatadogAPIKey, originalAPIKey)
		_ = os.Setenv(envDatadogSite, originalDatadogSite)
	}()
	_ = os.Unsetenv(envDatadogAPIKey)
	_ = os.Unsetenv(envDatadogSite)

	ctx := context.Background()
	_, err := newDatadogContext(ctx)
	require.Error(t, err)
}

func TestNewDatadogContextDDKeySet(t *testing.T) {
	// Save and restore env vars
	originalAPIKey := os.Getenv(envDatadogAPIKey)
	originalDatadogSite := os.Getenv(envDatadogSite)
	defer func() {
		_ = os.Setenv(envDatadogAPIKey, originalAPIKey)
		_ = os.Setenv(envDatadogSite, originalDatadogSite)
	}()
	_ = os.Setenv(envDatadogAPIKey, "1234")
	_ = os.Unsetenv(envDatadogSite)
	ctx := context.Background()
	datadogCtx, err := newDatadogContext(ctx)
	require.NoError(t, err)
	apiKeyMap, ok := datadogCtx.Value(datadog.ContextAPIKeys).(map[string]datadog.APIKey)
	require.True(t, ok)
	require.Equal(t, "1234", apiKeyMap["apiKeyAuth"].Key)
}

func TestNewDatadogContextDatadogSiteSet(t *testing.T) {
	// Save and restore env vars
	originalAPIKey := os.Getenv(envDatadogAPIKey)
	originalDatadogSite := os.Getenv(envDatadogSite)
	defer func() {
		_ = os.Setenv(envDatadogAPIKey, originalAPIKey)
		_ = os.Setenv(envDatadogSite, originalDatadogSite)
	}()
	_ = os.Setenv(envDatadogAPIKey, "1234")
	_ = os.Setenv(envDatadogSite, "test-site")
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

func TestShouldUploadLogs(t *testing.T) {
	// Save and restore env var and flag
	originalBranch := os.Getenv(envTCBuildBranch)
	originalFlag := roachtestflags.DatadogAlwaysUpload
	defer func() {
		_ = os.Setenv(envTCBuildBranch, originalBranch)
		roachtestflags.DatadogAlwaysUpload = originalFlag
	}()

	tests := []struct {
		name                 string
		branch               string
		datadogAlwaysUpload  bool
		expectedShouldUpload bool
	}{
		{
			name:                 "always upload flag set",
			branch:               "feature-branch",
			datadogAlwaysUpload:  true,
			expectedShouldUpload: true,
		},
		{
			name:                 "master branch",
			branch:               "master",
			datadogAlwaysUpload:  false,
			expectedShouldUpload: true,
		},
		{
			name:                 "release branch",
			branch:               "release-24.1",
			datadogAlwaysUpload:  false,
			expectedShouldUpload: true,
		},
		{
			name:                 "feature branch",
			branch:               "feature-new-stuff",
			datadogAlwaysUpload:  false,
			expectedShouldUpload: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = os.Setenv(envTCBuildBranch, tt.branch)
			roachtestflags.DatadogAlwaysUpload = tt.datadogAlwaysUpload

			result := ShouldUploadLogs()
			require.Equal(t, tt.expectedShouldUpload, result)
		})
	}
}
