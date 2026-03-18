// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package datadog

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

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
	Duration:        "6.67",
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
			result, err := parseRoachtestTimestamp(tt.input)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestParseDmesgTimestamp tests the parsing of an artifacts/1.dmesg.txt file
func TestParseDmesgTimestamp(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    string
		expectError bool
	}{
		{
			name:     "valid dmesg timestamp",
			input:    "Mon Feb  9 07:14:51 2026",
			expected: "2026-02-09T07:14:51Z",
		},
		{
			name:     "double digit day",
			input:    "Wed Dec 25 23:59:59 2025",
			expected: "2025-12-25T23:59:59Z",
		},
		{
			name:        "invalid format",
			input:       "2025/11/12 07:19:40",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseDmesgTimestamp(tt.input)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestParseJournalctlTimestamp(t *testing.T) {
	result, err := parseJournalctlTimestamp("Feb 09 07:14:54")
	require.NoError(t, err)
	// Journalctl timestamps lack a year; the parser assumes the current year.
	expectedYear := time.Now().UTC().Year()
	expected := fmt.Sprintf("%d-02-09T07:14:54Z", expectedYear)
	require.Equal(t, expected, result)
}

func TestParseCrdbV2Timestamp(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"260210 23:00:19.379701", "2026-02-10T23:00:19Z"},
		{"260211 01:03:31.169693", "2026-02-11T01:03:31Z"},
		{"250101 00:00:00.000000", "2025-01-01T00:00:00Z"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseCrdbV2Timestamp(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestShouldUploadArtifactFile(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		expected bool
	}{
		{"test.log", "test.log", true},
		{"test-post-assertions.log", "test-post-assertions.log", true},
		{"failure log", "failure_1.log", true},
		{"run log", "run_073000.071743733_n1-4_mkdir-p-storedir.log", true},
		{"node-ips.log", "node-ips.log", true},
		{"params.log", "params.log", true},
		{"dmesg txt", "1.dmesg.txt", true},
		{"journalctl txt", "1.journalctl.txt", true},
		{"failed suffix excluded", "run_073006.872795339_n1-4_test-e-v2537cockroac.failed", false},
		{"mixed-version-test.log excluded", "mixed-version-test.log", false},
		{"datadog.log", "datadog.log", true},
		{"test-teardown.log", "test-teardown.log", true},
		{"tsdump excluded", "tsdump.gob", false},
		{"tsdump yaml excluded", "tsdump.gob.yaml", false},
		{"json file excluded", "cluster.json", false},
		{"random txt excluded", "diskusage.txt", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, shouldUploadArtifactFile(tt.fileName))
		})
	}
}

func TestShouldUploadNodeFile(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		expected bool
	}{
		{"cockroach.exit.log", "cockroach.exit.log", true},
		{"cockroach.stderr.log", "cockroach.stderr.log", true},
		{"cockroach.stdout.log", "cockroach.stdout.log", true},
		{"diskusage.txt", "diskusage.txt", true},
		{"roachprod.log", "roachprod.log", true},
		// Segment and base-name files are not handled by shouldUploadNodeFile.
		{"segment file", "cockroach.teamcity-xxx.log", false},
		{"base-name file", "cockroach-health.log", false},
		{".DS_Store", ".DS_Store", false},
		{"pprof file", "memprof.2026-02-10.pprof", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, shouldUploadNodeFile(tt.fileName))
		})
	}
}

func TestIsNodeSegmentFile(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		expected bool
	}{
		{"segment cockroach", "cockroach.teamcity-xxx.ubuntu.2026-02-10T23_00_19Z.004918.log", true},
		{"segment health", "cockroach-health.teamcity-xxx.ubuntu.2026-02-10T23_00_19Z.004918.log", true},
		{"segment pebble", "cockroach-pebble.teamcity-xxx.ubuntu.2026-02-10T23_00_19Z.004918.log", true},
		{"segment kv-distribution", "cockroach-kv-distribution.teamcity-xxx.ubuntu.2026-02-10T23_00_19Z.004918.log", true},
		// Special files are not segments (no date in name).
		{"cockroach.exit.log", "cockroach.exit.log", false},
		{"cockroach.stderr.log", "cockroach.stderr.log", false},
		{"cockroach.stdout.log", "cockroach.stdout.log", false},
		// Base-name files are not segments.
		{"base cockroach.log", "cockroach.log", false},
		{"base cockroach-health", "cockroach-health.log", false},
		// Non-.log files are not segments.
		{".DS_Store", ".DS_Store", false},
		{"pprof file", "memprof.2026-02-10.pprof", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, isNodeSegmentFile(tt.fileName))
		})
	}
}

func TestIsNodeBaseFile(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		expected bool
	}{
		{"cockroach.log", "cockroach.log", true},
		{"cockroach-health.log", "cockroach-health.log", true},
		{"cockroach-pebble.log", "cockroach-pebble.log", true},
		{"cockroach-kv-distribution.log", "cockroach-kv-distribution.log", true},
		{"cockroach-sql-exec.log", "cockroach-sql-exec.log", true},
		// Special files are not base files.
		{"cockroach.exit.log", "cockroach.exit.log", false},
		{"cockroach.stderr.log", "cockroach.stderr.log", false},
		{"cockroach.stdout.log", "cockroach.stdout.log", false},
		// Segment files are not base files.
		{"segment file", "cockroach-health.teamcity-xxx.log", false},
		// Non-.log files are not base files.
		{".DS_Store", ".DS_Store", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, isNodeBaseFile(tt.fileName))
		})
	}
}

func TestNodeFileChannel(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		expected string
	}{
		{"segment cockroach", "cockroach.teamcity-xxx.ubuntu.2026-02-10T23_00_19Z.004918.log", "cockroach"},
		{"segment health", "cockroach-health.teamcity-xxx.ubuntu.2026-02-10T23_00_19Z.004918.log", "cockroach-health"},
		{"base cockroach", "cockroach.log", "cockroach"},
		{"base health", "cockroach-health.log", "cockroach-health"},
		{"base kv-distribution", "cockroach-kv-distribution.log", "cockroach-kv-distribution"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, nodeFileChannel(tt.fileName))
		})
	}
}

func TestSelectParser(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		wantRe   string
	}{
		{"test.log", "test.log", roachtestLineRegex.String()},
		{"run log", "run_073000.log", roachtestLineRegex.String()},
		{"failure log", "failure_1.log", roachtestLineRegex.String()},
		{"roachprod.log", "roachprod.log", roachtestLineRegex.String()},
		{"diskusage.txt", "diskusage.txt", roachtestLineRegex.String()},
		{"dmesg", "1.dmesg.txt", dmesgLineRegex.String()},
		{"journalctl", "1.journalctl.txt", journalctlLineRegex.String()},
		{"cockroach segment", "cockroach.teamcity-xxx.log", crdbV2LineRegex.String()},
		{"cockroach-health segment", "cockroach-health.teamcity-xxx.log", crdbV2LineRegex.String()},
		{"cockroach.exit.log", "cockroach.exit.log", crdbV2LineRegex.String()},
		{"cockroach.stderr.log", "cockroach.stderr.log", crdbV2LineRegex.String()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := selectParser(tt.fileName)
			require.Equal(t, tt.wantRe, parser.lineRegex.String())
		})
	}
}

func TestDiscoverLogFiles(t *testing.T) {
	// Create a representative artifact directory structure:
	//
	//   tmpDir/
	//   ├── test.log                  ✓ artifact
	//   ├── run_073000.log            ✓ artifact
	//   ├── failure_1.log             ✓ artifact
	//   ├── 1.dmesg.txt               ✓ artifact
	//   ├── 1.journalctl.txt          ✓ artifact
	//   ├── cluster.json              ✗ not .log/.txt
	//   ├── tsdump.gob                ✗ tsdump excluded
	//   ├── run_xxx.failed            ✗ .failed excluded
	//   ├── mixed-version-test.log    ✗ explicitly excluded
	//   └── logs/
	//       ├── 1.cockroach.log       ✗ not .unredacted dir
	//       └── 1.unredacted/
	//           ├── cockroach.teamcity-x...2026-02-10T...log    ✓ segment
	//           ├── cockroach-health.teamcity-x...2026-...log ✓ segment
	//           ├── cockroach-health.log               ✗ base-name (has segment)
	//           ├── cockroach-pebble.log               ✓ base-name fallback (no segments)
	//           ├── cockroach.exit.log                 ✓ special file
	//           ├── diskusage.txt                      ✓ special file
	//           ├── .DS_Store                          ✗ not a log file
	//           └── goroutine_dump/                    ✗ directory, skipped
	//               └── dump.pb.gz
	tmpDir := t.TempDir()

	// Helper to create an empty file, creating parent dirs as needed.
	touch := func(relPath string) {
		absPath := filepath.Join(tmpDir, relPath)
		require.NoError(t, os.MkdirAll(filepath.Dir(absPath), 0777))
		require.NoError(t, os.WriteFile(absPath, nil, 0666))
	}

	// Top-level artifact files.
	touch("test.log")
	touch("run_073000.log")
	touch("failure_1.log")
	touch("1.dmesg.txt")
	touch("1.journalctl.txt")
	touch("cluster.json")           // excluded
	touch("tsdump.gob")             // excluded
	touch("run_xxx.failed")         // excluded
	touch("mixed-version-test.log") // excluded

	// logs/ directory with node-level logs.
	touch("logs/1.cockroach.log") // excluded (not .unredacted dir)

	// 1.unredacted/ with segments, base-names, and special files.
	touch("logs/1.unredacted/cockroach.teamcity-x.ubuntu.2026-02-10T23_00_19Z.004918.log")
	touch("logs/1.unredacted/cockroach-health.teamcity-x.ubuntu.2026-02-10T23_00_19Z.004918.log")
	touch("logs/1.unredacted/cockroach-health.log") // excluded (has segment)
	touch("logs/1.unredacted/cockroach-pebble.log") // included (no segment, fallback)
	touch("logs/1.unredacted/cockroach.exit.log")
	touch("logs/1.unredacted/diskusage.txt")
	touch("logs/1.unredacted/.DS_Store")                 // excluded
	touch("logs/1.unredacted/goroutine_dump/dump.pb.gz") // excluded (in subdir)

	l, err := logger.RootLogger(os.DevNull, logger.NoTee)
	require.NoError(t, err)
	files, err := discoverLogFiles(l, tmpDir)
	require.NoError(t, err)

	// Convert to relative paths for readability.
	var relPaths []string
	for _, f := range files {
		rel, relErr := filepath.Rel(tmpDir, f)
		require.NoError(t, relErr)
		relPaths = append(relPaths, rel)
	}
	sort.Strings(relPaths)

	expected := []string{
		"1.dmesg.txt",
		"1.journalctl.txt",
		"failure_1.log",
		"logs/1.unredacted/cockroach-health.teamcity-x.ubuntu.2026-02-10T23_00_19Z.004918.log",
		"logs/1.unredacted/cockroach-pebble.log",
		"logs/1.unredacted/cockroach.exit.log",
		"logs/1.unredacted/cockroach.teamcity-x.ubuntu.2026-02-10T23_00_19Z.004918.log",
		"logs/1.unredacted/diskusage.txt",
		"run_073000.log",
		"test.log",
	}
	require.Equal(t, expected, relPaths)
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

func TestShouldUploadLogsToDatadog(t *testing.T) {
	// Save and restore env var and flags
	originalBranch := os.Getenv(envTCBuildBranch)
	originalSendLogsAnyBranch := roachtestflags.DatadogSendLogsAnyBranch
	originalSendLogsAnyResult := roachtestflags.DatadogSendLogsAnyResult
	defer func() {
		_ = os.Setenv(envTCBuildBranch, originalBranch)
		roachtestflags.DatadogSendLogsAnyBranch = originalSendLogsAnyBranch
		roachtestflags.DatadogSendLogsAnyResult = originalSendLogsAnyResult
	}()

	tests := []struct {
		name                 string
		branch               string
		testFailed           bool
		sendLogsAnyBranch    bool
		sendLogsAnyResult    bool
		expectedShouldUpload bool
	}{
		// Default behavior (no flags): upload only failures from master/release
		{
			name:                 "default: master branch, test passed",
			branch:               "master",
			testFailed:           false,
			sendLogsAnyBranch:    false,
			sendLogsAnyResult:    false,
			expectedShouldUpload: false,
		},
		{
			name:                 "default: master branch, test failed",
			branch:               "master",
			testFailed:           true,
			sendLogsAnyBranch:    false,
			sendLogsAnyResult:    false,
			expectedShouldUpload: true,
		},
		{
			name:                 "default: release branch, test failed",
			branch:               "release-24.1",
			testFailed:           true,
			sendLogsAnyBranch:    false,
			sendLogsAnyResult:    false,
			expectedShouldUpload: true,
		},
		{
			name:                 "default: feature branch, test failed",
			branch:               "feature-new-stuff",
			testFailed:           true,
			sendLogsAnyBranch:    false,
			sendLogsAnyResult:    false,
			expectedShouldUpload: false,
		},
		// send-logs-any-branch only: upload failures from any branch
		{
			name:                 "send-logs-any-branch: feature branch, test passed",
			branch:               "feature-branch",
			testFailed:           false,
			sendLogsAnyBranch:    true,
			sendLogsAnyResult:    false,
			expectedShouldUpload: false,
		},
		{
			name:                 "send-logs-any-branch: feature branch, test failed",
			branch:               "feature-branch",
			testFailed:           true,
			sendLogsAnyBranch:    true,
			sendLogsAnyResult:    false,
			expectedShouldUpload: true,
		},
		// send-logs-any-result only: upload all results from master/release
		{
			name:                 "send-logs-any-result: master branch, test passed",
			branch:               "master",
			testFailed:           false,
			sendLogsAnyBranch:    false,
			sendLogsAnyResult:    true,
			expectedShouldUpload: true,
		},
		{
			name:                 "send-logs-any-result: master branch, test failed",
			branch:               "master",
			testFailed:           true,
			sendLogsAnyBranch:    false,
			sendLogsAnyResult:    true,
			expectedShouldUpload: true,
		},
		{
			name:                 "send-logs-any-result: feature branch, test passed",
			branch:               "feature-branch",
			testFailed:           false,
			sendLogsAnyBranch:    false,
			sendLogsAnyResult:    true,
			expectedShouldUpload: false,
		},
		{
			name:                 "send-logs-any-result: feature branch, test failed",
			branch:               "feature-branch",
			testFailed:           true,
			sendLogsAnyBranch:    false,
			sendLogsAnyResult:    true,
			expectedShouldUpload: false,
		},
		// Both flags: upload all results from any branch
		{
			name:                 "both flags: feature branch, test passed",
			branch:               "feature-branch",
			testFailed:           false,
			sendLogsAnyBranch:    true,
			sendLogsAnyResult:    true,
			expectedShouldUpload: true,
		},
		{
			name:                 "both flags: feature branch, test failed",
			branch:               "feature-branch",
			testFailed:           true,
			sendLogsAnyBranch:    true,
			sendLogsAnyResult:    true,
			expectedShouldUpload: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = os.Setenv(envTCBuildBranch, tt.branch)
			roachtestflags.DatadogSendLogsAnyBranch = tt.sendLogsAnyBranch
			roachtestflags.DatadogSendLogsAnyResult = tt.sendLogsAnyResult
			result := ShouldUploadLogsToDatadog(tt.testFailed)
			require.Equal(t, tt.expectedShouldUpload, result)
		})
	}
}

// Log file parsing tests
//
// The tests below cover parsing of the various log file types found in a
// roachtest artifacts directory. The artifact structure and parser used
// for each file type:
//
//	artifacts/
//	├── test.log                                          roachtestParser
//	├── test-teardown.log                                 roachtestParser
//	├── params.log                                        roachtestParser
//	├── run_<time>_n<nodes>_<label>.log                   roachtestParser
//	├── failure_*.log                                     roachtestParser (raw fallback)
//	├── node-ips.log                                      roachtestParser (raw fallback)
//	├── datadog.log                                       roachtestParser
//	├── *.dmesg.txt                                       dmesgParser
//	├── *.journalctl.txt                                  journalctlParser
//	└── logs/
//	    └── N.unredacted/
//	        ├── cockroach.<host>.<ts>.log                  crdbV2Parser
//	        ├── cockroach-health.<host>.<ts>.log           crdbV2Parser
//	        ├── cockroach-kv-distribution.<host>.<ts>.log  crdbV2Parser
//	        ├── cockroach-kv-exec.<host>.<ts>.log          crdbV2Parser
//	        ├── cockroach-pebble.<host>.<ts>.log           crdbV2Parser
//	        ├── cockroach-security.<host>.<ts>.log         crdbV2Parser
//	        ├── cockroach-sql-audit.<host>.<ts>.log        crdbV2Parser
//	        ├── cockroach-sql-auth.<host>.<ts>.log         crdbV2Parser
//	        ├── cockroach-sql-schema.<host>.<ts>.log       crdbV2Parser
//	        ├── cockroach-stderr.<host>.<ts>.log           crdbV2Parser
//	        ├── cockroach-telemetry.<host>.<ts>.log        crdbV2Parser
//	        ├── cockroach.stderr.log                       crdbV2Parser (raw fallback)
//	        ├── cockroach.stdout.log                       crdbV2Parser (raw fallback)
//	        ├── cockroach.exit.log                         crdbV2Parser (raw fallback)
//	        ├── diskusage.txt                              roachtestParser (raw fallback)
//	        └── roachprod.log                              roachtestParser

// parseTestLogResult holds the output of parseTestLog.
type parseTestLogResult struct {
	validEntries []datadogV2.HTTPLogItem
	skipCount    int
}

// parseTestLog is a test helper that opens a testdata file, scans it
// with the given parser, and returns parsed entries and skip count.
// It handles metadata save/restore and runs a simplified version of
// the multiline scan loop from parseAndUploadLogFile (without the
// maxContinuationLines cap). If logName is non-empty it overrides
// unittestLogMetadata.LogName for this test.
func parseTestLog(
	t *testing.T, testdataFile string, parser logFileParser, logName string,
) parseTestLogResult {
	t.Helper()

	originalLogMetadata := unittestLogMetadata
	t.Cleanup(func() {
		unittestLogMetadata = originalLogMetadata
		unittestLogMetadata.Tags = map[string]string{}
	})

	if logName != "" {
		unittestLogMetadata.LogName = logName
	}
	unittestLogMetadata.Tags = unittestLogMetadata.makeTags()

	file, err := os.Open(filepath.Join("testdata", testdataFile))
	require.NoError(t, err)
	t.Cleanup(func() { file.Close() })

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var result parseTestLogResult
	var currentLines []string

	finalize := func() {
		if len(currentLines) == 0 {
			return
		}
		message := strings.Join(currentLines, "\n")
		entry, err := parseLogLine(message, unittestLogMetadata, parser)
		if err != nil {
			result.skipCount++
		} else {
			result.validEntries = append(result.validEntries, *entry)
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		if parser.lineRegex.MatchString(line) {
			finalize()
			currentLines = []string{line}
		} else {
			if len(currentLines) > 0 {
				currentLines = append(currentLines, line)
			} else {
				result.skipCount++
			}
		}
	}
	finalize()
	require.NoError(t, scanner.Err())

	return result
}

// TestParseLogFileRoachtest tests the parsing of a roachtest artifacts/test.log file
func TestParseLogFileRoachtest(t *testing.T) {
	r := parseTestLog(t, "test_roachtest_log", roachtestParser, "")

	// The test log has 55 lines of content. 44 lines start with a timestamp
	// and the remaining 11 are continuation lines absorbed into the preceding
	// entry. No lines are skipped.
	require.Equal(t, 44, len(r.validEntries), "expected 44 multiline entries")
	require.Equal(t, 0, r.skipCount, "all non-timestamp lines should be absorbed as continuations")

	// Verify the first entry (single-line).
	require.Equal(t, "2025/11/12 07:19:40 test_impl.go:197: Runtime assertions enabled", r.validEntries[0].Message)

	// Verify multiline entries contain their continuation lines.
	// Entry starting at line 8 should include continuation lines 9-10.
	require.Contains(t, r.validEntries[7].Message, "checking certs.tar")
	require.Contains(t, r.validEntries[7].Message, "initializing certs")
	require.Contains(t, r.validEntries[7].Message, "distributing certs")

	// Entry starting at line 20 (18th timestamp = index 17) should include
	// NOTICE and SET CLUSTER SETTING continuation lines (21-24).
	require.Contains(t, r.validEntries[17].Message, "SET CLUSTER SETTING")
	require.Contains(t, r.validEntries[17].Message, "NOTICE:")
	require.Contains(t, r.validEntries[17].Message, "executing sql")

	// Check that entry fields are set correctly
	require.Equal(t, defaultDDSource, *r.validEntries[0].Ddsource)
	require.Equal(t, defaultService, *r.validEntries[0].Service)
	require.Equal(t, unittestLogMetadata.TCHost, *r.validEntries[0].Hostname)

	// Verify ddtags contains all expected tags
	ddTags := r.validEntries[0].Ddtags
	require.Contains(t, *ddTags, "env:"+defaultEnv)
	require.Contains(t, *ddTags, "version:"+unittestLogMetadata.Version)
	require.Contains(t, *ddTags, "platform:"+unittestLogMetadata.Platform)
	require.Contains(t, *ddTags, "cloud:"+unittestLogMetadata.Cloud)
	require.Contains(t, *ddTags, "name:"+unittestLogMetadata.TestName)
	require.Contains(t, *ddTags, "owner:"+unittestLogMetadata.Owner)

	// Verify timestamp is set in AdditionalProperties
	require.NotNil(t, r.validEntries[0].AdditionalProperties)
	timestamp, ok := r.validEntries[0].AdditionalProperties["timestamp"]
	require.True(t, ok, "timestamp should be in AdditionalProperties")
	require.Equal(t, "2025-11-12T07:19:40Z", timestamp)
}

// TestParseLogFileRunLog tests the parsing of an artifacts/run_*.log file, e.g.
// run_094348.528407890_n1-9_Setup.log. These files are generated by
// roachtest's LoggerForCmd and contain a mix of timestamped roachtest
// log lines and raw roachprod command output.
func TestParseLogFileRunLog(t *testing.T) {
	r := parseTestLog(t, "test_run_time_nodes_cmdargs_log", roachtestParser, "")

	// Lines 1-2 are raw roachprod command stdout that appear before
	// the first timestamped roachtest log line. They are skipped
	// because they lack a timestamp prefix.
	require.Equal(t, 2, r.skipCount, "lines before first timestamp should be skipped")

	// Lines 3 and 14 start with a roachtest timestamp, producing 2 entries.
	// The remaining lines are continuation lines absorbed into those entries.
	require.Equal(t, 2, len(r.validEntries), "expected 2 multiline entries")

	// First entry starts at line 3 and includes continuation lines 4-13.
	require.Contains(t, r.validEntries[0].Message, "running cmd: echo '/dev/sdb'")
	require.Contains(t, r.validEntries[0].Message, "dmsetup-disk-stall")
	require.Contains(t, r.validEntries[0].Message, "<ok>")

	// Second entry starts at line 14 and includes continuation lines 15-26.
	require.Contains(t, r.validEntries[1].Message, "running cmd: echo '# disabled during tests'")
	require.Contains(t, r.validEntries[1].Message, "sudo udevadm control --reload")

	// Verify timestamp parsing.
	timestamp, ok := r.validEntries[0].AdditionalProperties["timestamp"]
	require.True(t, ok)
	require.Equal(t, "2026-03-17T09:43:49Z", timestamp)
}

// TestParseLogFileMixedVersion tests the parsing of artifacts/mixed-version-test.log
func TestParseLogFileMixedVersion(t *testing.T) {
	r := parseTestLog(t, "test_mixedversion_log", roachtestParser, "")

	// The mixed-version test log has 3729 lines of content. 1731 lines
	// match the timestamp pattern (388 standard + 1343 bracket-prefixed)
	// and the remaining 1998 are continuation lines absorbed into
	// preceding entries. No lines are skipped.
	require.Equal(t, 1731, len(r.validEntries), "expected 1731 entries")
	require.Equal(t, 0, r.skipCount, "all non-timestamp lines should be absorbed as continuations")

	// Verify a standard-prefix entry (line 1).
	require.Equal(t,
		"2026/02/08 07:29:33 test_impl.go:208: Runtime assertions disabled",
		r.validEntries[0].Message)

	// Verify a bracket-prefixed entry (line 6) has correct timestamp
	// and retains the bracket prefix in the message.
	require.Contains(t, r.validEntries[5].Message, "[mixed-version-test]")
	ts, ok := r.validEntries[5].AdditionalProperties["timestamp"]
	require.True(t, ok)
	require.Equal(t, "2026-02-08T07:30:00Z", ts)

	// Verify multiline grouping: entry at line 7 includes the plan tree
	// continuation lines (lines 8-165).
	require.Contains(t, r.validEntries[6].Message, "mixed-version test:")
	require.Contains(t, r.validEntries[6].Message, "Seed:")
	require.Contains(t, r.validEntries[6].Message, "├── install fixtures")
}

// TestParseLogFileFailure tests the parsing of a failure_*.log file
// e.g. artifacts/failure_1.log file
func TestParseLogFileFailure(t *testing.T) {
	r := parseTestLog(t, "test_failure_log", roachtestParser, "failure_1.log")

	// failure_*.log files contain raw stack traces with no timestamps.
	// No lines match the timestamp regex, so 0 entries are produced and
	// all 14 lines are skipped. In production, parseAndUploadLogFile
	// handles this by falling back to uploading the entire file as a
	// single raw log entry.
	require.Equal(t, 0, len(r.validEntries), "no lines should match the timestamp regex")
	require.Equal(t, 14, r.skipCount, "all lines should be skipped")

	// Verify the file content can be read as a single raw entry (the
	// fallback path in parseAndUploadLogFile).
	testLogPath := filepath.Join("testdata", "test_failure_log")
	content, err := os.ReadFile(testLogPath)
	require.NoError(t, err)
	require.Contains(t, string(content), "test monitor: EOF")
	require.Contains(t, string(content), "stack trace")
	require.Contains(t, string(content), "withstack.withStack")
}

func TestParseLogFileDmesg(t *testing.T) {
	r := parseTestLog(t, "test_dmesg_log", dmesgParser, "1.dmesg.txt")

	// The dmesg test log has 553 lines, all matching the dmesg timestamp
	// pattern. No continuation lines, no skipped lines.
	require.Equal(t, 553, len(r.validEntries), "expected 553 dmesg entries")
	require.Equal(t, 0, r.skipCount, "no lines should be skipped")

	// Verify the first entry message and timestamp.
	require.Contains(t, r.validEntries[0].Message, "Linux version 6.5.0-1016-gcp")
	ts, ok := r.validEntries[0].AdditionalProperties["timestamp"]
	require.True(t, ok)
	require.Equal(t, "2026-02-09T07:14:51Z", ts)

	// Verify a later entry with a different second-level timestamp (line 359).
	require.Contains(t, r.validEntries[358].Message, "i8042 KBD port")
	ts359, ok := r.validEntries[358].AdditionalProperties["timestamp"]
	require.True(t, ok)
	require.Equal(t, "2026-02-09T07:14:52Z", ts359)

	// Verify tags include the dmesg file name.
	ddTags := r.validEntries[0].Ddtags
	require.Contains(t, *ddTags, "log_file:1.dmesg.txt")
}

// TestParseLogFileJournalctl tests the parsing of a journalctl file
// e.g. artifacts/1.journalctl.txt file
func TestParseLogFileJournalctl(t *testing.T) {
	r := parseTestLog(t, "test_journalctl_log", journalctlParser, "1.journalctl.txt")

	// The journalctl test log has 2981 lines, all matching the journalctl
	// timestamp pattern. No continuation lines, no skipped lines.
	require.Equal(t, 2981, len(r.validEntries), "expected 2981 journalctl entries")
	require.Equal(t, 0, r.skipCount, "no lines should be skipped")

	// Verify the first entry message and timestamp.
	require.Contains(t, r.validEntries[0].Message, "ubuntu kernel: Linux version 6.5.0-1016-gcp")
	ts, ok := r.validEntries[0].AdditionalProperties["timestamp"]
	require.True(t, ok)
	// Journalctl timestamps lack a year; parseJournalctlTimestamp assumes
	// the current year, which is correct in production since logs are
	// always from the recent past. However, to handle the year rollover
	// edge case (e.g. a Dec 31 log parsed on Jan 1), the parser
	// decrements the year if the result would be in the future.
	// (relative to current time)
	//
	// In this test, the testdata date (Feb 09) is arbitrary, so if the
	// test runs before Feb 09 the parser will roll back to the previous
	// year. We mirror that logic here to compute the expected year.
	// e.g. test runs on Jan 1st 2026, the log entry will be parsed as
	// Feb 9 07:14:54 with year 2025, so the expected year needs to be
	// decremented to 2025 as well
	now := time.Now().UTC()
	expectedYear := now.Year()
	if time.Date(expectedYear, 2, 9, 7, 14, 54, 0, time.UTC).After(now) {
		expectedYear--
	}
	require.Equal(t, fmt.Sprintf("%d-02-09T07:14:54Z", expectedYear), ts)

	// Verify the last entry.
	last := r.validEntries[len(r.validEntries)-1]
	require.Contains(t, last.Message, "pam_unix(sudo:session): session opened for user root")

	// Verify tags include the journalctl file name.
	ddTags := r.validEntries[0].Ddtags
	require.Contains(t, *ddTags, "log_file:1.journalctl.txt")
}

// TestParseLogFileCockroach tests the parsing of CockroachDB node log files
// from artifacts/logs/N.unredacted/. These include segment files and
// base-name symlinks across all log channels:
//
//	cockroach.log
//	cockroach-health.log
//	cockroach-kv-distribution.log
//	cockroach-kv-exec.log
//	cockroach-pebble.log
//	cockroach-security.log
//	cockroach-sql-audit.log
//	cockroach-sql-auth.log
//	cockroach-sql-schema.log
//	cockroach-stderr.log
//	cockroach-telemetry.log
//
// Segment files follow the pattern:
//
//	cockroach-health.teamcity-21260621-1773728561-75-n10cpu2-0001.ubuntu.2026-03-17T09_44_19Z.003965.log
//
// Both segment and base-name files are discovered by discoverLogFiles, and base-name files are only uploaded if no segment files exist for the same channel.
func TestParseLogFileCockroach(t *testing.T) {
	r := parseTestLog(t, "test_cockroach_log", crdbV2Parser,
		"cockroach.teamcity-xxx.ubuntu.2026-02-11T00_19_40Z.log")

	// All 7 lines match the crdb-v2 timestamp pattern.
	require.Equal(t, 7, len(r.validEntries), "expected 7 crdb-v2 entries")
	require.Equal(t, 0, r.skipCount, "no lines should be skipped")

	// Verify the first entry timestamp and content.
	require.Contains(t, r.validEntries[0].Message, "file created at:")
	ts, ok := r.validEntries[0].AdditionalProperties["timestamp"]
	require.True(t, ok)
	require.Equal(t, "2026-02-11T00:19:40Z", ts)

	// Verify the last entry is the structured JSON event log.
	last := r.validEntries[len(r.validEntries)-1]
	require.Contains(t, last.Message, "aggregated_contention_info")
	tsLast, ok := last.AdditionalProperties["timestamp"]
	require.True(t, ok)
	require.Equal(t, "2026-02-11T00:19:40Z", tsLast)

	// Verify tags include the cockroach segment file name.
	ddTags := r.validEntries[0].Ddtags
	require.Contains(t, *ddTags, "log_file:cockroach.teamcity-xxx.ubuntu.2026-02-11T00_19_40Z.log")
}

// TestParseLogFileCockroachStderr tests the parsing of an artifacts/logs/N.unredacted/cockroach.stderr.log
// or artifacts/logs/N.unredacted/cockroach.stdout.log file. These files contain the startup banner and
// do not have crdb-v2 formatted log lines, so no lines match the crdb-v2
// timestamp regex and all lines are skipped. In parseAndUploadLogFile, this triggers the raw fallback
// which uploads the entire file as a single log entry.
func TestParseLogFileCockroachStderr(t *testing.T) {
	t.Run("startup banner", func(t *testing.T) {
		r := parseTestLog(t, "test_cockroach_stderr_log_0", crdbV2Parser, "cockroach.stderr.log")

		// No lines match the crdb-v2 timestamp pattern. All 16 lines are
		// skipped. In parseAndUploadLogFile, this triggers the raw fallback
		// which uploads the entire file as a single log entry.
		require.Equal(t, 0, len(r.validEntries), "no lines should match crdb-v2 format")
		require.Equal(t, 16, r.skipCount, "all lines should be skipped")
	})

	t.Run("startup warning", func(t *testing.T) {
		r := parseTestLog(t, "test_cockroach_stderr_log_1", crdbV2Parser, "cockroach.stderr.log")

		// No lines match the crdb-v2 timestamp pattern. All 11 lines are
		// skipped (startup command + deprecation warning + init message).
		require.Equal(t, 0, len(r.validEntries), "no lines should match crdb-v2 format")
		require.Equal(t, 11, r.skipCount, "all lines should be skipped")
	})
}
