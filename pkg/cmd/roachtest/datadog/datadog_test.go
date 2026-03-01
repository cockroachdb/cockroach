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

	testLogPath := filepath.Join("testdata", "test_log_simple")
	file, err := os.Open(testLogPath)
	require.NoError(t, err)
	defer file.Close()

	// Accumulate multiline entries the same way parseAndUploadLogFile does:
	// lines matching roachtestLineRegex start a new entry; non-matching lines are
	// appended as continuations of the current entry.
	scanner := bufio.NewScanner(file)
	validEntries := make([]datadogV2.HTTPLogItem, 0)
	skipCount := 0
	var currentLines []string

	finalize := func() {
		if len(currentLines) == 0 {
			return
		}
		message := strings.Join(currentLines, "\n")
		entry, err := parseLogLine(message, unittestLogMetadata, roachtestParser)
		if err != nil {
			skipCount++
		} else {
			validEntries = append(validEntries, *entry)
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		if roachtestLineRegex.MatchString(line) {
			finalize()
			currentLines = []string{line}
		} else {
			if len(currentLines) > 0 {
				currentLines = append(currentLines, line)
			} else {
				skipCount++
			}
		}
	}
	finalize()

	// The test log has 55 lines of content. 44 lines start with a timestamp
	// and the remaining 11 are continuation lines absorbed into the preceding
	// entry. No lines are skipped.
	require.Equal(t, 44, len(validEntries), "expected 44 multiline entries")
	require.Equal(t, 0, skipCount, "all non-timestamp lines should be absorbed as continuations")

	// Verify the first entry (single-line).
	require.Equal(t, "2025/11/12 07:19:40 test_impl.go:197: Runtime assertions enabled", validEntries[0].Message)

	// Verify multiline entries contain their continuation lines.
	// Entry starting at line 8 should include continuation lines 9-10.
	require.Contains(t, validEntries[7].Message, "checking certs.tar")
	require.Contains(t, validEntries[7].Message, "initializing certs")
	require.Contains(t, validEntries[7].Message, "distributing certs")

	// Entry starting at line 20 (18th timestamp = index 17) should include
	// NOTICE and SET CLUSTER SETTING continuation lines (21-24).
	require.Contains(t, validEntries[17].Message, "SET CLUSTER SETTING")
	require.Contains(t, validEntries[17].Message, "NOTICE:")
	require.Contains(t, validEntries[17].Message, "executing sql")

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

func TestParseLogFileMixedVersion(t *testing.T) {
	originalLogMetadata := unittestLogMetadata
	defer func() {
		unittestLogMetadata = originalLogMetadata
		unittestLogMetadata.Tags = map[string]string{}
	}()
	l, err := logger.RootLogger(os.DevNull, logger.NoTee)
	require.NoError(t, err)
	defer l.Close()

	unittestLogMetadata.Tags = unittestLogMetadata.makeTags()

	testLogPath := filepath.Join("testdata", "test_log_mixedversion")
	file, err := os.Open(testLogPath)
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	validEntries := make([]datadogV2.HTTPLogItem, 0)
	skipCount := 0
	var currentLines []string

	finalize := func() {
		if len(currentLines) == 0 {
			return
		}
		message := strings.Join(currentLines, "\n")
		entry, err := parseLogLine(message, unittestLogMetadata, roachtestParser)
		if err != nil {
			skipCount++
		} else {
			validEntries = append(validEntries, *entry)
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		if roachtestLineRegex.MatchString(line) {
			finalize()
			currentLines = []string{line}
		} else {
			if len(currentLines) > 0 {
				currentLines = append(currentLines, line)
			} else {
				skipCount++
			}
		}
	}
	finalize()
	require.NoError(t, scanner.Err())

	// The mixed-version test log has 3729 lines of content. 1731 lines
	// match the timestamp pattern (388 standard + 1343 bracket-prefixed)
	// and the remaining 1998 are continuation lines absorbed into
	// preceding entries. No lines are skipped.
	require.Equal(t, 1731, len(validEntries), "expected 1731 entries")
	require.Equal(t, 0, skipCount, "all non-timestamp lines should be absorbed as continuations")

	// Verify a standard-prefix entry (line 1).
	require.Equal(t,
		"2026/02/08 07:29:33 test_impl.go:208: Runtime assertions disabled",
		validEntries[0].Message)

	// Verify a bracket-prefixed entry (line 6) has correct timestamp
	// and retains the bracket prefix in the message.
	require.Contains(t, validEntries[5].Message, "[mixed-version-test]")
	ts, ok := validEntries[5].AdditionalProperties["timestamp"]
	require.True(t, ok)
	require.Equal(t, "2026-02-08T07:30:00Z", ts)

	// Verify multiline grouping: entry at line 7 includes the plan tree
	// continuation lines (lines 8-165).
	require.Contains(t, validEntries[6].Message, "mixed-version test:")
	require.Contains(t, validEntries[6].Message, "Seed:")
	require.Contains(t, validEntries[6].Message, "├── install fixtures")
}

func TestParseLogFileFailure(t *testing.T) {
	originalLogMetadata := unittestLogMetadata
	defer func() {
		unittestLogMetadata = originalLogMetadata
		unittestLogMetadata.Tags = map[string]string{}
	}()
	l, err := logger.RootLogger(os.DevNull, logger.NoTee)
	require.NoError(t, err)
	defer l.Close()

	unittestLogMetadata.LogName = "failure_1.log"
	unittestLogMetadata.Tags = unittestLogMetadata.makeTags()

	testLogPath := filepath.Join("testdata", "test_failure_log")
	file, err := os.Open(testLogPath)
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	validEntries := make([]datadogV2.HTTPLogItem, 0)
	skipCount := 0
	var currentLines []string

	finalize := func() {
		if len(currentLines) == 0 {
			return
		}
		message := strings.Join(currentLines, "\n")
		entry, err := parseLogLine(message, unittestLogMetadata, roachtestParser)
		if err != nil {
			skipCount++
		} else {
			validEntries = append(validEntries, *entry)
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		if roachtestLineRegex.MatchString(line) {
			finalize()
			currentLines = []string{line}
		} else {
			if len(currentLines) > 0 {
				currentLines = append(currentLines, line)
			} else {
				skipCount++
			}
		}
	}
	finalize()
	require.NoError(t, scanner.Err())

	// failure_*.log files contain raw stack traces with no timestamps.
	// No lines match the timestamp regex, so 0 entries are produced and
	// all 14 lines are skipped. In production, parseAndUploadLogFile
	// handles this by falling back to uploading the entire file as a
	// single raw log entry.
	require.Equal(t, 0, len(validEntries), "no lines should match the timestamp regex")
	require.Equal(t, 14, skipCount, "all lines should be skipped")

	// Verify the file content can be read as a single raw entry (the
	// fallback path in parseAndUploadLogFile).
	content, err := os.ReadFile(testLogPath)
	require.NoError(t, err)
	require.Contains(t, string(content), "test monitor: EOF")
	require.Contains(t, string(content), "stack trace")
	require.Contains(t, string(content), "withstack.withStack")
}

func TestParseLogFileDmesg(t *testing.T) {
	originalLogMetadata := unittestLogMetadata
	defer func() {
		unittestLogMetadata = originalLogMetadata
		unittestLogMetadata.Tags = map[string]string{}
	}()
	l, err := logger.RootLogger(os.DevNull, logger.NoTee)
	require.NoError(t, err)
	defer l.Close()

	unittestLogMetadata.LogName = "1.dmesg.txt"
	unittestLogMetadata.Tags = unittestLogMetadata.makeTags()

	testLogPath := filepath.Join("testdata", "test_log_dmesg")
	file, err := os.Open(testLogPath)
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	validEntries := make([]datadogV2.HTTPLogItem, 0)
	skipCount := 0
	var currentLines []string

	finalize := func() {
		if len(currentLines) == 0 {
			return
		}
		message := strings.Join(currentLines, "\n")
		entry, err := parseLogLine(message, unittestLogMetadata, dmesgParser)
		if err != nil {
			skipCount++
		} else {
			validEntries = append(validEntries, *entry)
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		if dmesgLineRegex.MatchString(line) {
			finalize()
			currentLines = []string{line}
		} else {
			if len(currentLines) > 0 {
				currentLines = append(currentLines, line)
			} else {
				skipCount++
			}
		}
	}
	finalize()
	require.NoError(t, scanner.Err())

	// The dmesg test log has 553 lines, all matching the dmesg timestamp
	// pattern. No continuation lines, no skipped lines.
	require.Equal(t, 553, len(validEntries), "expected 553 dmesg entries")
	require.Equal(t, 0, skipCount, "no lines should be skipped")

	// Verify the first entry message and timestamp.
	require.Contains(t, validEntries[0].Message, "Linux version 6.5.0-1016-gcp")
	ts, ok := validEntries[0].AdditionalProperties["timestamp"]
	require.True(t, ok)
	require.Equal(t, "2026-02-09T07:14:51Z", ts)

	// Verify a later entry with a different second-level timestamp (line 359).
	require.Contains(t, validEntries[358].Message, "i8042 KBD port")
	ts359, ok := validEntries[358].AdditionalProperties["timestamp"]
	require.True(t, ok)
	require.Equal(t, "2026-02-09T07:14:52Z", ts359)

	// Verify tags include the dmesg file name.
	ddTags := validEntries[0].Ddtags
	require.Contains(t, *ddTags, "log_file:1.dmesg.txt")
}

func TestParseLogFileJournalctl(t *testing.T) {
	originalLogMetadata := unittestLogMetadata
	defer func() {
		unittestLogMetadata = originalLogMetadata
		unittestLogMetadata.Tags = map[string]string{}
	}()
	l, err := logger.RootLogger(os.DevNull, logger.NoTee)
	require.NoError(t, err)
	defer l.Close()

	unittestLogMetadata.LogName = "1.journalctl.txt"
	unittestLogMetadata.Tags = unittestLogMetadata.makeTags()

	testLogPath := filepath.Join("testdata", "test_log_journalctl")
	file, err := os.Open(testLogPath)
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	validEntries := make([]datadogV2.HTTPLogItem, 0)
	skipCount := 0
	var currentLines []string

	finalize := func() {
		if len(currentLines) == 0 {
			return
		}
		message := strings.Join(currentLines, "\n")
		entry, err := parseLogLine(message, unittestLogMetadata, journalctlParser)
		if err != nil {
			skipCount++
		} else {
			validEntries = append(validEntries, *entry)
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		if journalctlLineRegex.MatchString(line) {
			finalize()
			currentLines = []string{line}
		} else {
			if len(currentLines) > 0 {
				currentLines = append(currentLines, line)
			} else {
				skipCount++
			}
		}
	}
	finalize()
	require.NoError(t, scanner.Err())

	// The journalctl test log has 2981 lines, all matching the journalctl
	// timestamp pattern. No continuation lines, no skipped lines.
	require.Equal(t, 2981, len(validEntries), "expected 2981 journalctl entries")
	require.Equal(t, 0, skipCount, "no lines should be skipped")

	// Verify the first entry message and timestamp.
	require.Contains(t, validEntries[0].Message, "ubuntu kernel: Linux version 6.5.0-1016-gcp")
	ts, ok := validEntries[0].AdditionalProperties["timestamp"]
	require.True(t, ok)
	// Journalctl timestamps lack a year; the parser uses the current year.
	expectedYear := time.Now().UTC().Year()
	require.Equal(t, fmt.Sprintf("%d-02-09T07:14:54Z", expectedYear), ts)

	// Verify the last entry.
	last := validEntries[len(validEntries)-1]
	require.Contains(t, last.Message, "pam_unix(sudo:session): session opened for user root")

	// Verify tags include the journalctl file name.
	ddTags := validEntries[0].Ddtags
	require.Contains(t, *ddTags, "log_file:1.journalctl.txt")
}

func TestParseLogFileCockroach(t *testing.T) {
	originalLogMetadata := unittestLogMetadata
	defer func() {
		unittestLogMetadata = originalLogMetadata
		unittestLogMetadata.Tags = map[string]string{}
	}()
	l, err := logger.RootLogger(os.DevNull, logger.NoTee)
	require.NoError(t, err)
	defer l.Close()

	unittestLogMetadata.LogName = "cockroach.teamcity-xxx.ubuntu.2026-02-11T00_19_40Z.log"
	unittestLogMetadata.Tags = unittestLogMetadata.makeTags()

	testLogPath := filepath.Join("testdata", "test_cockroach_log")
	file, err := os.Open(testLogPath)
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	validEntries := make([]datadogV2.HTTPLogItem, 0)
	skipCount := 0
	var currentLines []string

	finalize := func() {
		if len(currentLines) == 0 {
			return
		}
		message := strings.Join(currentLines, "\n")
		entry, err := parseLogLine(message, unittestLogMetadata, crdbV2Parser)
		if err != nil {
			skipCount++
		} else {
			validEntries = append(validEntries, *entry)
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		if crdbV2LineRegex.MatchString(line) {
			finalize()
			currentLines = []string{line}
		} else {
			if len(currentLines) > 0 {
				currentLines = append(currentLines, line)
			} else {
				skipCount++
			}
		}
	}
	finalize()
	require.NoError(t, scanner.Err())

	// All 7 lines match the crdb-v2 timestamp pattern.
	require.Equal(t, 7, len(validEntries), "expected 7 crdb-v2 entries")
	require.Equal(t, 0, skipCount, "no lines should be skipped")

	// Verify the first entry timestamp and content.
	require.Contains(t, validEntries[0].Message, "file created at:")
	ts, ok := validEntries[0].AdditionalProperties["timestamp"]
	require.True(t, ok)
	require.Equal(t, "2026-02-11T00:19:40Z", ts)

	// Verify the last entry is the structured JSON event log.
	last := validEntries[len(validEntries)-1]
	require.Contains(t, last.Message, "aggregated_contention_info")
	tsLast, ok := last.AdditionalProperties["timestamp"]
	require.True(t, ok)
	require.Equal(t, "2026-02-11T00:19:40Z", tsLast)

	// Verify tags include the cockroach segment file name.
	ddTags := validEntries[0].Ddtags
	require.Contains(t, *ddTags, "log_file:cockroach.teamcity-xxx.ubuntu.2026-02-11T00_19_40Z.log")
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
