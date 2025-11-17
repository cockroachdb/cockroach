// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package datadog

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

const (
	datadogIngestURL = "https://http-intake.logs.us5.datadoghq.com/api/v2/logs"

	// Static tags for all roachtest logs
	defaultEnv      = "test-infra"
	defaultDDSource = "teamcity-test"
	defaultService  = "roachtest-test"
)

// LogEntry represents a single log entry to be sent to Datadog
type LogEntry struct {
	Message   string `json:"message"`
	DDSource  string `json:"ddsource"`
	Service   string `json:"service"`
	DDTags    string `json:"ddtags"`
	Host      string `json:"host"`
	Timestamp string `json:"timestamp"`
}

// UploadConfig contains the configuration for uploading logs to Datadog
type UploadConfig struct {
	TestName string
	Cloud    string // gcp, aws, azure
	Platform string // e.g., linux-amd64, linux-arm64
	// fixme Host is currently the roachprod cluster name, but i want the TC agent here
	// can be inferred fromt the TC build log, not sure if we have access to it in roachtest
	// FIXME use an env var to fetch this?
	// it seems to be google.instance.name AND / OR teamcity.agent.hostname AND / OR teamcity.agent.name
	// e.g. gce-agent-nightlies-roachtest-20240520-no-preempt-29
	Host    string // cluster name
	Version string // branch name (master, release-25.1, etc.)
	BuildID string // CI build/job number (optional, e.g., "9822")
	APIKey  string
	Cluster string // roachprod cluster ID
}

// logLineRegex matches roachtest log lines: YYYY/MM/DD HH:MM:SS file.go:line: message
var logLineRegex = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) `)

// cloudToDatadogTag converts a roachtest Cloud to a Datadog cloud tag value
func cloudToDatadogTag(cloud spec.Cloud) string {
	switch cloud {
	case spec.GCE:
		return "gcp"
	case spec.AWS:
		return "aws"
	case spec.Azure:
		return "azure"
	case spec.IBM:
		return "ibm"
	default:
		return "unknown"
	}
}

// buildPlatformTag constructs the platform tag from OS and CPU architecture
func buildPlatformTag(os string, arch vm.CPUArch) string {
	return fmt.Sprintf("%s-%s", os, arch)
}

// ShouldUploadLogs determines if logs should be uploaded based on environment variables
func ShouldUploadLogs() bool {
	apiKey := os.Getenv("DATADOG_API_KEY")
	branch := os.Getenv("TC_BUILD_BRANCH")
	buildID := os.Getenv("TC_BUILD_ID")

	// Must have all required env vars
	if apiKey == "" || branch == "" || buildID == "" {
		return false
	}

	// Only upload from master and release branches
	if branch == "master" {
		return true
	}
	if strings.HasPrefix(branch, "release-") {
		return true
	}

	return false
}

// BuildUploadConfig creates an UploadConfig from runtime test information
func BuildUploadConfig(
	testName string,
	cloud spec.Cloud,
	osName string,
	arch vm.CPUArch,
	hostName string,
	clusterName string,
) UploadConfig {
	return UploadConfig{
		TestName: testName,
		Cloud:    cloudToDatadogTag(cloud),
		Platform: buildPlatformTag(osName, arch),
		Host:     hostName,
		Version:  os.Getenv("TC_BUILD_BRANCH"),
		APIKey:   os.Getenv("DATADOG_API_KEY"),
		Cluster:  clusterName,
	}
}

// UploadTestLog reads a test.log file, transforms it to Datadog format, and uploads it
func UploadTestLog(ctx context.Context, l *logger.Logger, logPath string, cfg UploadConfig) error {
	if cfg.APIKey == "" {
		return errors.New("DATADOG_API_KEY is required")
	}

	l.Printf("uploading test logs to Datadog for test: %s", cfg.TestName)

	// Read and parse log file
	entries, err := parseLogFile(l, logPath, cfg)
	if err != nil {
		return errors.Wrapf(err, "failed to parse log file: %s", logPath)
	}

	if len(entries) == 0 {
		l.Printf("no valid log entries found in %s, skipping upload", logPath)
		return nil
	}

	l.Printf("parsed %d log entries from %s", len(entries), logPath)

	// Upload to Datadog
	if err := uploadEntries(ctx, entries, cfg.APIKey); err != nil {
		return errors.Wrap(err, "failed to upload logs to Datadog")
	}

	l.Printf("successfully uploaded %d log entries to Datadog", len(entries))
	return nil
}

// parseLogFile reads a log file and converts it to Datadog log entries
func parseLogFile(l *logger.Logger, logPath string, cfg UploadConfig) ([]LogEntry, error) {
	file, err := os.Open(logPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Build the ddtags string
	ddtags := fmt.Sprintf("env:%s,version:%s,platform:%s,cloud:%s,name:%s,cluster:%s",
		defaultEnv, cfg.Version, cfg.Platform, cfg.Cloud, cfg.TestName, cfg.Cluster)

	// TODO: how will we infer this during runtime?
	// Add build ID if present
	if cfg.BuildID != "" {
		ddtags = fmt.Sprintf("%s,build_id:%s", ddtags, cfg.BuildID)
	}

	var entries []LogEntry
	scanner := bufio.NewScanner(file)

	// Increase buffer size for long log lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()

		// Only process lines that match the timestamp pattern
		matches := logLineRegex.FindStringSubmatch(line)
		if len(matches) < 2 {
			continue
		}

		timestampStr := matches[1]
		timestamp, err := parseTimestamp(timestampStr)
		if err != nil {
			// If we can't parse the timestamp, skip this line
			continue
		}

		entry := LogEntry{
			Message:   line,
			DDSource:  defaultDDSource,
			Service:   defaultService,
			DDTags:    ddtags,
			Host:      cfg.Host,
			Timestamp: timestamp,
		}
		//l.Printf("entry: %+v", entry)

		entries = append(entries, entry)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

// parseTimestamp converts a roachtest timestamp (YYYY/MM/DD HH:MM:SS) to ISO8601
func parseTimestamp(ts string) (string, error) {
	// Parse: 2025/11/12 07:19:40
	t, err := time.Parse("2006/01/02 15:04:05", ts)
	if err != nil {
		return "", err
	}
	// Convert to ISO8601 (RFC3339)
	return t.UTC().Format(time.RFC3339), nil
}

// uploadEntries sends log entries to Datadog in batches
func uploadEntries(ctx context.Context, entries []LogEntry, apiKey string) error {
	// Datadog recommends batches of up to 1000 entries or 5MB
	const batchSize = 1000

	for i := 0; i < len(entries); i += batchSize {
		end := i + batchSize
		if end > len(entries) {
			end = len(entries)
		}

		batch := entries[i:end]
		if err := uploadBatch(ctx, batch, apiKey); err != nil {
			return errors.Wrapf(err, "failed to upload batch %d-%d", i, end)
		}
	}

	return nil
}

// uploadBatch sends a single batch of log entries to Datadog
func uploadBatch(ctx context.Context, entries []LogEntry, apiKey string) error {
	payload, err := json.Marshal(entries)
	if err != nil {
		return errors.Wrap(err, "failed to marshal log entries")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", datadogIngestURL, bytes.NewReader(payload))
	if err != nil {
		return errors.Wrap(err, "failed to create HTTP request")
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("DD-API-KEY", apiKey)

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to send HTTP request")
	}
	defer resp.Body.Close()

	// Datadog returns 202 Accepted on success
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return errors.Newf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
