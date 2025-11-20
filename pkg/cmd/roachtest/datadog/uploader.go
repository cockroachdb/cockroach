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
	"regexp"
	"strings"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

const (
	// Static tags for all roachtest logs
	defaultEnv      = "test-infra"
	defaultDDSource = "teamcity-test"
	defaultService  = "roachtest-test"
)

// UploadConfig contains the configuration for uploading logs to Datadog
type UploadConfig struct {
	TestName string
	Owner    string // roachtest registry owner
	Cloud    string
	Platform string // e.g., linux-amd64, linux-arm64
	Host     string // teamcity agent hostname e.g., gce-agent-nightlies-roachtest-20240520-no-preempt-43
	Version  string // branch name e.g., master, release-25.1
	BuildID  string // TC Build ID
	Cluster  string // roachprod cluster ID
}

// logLineRegex matches roachtest log lines: YYYY/MM/DD HH:MM:SS file.go:message
var logLineRegex = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) `)

// cloudToDatadogTag converts a roachtest Cloud to a Datadog cloud tag value
func cloudToDatadogTag(cloud spec.Cloud) string {
	switch cloud {
	case spec.GCE:
		return "gce"
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

// ShouldUploadLogs checks if we are on a release branch or master or if
// roachtestflags.DatadogAlwaysUpload is set
func ShouldUploadLogs() bool {
	if roachtestflags.DatadogAlwaysUpload {
		return true
	}
	branch := os.Getenv("TC_BUILD_BRANCH")
	if branch == "master" || strings.HasPrefix(branch, "release-") {
		return true
	}
	return false
}

// BuildUploadConfig creates an UploadConfig from runtime test information
func BuildUploadConfig(
	testName string,
	owner registry.Owner,
	cloud spec.Cloud,
	osName string,
	arch vm.CPUArch,
	clusterName string,
	hostName string,
) UploadConfig {
	return UploadConfig{
		TestName: testName,
		Owner:    string(owner),
		Cloud:    cloudToDatadogTag(cloud),
		Platform: buildPlatformTag(osName, arch),
		Cluster:  clusterName,
		Host:     hostName,
		Version:  os.Getenv("TC_BUILD_BRANCH"),
		BuildID:  os.Getenv("TC_BUILD_ID"),
	}
}

// MaybeUploadTestLog reads the log at logPath and transforms it into Datadog
// HTTPLogItem JSON entries, then uploads them to Datadog.
// Will return an error if credentials are not set
func MaybeUploadTestLog(
	ctx context.Context, l *logger.Logger, logPath string, cfg UploadConfig,
) error {

	datadogCtx, err := newDatadogContext(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create Datadog client API context")
	}

	// Create Datadog logs API client
	configuration := datadog.NewConfiguration()
	apiClient := datadog.NewAPIClient(configuration)
	logsAPI := datadogV2.NewLogsApi(apiClient)

	// Read and parse log file
	entries, err := parseLogFile(l, logPath, cfg)
	if err != nil {
		return errors.Wrapf(err, "failed to parse log file: %s", logPath)
	}
	if len(entries) == 0 {
		return errors.Newf("no valid log entries found in %s, skipping upload", logPath)
	}

	l.Printf("parsed %d log entries from %s", len(entries), logPath)

	// Upload to Datadog
	if err := uploadEntries(datadogCtx, logsAPI, entries); err != nil {
		return errors.Wrap(err, "failed to upload logs to Datadog")
	}

	l.Printf("successfully uploaded %d log entries to Datadog", len(entries))
	return nil
}

// newDatadogContext returns a derived context from the provided context with
// the expected Datadog API client key values set.
func newDatadogContext(ctx context.Context) (context.Context, error) {

	// Use DD_SITE and DD_API_KEY env vars if they are set, otherwise use
	// roachtestflags values
	if datadogSite := os.Getenv("DD_SITE"); datadogSite == "" {
		if datadogSite = roachtestflags.DatadogSite; datadogSite == "" {
			return nil, errors.New("DD_SITE env var or datadog-site flag is required")
		}
		if err := os.Setenv("DD_SITE", datadogSite); err != nil {
			return nil, err
		}
	}
	if datadogAPIKey := os.Getenv("DD_API_KEY"); datadogAPIKey == "" {
		if datadogAPIKey = roachtestflags.DatadogAPIKey; datadogAPIKey == "" {
			if datadogAPIKey = os.Getenv("DATADOG_API_KEY"); datadogAPIKey == "" {
				return nil, errors.New("DD_API_KEY env var, datadog-api-key flag, or DATADOG_API_KEY env var is required")
			}
		}
		// Copy API key to DD_API_KEY which is where datadog.NewDefaultContext
		// expects it to be
		if err := os.Setenv("DD_API_KEY", datadogAPIKey); err != nil {
			return nil, err
		}
	}

	// datadog.NewDefaultContext expects DD_SITE and DD_API_KEY to be set
	return datadog.NewDefaultContext(ctx), nil
}

// parseLogFile reads a log file and converts it to a datadogV2.HTTPLogItem
// Returns an error if log file not found or if no entries are parsed
func parseLogFile(
	l *logger.Logger, logPath string, cfg UploadConfig,
) ([]datadogV2.HTTPLogItem, error) {
	file, err := os.Open(logPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Build the ddtags string, empty tags won't cause errors
	ddtags := fmt.Sprintf("env:%s,version:%s,platform:%s,cloud:%s,name:%s,cluster:%s,build_id:%s,owner:%s",
		defaultEnv, cfg.Version, cfg.Platform, cfg.Cloud, cfg.TestName, cfg.Cluster, cfg.BuildID, cfg.Owner)

	var entries []datadogV2.HTTPLogItem
	scanner := bufio.NewScanner(file)

	// Increase buffer size for long log lines
	buf := make([]byte, 0, 64*1024) // 64 KB
	scanner.Buffer(buf, 1024*1024)  // 1 MB max line size
	skipCount := 0
	skipCountLogLimit := 10 // Only log the first 10 skipped lines
	for scanner.Scan() {
		line := scanner.Text()

		// Only process well-formed log lines
		matches := logLineRegex.FindStringSubmatch(line)
		if len(matches) < 2 {
			if skipCount < skipCountLogLimit {
				l.Printf("skipping line %q, does not match pattern", line)
			}
			skipCount++
			continue
		}

		timestampStr := matches[1]
		// Datadog supports parsing timestamps in log entries, but it's simpler to
		// parse the timestamp ourselves and not worry about config on the cluster
		timestamp, err := parseTimestamp(timestampStr)
		if err != nil {
			// If we can't parse the timestamp, skip this line
			if skipCount < skipCountLogLimit {
				l.Printf("skipping line %q, failed to parse timestamp: %s", line, err)
			}
			skipCount++
			continue
		}

		// Create HTTPLogItem
		entry := datadogV2.NewHTTPLogItem(line)
		entry.SetDdsource(defaultDDSource)
		entry.SetService(defaultService)
		entry.SetDdtags(ddtags)
		entry.SetHostname(cfg.Host)
		entry.AdditionalProperties = map[string]string{
			"timestamp": timestamp,
		}

		entries = append(entries, *entry)
	}
	l.Printf("parsed %d log entries from %s, skipped %d lines", len(entries), logPath, skipCount)
	if err = scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to scan log file, potentially encountered 1MB log line")
	}
	if len(entries) == 0 {
		return nil, errors.Newf("no valid log entries found in %s", logPath)
	}
	return entries, nil
}

// parseTimestamp converts a roachtest timestamp (YYYY/MM/DD HH:MM:SS) to
// ISO8601 (RFC3339)
func parseTimestamp(ts string) (string, error) {
	t, err := time.Parse("2006/01/02 15:04:05", ts)
	if err != nil {
		return "", err
	}
	return t.UTC().Format(time.RFC3339), nil
}

// uploadEntries sends log entries to Datadog in batches
func uploadEntries(
	ctx context.Context, logsAPI *datadogV2.LogsApi, entries []datadogV2.HTTPLogItem,
) error {
	// Any individual log entry exceeding 1MB is accepted and truncated by Datadog
	// Maximum batch size is 1000 log entries
	// Maximum payload size is 1MB
	const batchSize = 1000

	for i := 0; i < len(entries); i += batchSize {
		end := i + batchSize
		if end > len(entries) {
			end = len(entries)
		}

		batch := entries[i:end]
		if err := uploadBatch(ctx, logsAPI, batch); err != nil {
			return errors.Wrapf(err, "failed to upload batch %d-%d", i, end)
		}
	}
	return nil
}

// uploadBatch sends a single batch of log entries to Datadog using the API client
func uploadBatch(
	ctx context.Context, logsAPI *datadogV2.LogsApi, entries []datadogV2.HTTPLogItem,
) error {
	_, resp, err := logsAPI.SubmitLog(ctx, entries)
	if err != nil {
		return errors.Wrap(err, "failed to submit logs to Datadog")
	}
	defer resp.Body.Close()

	// Datadog returns 202 Accepted on success
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.Newf("unexpected status code %d from Datadog API", resp.StatusCode)
	}
	return nil
}
