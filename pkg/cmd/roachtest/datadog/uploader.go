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
	"sync/atomic"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/errors"
)

const (
	// Static tags for all roachtest logs
	// TODO remove appended -test from tags after e2e verification
	defaultEnv      = "test-infra"
	defaultDDSource = "teamcity-test"
	defaultService  = "roachtest-test"
)

// LogMetadata contains the configuration for uploading logs to Datadog
type LogMetadata struct {
	TestName string
	Owner    string
	Cloud    string
	Platform string // e.g., linux-amd64, linux-arm64
	Host     string // teamcity agent hostname e.g., gce-agent-nightlies-roachtest-20240520-no-preempt-43
	Version  string // branch name e.g., master, release-25.1
	BuildID  string // TC Build ID
	Cluster  string // roachprod cluster
}

// makeTags builds the ddtags string, empty tags won't cause errors
func (m LogMetadata) makeTags() string {
	return fmt.Sprintf("env:%s,version:%s,platform:%s,cloud:%s,name:%s,cluster:%s,build_id:%s,owner:%s",
		defaultEnv, m.Version, m.Platform, m.Cloud, m.TestName, m.Cluster, m.BuildID, m.Owner)
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

// BuildLogMetadata creates an LogMetadata from runtime test information
func BuildLogMetadata(
	l *logger.Logger,
	spec *registry.TestSpec,
	cloud spec.Cloud,
	osName string,
	arch vm.CPUArch,
	clusterName string,
) LogMetadata {

	m := LogMetadata{
		TestName: spec.Name,
		Owner:    string(spec.Owner),
		Cloud:    cloud.String(),
		Platform: buildPlatformTag(osName, arch),
		Cluster:  clusterName,
		Version:  os.Getenv("TC_BUILD_BRANCH"),
		BuildID:  os.Getenv("TC_BUILD_ID"),
	}

	// Find hostname from teamcity's build properties file
	tcBuildPropertiesPath := os.Getenv("TEAMCITY_BUILD_PROPERTIES_FILE")
	if tcBuildPropertiesPath != "" {
		file, err := os.Open(tcBuildPropertiesPath)
		if err == nil {
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				l.Printf("line: %s", line)
				if strings.HasPrefix(line, "teamcity.agent.name=") {
					l.Printf("found hostname: %s", strings.TrimPrefix(line, "teamcity.agent.name="))
					m.Host = strings.TrimPrefix(line, "teamcity.agent.name=")
					break
				}
			}

		}
	}
	return m
}

// MaybeUploadTestLog reads the log at logPath and transforms it into Datadog
// HTTPLogItem JSON entries, then uploads them to Datadog in batches.
// Processes the file in streaming fashion to avoid holding all entries in memory.
// Will return an error if credentials are not set
func MaybeUploadTestLog(
	ctx context.Context, l *logger.Logger, logPath string, cfg LogMetadata,
) error {

	datadogLogger, err := l.ChildLogger("datadog", logger.QuietStdout)
	if err != nil {
		return err
	}
	datadogCtx, err := newDatadogContext(ctx)
	if err != nil {
		return err
	}

	// Create Datadog logs API client
	configuration := datadog.NewConfiguration()
	apiClient := datadog.NewAPIClient(configuration)
	logsAPI := datadogV2.NewLogsApi(apiClient)

	// Parse and upload log file in batches to avoid holding everything in memory
	totalEntries, err := parseAndUploadLogFile(datadogCtx, datadogLogger, logsAPI, logPath, cfg)
	if err != nil {
		return errors.Wrapf(err, "failed to parse and upload log file: %s", logPath)
	}

	l.Printf("successfully uploaded %d log entries to Datadog", totalEntries)
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

// parseAndUploadLogFile reads a log file, parses it, and uploads entries in batches
// using concurrent workers. This streaming approach avoids holding all log entries
// in memory at once, and parallelizes network I/O for faster uploads.
// Returns the total number of entries uploaded.
func parseAndUploadLogFile(
	ctx context.Context,
	l *logger.Logger,
	logsAPI *datadogV2.LogsApi,
	logPath string,
	logMeta LogMetadata,
) (int, error) {
	const numWorkers = 10
	const batchSize = 1000 // Datadog max batch size

	g := ctxgroup.WithContext(ctx)
	batches := make(chan []datadogV2.HTTPLogItem, 20) // Buffered channel for batches

	// Track total uploaded entries across all workers using atomic operations
	var totalUploaded atomic.Int64

	// Start worker goroutines to upload batches concurrently
	for i := 0; i < numWorkers; i++ {
		g.GoCtx(func(ctx context.Context) error {
			for batch := range batches {
				if err := uploadBatch(ctx, logsAPI, batch); err != nil {
					return errors.Wrapf(err, "failed to upload batch")
				}
				count := totalUploaded.Add(int64(len(batch)))
				l.Printf("uploaded batch of %d entries (total: %d)", len(batch), count)
			}
			return nil
		})
	}

	// Producer goroutine to parse file and send batches to workers
	g.GoCtx(func(ctx context.Context) error {
		defer close(batches) // Close channel when done to signal workers to exit

		file, err := os.Open(logPath)
		if err != nil {
			return err
		}
		defer file.Close()

		//ddtags := fmt.Sprintf("env:%s,version:%s,platform:%s,cloud:%s,name:%s,cluster:%s,build_id:%s,owner:%s", // I think i can move this out of the channel
		//	defaultEnv, cfg.Version, cfg.Platform, cfg.Cloud, cfg.TestName, cfg.Cluster, cfg.BuildID, cfg.Owner)
		ddTags := logMeta.makeTags()

		batch := make([]datadogV2.HTTPLogItem, 0, batchSize)

		scanner := bufio.NewScanner(file)
		// Increase buffer size for long log lines
		buf := make([]byte, 0, 64*1024) // 64 KB
		scanner.Buffer(buf, 1024*1024)  // 1 MB max line size

		skipCount := 0
		//skipCountLogLimit := 10 // Only log the first 10 skipped lines

		for scanner.Scan() {
			line := scanner.Text()

			entry, err := parseLogLine(line, ddTags, logMeta)
			if err != nil {
				l.Printf("skipping line %q: %s", line, err)
				skipCount++
				continue
			}

			batch = append(batch, *entry)

			// Send batch to workers when full
			if len(batch) >= batchSize {
				// Make a copy of the batch to send to workers
				batchCopy := make([]datadogV2.HTTPLogItem, len(batch))
				copy(batchCopy, batch)

				select {
				case batches <- batchCopy:
					batch = batch[:0] // Reset batch, reusing underlying array
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		if err = scanner.Err(); err != nil {
			return errors.Wrap(err, "failed to scan log file, potentially encountered 1MB log line")
		}

		// Send any remaining entries in the final batch
		if len(batch) > 0 {
			batchCopy := make([]datadogV2.HTTPLogItem, len(batch))
			copy(batchCopy, batch)

			select {
			case batches <- batchCopy:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		l.Printf("parsed log file %s, skipped %d lines", logPath, skipCount)
		return nil
	})

	// Wait for all goroutines to complete
	if err := g.Wait(); err != nil {
		return int(totalUploaded.Load()), err
	}

	totalEntries := int(totalUploaded.Load())
	l.Printf("successfully uploaded %d total log entries from %s", totalEntries, logPath)

	if totalEntries == 0 {
		return 0, errors.Newf("no valid log entries found in %s", logPath)
	}

	return totalEntries, nil
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

// parseLogLine parses a single log line into an HTTPLogItem.
// Returns an error if the line doesn't match the expected pattern or has invalid timestamp.
func parseLogLine(line string, ddtags string, logMeta LogMetadata) (*datadogV2.HTTPLogItem, error) {
	// Only process well-formed log lines
	matches := logLineRegex.FindStringSubmatch(line)
	if len(matches) < 2 {
		return nil, errors.New("line does not match pattern")
	}

	timestampStr := matches[1]
	timestamp, err := parseTimestamp(timestampStr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse timestamp")
	}

	// Create HTTPLogItem
	entry := datadogV2.NewHTTPLogItem(line)
	entry.SetDdsource(defaultDDSource)
	entry.SetService(defaultService)
	entry.SetDdtags(ddtags)
	entry.SetHostname(logMeta.Host)
	entry.AdditionalProperties = map[string]string{
		"timestamp": timestamp,
	}

	return entry, nil
}

// uploadBatch sends a single batch of log entries to Datadog using the API client
// datadog-api-client-go v2.50.0 will silently fail to upload entries with
// timestamps >18 hours from the current time. This can be avoided by not using
// the API client and instead making a direct HTTP request to the Datadog API.
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
