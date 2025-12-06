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

var (
	tcBuildPropertyFile = os.Getenv("TEAMCITY_BUILD_PROPERTIES_FILE")
	// logLineRegex matches roachtest test.log lines: YYYY/MM/DD HH:MM:SS file.go:message
	logLineRegex = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) `)
)

const (
	// TeamCity Build Property Value Prefixes
	tcSystemBuildPropertyAgentNamePrefix   = "agent.name="
	tcSystemBuildPropertyBuildNumberPrefix = "build.number="
	tcSystemBuildPropertyClusterNamePrefix = "cluster.name="
)

const (
	// Datadog Log Entry Reserved Tag Names
	envTagName = "env"

	// Datadog Log Entry User Defined Tag Names
	versionTagName  = "version"
	platformTagName = "platform"
	cloudTagName    = "cloud"
	testNameTagName = "name"
	ownerTagName    = "owner"
	logFileTagName  = "log_file"

	// Datadog Log Entry User Defined Attribute Names
	timestampAttributeName   = "timestamp"
	clusterAttributeName     = "cluster"
	buildNumberAttributeName = "build_number"

	// Datadog Log Entry Static Values for roachtest test.log
	// TODO remove appended -test from tags after e2e verification
	defaultEnv      = "ci"
	defaultDDSource = "teamcity"
	defaultService  = "roachtest"
	logName         = "test.log"
)

// LogMetadata contains the test metadata that will be associated with each log
// entry in Datadog. Fields in LogMetadata will be passed along as either tags
// or log entry fields / attributes.
// Typically low and medium cardinality fields are added as tags,
// high cardinality fields are added as fields / attributes.
type LogMetadata struct {
	TestName        string
	Owner           string
	Cloud           string
	Platform        string // e.g., linux-amd64, linux-arm64
	Version         string // branch name e.g., master, release-25.1
	Cluster         string // roachprod cluster
	TCHost          string // Teamcity Agent Name e.g., gce-agent-nightlies-roachtest-20240520-no-preempt-43
	TCBuildNumber   string // TeamCity Build Configuration execution instance
	TCBuildConfName string // e.g. Roachtest Nightly - GCE (Bazel)
	Tags            map[string]string
}

// buildPlatformField constructs the platform field from OS and CPU architecture
func buildPlatformField(os string, arch vm.CPUArch) string {
	return fmt.Sprintf("%s-%s", os, arch)
}

// formatTags converts a map of tag names and values to a string in Datadog
// format: "key1:value1,key2:value2"
func formatTags(tags map[string]string) string {
	var result string
	for k, v := range tags {
		if result != "" {
			result += ","
		}
		result += fmt.Sprintf("%s:%s", k, v)
	}
	return result
}

// buildTags constructs a string representation of tags to be added to Datadog
// log entries. Empty tag values do not cause errors
func (m LogMetadata) buildTags() map[string]string {
	tagMap := map[string]string{
		envTagName:      defaultEnv,
		versionTagName:  m.Version,
		platformTagName: m.Platform, // TODO Fixme
		cloudTagName:    m.Cloud,
		testNameTagName: m.TestName,
		ownerTagName:    m.Owner,
		logFileTagName:  logName,
	}
	return tagMap
}

// BuildLogMetadata creates an LogMetadata from runtime test information
func BuildLogMetadata(
	l *logger.Logger,
	spec *registry.TestSpec,
	cloud spec.Cloud,
	osName string, // FIXME
	arch vm.CPUArch,
	clusterName string,
) LogMetadata {
	l.Printf("os: %s", osName) // TODO delete this, just DEBUG

	// Fill in metadata from roachtest
	m := LogMetadata{
		TestName: spec.Name,
		Owner:    string(spec.Owner),
		Cloud:    cloud.String(),
		Platform: buildPlatformField(osName, arch),
		Cluster:  clusterName,
		Version:  os.Getenv("TC_BUILD_BRANCH"),
	}

	// Teamcity System Properties as key value pairs i.e., agent.name=gce-agent...
	// On TC UI, these properties are prefixed with "system."
	// In the properties file, the "system." prefix is omitted.
	l.Printf("teamcity build properties file: %s", tcBuildPropertyFile)
	if tcBuildPropertyFile != "" {
		file, err := os.Open(tcBuildPropertyFile)
		if err != nil {
			l.Printf("failed to open teamcity build properties file: %s", err)
		} else {
			defer file.Close()
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, tcSystemBuildPropertyAgentNamePrefix) {
					m.TCHost = strings.TrimPrefix(line, tcSystemBuildPropertyAgentNamePrefix)
				} else if strings.HasPrefix(line, tcSystemBuildPropertyBuildNumberPrefix) {
					m.TCBuildNumber = strings.TrimPrefix(line, tcSystemBuildPropertyBuildNumberPrefix)
				} else if strings.HasPrefix(line, tcSystemBuildPropertyClusterNamePrefix) {
					m.TCBuildConfName = strings.TrimPrefix(line, tcSystemBuildPropertyClusterNamePrefix)
				}
			}
		}
	}

	// Build Tags
	m.Tags = m.buildTags()

	l.Printf("Datadog Log Entry Metadata: %+v", m)
	return m
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
		return errors.Wrapf(err, "failed to parse and upload log file: %s, uploaded %d log entries",
			logPath, totalEntries)
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
// in memory at once and parallelizes network I/O for faster uploads.
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

	// Producer goroutine to parse file serially and send batches to workers
	g.GoCtx(func(ctx context.Context) error {
		defer close(batches) // Close channel when done to signal workers to exit

		file, err := os.Open(logPath)
		if err != nil {
			return err
		}
		defer file.Close()
		batch := make([]datadogV2.HTTPLogItem, 0, batchSize)

		scanner := bufio.NewScanner(file)
		// Increase buffer size for long log lines
		buf := make([]byte, 0, 64*1024) // 64 KB
		scanner.Buffer(buf, 1024*1024)  // 1 MB max line size

		skipCount := 0

		for scanner.Scan() {
			line := scanner.Text()

			entry, err := parseLogLine(line, logMeta)
			if err != nil {
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
func parseLogLine(line string, logMeta LogMetadata) (*datadogV2.HTTPLogItem, error) {
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
	entry.SetDdtags(formatTags(logMeta.Tags))
	entry.SetHostname(logMeta.TCHost)
	entry.AdditionalProperties = map[string]string{
		timestampAttributeName:   timestamp,
		clusterAttributeName:     logMeta.Cluster,
		buildNumberAttributeName: logMeta.TCBuildNumber,
	}
	// Duplicate tags into attributes for an improved UI experience
	for k, v := range logMeta.Tags {
		entry.AdditionalProperties[k] = v
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
