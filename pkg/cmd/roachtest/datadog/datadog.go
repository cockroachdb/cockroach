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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

const (
	// TeamCity environment variables
	envTCBuildBranch = "TC_BUILD_BRANCH"
	envTCBuildProps  = "TEAMCITY_BUILD_PROPERTIES_FILE"

	// Datadog environment variables
	envDatadogAPIKey = "DD_API_KEY"
	envDatadogSite   = "DD_SITE"
)

var (
	tcBuildPropertyFile = os.Getenv(envTCBuildProps)
	// logLineRegex matches roachtest test.log lines: YYYY/MM/DD HH:MM:SS file.go:message
	logLineRegex = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) `)
)

const (
	// TeamCity Build Property Value Prefixes
	// System Properties viewable from TeamCity UI under Parameters
	// When written to file envTCBuildProps, these properties are not prefixed
	// with "system" while on the UI they are prefixed with "system."
	tcSystemBuildPropertyAgentNamePrefix   = "agent.name="
	tcSystemBuildPropertyBuildNumberPrefix = "build.number="
	tcSystemBuildConfigurationPrefix       = "teamcity.buildType.id="

	// Test Result Values
	testResultPass = "PASS"
	testResultFail = "FAIL"
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
	timestampAttributeName          = "timestamp"
	clusterAttributeName            = "cluster"
	buildNumberAttributeName        = "build_number"
	buildConfigurationAttributeName = "build_configuration"
	resultAttributeName             = "result"
	durationAttributeName           = "duration"

	// Datadog Log Entry Static Values for roachtest test.log
	defaultEnv      = "ci"
	defaultDDSource = "teamcity"
	defaultService  = "roachtest"
)

// LogMetadata contains the test metadata that will be associated with each log
// entry in Datadog. Fields in LogMetadata will be passed along as either tags
// or log entry attributes.
// Typically, low-cardinality fields are added as tags,
// high-cardinality fields are added as attributes.
type LogMetadata struct {
	TestName        string
	Result          string // PASS or FAIL
	Duration        string // Duration of the test in seconds
	Owner           string
	Cloud           string
	Platform        string // e.g., linux-amd64, linux-arm64
	Version         string // branch name e.g., master, release-25.1
	Cluster         string // roachprod cluster e.g. teamcity-20801641-1764720129-01-n4cpu4
	TCHost          string // Teamcity Agent Name e.g., gce-agent-nightlies-roachtest-20240520-no-preempt-43
	TCBuildConfName string // TeamCity Build Configuration name e.g. Cockroach_Nightlies_RoachtestNightlyGceBazel
	TCBuildNumber   string // TeamCity Build Configuration execution instance
	LogName         string // e.g. test.log
	Tags            map[string]string
}

// makePlatform makes the platform field from OS and CPU architecture
func makePlatform(os string, arch vm.CPUArch) string {
	return fmt.Sprintf("%s-%s", os, arch)
}

// makeResult converts a test result bool to a string value
func makeResult(result bool) string {
	if result {
		return testResultPass
	}
	return testResultFail
}

// formatTags converts a map of tag names and values to a string in Datadog
// format: "key1:value1,key2:value2"
// Datadog client accepts empty tag values, that tag will be ignored
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

// makeTags makes a map of tag names and values to be added to Datadog log
// entries. Empty tag values do not cause errors.
func (m LogMetadata) makeTags() map[string]string {
	tagMap := map[string]string{
		envTagName:      defaultEnv,
		testNameTagName: m.TestName,
		ownerTagName:    m.Owner,
		cloudTagName:    m.Cloud,
		platformTagName: m.Platform,
		versionTagName:  m.Version,
		logFileTagName:  m.LogName,
	}
	return tagMap
}

// NewLogMetadata creates a LogMetadata from test execution information
func NewLogMetadata(
	l *logger.Logger,
	spec *registry.TestSpec,
	result bool,
	duration string,
	cloud spec.Cloud,
	osName string,
	arch vm.CPUArch,
	clusterName string,
	logName string,
) LogMetadata {

	m := LogMetadata{
		TestName: spec.Name,
		Result:   makeResult(result),
		Duration: duration,
		Owner:    string(spec.Owner),
		Cloud:    cloud.String(),
		Platform: makePlatform(osName, arch),
		Cluster:  clusterName,
		Version:  os.Getenv(envTCBuildBranch),
		LogName:  logName,
	}

	// Teamcity System Properties are available in the build properties file
	// separated by line as key value pairs, i.e., agent.name=gce-agent
	// On TC UI, these properties are prefixed with "system."
	// In the properties file that prefix is omitted.
	l.Printf("teamcity build properties file: %s", tcBuildPropertyFile)
	if tcBuildPropertyFile != "" {
		file, err := os.Open(tcBuildPropertyFile)
		if err != nil {
			// Reading the properties file is best effort. If it fails, we will
			// continue to process log events without the teamcity metadata.
			l.Printf("failed to open teamcity build properties file: %s", err)
		} else {
			defer file.Close()
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				switch {
				case strings.HasPrefix(line, tcSystemBuildPropertyAgentNamePrefix):
					m.TCHost = strings.TrimPrefix(line, tcSystemBuildPropertyAgentNamePrefix)
				case strings.HasPrefix(line, tcSystemBuildPropertyBuildNumberPrefix):
					m.TCBuildNumber = strings.TrimPrefix(line, tcSystemBuildPropertyBuildNumberPrefix)
				case strings.HasPrefix(line, tcSystemBuildConfigurationPrefix):
					m.TCBuildConfName = strings.TrimPrefix(line, tcSystemBuildConfigurationPrefix)
				}
			}
		}
	}

	// Make tag map from metadata fields
	m.Tags = m.makeTags()

	l.Printf("Datadog Log Entry Metadata: %+v", m)
	return m
}

// ShouldUploadLogs checks if we are on a release branch or master or if
// roachtestflags.DatadogAlwaysUpload is set
func ShouldUploadLogs() bool {
	if roachtestflags.DatadogAlwaysUpload {
		return true
	}
	branch := os.Getenv(envTCBuildBranch)
	if branch == "master" || strings.HasPrefix(branch, "release-") {
		return true
	}
	return false
}

// MaybeUploadTestLog reads the log at logPath and transforms each
// well-formatted line into Datadog HTTPLogItem JSON entries, then uploads them
// to Datadog in batches. Uses a worker pool to parallelize network I/O and not
// hold the entire log in memory.
// Return an error if datadog credentials are not set.
func MaybeUploadTestLog(
	ctx context.Context, l *logger.Logger, logPath string, cfg LogMetadata,
) error {
	uploadCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	datadogCtx, err := newDatadogContext(uploadCtx)
	if err != nil {
		return err
	}

	datadogLogger, err := l.ChildLogger("datadog", logger.QuietStdout)
	if err != nil {
		return err
	}

	// Create Datadog Logs API client
	// Default timeout is 60 seconds per HTTP request
	configuration := datadog.NewConfiguration()
	apiClient := datadog.NewAPIClient(configuration)
	logsAPI := datadogV2.NewLogsApi(apiClient)

	totalEntries, err := parseAndUploadLogFile(datadogCtx, datadogLogger, logsAPI, logPath, cfg)
	if err != nil {
		return errors.Wrapf(err, "failed to parse and upload log file: %s, uploaded %d log entries",
			logPath, totalEntries)
	}
	l.Printf("successfully uploaded %d log entries to Datadog", totalEntries)
	return nil
}

// newDatadogContext returns a derived datadog context from the provided context
// The context requires DD_API_KEY and DD_SITE to be set.
func newDatadogContext(ctx context.Context) (context.Context, error) {

	// Use DD_SITE and DD_API_KEY env vars if they are set, otherwise look for
	// values in roachtestflags and then set those values to the expected env
	// vars.
	if datadogSite := os.Getenv(envDatadogSite); datadogSite == "" {
		if datadogSite = roachtestflags.DatadogSite; datadogSite == "" {
			return nil, errors.Newf("%s env var or datadog-site flag is required", envDatadogSite)
		}
		if err := os.Setenv(envDatadogSite, datadogSite); err != nil {
			return nil, err
		}
	}
	if datadogAPIKey := os.Getenv(envDatadogAPIKey); datadogAPIKey == "" {
		return nil, errors.Newf(
			"%s env var is required", envDatadogAPIKey)
	}
	return datadog.NewDefaultContext(ctx), nil
}

// parseAndUploadLogFile reads and parses the log file serially. When a batch
// is full, it is sent to a worker pool for upload to parallelize network I/O.
// Best effort, a single worker uploading to datadog failing will not cancel
// the entire upload.
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

	// Create a task manager for this operation
	t := task.NewManager(ctx, l)
	defer t.Terminate(l)

	// Create an error group - errors are returned via WaitE()
	g := t.NewErrorGroup(task.WithContext(ctx), task.Logger(l))

	batches := make(chan []datadogV2.HTTPLogItem, 20) // Buffered channel for batches
	var totalUploaded atomic.Int64
	var totalFailed atomic.Int64

	// Start worker goroutines to upload batches concurrently
	for i := 0; i < numWorkers; i++ {
		g.Go(func(ctx context.Context, l *logger.Logger) error {
			for {
				select {
				case batch, ok := <-batches:
					if !ok {
						return nil
					}
					if err := uploadBatch(ctx, logsAPI, batch); err != nil {
						totalFailed.Add(int64(len(batch)))
						l.Printf("failed to upload batch of %d entries: %+v", len(batch), err)
						continue
					}
					count := totalUploaded.Add(int64(len(batch)))
					l.Printf("uploaded batch of %d entries (total so far: %d)", len(batch), count)
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	// Producer goroutine to parse file serially and send batches to workers
	g.Go(func(ctx context.Context, l *logger.Logger) error {
		defer close(batches) // Close channel when done to signal workers to exit

		file, err := os.Open(logPath)
		if err != nil {
			return err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		// Increase buffer size for long log lines
		buf := make([]byte, 0, 64*1024) // 64 KB
		scanner.Buffer(buf, 1024*1024)  // 1 MB max line size

		skipCount := 0
		batch := make([]datadogV2.HTTPLogItem, 0, batchSize)

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
			return errors.Wrap(err, "failed to scan log file, potentially encountered log line >1MB")
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
	if err := g.WaitE(); err != nil {
		return int(totalUploaded.Load()), err
	}

	totalEntriesSuccess := int(totalUploaded.Load())
	totalEntriesFailed := int(totalFailed.Load())

	if totalEntriesSuccess == 0 && totalEntriesFailed == 0 {
		return 0, errors.Newf(
			"No valid log entries found in %s . If expecting logs check the regex and timestamp", logPath)
	}
	if totalEntriesSuccess == 0 && totalEntriesFailed > 0 {
		return 0, errors.Newf("failed to upload all %d log entries from %s", totalEntriesFailed, logPath)
	}

	l.Printf("successfully uploaded %d log entries from %s", totalEntriesSuccess, logPath)
	l.Printf("failed to upload %d log entries from %s", totalEntriesFailed, logPath)
	return totalEntriesSuccess, nil
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
		timestampAttributeName:          timestamp,
		clusterAttributeName:            logMeta.Cluster,
		buildNumberAttributeName:        logMeta.TCBuildNumber,
		buildConfigurationAttributeName: logMeta.TCBuildConfName,
		resultAttributeName:             logMeta.Result,
		durationAttributeName:           logMeta.Duration,
	}
	// Duplicate tags into attributes for an improved UI experience
	for k, v := range logMeta.Tags {
		entry.AdditionalProperties[k] = v
	}
	return entry, nil
}

// uploadBatch sends a single batch of log entries to Datadog using the API client
func uploadBatch(
	ctx context.Context, logsAPI *datadogV2.LogsApi, entries []datadogV2.HTTPLogItem,
) error {
	// N.B. datadog-api-client-go v2.50.0 will silently fail to upload entries
	// with timestamps >18 hours from the current time. This can be avoided by
	// not using the API client and instead making a direct HTTP request to the
	// Datadog API. This shouldn't be an issue during runtime, but was observed
	// when backfilling.
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
