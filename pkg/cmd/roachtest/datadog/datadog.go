// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package datadog

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

// logFileParser pairs a line-start regex with a timestamp parser. The regex
// is used as a delimiter in the scanning loop (matching lines start a new log
// event) and must have a capture group for the raw timestamp string. The
// timestamp parser converts that raw string to RFC3339.
type logFileParser struct {
	lineRegex      *regexp.Regexp
	parseTimestamp func(string) (string, error)
}

var (
	tcBuildPropertyFile = os.Getenv(envTCBuildProps)

	// roachtestLineRegex matches roachtest log lines. It handles three prefix
	// variants that appear across different log files:
	//   YYYY/MM/DD HH:MM:SS file.go:message              (test.log)
	//   [step-name] YYYY/MM/DD HH:MM:SS file.go:message  (mixed-version entries)
	//   logname: YYYY/MM/DD HH:MM:SS file.go:message      (run_*.log, params.log, etc.)
	roachtestLineRegex = regexp.MustCompile(`^(?:\[[^\]]+\] )?(?:[\w._-]+: )?(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) `)

	// dmesgLineRegex matches dmesg log lines:
	//   [Mon Feb  9 07:14:51 2026] message
	dmesgLineRegex = regexp.MustCompile(`^\[(\w{3} \w{3} +\d{1,2} \d{2}:\d{2}:\d{2} \d{4})\] `)

	// journalctlLineRegex matches journalctl log lines:
	//   Feb 09 07:14:54 hostname process: message
	journalctlLineRegex = regexp.MustCompile(`^(\w{3} \d{2} \d{2}:\d{2}:\d{2}) `)

	// crdbV2LineRegex matches CockroachDB crdb-v2 log lines:
	//   I260210 23:00:19.379701 1 util/log/file_sync_buffer.go:237 ...
	//   W260211 01:03:31.169693 1354110 13@kv/kvserver/queue.go:1475 ...
	crdbV2LineRegex = regexp.MustCompile(`^[IWEF](\d{6} \d{2}:\d{2}:\d{2}\.\d{6}) `)

	roachtestParser = logFileParser{
		lineRegex:      roachtestLineRegex,
		parseTimestamp: parseRoachtestTimestamp,
	}
	dmesgParser = logFileParser{
		lineRegex:      dmesgLineRegex,
		parseTimestamp: parseDmesgTimestamp,
	}
	journalctlParser = logFileParser{
		lineRegex:      journalctlLineRegex,
		parseTimestamp: parseJournalctlTimestamp,
	}
	crdbV2Parser = logFileParser{
		lineRegex:      crdbV2LineRegex,
		parseTimestamp: parseCrdbV2Timestamp,
	}
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

// ShouldUploadLogsToDatadog determines if test logs should be uploaded to Datadog based
// on flag settings, branch, and test result.
//
// Default behavior: Upload logs only from failed tests on master/release branches.
// --datadog-send-logs-any-branch: Upload from any branch.
// --datadog-send-logs-any-result: Upload any result from master/release branches.
// Both flags: Upload any result from any branch.
func ShouldUploadLogsToDatadog(testFailed bool) bool {
	// Determine if we're on a branch that should upload
	branch := os.Getenv(envTCBuildBranch)
	isReleaseBranch := branch == "master" || strings.HasPrefix(branch, "release-")

	// Branch check: passes if send-logs-any-branch is set OR on release branch
	branchCheckPassed := roachtestflags.DatadogSendLogsAnyBranch || isReleaseBranch

	// Result check: passes if send-logs-any-result is set OR test failed
	resultCheckPassed := roachtestflags.DatadogSendLogsAnyResult || testFailed

	// Upload if both checks pass
	return branchCheckPassed && resultCheckPassed
}

// shouldUploadArtifactFile returns true if the given top-level artifact file
// name should be uploaded to Datadog. Uses an allowlist approach: only .log
// and select .txt files are included, with specific exclusions for files that
// are duplicated, empty, or not useful.
func shouldUploadArtifactFile(name string) bool {
	// Exclude rules (checked first).
	if strings.HasSuffix(name, ".failed") {
		// these files are empty
		return false
	}
	if name == "mixed-version-test.log" {
		// logs from mixed-version-test.log will be in test.log
		return false
	}
	if strings.HasPrefix(name, "tsdump.") {
		return false
	}

	// Include rules (allowlist).
	if strings.HasSuffix(name, ".log") {
		return true
	}
	if strings.HasSuffix(name, ".dmesg.txt") {
		return true
	}
	if strings.HasSuffix(name, ".journalctl.txt") {
		return true
	}

	return false
}

// segmentDateRegex matches the ISO 8601 date embedded in segment file names.
var segmentDateRegex = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T`)

// isNodeSegmentFile returns true if the file is a segment file (has a
// hostname/timestamp in the name, e.g. cockroach-health.teamcity-xxx.ubuntu.2026-02-10T23_00_19Z.004918.log).
func isNodeSegmentFile(name string) bool {
	if !strings.HasSuffix(name, ".log") {
		return false
	}
	// Segment files contain an ISO 8601 date in their name.
	// Base-name files (cockroach-health.log) and special files
	// (cockroach.exit.log) never contain dates.
	return segmentDateRegex.MatchString(name)
}

// isNodeBaseFile returns true if the file is a base-name log file
// (e.g. cockroach-health.log). These duplicate the last segment and
// are only uploaded as a fallback when no segments exist for the channel.
func isNodeBaseFile(name string) bool {
	if !strings.HasSuffix(name, ".log") {
		return false
	}
	// Exclude special files that don't have segments.
	if name == "cockroach.exit.log" ||
		name == "cockroach.stderr.log" ||
		name == "cockroach.stdout.log" {
		return false
	}
	baseName := strings.TrimSuffix(name, ".log")
	return !strings.Contains(baseName, ".")
}

// nodeFileChannel extracts the channel prefix from a node log file name.
// For example:
//
//	cockroach-health.teamcity-xxx.log → cockroach-health
//	cockroach-health.log              → cockroach-health
//	cockroach.teamcity-xxx.log        → cockroach
//	cockroach.log                     → cockroach
func nodeFileChannel(name string) string {
	baseName := strings.TrimSuffix(name, ".log")
	if idx := strings.Index(baseName, "."); idx != -1 {
		return baseName[:idx]
	}
	return baseName
}

// shouldUploadNodeFile returns true if the given file name from a
// logs/N.unredacted/ directory should be uploaded. This covers
// non-log files and special cockroach files. Segment vs base-name
// filtering is handled by discoverLogFiles.
func shouldUploadNodeFile(name string) bool {
	if name == "diskusage.txt" {
		return true
	}
	if name == "roachprod.log" {
		return true
	}
	// Special cockroach files that don't have segments.
	if name == "cockroach.exit.log" ||
		name == "cockroach.stderr.log" ||
		name == "cockroach.stdout.log" {
		return true
	}
	return false
}

// discoverLogFiles returns the list of file paths in artifactsDir that should
// be uploaded to Datadog. Top-level files are filtered by shouldUploadArtifactFile.
// Additionally, node-level logs from logs/N.unredacted/ directories are
// included, filtered by shouldUploadNodeFile.
func discoverLogFiles(l *logger.Logger, artifactsDir string) ([]string, error) {
	entries, err := os.ReadDir(artifactsDir)
	if err != nil {
		return nil, errors.Wrapf(err, "reading artifacts directory %s", artifactsDir)
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if shouldUploadArtifactFile(entry.Name()) {
			files = append(files, filepath.Join(artifactsDir, entry.Name()))
		}
	}

	// Scan logs/N.unredacted/ directories for node-level CockroachDB logs.
	// The logs/ directory may not exist (e.g. no cluster was created).
	logsDir := filepath.Join(artifactsDir, "logs")
	logsDirEntries, _ := os.ReadDir(logsDir)
	for _, nodeDirEntry := range logsDirEntries {
		if !nodeDirEntry.IsDir() {
			continue
		}
		if !strings.HasSuffix(nodeDirEntry.Name(), ".unredacted") {
			continue
		}
		unredactedDir := filepath.Join(logsDir, nodeDirEntry.Name())
		nodeFiles, err := os.ReadDir(unredactedDir)
		if err != nil {
			l.Printf("WARN: failed to read node log directory %s: %v", unredactedDir, err)
			continue
		}

		// Collect segment files and base-name fallbacks separately.
		// Base-name files (e.g. cockroach-health.log) duplicate the last
		// segment, so they are only included if no segments exist for
		// that channel.
		segmentChannels := make(map[string]bool)
		var baseFiles []os.DirEntry
		for _, f := range nodeFiles {
			if f.IsDir() {
				continue
			}
			if shouldUploadNodeFile(f.Name()) {
				files = append(files, filepath.Join(unredactedDir, f.Name()))
			} else if isNodeSegmentFile(f.Name()) {
				channel := nodeFileChannel(f.Name())
				segmentChannels[channel] = true
				files = append(files, filepath.Join(unredactedDir, f.Name()))
			} else if isNodeBaseFile(f.Name()) {
				baseFiles = append(baseFiles, f)
			}
		}
		// Include base-name files only for channels with no segments.
		for _, f := range baseFiles {
			channel := nodeFileChannel(f.Name())
			if !segmentChannels[channel] {
				files = append(files, filepath.Join(unredactedDir, f.Name()))
			}
		}
	}

	return files, nil
}

// MaybeUploadTestLogs discovers files that contain logs in artifactsDir and
// uploads them to Datadog. The LogName and Tags fields of cfg are set
// per-file. Upload is best effort, a single file uploading to datadog failing
// will not cancel the entire upload. Uses a worker pool to parallelize
// network I/O and not hold the entire log in memory.
// Returns an error if datadog credentials are not set.
func MaybeUploadTestLogs(
	ctx context.Context, l *logger.Logger, artifactsDir string, cfg LogMetadata,
) error {
	uploadCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
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

	files, err := discoverLogFiles(l, artifactsDir)
	if err != nil {
		return err
	}

	l.Printf("discovered %d log files to upload from %s", len(files), artifactsDir)
	l.Printf("files: %v", files)
	overallStart := timeutil.Now()
	var totalBytes int64
	for _, logPath := range files {
		// Use relative path from artifactsDir as the log name so that
		// node-level logs include their directory context
		// (e.g. "logs/1.unredacted/cockroach-health.teamcity-...log").
		logName, err := filepath.Rel(artifactsDir, logPath)
		if err != nil {
			l.Printf("failed to compute relative path for %s and %s: %v", artifactsDir, logPath, err)
		}

		if info, statErr := os.Stat(logPath); statErr == nil {
			totalBytes += info.Size()
		}

		// Create a per-file copy of metadata with the correct LogName and tags.
		fileCfg := cfg
		fileCfg.LogName = logName
		fileCfg.Tags = fileCfg.makeTags()

		// selectParser uses the base file name to choose the right parser.
		parser := selectParser(filepath.Base(logPath))
		fileStart := timeutil.Now()
		totalEntries, uploadErr := parseAndUploadLogFile(
			datadogCtx, datadogLogger, logsAPI, logPath, fileCfg, parser,
		)
		if uploadErr != nil {
			// Best effort: log and continue.
			l.Printf("error uploading %s to Datadog (%d entries uploaded): %v",
				logName, totalEntries, uploadErr)
		}
		l.Printf("uploaded %d log events from %s in %s", totalEntries, logName, timeutil.Since(fileStart))
	}
	l.Printf("finished uploading all log files to Datadog in %s (%s total)",
		timeutil.Since(overallStart), humanizeBytes(totalBytes))
	return nil
}

func humanizeBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
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
	parser logFileParser,
) (int, error) {

	const numWorkers = 10
	// Datadog allows up to 1000 items per batch, but large multiline entries
	// can push the payload over the 5MB size limit. We use a lower batch size
	// to stay under that threshold.
	const batchSize = 500

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

		// sendBatch sends the current batch to workers when full.
		sendBatch := func() error {
			if len(batch) >= batchSize {
				batchCopy := make([]datadogV2.HTTPLogItem, len(batch))
				copy(batchCopy, batch)

				select {
				case batches <- batchCopy:
					batch = batch[:0] // Reset batch, reusing underlying array
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}

		// finalizeLogEvent parses the accumulated lines as a single log event
		// and appends it to the current batch. If continuation lines were
		// dropped due to the cap, a truncation marker is appended.
		finalizeLogEvent := func(lines []string, droppedLogLines int) {
			message := strings.Join(lines, "\n")
			if droppedLogLines > 0 {
				message += fmt.Sprintf("\n[truncated %d continuation lines]", droppedLogLines)
			}
			entry, err := parseLogLine(message, logMeta, parser)
			if err != nil {
				skipCount++
			} else {
				batch = append(batch, *entry)
			}
		}

		// Use the parser's regex as a delimiter: lines matching the
		// timestamp pattern start a new log event entry, and subsequent
		// non-matching lines are appended as continuations of the current
		// log event. Continuation lines are capped at maxContinuationLines
		// to bound memory usage and respect Datadog entry size limits.
		const maxContinuationLines = 500
		var currentLogLines []string
		droppedLogLines := 0

		for scanner.Scan() {
			logLine := scanner.Text()

			if parser.lineRegex.MatchString(logLine) {
				// New log event found.
				// Finalize the previous accumulated log event (if any).
				if len(currentLogLines) > 0 {
					finalizeLogEvent(currentLogLines, droppedLogLines)
					if err := sendBatch(); err != nil {
						return err
					}
				}
				// Start accumulating a new log event.
				currentLogLines = []string{logLine}
				droppedLogLines = 0
			} else {
				if len(currentLogLines) > 0 {
					if len(currentLogLines) <= maxContinuationLines {
						// Continuation log line — append to current log event.
						currentLogLines = append(currentLogLines, logLine)
					} else {
						droppedLogLines++
					}
				} else {
					// Line before any timestamp match — no log event to attach to.
					skipCount++
				}
			}
		}

		// Finalize the last accumulated log event after EOF.
		if len(currentLogLines) > 0 {
			finalizeLogEvent(currentLogLines, droppedLogLines)
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
		// No lines matched the timestamp regex (e.g. failure_*.log files
		// that contain raw stack traces). Fall back to uploading up to 1MB
		// of the file as a single log entry without an extracted timestamp.
		const maxRawUploadBytes = 1 << 20 // 1MB
		f, readErr := os.Open(logPath)
		if readErr != nil {
			return 0, errors.Wrapf(readErr, "reading %s for raw upload", logPath)
		}
		defer f.Close()
		content, readErr := io.ReadAll(io.LimitReader(f, int64(maxRawUploadBytes)+1))
		if readErr != nil {
			return 0, errors.Wrapf(readErr, "reading %s for raw upload", logPath)
		}
		if len(content) == 0 {
			l.Printf("skipping empty file %s", logPath)
			return 0, nil
		}
		truncated := len(content) > maxRawUploadBytes
		if truncated {
			content = content[:maxRawUploadBytes]
		}
		msg := string(content)
		if truncated {
			msg += "\n[truncated]"
			l.Printf("WARN: %s exceeds 1MB, truncating for raw upload", logPath)
		}
		entry := datadogV2.NewHTTPLogItem(msg)
		entry.SetDdsource(defaultDDSource)
		entry.SetService(defaultService)
		entry.SetDdtags(formatTags(logMeta.Tags))
		entry.SetHostname(logMeta.TCHost)
		entry.AdditionalProperties = map[string]string{
			clusterAttributeName:            logMeta.Cluster,
			buildNumberAttributeName:        logMeta.TCBuildNumber,
			buildConfigurationAttributeName: logMeta.TCBuildConfName,
			resultAttributeName:             logMeta.Result,
			durationAttributeName:           logMeta.Duration,
		}
		for k, v := range logMeta.Tags {
			entry.AdditionalProperties[k] = v
		}
		if err := uploadBatch(ctx, logsAPI, []datadogV2.HTTPLogItem{*entry}); err != nil {
			return 0, errors.Wrapf(err, "uploading raw file %s", logPath)
		}
		l.Printf("uploaded %s as single raw log entry (no timestamp lines matched)", logPath)
		return 1, nil
	}
	if totalEntriesSuccess == 0 && totalEntriesFailed > 0 {
		return 0, errors.Newf("failed to upload all %d log entries from %s", totalEntriesFailed, logPath)
	}

	l.Printf("successfully uploaded %d log entries from %s", totalEntriesSuccess, logPath)
	l.Printf("failed to upload %d log entries from %s", totalEntriesFailed, logPath)
	return totalEntriesSuccess, nil
}

// parseRoachtestTimestamp converts a roachtest timestamp (YYYY/MM/DD HH:MM:SS)
// to ISO8601 (RFC3339).
func parseRoachtestTimestamp(ts string) (string, error) {
	t, err := time.Parse("2006/01/02 15:04:05", ts)
	if err != nil {
		return "", err
	}
	return t.UTC().Format(time.RFC3339), nil
}

// parseDmesgTimestamp converts a dmesg timestamp (Day Mon DD HH:MM:SS YYYY)
// to ISO8601 (RFC3339).
func parseDmesgTimestamp(ts string) (string, error) {
	t, err := time.Parse("Mon Jan _2 15:04:05 2006", ts)
	if err != nil {
		return "", err
	}
	return t.UTC().Format(time.RFC3339), nil
}

// parseJournalctlTimestamp converts a journalctl timestamp (Mon DD HH:MM:SS)
// to ISO8601 (RFC3339). Journalctl timestamps lack a year, so the current
// year is assumed.
func parseJournalctlTimestamp(ts string) (string, error) {
	t, err := time.Parse("Jan 02 15:04:05", ts)
	if err != nil {
		return "", err
	}
	// Journalctl timestamps don't include a year; assume current year.
	now := timeutil.Now().UTC()
	t = t.AddDate(now.Year(), 0, 0)
	// Journalctl timestamps have no year. If applying the current year
	// produces a future date (e.g. a "Dec 31" entry parsed on Jan 1),
	// the entry must be from the previous year.
	if t.After(now) {
		t = t.AddDate(-1, 0, 0)
	}
	return t.UTC().Format(time.RFC3339), nil
}

// parseCrdbV2Timestamp converts a crdb-v2 timestamp (YYMMDD HH:MM:SS.UUUUUU)
// to ISO8601 (RFC3339).
func parseCrdbV2Timestamp(ts string) (string, error) {
	t, err := time.Parse("060102 15:04:05.000000", ts)
	if err != nil {
		return "", err
	}
	return t.UTC().Format(time.RFC3339), nil
}

// selectParser returns the appropriate logFileParser for the given file name.
func selectParser(fileName string) logFileParser {
	if strings.HasSuffix(fileName, ".dmesg.txt") {
		return dmesgParser
	}
	if strings.HasSuffix(fileName, ".journalctl.txt") {
		return journalctlParser
	}
	// CockroachDB node log files use the crdb-v2 format.
	// Segment files: cockroach.teamcity-..., cockroach-health.teamcity-..., etc.
	// Special files (cockroach.exit.log, cockroach.stderr.log, cockroach.stdout.log)
	// don't match crdb-v2 format but will hit the raw fallback.
	if strings.HasPrefix(fileName, "cockroach") {
		return crdbV2Parser
	}
	return roachtestParser
}

// parseLogLine parses a log entry (which may span multiple lines) into an
// HTTPLogItem. The first line must match the parser's regex; subsequent lines
// are continuation lines that were appended by the caller.
// Returns an error if the entry doesn't match the expected pattern or has an
// invalid timestamp.
func parseLogLine(
	line string, logMeta LogMetadata, parser logFileParser,
) (*datadogV2.HTTPLogItem, error) {
	matches := parser.lineRegex.FindStringSubmatch(line)
	if len(matches) < 2 {
		return nil, errors.New("line does not match pattern")
	}

	timestampStr := matches[1]
	timestamp, err := parser.parseTimestamp(timestampStr)
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
