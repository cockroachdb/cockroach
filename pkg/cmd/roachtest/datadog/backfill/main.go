// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"archive/zip"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/datadog"
	"github.com/cockroachdb/cockroach/pkg/release"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

/*
Build
./dev build pkg/cmd/roachtest/datadog/backfill

Usage (single path)
./bin/backfill --dry-run --platform=linux-amd64 \
  "/Users/wchoe/Downloads/Roachtests_Roachtest_Nightly_-_GCE_Bazel_9842_artifacts"

Usage (multiple paths - recommended to quote each path)
./bin/backfill --dry-run --platform=linux-amd64 \
  "/Users/wchoe/Downloads/Roachtests_Roachtest_Nightly_-_GCE_Bazel_9822_artifacts" \
  "/Users/wchoe/Downloads/Roachtests_Roachtest_Nightly_-_GCE_Bazel_9832_artifacts" \
  "/Users/wchoe/Downloads/Roachtests_Roachtest_Nightly_-_GCE_Bazel_9842_artifacts"

# Or with glob pattern
./bin/backfill --dry-run --platform=linux-amd64 \
  /Users/wchoe/Downloads/Roachtests_Roachtest_Nightly_-_GCE_Bazel_*_artifacts

Note: The tool automatically extracts:
- Cloud provider from filename (GCE->gcp, AWS->aws, Azure->azure)
- Build/job number from filename (e.g., 9842 from "..._Bazel_9842_artifacts")
  This is added as the "build_id" tag in Datadog logs.

*/

var (
	platform = flag.String("platform", "", "Platform tag (e.g., linux-amd64). Auto-detected from filename if possible.")
	apiKey   = flag.String("api-key", "", "Datadog API key. Defaults to DATADOG_API_KEY environment variable.")
	version  = flag.String("version", "master", "Version/branch tag for the logs")
	dryRun   = flag.Bool("dry-run", false, "Parse and show what would be uploaded without actually uploading")
)

// inferCloudFromFilename extracts the cloud provider from the artifact filename/path
// Examples:
//   - "Roachtests_Roachtest_Nightly_-_GCE_Bazel_9842_artifacts" -> "gcp"
//   - "Roachtests_Roachtest_Nightly_-_AWS_Bazel_1234_artifacts" -> "aws"
//   - "Roachtests_Roachtest_Nightly_-_Azure_Bazel_5678_artifacts" -> "azure"
func inferCloudFromFilename(filename string) (string, error) {
	upper := strings.ToUpper(filename)

	if strings.Contains(upper, "GCE") {
		return "gcp", nil
	}
	if strings.Contains(upper, "AWS") {
		return "aws", nil
	}
	if strings.Contains(upper, "AZURE") {
		return "azure", nil
	}
	if strings.Contains(upper, "IBM") {
		return "ibm", nil
	}

	return "", fmt.Errorf("unable to infer cloud from filename %s", filename)
}

// buildIDRegex matches the build/job number in artifact filenames
// Example: "Roachtests_Roachtest_Nightly_-_GCE_Bazel_9842_artifacts" -> "9842"
var buildIDRegex = regexp.MustCompile(`_(\d+)_artifacts`)

// extractBuildID extracts the CI build/job number from the artifact filename
// Returns empty string if not found
func extractBuildID(filename string) (string, error) {
	matches := buildIDRegex.FindStringSubmatch(filename)
	if len(matches) >= 2 {
		return matches[1], nil
	}
	return "", fmt.Errorf("unable to extract build ID from filename %s", filename)
}

// inferPlatformFromFilename attempts to detect platform from filename patterns
// Returns empty string if unable to infer
// Inferring the platform from the filename is not currently possible, commenting out for now
//func inferPlatformFromFilename(l *logger.Logger, filename string) string {
//	upper := strings.ToUpper(filename)
//	l.Printf("inferring platform from filename: %s", filename)
//
//	// Check for ARM64 patterns
//	if strings.Contains(upper, "ARM64") || strings.Contains(upper, "AARCH64") {
//		return "linux-arm64"
//	}
//
//	// Check for FIPS pattern
//	if strings.Contains(upper, "FIPS") {
//		return "linux-fips"
//	}
//
//	// Check for s390x
//	if strings.Contains(upper, "S390X") {
//		return "linux-s390x"
//	}
//
//	// Default to amd64 if we see standard patterns
//	if strings.Contains(upper, "BAZEL") || strings.Contains(upper, "NIGHTLY") {
//		return "linux-amd64"
//	}
//
//	return ""
//}

// testLogPathRegex matches paths like:
//   - <base>/test-name/run_1/test.log
//   - <base>/test-name/run_1/artifacts/test.log
//   - <base>/test-name/param1=value1/param2=value2/run_1/test.log
var testLogPathRegex = regexp.MustCompile(`^(.+?)/run_\d+/(?:artifacts/)?test\.log$`)

// extractTestName extracts the test name from a test.log path
// For example:
//   - "acceptance/event-log/run_1/artifacts/test.log" -> "acceptance/event-log"
//   - "kv0/enc=false/nodes=3/run_1/test.log" -> "kv0/enc=false/nodes=3"
func extractTestName(relPath string) string {
	matches := testLogPathRegex.FindStringSubmatch(relPath)
	if len(matches) >= 2 {
		return matches[1]
	}
	return "unknown"
}

// processZipFile extracts and processes a zip file
func processZipFile(
	ctx context.Context, l *logger.Logger, zipPath string, cfg datadog.LogMetadata,
) error {
	// Create temp directory for extraction
	tempDir, err := os.MkdirTemp("", "roachtest-datadog-backfill-")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			l.Printf("warning: failed to remove temp directory %s: %v", tempDir, err)
		}
	}()

	l.Printf("extracting %s to %s", zipPath, tempDir)

	// Extract zip
	if err := extractZip(zipPath, tempDir); err != nil {
		return fmt.Errorf("failed to extract zip: %w", err)
	}

	// Process the extracted directory
	return processDirectory(ctx, l, tempDir, cfg)
}

// extractZip extracts a zip file to a destination directory
func extractZip(zipPath, destDir string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = r.Close() // Best effort close
	}()

	for _, f := range r.File {
		fpath := filepath.Join(destDir, f.Name)

		// Check for ZipSlip vulnerability
		if !strings.HasPrefix(fpath, filepath.Clean(destDir)+string(os.PathSeparator)) {
			return fmt.Errorf("invalid file path: %s", fpath)
		}

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(fpath, os.ModePerm); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}

		rc, err := f.Open()
		if err != nil {
			outFile.Close()
			return err
		}

		_, err = io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()

		if err != nil {
			return err
		}
	}

	return nil
}

// logFileInfo tracks a test.log file and its source location
type logFileInfo struct {
	path           string // actual file path (might be in temp dir if extracted)
	originalPath   string // original path relative to base dir (for test name extraction)
	fromExtraction bool   // whether this came from an extracted zip
}

// processDirectory walks a directory and uploads all test.log files
// It also extracts any artifacts.zip files and processes test.log files within them
func processDirectory(
	ctx context.Context, l *logger.Logger, dirPath string, cfg datadog.LogMetadata,
) error {
	var logFiles []logFileInfo
	var artifactZips []string

	// Find all test.log files and artifacts.zip files
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		baseName := filepath.Base(path)
		if baseName == "test.log" {
			relPath, _ := filepath.Rel(dirPath, path)
			logFiles = append(logFiles, logFileInfo{
				path:           path,
				originalPath:   relPath,
				fromExtraction: false,
			})
		} else if baseName == "artifacts.zip" {
			artifactZips = append(artifactZips, path)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
	}

	l.Printf("found %d test.log files and %d artifacts.zip files", len(logFiles), len(artifactZips))

	// Track temp directories for cleanup
	var tempDirs []string
	defer func() {
		for _, dir := range tempDirs {
			if err := os.RemoveAll(dir); err != nil {
				l.Printf("warning: failed to remove temp directory %s: %v", dir, err)
			}
		}
	}()

	// Extract artifacts.zip files and find test.log files within them
	for _, zipPath := range artifactZips {
		tempDir, err := os.MkdirTemp("", "artifacts-extract-")
		if err != nil {
			l.Printf("warning: failed to create temp dir for %s: %v", zipPath, err)
			continue
		}
		tempDirs = append(tempDirs, tempDir)

		l.Printf("extracting %s", zipPath)
		if err := extractZip(zipPath, tempDir); err != nil {
			l.Printf("warning: failed to extract %s: %v", zipPath, err)
			continue
		}

		// Get the path to the zip relative to dirPath for test name extraction
		// e.g., "tpcc/w=100/nodes=3/chaos=true/run_1/artifacts.zip"
		relZipPath, _ := filepath.Rel(dirPath, zipPath)

		// Find test.log in the extracted directory
		err = filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && filepath.Base(path) == "test.log" {
				// Construct the "original" path as if test.log was in the same dir as the zip
				// Remove "/artifacts.zip" and add "/test.log"
				originalPath := strings.TrimSuffix(relZipPath, "/artifacts.zip") + "/test.log"

				logFiles = append(logFiles, logFileInfo{
					path:           path,
					originalPath:   originalPath,
					fromExtraction: true,
				})
			}
			return nil
		})

		if err != nil {
			l.Printf("warning: failed to walk extracted dir %s: %v", tempDir, err)
		}
	}

	l.Printf("total %d test.log files to process (including extracted)", len(logFiles))

	// Process each test.log
	for _, logInfo := range logFiles {
		// Extract test name from the original path
		testName := extractTestName(logInfo.originalPath)

		// Create a new config with the test name
		testCfg := cfg
		testCfg.TestName = testName
		l.Printf("testCfg: %+v", testCfg)

		if *dryRun {
			l.Printf("[DRY RUN] would upload %s (test: %s, cloud: %s, platform: %s, build_id: %s)",
				logInfo.originalPath, testName, testCfg.Cloud, testCfg.Platform, testCfg.BuildID)
			continue
		}

		l.Printf("uploading %s (test: %s)", logInfo.originalPath, testName)

		if err := datadog.MaybeUploadTestLog(ctx, l, logInfo.path, testCfg); err != nil {
			l.Printf("error uploading %s: %v", logInfo.originalPath, err)
			// Continue with other logs even if one fails
			continue
		}
	}

	return nil
}

// processArtifactPath processes a single artifact path (directory or zip)
func processArtifactPath(
	ctx context.Context, l *logger.Logger, artifactPath string, key string,
) error {
	// Infer cloud from filename
	cloud, err := inferCloudFromFilename(filepath.Base(artifactPath))
	if err != nil {
		return err
	}

	// Extract build ID from filename
	buildID, err := extractBuildID(filepath.Base(artifactPath))
	if err != nil {
		return err
	}
	l.Printf("extracted build ID: %s", buildID)

	// Infer or require platform
	plat := *platform
	//if plat == "" {
	//	plat = inferPlatformFromFilename(l, filepath.Base(artifactPath))
	//	l.Printf("inferred platform from filename: %s", plat)
	//}
	if plat == "" {
		return fmt.Errorf("Please specify --platform flag (e.g., --platform=linux-amd64)")
	}
	found := false
	for _, supportedPlatform := range release.DefaultPlatforms() {
		if string(supportedPlatform) == plat {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("Unsupported platform: %s", plat)
	}

	//if plat
	l.Printf("detected platform: %s", plat)

	// Set API key via environment variable for the Datadog client
	if key != "" {
		if err := os.Setenv("DATADOG_API_KEY", key); err != nil {
			return fmt.Errorf("failed to set DATADOG_API_KEY: %w", err)
		}
	}

	// Create upload config
	cfg := datadog.LogMetadata{
		Cloud:    cloud,
		Platform: plat,
		// Placeholder host for backfilled logs, for runtime logs this should be something like
		// e.g. gce-agent-nightlies-roachtest-20240520-no-preempt-29
		Host:    "backfill",
		Version: *version,
		BuildID: buildID,
		Cluster: "backfill",
		Owner:   "unknown", // Not available for backfilled logs
	}

	// Check if path is a zip file or directory
	info, err := os.Stat(artifactPath)
	if err != nil {
		return err
	}

	l.Printf("processing %s (cloud: %s, platform: %s, version: %s, build_id: %s)", artifactPath, cloud, plat, *version, buildID)

	if info.IsDir() {
		// Process directory directly
		return processDirectory(ctx, l, artifactPath, cfg)
	} else if strings.HasSuffix(artifactPath, ".zip") {
		// Extract and process zip
		return processZipFile(ctx, l, artifactPath, cfg)
	} else {
		return fmt.Errorf("path must be a directory or .zip file")
	}
}

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <artifact-path> [<artifact-path>...]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nProcess one or more artifact directories or zip files.\n")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	artifactPaths := flag.Args()

	// Get API key from flag or environment
	key := *apiKey
	if key == "" {
		key = os.Getenv("DATADOG_API_KEY")
	}
	if key == "" && !*dryRun {
		fmt.Fprintf(os.Stderr, "Error: DATADOG_API_KEY is required (set via --api-key flag or DATADOG_API_KEY env var)\n")
		os.Exit(1)
	}

	// Create logger - use a temp file for the log output
	//logFile, err := os.CreateTemp("", "roachtest-datadog-backfill-*.log")
	//fmt.Fprintf("log file: %s", logFile.Name())
	//if err != nil {
	//	fmt.Fprintf(os.Stderr, "Error creating log file: %v\n", err)
	//	os.Exit(1)
	//}
	//logPath := logFile.Name()
	//logFile.Close()

	l, err := logger.RootLogger("backfill.log", logger.NoTee)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating logger: %v\n", err)
		return
	}
	defer l.Close()

	ctx := context.Background()

	l.Printf("processing %d artifact path(s)", len(artifactPaths))

	// Process each artifact path
	var failedPaths []string
	for i, artifactPath := range artifactPaths {
		l.Printf("[%d/%d] starting: %s", i+1, len(artifactPaths), artifactPath)

		if err := processArtifactPath(ctx, l, artifactPath, key); err != nil {
			l.Printf("[%d/%d] error processing %s: %v", i+1, len(artifactPaths), artifactPath, err)
			failedPaths = append(failedPaths, artifactPath)
			// Continue with other paths even if one fails
			continue
		}

		l.Printf("[%d/%d] completed: %s", i+1, len(artifactPaths), artifactPath)
	}

	if len(failedPaths) > 0 {
		l.Printf("failed to process %d path(s):", len(failedPaths))
		for _, path := range failedPaths {
			l.Printf("  - %s", path)
		}
		return
	}

	l.Printf("done! successfully processed all %d artifact path(s)", len(artifactPaths))
}

/*
// Logic for uploading batches of log entries to datadog, needed for backfilling
// because using the datadog api client can't backfill, but this can

//// TODO remove when parallel upload is working
//// uploadEntries sends log entries to Datadog in batches
//func uploadEntries(
//	ctx context.Context, logsAPI *datadogV2.LogsApi, entries []datadogV2.HTTPLogItem,
//) error {
//	// Any individual log entry exceeding 1MB is accepted and truncated by Datadog
//	// Maximum batch size is 1000 log entries
//	// Maximum payload size is 1MB
//	const batchSize = 1000
//
//	for i := 0; i < len(entries); i += batchSize {
//		end := i + batchSize
//		if end > len(entries) {
//			end = len(entries)
//		}
//
//		batch := entries[i:end]
//		if err := uploadBatch(ctx, logsAPI, batch); err != nil {
//			return errors.Wrapf(err, "failed to upload batch %d-%d", i, end)
//		}
//	}
//	return nil
//}

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
*/
