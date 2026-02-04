// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/klauspost/compress/zstd"
	"github.com/spf13/cobra"
)

var debugTimeSeriesUploadBenchmarkCmd = &cobra.Command{
	Use:   "upload-benchmark <input.gob>",
	Short: "benchmark upload time to Datadog for different compression formats",
	Long: `
Creates compressed versions of a tsdump file and benchmarks processing time.

Use --dry-run to measure processing time without actual network uploads.
This isolates decompression + processing time from network latency.

Example:
  cockroach debug tsdump upload-benchmark /path/to/tsdump.gob \
    --cluster-label=my-cluster --dry-run
`,
	Args: cobra.ExactArgs(1),
	RunE: clierrorplus.MaybeDecorateError(runUploadBenchmark),
}

func init() {
	debugTimeSeriesDumpCmd.AddCommand(debugTimeSeriesUploadBenchmarkCmd)

	f := debugTimeSeriesUploadBenchmarkCmd.Flags()
	f.BoolVar(&debugTimeSeriesDumpOpts.dryRun, "dry-run", false,
		"skip actual uploads, measure processing time only")
	f.StringVar(&debugTimeSeriesDumpOpts.ddApiKey, "dd-api-key", "",
		"Datadog API key (required unless --dry-run)")
	f.StringVar(&debugTimeSeriesDumpOpts.clusterLabel, "cluster-label", "",
		"cluster label for tagging uploads (required)")
	f.StringVar(&debugTimeSeriesDumpOpts.ddSite, "dd-site", defaultDDSite,
		"Datadog site to upload to")
	f.IntVar(&debugTimeSeriesDumpOpts.noOfUploadWorkers, "upload-workers", 75,
		"number of workers for parallel upload")
}

func runUploadBenchmark(cmd *cobra.Command, args []string) error {
	inputPath := args[0]

	if !debugTimeSeriesDumpOpts.dryRun && debugTimeSeriesDumpOpts.ddApiKey == "" {
		return errors.New("--dd-api-key is required (or use --dry-run)")
	}
	if debugTimeSeriesDumpOpts.clusterLabel == "" {
		return errors.New("--cluster-label is required")
	}

	fmt.Printf("Input file: %s\n", inputPath)
	fmt.Printf("Cluster label: %s\n", debugTimeSeriesDumpOpts.clusterLabel)
	if debugTimeSeriesDumpOpts.dryRun {
		fmt.Println("Mode: DRY-RUN (no actual uploads, measuring processing time only)")
	} else {
		fmt.Println("Mode: LIVE (uploading to Datadog)")
	}
	fmt.Println()

	tmpDir, err := os.MkdirTemp("", "tsdump-upload-benchmark-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Test configurations
	tests := []struct {
		name       string
		suffix     string
		path       string
		wrapWriter func(io.Writer) io.WriteCloser
	}{
		{"Raw GOB", "-raw", tmpDir + "/tsdump.gob", nil},
		{"GOB + Gzip", "-gzip", tmpDir + "/tsdump.gob.gz", func(w io.Writer) io.WriteCloser {
			return gzip.NewWriter(w)
		}},
		{"GOB + ZSTD", "-zstd", tmpDir + "/tsdump.gob.zst", func(w io.Writer) io.WriteCloser {
			zw, _ := zstd.NewWriter(w)
			return zw
		}},
	}

	results := []struct {
		name       string
		size       int64
		createTime float64
		uploadTime float64
		status     string
	}{}

	originalLabel := debugTimeSeriesDumpOpts.clusterLabel

	// Process each format one at a time: create → upload → delete
	for _, t := range tests {
		fmt.Printf("\n=== %s ===\n", t.name)

		// Step 1: Create file
		fmt.Printf("  Creating file...\n")
		start := time.Now()
		size, err := createCompressedTestFile(inputPath, t.path, t.wrapWriter)
		createTime := time.Since(start).Seconds()
		if err != nil {
			fmt.Printf("  Error creating: %v\n", err)
			results = append(results, struct {
				name       string
				size       int64
				createTime float64
				uploadTime float64
				status     string
			}{t.name, 0, createTime, 0, "Error: " + err.Error()})
			continue
		}
		fmt.Printf("  Created: %s (%.1fs)\n", formatBytes(size), createTime)

		// Step 2: Upload/Process
		debugTimeSeriesDumpOpts.clusterLabel = originalLabel + t.suffix
		if debugTimeSeriesDumpOpts.dryRun {
			fmt.Printf("  Processing (dry-run)...\n")
		} else {
			fmt.Printf("  Uploading to Datadog (cluster: %s)...\n", debugTimeSeriesDumpOpts.clusterLabel)
		}

		datadogWriter, err := makeDatadogWriter(
			debugTimeSeriesDumpOpts.ddSite,
			false, // init mode = false
			debugTimeSeriesDumpOpts.ddApiKey,
			datadogSeriesThreshold,
			"",
			debugTimeSeriesDumpOpts.noOfUploadWorkers,
			false,
		)
		if err != nil {
			results = append(results, struct {
				name       string
				size       int64
				createTime float64
				uploadTime float64
				status     string
			}{t.name, size, createTime, 0, "Error: " + err.Error()})
			_ = os.Remove(t.path)
			continue
		}

		uploadStart := time.Now()
		err = datadogWriter.upload(t.path)
		uploadTime := time.Since(uploadStart).Seconds()

		status := "Success"
		if err != nil {
			status = "Error: " + err.Error()
		}
		fmt.Printf("  Done: %.1fs\n", uploadTime)

		results = append(results, struct {
			name       string
			size       int64
			createTime float64
			uploadTime float64
			status     string
		}{t.name, size, createTime, uploadTime, status})

		// Step 3: Delete file immediately to save disk space
		_ = os.Remove(t.path)
		fmt.Printf("  File deleted.\n")
	}

	debugTimeSeriesDumpOpts.clusterLabel = originalLabel

	// Results
	processCol := "Upload"
	if debugTimeSeriesDumpOpts.dryRun {
		processCol = "Process"
	}
	fmt.Println("\n" + strings.Repeat("═", 95))
	fmt.Printf("%-15s %12s %12s %12s %12s %15s\n", "Format", "Size", "Create", processCol, "Total", "Status")
	fmt.Println(strings.Repeat("─", 95))
	for _, r := range results {
		total := r.createTime + r.uploadTime
		fmt.Printf("%-15s %12s %12s %12s %12s %15s\n",
			r.name,
			formatBytes(r.size),
			formatDuration(r.createTime),
			formatDuration(r.uploadTime),
			formatDuration(total),
			r.status)
	}
	fmt.Println(strings.Repeat("═", 95))

	return nil
}

func createCompressedTestFile(inputPath, outputPath string, wrapWriter func(io.Writer) io.WriteCloser) (int64, error) {
	inFile, dec, err := openInputGob(inputPath)
	if err != nil {
		return 0, err
	}
	defer inFile.Close()

	outFile, err := os.Create(outputPath)
	if err != nil {
		return 0, err
	}
	defer outFile.Close()

	bufWriter := bufio.NewWriterSize(outFile, 1024*1024)
	var w io.Writer = bufWriter
	var closer io.WriteCloser

	if wrapWriter != nil {
		closer = wrapWriter(bufWriter)
		w = closer
	}

	enc := gob.NewEncoder(w)
	for {
		var kv roachpb.KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		if err := enc.Encode(&kv); err != nil {
			return 0, err
		}
	}

	if closer != nil {
		closer.Close()
	}
	bufWriter.Flush()

	fi, _ := os.Stat(outputPath)
	return fi.Size(), nil
}
