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

	arrowparquet "github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/compress"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tsdumpmeta"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/errors"
	"github.com/klauspost/compress/zstd"
	"github.com/spf13/cobra"
)

var debugTimeSeriesBenchmarkCmd = &cobra.Command{
	Use:   "benchmark <input.gob>",
	Short: "benchmark different tsdump output formats",
	Long: `
Reads a raw tsdump .gob file and streams it to different output formats,
measuring write time and file size for each:

  - GOB (raw, ZSTD, Gzip)
  - Parquet (1M row groups, uncompressed and ZSTD)

This streams data from the input file without loading everything into memory.

Example:
  cockroach debug tsdump benchmark /path/to/tsdump.gob
`,
	Args: cobra.ExactArgs(1),
	RunE: clierrorplus.MaybeDecorateError(runTsdumpBenchmark),
}

func init() {
	debugTimeSeriesDumpCmd.AddCommand(debugTimeSeriesBenchmarkCmd)
}

type benchResult struct {
	format     string
	size       int64
	writeTime  float64
	datapoints int64
}

// Compression type for GOB benchmarks
type gobCompression int

const (
	gobNone gobCompression = iota
	gobZstd
	gobGzip
)

func runTsdumpBenchmark(cmd *cobra.Command, args []string) error {
	inputPath := args[0]

	// Get original file size
	inputInfo, err := os.Stat(inputPath)
	if err != nil {
		return errors.Wrapf(err, "failed to stat input file %s", inputPath)
	}
	originalSize := inputInfo.Size()

	fmt.Printf("Input file: %s\n", inputPath)
	fmt.Printf("Original size: %s\n\n", formatBytes(originalSize))

	// Create temp directory for output files
	tmpDir, err := os.MkdirTemp("", "tsdump-benchmark-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()
	fmt.Printf("Output directory: %s\n\n", tmpDir)

	results := []benchResult{}

	// GOB benchmarks
	gobTests := []struct {
		name        string
		path        string
		compression gobCompression
	}{
		{"GOB", tmpDir + "/tsdump.gob", gobNone},
		{"GOB + ZSTD", tmpDir + "/tsdump.gob.zst", gobZstd},
		{"GOB + Gzip", tmpDir + "/tsdump.gob.gz", gobGzip},
	}

	for _, t := range gobTests {
		fmt.Printf("Testing %s...\n", t.name)
		result, err := benchmarkGob(inputPath, t.path, t.compression)
		if err != nil {
			fmt.Printf("  Error: %v\n", err)
		} else {
			results = append(results, result)
			fmt.Printf("  Done: %s, %d entries, %.1fs\n", formatBytes(result.size), result.datapoints, result.writeTime)
		}
		// Delete file immediately to save disk space for large files
		_ = os.Remove(t.path)
	}

	// Parquet benchmarks (1M row group size)
	parquetTests := []struct {
		name         string
		path         string
		codec        compress.Compression
		rowGroupSize int
	}{
		{"Parquet (1M)", tmpDir + "/tsdump-1m.parquet", compress.Codecs.Uncompressed, 1_000_000},
		{"Parquet (1M) + ZSTD", tmpDir + "/tsdump-1m.parquet.zst", compress.Codecs.Zstd, 1_000_000},
	}

	for _, t := range parquetTests {
		fmt.Printf("Testing %s...\n", t.name)
		result, err := benchmarkParquet(inputPath, t.path, t.codec, t.rowGroupSize)
		if err != nil {
			fmt.Printf("  Error: %v\n", err)
		} else {
			result.format = t.name
			results = append(results, result)
			fmt.Printf("  Done: %s, %d datapoints, %.1fs\n", formatBytes(result.size), result.datapoints, result.writeTime)
		}
		// Delete file immediately to save disk space for large files
		_ = os.Remove(t.path)
	}

	// Print results table
	fmt.Println("\n" + strings.Repeat("═", 95))
	fmt.Printf("%-25s %12s %14s %12s %15s\n", "Format", "Size", "Datapoints", "Time", "vs Original")
	fmt.Println(strings.Repeat("─", 95))

	for _, r := range results {
		reduction := "-"
		if r.size < originalSize {
			pct := 100.0 * float64(originalSize-r.size) / float64(originalSize)
			reduction = fmt.Sprintf("%.1f%% smaller", pct)
		} else if r.size > originalSize {
			pct := 100.0 * float64(r.size-originalSize) / float64(originalSize)
			reduction = fmt.Sprintf("%.1f%% larger", pct)
		}

		fmt.Printf("%-25s %12s %14d %12s %15s\n",
			r.format,
			formatBytes(r.size),
			r.datapoints,
			formatDuration(r.writeTime),
			reduction,
		)
	}
	fmt.Println(strings.Repeat("═", 95))

	return nil
}

// openInputGob opens a .gob file and returns a decoder, handling embedded metadata
func openInputGob(inputPath string) (*os.File, *gob.Decoder, error) {
	inFile, err := os.Open(inputPath)
	if err != nil {
		return nil, nil, err
	}

	dec := gob.NewDecoder(inFile)

	// Skip embedded metadata if present
	_, metadataErr := tsdumpmeta.Read(dec)
	if metadataErr != nil {
		// No embedded metadata, restart from beginning
		if _, err := inFile.Seek(0, io.SeekStart); err != nil {
			inFile.Close()
			return nil, nil, err
		}
		dec = gob.NewDecoder(inFile)
	}

	return inFile, dec, nil
}

// benchmarkGob reads from a .gob file and writes to another GOB file with optional compression
func benchmarkGob(inputPath, outputPath string, compression gobCompression) (benchResult, error) {
	result := benchResult{}
	switch compression {
	case gobNone:
		result.format = "GOB"
	case gobZstd:
		result.format = "GOB + ZSTD"
	case gobGzip:
		result.format = "GOB + Gzip"
	}

	inFile, dec, err := openInputGob(inputPath)
	if err != nil {
		return result, err
	}
	defer inFile.Close()

	outFile, err := os.Create(outputPath)
	if err != nil {
		return result, err
	}
	defer outFile.Close()

	bufWriter := bufio.NewWriterSize(outFile, 1024*1024)
	var w io.Writer = bufWriter
	var closer io.Closer

	switch compression {
	case gobZstd:
		zw, err := zstd.NewWriter(bufWriter)
		if err != nil {
			return result, err
		}
		w = zw
		closer = zw
	case gobGzip:
		gw := gzip.NewWriter(bufWriter)
		w = gw
		closer = gw
	}

	enc := gob.NewEncoder(w)

	start := time.Now()
	for {
		var kv roachpb.KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			return result, err
		}

		if err := enc.Encode(&kv); err != nil {
			return result, err
		}
		result.datapoints++

		if result.datapoints%1_000_000 == 0 {
			fmt.Printf("  [gob] Processed %d entries...\n", result.datapoints)
		}
	}

	if closer != nil {
		if err := closer.Close(); err != nil {
			return result, err
		}
	}

	if err := bufWriter.Flush(); err != nil {
		return result, err
	}

	result.writeTime = time.Since(start).Seconds()

	fi, err := os.Stat(outputPath)
	if err != nil {
		return result, err
	}
	result.size = fi.Size()

	return result, nil
}

// benchmarkParquet writes Parquet using batched columnar approach with configurable row group size
func benchmarkParquet(inputPath, outputPath string, codec compress.Compression, rowGroupSize int) (benchResult, error) {
	result := benchResult{}

	inFile, dec, err := openInputGob(inputPath)
	if err != nil {
		return result, err
	}
	defer inFile.Close()

	outFile, err := os.Create(outputPath)
	if err != nil {
		return result, err
	}
	defer outFile.Close()

	// Define Parquet schema
	fields := schema.FieldList{
		schema.NewByteArrayNode("name", arrowparquet.Repetitions.Required, -1),
		schema.NewByteArrayNode("source", arrowparquet.Repetitions.Required, -1),
		schema.NewInt64Node("timestamp_nanos", arrowparquet.Repetitions.Required, -1),
		schema.NewFloat64Node("value", arrowparquet.Repetitions.Required, -1),
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, fields, -1)
	if err != nil {
		return result, err
	}

	writerProps := arrowparquet.NewWriterProperties(
		arrowparquet.WithCompression(codec),
		arrowparquet.WithDataPageSize(1<<20),
	)

	writer := file.NewParquetWriter(outFile, root, file.WithWriterProps(writerProps))

	// Buffers for row group
	names := make([]arrowparquet.ByteArray, 0, rowGroupSize)
	sources := make([]arrowparquet.ByteArray, 0, rowGroupSize)
	timestamps := make([]int64, 0, rowGroupSize)
	values := make([]float64, 0, rowGroupSize)
	rowGroupCount := 0

	writeRowGroup := func() error {
		if len(names) == 0 {
			return nil
		}
		rgWriter := writer.AppendRowGroup()

		nameCol, err := rgWriter.NextColumn()
		if err != nil {
			return err
		}
		_, _ = nameCol.(*file.ByteArrayColumnChunkWriter).WriteBatch(names, nil, nil)
		_ = nameCol.Close()

		sourceCol, err := rgWriter.NextColumn()
		if err != nil {
			return err
		}
		_, _ = sourceCol.(*file.ByteArrayColumnChunkWriter).WriteBatch(sources, nil, nil)
		_ = sourceCol.Close()

		tsCol, err := rgWriter.NextColumn()
		if err != nil {
			return err
		}
		_, _ = tsCol.(*file.Int64ColumnChunkWriter).WriteBatch(timestamps, nil, nil)
		_ = tsCol.Close()

		valCol, err := rgWriter.NextColumn()
		if err != nil {
			return err
		}
		_, _ = valCol.(*file.Float64ColumnChunkWriter).WriteBatch(values, nil, nil)
		_ = valCol.Close()

		if err := rgWriter.Close(); err != nil {
			return err
		}

		rowGroupCount++
		names = names[:0]
		sources = sources[:0]
		timestamps = timestamps[:0]
		values = values[:0]

		return nil
	}

	start := time.Now()
	kvCount := int64(0)

	for {
		var kv roachpb.KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			_ = writer.Close()
			return result, err
		}
		kvCount++

		var tsData *tspb.TimeSeriesData
		dumper := ts.DefaultDumper{Send: func(d *tspb.TimeSeriesData) error {
			tsData = d
			return nil
		}}
		if err := dumper.Dump(&kv); err != nil {
			_ = writer.Close()
			return result, err
		}
		if tsData == nil {
			continue
		}

		for _, dp := range tsData.Datapoints {
			names = append(names, []byte(tsData.Name))
			sources = append(sources, []byte(tsData.Source))
			timestamps = append(timestamps, dp.TimestampNanos)
			values = append(values, dp.Value)
			result.datapoints++

			if len(names) >= rowGroupSize {
				if err := writeRowGroup(); err != nil {
					_ = writer.Close()
					return result, err
				}
				fmt.Printf("  [parquet] Wrote row group %d (%d datapoints)...\n", rowGroupCount, result.datapoints)
			}
		}

		if kvCount%100_000 == 0 {
			fmt.Printf("  [parquet] Processed %d entries (%d datapoints)...\n", kvCount, result.datapoints)
		}
	}

	if len(names) > 0 {
		if err := writeRowGroup(); err != nil {
			_ = writer.Close()
			return result, err
		}
	}

	if err := writer.Close(); err != nil {
		return result, err
	}

	result.writeTime = time.Since(start).Seconds()

	fi, err := os.Stat(outputPath)
	if err != nil {
		return result, err
	}
	result.size = fi.Size()

	fmt.Printf("  [parquet] Final: %d row groups, %d datapoints\n", rowGroupCount, result.datapoints)
	return result, nil
}

// formatBytes formats a byte count in human-readable form.
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// formatDuration formats a duration in seconds to a human-readable string.
func formatDuration(seconds float64) string {
	if seconds < 0.001 {
		return fmt.Sprintf("%.2f ms", seconds*1000)
	}
	if seconds < 1 {
		return fmt.Sprintf("%.0f ms", seconds*1000)
	}
	if seconds < 60 {
		return fmt.Sprintf("%.1f s", seconds)
	}
	minutes := int(seconds) / 60
	secs := seconds - float64(minutes*60)
	return fmt.Sprintf("%dm %.0fs", minutes, secs)
}
