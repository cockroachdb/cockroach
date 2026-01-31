// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/klauspost/compress/zstd"
	"github.com/spf13/cobra"
)

var debugTimeSeriesVerifyCmd = &cobra.Command{
	Use:   "verify <input-file>",
	Short: "Verify data integrity after compression round-trip",
	Long: `Verifies that GOB data remains identical after compression and decompression.

This command:
1. Computes SHA256 checksum of the raw GOB data
2. Compresses to Gzip, decompresses, and computes checksum
3. Compresses to ZSTD, decompresses, and computes checksum
4. Compares all checksums to verify data integrity

Example:
  cockroach debug tsdump verify mydata.gob
`,
	Args: cobra.ExactArgs(1),
	RunE: runVerifyIntegrity,
}

func init() {
	debugTimeSeriesDumpCmd.AddCommand(debugTimeSeriesVerifyCmd)
}

type verifyResult struct {
	format   string
	checksum string
	count    int64
	duration time.Duration
	match    bool
}

func runVerifyIntegrity(cmd *cobra.Command, args []string) error {
	inputPath := args[0]

	fmt.Println("=== Data Integrity Verification ===")
	fmt.Printf("Input file: %s\n\n", inputPath)

	// Step 1: Compute checksum of raw data
	fmt.Println("Step 1: Computing raw data checksum...")
	rawResult, err := computeRawChecksum(inputPath)
	if err != nil {
		return fmt.Errorf("failed to compute raw checksum: %w", err)
	}
	rawResult.match = true // Reference, always matches itself
	fmt.Printf("  Raw: %s (%d entries, %.2fs)\n", rawResult.checksum, rawResult.count, rawResult.duration.Seconds())

	// Step 2: Verify Gzip round-trip
	fmt.Println("\nStep 2: Verifying Gzip round-trip...")
	gzipResult, err := verifyGzipRoundTrip(inputPath)
	if err != nil {
		return fmt.Errorf("failed to verify Gzip: %w", err)
	}
	gzipResult.match = gzipResult.checksum == rawResult.checksum
	fmt.Printf("  Gzip: %s (%d entries, %.2fs)\n", gzipResult.checksum, gzipResult.count, gzipResult.duration.Seconds())

	// Step 3: Verify ZSTD round-trip
	fmt.Println("\nStep 3: Verifying ZSTD round-trip...")
	zstdResult, err := verifyZstdRoundTrip(inputPath)
	if err != nil {
		return fmt.Errorf("failed to verify ZSTD: %w", err)
	}
	zstdResult.match = zstdResult.checksum == rawResult.checksum
	fmt.Printf("  ZSTD: %s (%d entries, %.2fs)\n", zstdResult.checksum, zstdResult.count, zstdResult.duration.Seconds())

	// Print summary
	fmt.Println("\n=== Verification Summary ===")
	fmt.Printf("%-10s %-64s %-10s %-10s\n", "Format", "SHA256 Checksum", "Entries", "Match")
	fmt.Println(string(bytes.Repeat([]byte("-"), 100)))

	results := []verifyResult{rawResult, gzipResult, zstdResult}
	allMatch := true
	for _, r := range results {
		matchStr := "✓ YES"
		if !r.match {
			matchStr = "✗ NO"
			allMatch = false
		}
		fmt.Printf("%-10s %-64s %-10d %-10s\n", r.format, r.checksum, r.count, matchStr)
	}

	fmt.Println()
	if allMatch {
		fmt.Println("✓ SUCCESS: All checksums match! Data integrity verified.")
	} else {
		fmt.Println("✗ FAILURE: Checksums do not match! Data corruption detected.")
		return fmt.Errorf("data integrity verification failed")
	}

	return nil
}

// computeRawChecksum reads the raw GOB file and computes SHA256 of all KeyValue entries
func computeRawChecksum(inputPath string) (verifyResult, error) {
	start := time.Now()

	f, err := os.Open(inputPath)
	if err != nil {
		return verifyResult{}, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	dec := gob.NewDecoder(reader)

	hasher := sha256.New()
	var count int64

	for {
		var kv roachpb.KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			return verifyResult{}, fmt.Errorf("decode error at entry %d: %w", count, err)
		}

		// Hash the serialized KeyValue
		if err := hashKeyValue(hasher, &kv); err != nil {
			return verifyResult{}, err
		}
		count++
	}

	return verifyResult{
		format:   "Raw",
		checksum: fmt.Sprintf("%x", hasher.Sum(nil)),
		count:    count,
		duration: time.Since(start),
	}, nil
}

// verifyGzipRoundTrip compresses to Gzip, decompresses, and computes checksum
func verifyGzipRoundTrip(inputPath string) (verifyResult, error) {
	start := time.Now()

	// Create temp file for compressed data
	tmpFile, err := os.CreateTemp("", "verify-*.gob.gz")
	if err != nil {
		return verifyResult{}, err
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	// Step 1: Compress to Gzip
	if err := compressToGzip(inputPath, tmpFile); err != nil {
		tmpFile.Close()
		return verifyResult{}, fmt.Errorf("compression failed: %w", err)
	}
	tmpFile.Close()

	// Step 2: Decompress and compute checksum
	checksum, count, err := decompressAndHashGzip(tmpPath)
	if err != nil {
		return verifyResult{}, fmt.Errorf("decompression failed: %w", err)
	}

	return verifyResult{
		format:   "Gzip",
		checksum: checksum,
		count:    count,
		duration: time.Since(start),
	}, nil
}

// verifyZstdRoundTrip compresses to ZSTD, decompresses, and computes checksum
func verifyZstdRoundTrip(inputPath string) (verifyResult, error) {
	start := time.Now()

	// Create temp file for compressed data
	tmpFile, err := os.CreateTemp("", "verify-*.gob.zst")
	if err != nil {
		return verifyResult{}, err
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	// Step 1: Compress to ZSTD
	if err := compressToZstd(inputPath, tmpFile); err != nil {
		tmpFile.Close()
		return verifyResult{}, fmt.Errorf("compression failed: %w", err)
	}
	tmpFile.Close()

	// Step 2: Decompress and compute checksum
	checksum, count, err := decompressAndHashZstd(tmpPath)
	if err != nil {
		return verifyResult{}, fmt.Errorf("decompression failed: %w", err)
	}

	return verifyResult{
		format:   "ZSTD",
		checksum: checksum,
		count:    count,
		duration: time.Since(start),
	}, nil
}

// compressToGzip streams input GOB to Gzip-compressed output
func compressToGzip(inputPath string, outFile *os.File) error {
	inFile, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer inFile.Close()

	reader := bufio.NewReader(inFile)
	dec := gob.NewDecoder(reader)

	bufWriter := bufio.NewWriter(outFile)
	gzWriter := gzip.NewWriter(bufWriter)
	enc := gob.NewEncoder(gzWriter)

	for {
		var kv roachpb.KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := enc.Encode(&kv); err != nil {
			return err
		}
	}

	if err := gzWriter.Close(); err != nil {
		return err
	}
	return bufWriter.Flush()
}

// compressToZstd streams input GOB to ZSTD-compressed output
func compressToZstd(inputPath string, outFile *os.File) error {
	inFile, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer inFile.Close()

	reader := bufio.NewReader(inFile)
	dec := gob.NewDecoder(reader)

	bufWriter := bufio.NewWriter(outFile)
	zstdWriter, err := zstd.NewWriter(bufWriter)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(zstdWriter)

	for {
		var kv roachpb.KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := enc.Encode(&kv); err != nil {
			return err
		}
	}

	if err := zstdWriter.Close(); err != nil {
		return err
	}
	return bufWriter.Flush()
}

// decompressAndHashGzip decompresses Gzip and computes SHA256
func decompressAndHashGzip(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	gzReader, err := gzip.NewReader(f)
	if err != nil {
		return "", 0, err
	}
	defer gzReader.Close()

	dec := gob.NewDecoder(gzReader)
	hasher := sha256.New()
	var count int64

	for {
		var kv roachpb.KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			return "", 0, fmt.Errorf("decode error at entry %d: %w", count, err)
		}

		if err := hashKeyValue(hasher, &kv); err != nil {
			return "", 0, err
		}
		count++
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), count, nil
}

// decompressAndHashZstd decompresses ZSTD and computes SHA256
func decompressAndHashZstd(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	zstdReader, err := zstd.NewReader(f)
	if err != nil {
		return "", 0, err
	}
	defer zstdReader.Close()

	dec := gob.NewDecoder(zstdReader)
	hasher := sha256.New()
	var count int64

	for {
		var kv roachpb.KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			return "", 0, fmt.Errorf("decode error at entry %d: %w", count, err)
		}

		if err := hashKeyValue(hasher, &kv); err != nil {
			return "", 0, err
		}
		count++
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), count, nil
}

// hashKeyValue deterministically hashes a KeyValue entry
func hashKeyValue(h io.Writer, kv *roachpb.KeyValue) error {
	// Hash the key
	if _, err := h.Write(kv.Key); err != nil {
		return err
	}

	// Hash the value's raw bytes
	if _, err := h.Write(kv.Value.RawBytes); err != nil {
		return err
	}

	// Hash the timestamp (as bytes)
	tsBytes := make([]byte, 16)
	tsBytes[0] = byte(kv.Value.Timestamp.WallTime >> 56)
	tsBytes[1] = byte(kv.Value.Timestamp.WallTime >> 48)
	tsBytes[2] = byte(kv.Value.Timestamp.WallTime >> 40)
	tsBytes[3] = byte(kv.Value.Timestamp.WallTime >> 32)
	tsBytes[4] = byte(kv.Value.Timestamp.WallTime >> 24)
	tsBytes[5] = byte(kv.Value.Timestamp.WallTime >> 16)
	tsBytes[6] = byte(kv.Value.Timestamp.WallTime >> 8)
	tsBytes[7] = byte(kv.Value.Timestamp.WallTime)
	tsBytes[8] = byte(kv.Value.Timestamp.Logical >> 24)
	tsBytes[9] = byte(kv.Value.Timestamp.Logical >> 16)
	tsBytes[10] = byte(kv.Value.Timestamp.Logical >> 8)
	tsBytes[11] = byte(kv.Value.Timestamp.Logical)
	// Pad remaining bytes
	if _, err := h.Write(tsBytes); err != nil {
		return err
	}

	return nil
}
