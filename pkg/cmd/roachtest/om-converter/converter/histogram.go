// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package converter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/sink"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram/exporter"
	"github.com/cockroachdb/errors"
	"github.com/codahale/hdrhistogram"
)

// Default configuration values
const (
	// DefaultMaxEntryAge defines the default for how old an entry can be before it's cleaned up
	DefaultMaxEntryAge = 2 * 24 * time.Hour // 2 days

	// DefaultMaxCleanupBatchSize is the default limit for how many entries we clean up at once
	DefaultMaxCleanupBatchSize = 1000

	// DefaultCleanupInterval is the default interval between cleanup runs
	DefaultCleanupInterval = 2 * time.Minute
)

// Configurable settings - can be modified via CLI flags
var (
	// MaxEntryAge defines how old an entry can be before it's cleaned up
	MaxEntryAge = DefaultMaxEntryAge

	// MaxCleanupBatchSize limits how many entries we clean up at once to prevent memory spikes
	MaxCleanupBatchSize = DefaultMaxCleanupBatchSize

	// CleanupInterval controls how often cleanup runs
	CleanupInterval = DefaultCleanupInterval

	re = regexp.MustCompile(`warehouses=\d+/stats\.json$`)
)

// ConfigureCleanup allows setting cleanup parameters from external code
func ConfigureCleanup(maxAge time.Duration, batchSize int, interval time.Duration) {
	MaxEntryAge = maxAge
	MaxCleanupBatchSize = batchSize
	CleanupInterval = interval
}

// HistogramConverter handles the conversion of histogram data to OpenMetrics format
type HistogramConverter struct {
	specs           *[]registry.TestSpec
	sink            sink.Sink
	maxWareHouse    map[string]float64
	lastCleanupTime time.Time    // track when we last did cleanup to avoid doing it too often
	mu              sync.RWMutex // protects for concurrent access
}

// NewHistogramConverter creates a new histogram converter instance
func NewHistogramConverter(sink sink.Sink, specs *[]registry.TestSpec) *HistogramConverter {
	hc := &HistogramConverter{
		sink:            sink,
		specs:           specs,
		maxWareHouse:    make(map[string]float64),
		lastCleanupTime: time.Now(), // Initialize to avoid immediate cleanup
	}

	return hc
}

// shouldRunCleanup determines if we should run cleanup based on time elapsed
func (hc *HistogramConverter) shouldRunCleanup() bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	// Run cleanup if enough time has passed or maxWareHouse map is getting large
	return time.Since(hc.lastCleanupTime) > CleanupInterval || len(hc.maxWareHouse) > 1000
}

// updateCleanupTime updates the last cleanup time
func (hc *HistogramConverter) updateCleanupTime() {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.lastCleanupTime = time.Now()
}

// Convert processes histogram data and converts it to OpenMetrics format
func (hc *HistogramConverter) Convert(labels []model.Label, src model.FileInfo) error {

	// Extract test information
	runDate, testName := getTestDateAndName(src)
	postProcessFn := hc.getTestPostProcessFunction(testName)

	// Get buffers from pool instead of creating new ones
	rawBuffer := getBuffer()
	aggregatedBuffer := getBuffer()

	// Ensure buffers are returned to the pool on function exit
	defer putBuffer(rawBuffer)
	defer putBuffer(aggregatedBuffer)

	metricsExporter, err := hc.initializeExporter(rawBuffer, src, labels)
	if err != nil {
		return errors.Wrap(err, "failed to initialize exporter")
	}

	// Process histogram data
	if err := hc.processHistogramData(metricsExporter, src.Content); err != nil {
		return errors.Wrap(err, "failed to process histogram data")
	}

	// Handle raw metrics output
	if err := hc.writeRawMetrics(metricsExporter, rawBuffer, src.Path); err != nil {
		return errors.Wrap(err, "failed to write raw metrics")
	}
	if postProcessFn == nil {
		return nil
	}

	// Handle aggregated metrics output
	if err := hc.writeAggregatedMetrics(rawBuffer, aggregatedBuffer, postProcessFn, src.Path, runDate, testName); err != nil {
		return errors.Wrap(err, "failed to write aggregated metrics")
	}

	// Clear buffers explicitly to help with memory pressure
	rawBuffer.Reset()
	aggregatedBuffer.Reset()

	return nil
}

// initializeExporter sets up the OpenMetrics exporter with proper configuration
func (hc *HistogramConverter) initializeExporter(
	buf *bytes.Buffer, src model.FileInfo, labels []model.Label,
) (*exporter.OpenMetricsExporter, error) {
	omExporter := &exporter.OpenMetricsExporter{}
	writer := io.Writer(buf)
	omExporter.Init(&writer)

	labelMap := hc.getLabels(src, labels)
	omExporter.SetLabels(&labelMap)

	return omExporter, nil
}

func (hc *HistogramConverter) getLabels(
	file model.FileInfo, labels []model.Label,
) map[string]string {
	labelMap := getLabelMap(file, labels, true, "", nil)
	testName := labelMap["test"]

	owner := ""
	for _, spec := range *hc.specs {
		if spec.Name == testName {
			owner = string(spec.Owner)
		}
	}
	if owner != "" {
		labelMap["owner"] = owner
	}
	return labelMap
}

// processHistogramData reads and processes histogram data with extreme safety
func (hc *HistogramConverter) processHistogramData(
	metricsExporter *exporter.OpenMetricsExporter, content []byte,
) error {
	scanner := bufio.NewScanner(bytes.NewReader(content))

	// Use a larger buffer for the scanner to handle large lines
	const maxScanTokenSize = 1024 * 1024 // 1MB
	buf := make([]byte, maxScanTokenSize)
	scanner.Buffer(buf, maxScanTokenSize)

	for scanner.Scan() {
		line := scanner.Text()

		// Try to parse with full error handling
		snapshot, err := parseHistogramSnapshot(line)
		if err != nil {
			continue // Skip invalid entries silently
		}

		// Double-check snapshot and histogram validity
		if snapshot == nil || snapshot.Hist == nil {
			continue // Skip invalid entries silently
		}

		// Try to write with full error handling
		if err := writeHistogramSnapshot(metricsExporter, snapshot); err != nil {
			continue // Skip problematic entries silently
		}
	}

	return scanner.Err()
}

// parseHistogramSnapshot parses a single line of histogram data with validation
func parseHistogramSnapshot(line string) (*exporter.SnapshotTick, error) {
	if line == "" {
		return nil, errors.New("empty line provided")
	}

	var snapshot exporter.SnapshotTick
	if err := json.Unmarshal([]byte(line), &snapshot); err != nil {
		return nil, errors.Wrap(err, "JSON unmarshal failed")
	}

	// Validate histogram data exists
	if snapshot.Hist == nil {
		return nil, errors.New("parsed snapshot contains nil histogram data")
	}

	return &snapshot, nil
}

// writeHistogramSnapshot writes a single histogram snapshot to the exporter
// with protection against nil pointer panics
func writeHistogramSnapshot(
	metricsExporter *exporter.OpenMetricsExporter, snapshot *exporter.SnapshotTick,
) error {
	// Defensive check
	if snapshot == nil {
		return errors.New("nil snapshot provided")
	}

	// Critical protection against nil Hist
	if snapshot.Hist == nil {
		return errors.New("nil histogram data")
	}

	// Wrap the import in a panic recovery
	var hist *hdrhistogram.Histogram
	var importErr error

	func() {
		defer func() {
			if r := recover(); r != nil {
				importErr = fmt.Errorf("panic in histogram import: %v", r)
			}
		}()
		hist = hdrhistogram.Import(snapshot.Hist)
	}()

	if importErr != nil {
		return importErr
	}

	if hist == nil {
		return errors.New("histogram import produced nil result")
	}

	return metricsExporter.SnapshotAndWrite(hist, snapshot.Now, snapshot.Elapsed, &snapshot.Name)
}

// writeRawMetrics handles writing the raw metrics data
func (hc *HistogramConverter) writeRawMetrics(
	metricsExporter *exporter.OpenMetricsExporter, buffer *bytes.Buffer, sourcePath string,
) error {
	return metricsExporter.Close(func() error {
		path := strings.Trim(strings.TrimSuffix(sourcePath, "stats.json"), `/`)
		return hc.sink.Sink(buffer, path, RawStatsFile)
	})
}

// writeAggregatedMetrics handles the aggregation and writing of processed metrics
func (hc *HistogramConverter) writeAggregatedMetrics(
	rawBuffer *bytes.Buffer,
	aggregatedBuffer *bytes.Buffer,
	postProcessFn func(string, *roachtestutil.HistogramMetric) (roachtestutil.AggregatedPerfMetrics, error),
	sourcePath string,
	runDate string,
	testName string,
) error {
	histograms, finalLabels, err := roachtestutil.GetHistogramMetrics(rawBuffer)
	if err != nil {
		return errors.Wrap(err, "failed to get histogram metrics")
	}

	// Add nil check before calling the post-process function
	if histograms == nil {
		fmt.Printf("Warning: No histograms found for %s\n", testName)
		return nil // Or handle this case appropriately
	}
	aggregatedMetrics, err := roachtestutil.PostProcessMetrics(testName, postProcessFn, histograms)
	if err != nil {
		return errors.Wrap(err, "failed to post-process metrics")
	}

	if strings.Contains(sourcePath, "tpccbench") {
		aggregatedMetrics = aggregatedMetrics[:2]
	}

	parsedTime, err := time.Parse("20060102", strings.Split(runDate, "-")[0])
	if err != nil {
		return errors.Wrap(err, "failed to parse run date")
	}
	if err := roachtestutil.GetAggregatedMetricBytes(aggregatedMetrics, finalLabels, parsedTime, aggregatedBuffer); err != nil {
		return errors.Wrap(err, "failed to get aggregated metric bytes")
	}

	return hc.sink.Sink(aggregatedBuffer, strings.Trim(strings.TrimSuffix(sourcePath, "stats.json"), `/`), AggregatedStatsFile)
}

// getTestPostProcessFunction finds the post-process function for a given test
func (hc *HistogramConverter) getTestPostProcessFunction(
	testName string,
) func(string, *roachtestutil.HistogramMetric) (roachtestutil.AggregatedPerfMetrics, error) {

	for _, name := range SkippedPostProcessTests {
		if strings.Contains(testName, name) {
			return nil
		}
	}

	for _, spec := range *hc.specs {
		if spec.Name == testName {
			return spec.GetPostProcessWorkloadMetricsFunction()
		}
	}
	return nil
}

// cleanupOldEntries removes entries older than MaxEntryAge
func (hc *HistogramConverter) cleanupOldEntries(referenceTime time.Time) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	// Find old keys to remove
	var keysToRemove []string
	for key := range hc.maxWareHouse {
		// Parse the date from the key format "YYYYMMDD-ID-testname"
		parts := strings.SplitN(key, "-", 2)
		if len(parts) < 2 || len(parts[0]) != 8 { // Ensure date format is YYYYMMDD (8 chars)
			continue
		}

		dateStr := parts[0]
		entryDate, err := time.Parse("20060102", dateStr)
		if err != nil {
			continue // Skip invalid dates silently
		}

		// If the entry is older than MaxEntryAge, mark it for removal
		if referenceTime.Sub(entryDate) > MaxEntryAge {
			keysToRemove = append(keysToRemove, key)
		}
	}

	// Remove old entries
	for _, key := range keysToRemove {
		delete(hc.maxWareHouse, key)
	}

	// If we removed a significant number of entries, force garbage collection
	if len(keysToRemove) > 100 {
		debug.FreeOSMemory()
	}
}
