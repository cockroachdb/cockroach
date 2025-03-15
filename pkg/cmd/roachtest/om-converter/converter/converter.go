// Package converter provides functionality for converting different types of metrics
// in the data processing pipeline.
package converter

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/sink"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
)

// Common errors
var (
	ErrInvalidMetricType = errors.New("invalid metric type")
	ErrWorkerCanceled    = errors.New("worker canceled")
)

// Converter defines the interface for metric converters
type Converter interface {
	Convert([]model.Label, model.FileInfo) error
}

// converterRegistry manages singleton instances of converters
type converterRegistry struct {
	histogram     Converter
	statsExporter Converter
	histogramOnce sync.Once
	statsOnce     sync.Once
}

// newConverterRegistry creates a new converter registry
func newConverterRegistry() *converterRegistry {
	return &converterRegistry{}
}

// getConverter returns the appropriate converter instance for the given metric type
func (r *converterRegistry) getConverter(
	sink sink.Sink, metricType string, specs []registry.TestSpec,
) Converter {
	switch metricType {
	case "histogram":
		r.histogramOnce.Do(func() {
			r.histogram = NewHistogramConverter(sink, specs)
		})
		return r.histogram
	case "statsexporter":
		r.statsOnce.Do(func() {
			r.statsExporter = NewStatsExporterConverter(sink)
		})
		return r.statsExporter
	default:
		return nil
	}
}

// WorkerPool manages a pool of converter workers
type WorkerPool struct {
	ctx           context.Context
	sourceChannel chan model.FileInfo
	sink          sink.Sink
	config        []model.Metric
	globalLabels  []model.Label
	numWorkers    int
	specs         []registry.TestSpec
	registry      *converterRegistry
}

// NewWorkerPool creates a new worker pool for converting metrics
func NewWorkerPool(
	ctx context.Context,
	sourceChannel chan model.FileInfo,
	sink sink.Sink,
	config []model.Metric,
	globalLabels []model.Label,
	numWorkers int,
	specs []registry.TestSpec,
) *WorkerPool {
	return &WorkerPool{
		ctx:           ctx,
		sourceChannel: sourceChannel,
		sink:          sink,
		config:        config,
		globalLabels:  globalLabels,
		numWorkers:    numWorkers,
		specs:         specs,
		registry:      newConverterRegistry(),
	}
}

// Start begins the conversion process with the configured number of workers
func (p *WorkerPool) Start() error {
	var wg sync.WaitGroup
	errChannel := make(chan error, p.numWorkers)

	// Start workers
	for i := 0; i < p.numWorkers; i++ {
		wg.Add(1)
		go p.worker(&wg, errChannel)
	}

	// Wait for completion
	return p.waitForCompletion(&wg, errChannel)
}

// worker processes files from the source channel
func (p *WorkerPool) worker(wg *sync.WaitGroup, errChannel chan error) {
	defer wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			errChannel <- ErrWorkerCanceled
			return
		case file, ok := <-p.sourceChannel:
			if !ok {
				return
			}
			if err := p.processFile(file); err != nil {
				errChannel <- err
				continue
			}
			fmt.Printf("Processed %v file\n", file.Path)
		}
	}
}

// combineLabels creates a new slice combining metric-specific and global labels
func (p *WorkerPool) combineLabels(metricLabels []model.Label) []model.Label {
	combinedLabels := make([]model.Label, 0, len(metricLabels)+len(p.globalLabels))

	// Copy metric-specific labels
	combinedLabels = append(combinedLabels, metricLabels...)

	// Copy global labels, avoiding duplicates
	for _, global := range p.globalLabels {
		isDuplicate := false
		for _, existing := range metricLabels {
			if existing.Name == global.Name {
				isDuplicate = true
				break
			}
		}
		if !isDuplicate {
			combinedLabels = append(combinedLabels, global)
		}
	}

	return combinedLabels
}

// processFile handles the conversion of a single file
func (p *WorkerPool) processFile(file model.FileInfo) error {
	metricType, labels := p.getBenchmarkMetric(file)
	if metricType == "" {
		return nil // Skip files that don't match any metric definition
	}

	converter := p.registry.getConverter(p.sink, metricType, p.specs)
	if converter == nil {
		return nil // Skip unsupported metric types
	}

	// Create a new combined slice of labels
	combinedLabels := p.combineLabels(labels)

	return converter.Convert(combinedLabels, file)
}

// getBenchmarkMetric finds the matching metric definition for a file
func (p *WorkerPool) getBenchmarkMetric(file model.FileInfo) (string, []model.Label) {
	for _, metric := range p.config {
		if matched, _ := regexp.MatchString(metric.Name, file.Path); matched {
			// Return a copy of the labels to prevent modification of the original
			labelsCopy := make([]model.Label, len(metric.Labels))
			copy(labelsCopy, metric.Labels)
			return metric.Type, labelsCopy
		}
	}
	// return default histogram if no match is found
	return "histogram", nil
}

// waitForCompletion waits for all workers to finish and collects any errors
func (p *WorkerPool) waitForCompletion(wg *sync.WaitGroup, errChannel chan error) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait for either completion or context cancellation
	select {
	case <-done:
		close(errChannel)
		return p.collectErrors(errChannel)
	case <-p.ctx.Done():
		return ErrWorkerCanceled
	}
}

// collectErrors collects all errors from the error channel
func (p *WorkerPool) collectErrors(errChannel chan error) error {
	var lastErr error
	for err := range errChannel {
		if err != nil && !errors.Is(err, ErrWorkerCanceled) {
			lastErr = err
		}
	}
	return lastErr
}

// StartConverter is the main entry point for starting the conversion process
func StartConverter(
	ctx context.Context,
	sourceChannel chan model.FileInfo,
	sink sink.Sink,
	config []model.Metric,
	globalLabels []model.Label,
	numWorkers int,
	specs []registry.TestSpec,
) error {
	pool := NewWorkerPool(ctx, sourceChannel, sink, config, globalLabels, numWorkers, specs)
	return pool.Start()
}
