package converter

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

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
	histogram        Converter
	statsExporter    Converter
	sysbenchExporter Converter
	tpceExporter     Converter
	histogramOnce    sync.Once
	statsOnce        sync.Once
	sysbenchOnce     sync.Once
	tpceOnce         sync.Once
}

// newConverterRegistry creates a new converter registry
func newConverterRegistry() *converterRegistry {
	return &converterRegistry{}
}

// getConverter returns the appropriate converter instance for the given metric type
func (r *converterRegistry) getConverter(
	sink sink.Sink, metricType string, specs *[]registry.TestSpec, metricSpec []model.Metric,
) Converter {
	switch metricType {
	case "histogram":
		r.histogramOnce.Do(func() {
			r.histogram = NewHistogramConverter(sink, specs)
		})
		return r.histogram
	case "statsexporter":
		r.statsOnce.Do(func() {
			r.statsExporter = NewStatsExporterConverter(sink, metricSpec)
		})
		return r.statsExporter
	case "sysbench":
		r.sysbenchOnce.Do(func() {
			r.sysbenchExporter = NewSysbenchConverter(sink)
		})
		return r.sysbenchExporter
	case "tpce":
		r.tpceOnce.Do(func() {
			r.tpceExporter = NewTpceConverter(sink)
		})
		return r.tpceExporter
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
	specs         *[]registry.TestSpec
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
	specs *[]registry.TestSpec,
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
	errChannel := make(chan string, p.numWorkers*100)

	// Start workers
	for i := 0; i < p.numWorkers; i++ {
		wg.Add(1)
		go p.worker(&wg, errChannel)
	}

	// Wait for completion
	return p.waitForCompletion(&wg, errChannel)
}

// worker processes files from the source channel with improved error handling
func (p *WorkerPool) worker(wg *sync.WaitGroup, errChannel chan string) {
	defer wg.Done()

	// Add panic recovery at the worker level
	defer func() {
		if r := recover(); r != nil {
			errMsg := fmt.Sprintf("Recovered from panic in worker: %v", r)
			errChannel <- errMsg
		}
	}()

	// Add a counter to periodically trigger garbage collection
	processed := 0
	const gcInterval = 25 // Run GC after processing this many files

	for {
		select {
		case <-p.ctx.Done():
			errChannel <- fmt.Sprintf(ErrWorkerCanceled.Error())
			return
		case file, ok := <-p.sourceChannel:
			if !ok {
				// Channel is closed, worker should exit
				return
			}

			// Process file with protection against individual file failures
			if err := p.processSafeFile(file); err != nil {
				select {
				case errChannel <- err.Error():
					// Error sent successfully
				default:
					// Channel is full, log and continue
					fmt.Printf("Warning: Error channel full, dropping error: %v\n", err)
				}
				continue
			}

			// Increment counter and periodically force GC to reclaim memory
			processed++
			if processed >= gcInterval {
				processed = 0
				runtime.GC()
				debug.FreeOSMemory()
			}
		}
	}
}

// processSafeFile wraps processFile with panic recovery
func (p *WorkerPool) processSafeFile(file model.FileInfo) (err error) {
	// Recover from any panics during file processing
	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, 4096)
			stack = stack[:runtime.Stack(stack, false)]
			err = fmt.Errorf("panic processing file %s: %v", file.Path, r)
		}
	}()

	return p.processFile(file)
}

// processFile handles the conversion of a single file with timeout
func (p *WorkerPool) processFile(file model.FileInfo) error {

	start := time.Now()

	defer fmt.Printf("Completed file %s completed in %f seconds\n", file.Path, time.Since(start).Seconds())
	// rest of the function

	// Create a timeout context for this specific file
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Minute)
	defer cancel()

	// Create a channel to receive the result
	result := make(chan error, 1)

	// Process the file in a separate goroutine
	go func() {
		metricType, labels := p.getBenchmarkMetric(file)
		if metricType == "" {
			result <- nil
			return
		}

		converter := p.registry.getConverter(p.sink, metricType, p.specs, p.config)
		if converter == nil {
			result <- nil
			return
		}

		combinedLabels := p.combineLabels(labels)
		result <- converter.Convert(combinedLabels, file)
	}()

	// Wait for either completion or timeout
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("processing file %s timed out after 5 minutes", file.Path)
		}
		return ctx.Err()
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

// getBenchmarkMetric finds the matching metric definition for a file
func (p *WorkerPool) getBenchmarkMetric(file model.FileInfo) (string, []model.Label) {
	for _, metric := range p.config {
		if matched, _ := regexp.MatchString(metric.Name, file.Path); matched {
			// Return a copy of the labels to prevent modification of the original
			labelsCopy := make([]model.Label, len(metric.Labels))
			for _, label := range metric.LabelFromPath {
				if !strings.Contains(file.Path, label) {
					continue
				}

				matches := regexp.MustCompile(fmt.Sprintf(`/%s=(\d+)?/`, label)).FindStringSubmatch(file.Path)

				labelsCopy = append(labelsCopy, model.Label{Name: label, Value: matches[1]})
			}
			copy(labelsCopy, metric.Labels)
			return metric.Type, labelsCopy
		}
	}
	// return default histogram if no match is found
	return "histogram", nil
}

// waitForCompletion waits for all workers to finish with a timeout and improved channel handling
func (p *WorkerPool) waitForCompletion(wg *sync.WaitGroup, errChannel chan string) error {
	// Create channel to signal completion
	done := make(chan struct{})

	// Start goroutine to signal when all workers are done
	go func() {
		wg.Wait()
		close(done)
	}()

	// Create a ticker to periodically report progress and check for stuck workers
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Track worker progress
	var lastErrorCount int
	var stuckCount int

	// Wait for either completion, context cancellation, or timeout
	for {
		select {
		case <-done:
			// All workers completed successfully
			close(errChannel)
			return p.collectErrors(errChannel)

		case <-p.ctx.Done():
			// Parent context was cancelled
			return ErrWorkerCanceled

		case <-ticker.C:
			// Check for progress - if error count hasn't changed recently, we might be stuck
			currentErrorCount := len(errChannel)
			if currentErrorCount == lastErrorCount {
				stuckCount++
				if stuckCount >= 100000 { // Stuck for ~5 minutes (10 * 30 seconds)
					return fmt.Errorf("operation appears to be stuck, no progress for 5 minutes")
				}

				// Log that we might be stuck
				fmt.Printf("WARNING: No progress detected for %d minutes. Workers may be stuck.\n",
					stuckCount/2)
			} else {
				// Reset stuck counter if we're making progress
				stuckCount = 0
			}
			lastErrorCount = currentErrorCount
		}
	}
}

// collectErrors collects all errors from the error channel
func (p *WorkerPool) collectErrors(errChannel chan string) error {
	var lastErr string
	for err := range errChannel {
		if err != "" && !strings.EqualFold(err, ErrWorkerCanceled.Error()) {
			lastErr = err
		}
	}
	if lastErr == "" {
		return nil
	}
	return fmt.Errorf("worker error: %s", lastErr)
}

// StartConverter is the main entry point for starting the conversion process
func StartConverter(
	ctx context.Context,
	sourceChannel chan model.FileInfo,
	sink sink.Sink,
	config []model.Metric,
	globalLabels []model.Label,
	numWorkers int,
	specs *[]registry.TestSpec,
) error {
	pool := NewWorkerPool(ctx, sourceChannel, sink, config, globalLabels, numWorkers, specs)
	return pool.Start()
}
