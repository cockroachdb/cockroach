package om_converter

import (
	"context"
	"os"
	"os/signal"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/converter"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/sink"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/source"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

// Constants for configuration
const (
	// ChannelBufferSize determines the buffer size for the source channel
	// Large buffer to prevent blocking on file processing
	ChannelBufferSize = 1024 // Reduced from 8192 to avoid excessive memory use
)

var GlobalLabels []model.Label

// StartProcess initializes and starts the conversion process using YAML configuration.
// It's a convenience wrapper around Start that handles YAML parsing.
func StartProcess(yamlContent []byte, numWorkers int, specs *[]registry.TestSpec) error {
	var config model.Config
	if err := yaml.Unmarshal(yamlContent, &config); err != nil {
		return errors.Wrap(err, "failed to parse YAML configuration")
	}

	var skippedPostProcess []string
	for _, spec := range config.Metrics {
		if spec.SkipPostProcess {
			skippedPostProcess = append(skippedPostProcess, spec.Name)
		}
	}

	converter.SetSkippedPostProcessTests(skippedPostProcess)

	converter.SetGlobalLabels(config.Global)
	return Start(&config, numWorkers, specs)
}

// Start initializes and runs the conversion pipeline with the provided configuration.
// It sets up the source, sink, and converter components and manages their lifecycle.
func Start(config *model.Config, numWorkers int, specs *[]registry.TestSpec) (err error) {
	// If numWorkers is not specified, use a reasonable default based on CPU count
	if numWorkers <= 0 {
		numWorkers = 2
	}

	// Create buffered channel for file information
	sourceChannel := make(chan model.FileInfo, ChannelBufferSize)
	defer close(sourceChannel)

	// Setup context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctrlC(ctx, cancel)

	// Initialize source component
	fileSource, err := initializeSource(ctx, config)
	if err != nil {
		return err
	}
	defer fileSource.Close()

	// Initialize sink component
	dataSink, err := initializeSink(ctx, config)
	if err != nil {
		return err
	}
	defer dataSink.Close()

	// Start processing pipeline
	return runProcessingPipeline(ctx, cancel, fileSource, dataSink, sourceChannel, config, numWorkers, specs)
}

// initializeSource creates and configures the source component
func initializeSource(ctx context.Context, config *model.Config) (source.Source, error) {
	fileSource, err := source.GetSource(ctx, config)
	if err != nil {
		return nil, errors.Wrap(err, "error creating source")
	}
	return fileSource, nil
}

// initializeSink creates and configures the sink component
func initializeSink(ctx context.Context, config *model.Config) (sink.Sink, error) {
	dataSink, err := sink.GetSink(ctx, config)
	if err != nil {
		return nil, errors.Wrap(err, "error creating sink")
	}
	return dataSink, nil
}

// runProcessingPipeline starts and manages the main processing pipeline
func runProcessingPipeline(
	ctx context.Context,
	cancel context.CancelFunc,
	fileSource source.Source,
	dataSink sink.Sink,
	sourceChannel chan model.FileInfo,
	config *model.Config,
	numWorkers int,
	specs *[]registry.TestSpec,
) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	// Start source goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := fileSource.Start(sourceChannel); err != nil {
			errCh <- errors.Wrap(err, "source processing failed")
			cancel()
		}
	}()

	// Start converter goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := converter.StartConverter(ctx, sourceChannel, dataSink, config.Metrics, config.Global, numWorkers, specs); err != nil {
			errCh <- errors.Wrap(err, "converter processing failed")
			cancel()
		}
	}()

	// Wait for completion and collect errors
	go func() {
		wg.Wait()
		close(errCh)
	}()

	return collectErrors(errCh)
}

// collectErrors combines all errors from the error channel into a single error
func collectErrors(errCh chan error) error {
	var finalErr error
	for err := range errCh {
		if finalErr == nil {
			finalErr = err
		} else {
			finalErr = errors.CombineErrors(finalErr, err)
		}
	}
	return finalErr
}

// ctrlC configures graceful shutdown on interrupt signals
func ctrlC(ctx context.Context, cancel context.CancelFunc) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	go func() {
		select {
		case <-sig:
			// Received interrupt signal
			cancel()
		case <-ctx.Done():
			// Context cancelled elsewhere
			return
		}
	}()
}
