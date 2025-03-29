// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package converter

import (
	"fmt"

	"github.com/cockroachdb/errors"
)

// Define specific error types for better error handling
var (
	// ErrProcessing represents errors in histogram data processing
	ErrProcessing = errors.New("histogram processing error")

	// ErrExporter represents errors in the metrics exporter
	ErrExporter = errors.New("metrics exporter error")

	// ErrPostProcessing represents errors in post-processing metrics
	ErrPostProcessing = errors.New("post-processing error")

	// ErrConfigError represents errors in configuration or setup
	ErrConfigError = errors.New("configuration error")
)

// wrapProcessingError wraps an error with context specific to data processing
func wrapProcessingError(err error, format string, args ...interface{}) error {
	msg := fmt.Sprintf(format, args...)
	return errors.WithMessagef(errors.WrapWithDepth(1, err, ErrProcessing.Error()), "%s", msg)
}

// wrapExporterError wraps an error with context specific to the exporter
func wrapExporterError(err error, format string, args ...interface{}) error {
	msg := fmt.Sprintf(format, args...)
	return errors.WithMessagef(errors.WrapWithDepth(1, err, ErrExporter.Error()), "%s", msg)
}

// wrapPostProcessingError wraps an error with context specific to post-processing
func wrapPostProcessingError(err error, format string, args ...interface{}) error {
	msg := fmt.Sprintf(format, args...)
	return errors.WithMessagef(errors.WrapWithDepth(1, err, ErrPostProcessing.Error()), "%s", msg)
}

// wrapConfigError wraps an error with context specific to configuration
func wrapConfigError(err error, format string, args ...interface{}) error {
	msg := fmt.Sprintf(format, args...)
	return errors.WithMessagef(errors.WrapWithDepth(1, err, ErrConfigError.Error()), "%s", msg)
}
