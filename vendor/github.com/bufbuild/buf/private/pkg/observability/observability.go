// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package observability

import (
	"io"
	"net/http"

	"github.com/bufbuild/buf/private/pkg/ioextended"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
)

// TraceExportCloser describes the interface used to export OpenCensus traces
// and cleaning of resources.
type TraceExportCloser interface {
	trace.Exporter
	io.Closer
}

// ViewExportCloser describes the interface used to export OpenCensus views
// and cleaning of resources.
type ViewExportCloser interface {
	view.Exporter
	io.Closer
}

// TraceViewExportCloser implements both OpenCensus view and trace exporting.
type TraceViewExportCloser interface {
	ViewExportCloser
	TraceExportCloser
}

// Start initializes tracing.
//
// Tracing is a global function due to how go.opencensus.io is written.
// The returned io.Closer needs to be called at the completion of the program.
func Start(options ...StartOption) io.Closer {
	startOptions := newStartOptions()
	for _, option := range options {
		option(startOptions)
	}
	if len(startOptions.traceExportClosers) == 0 && len(startOptions.traceViewExportClosers) == 0 {
		return ioextended.NopCloser
	}
	trace.ApplyConfig(
		trace.Config{
			DefaultSampler: trace.AlwaysSample(),
		},
	)
	var closers []io.Closer
	for _, traceExportCloser := range startOptions.traceExportClosers {
		traceExportCloser := traceExportCloser
		trace.RegisterExporter(traceExportCloser)
		closers = append(closers, traceExportCloser)
	}
	for _, viewExportCloser := range startOptions.viewExportClosers {
		viewExportCloser := viewExportCloser
		view.RegisterExporter(viewExportCloser)
		closers = append(closers, viewExportCloser)
	}
	for _, traceViewExportCloser := range startOptions.traceViewExportClosers {
		traceViewExportCloser := traceViewExportCloser
		trace.RegisterExporter(traceViewExportCloser)
		view.RegisterExporter(traceViewExportCloser)
		closers = append(closers, traceViewExportCloser)
	}
	return ioextended.ChainCloser(closers...)
}

// StartOption is an option for start.
type StartOption func(*startOptions)

// StartWithTraceExportCloser returns a new StartOption that adds the given TraceExportCloser.
func StartWithTraceExportCloser(traceExportCloser TraceExportCloser) StartOption {
	return func(startOptions *startOptions) {
		startOptions.traceExportClosers = append(startOptions.traceExportClosers, traceExportCloser)
	}
}

// StartWithViewExportCloser returns a new StartOption that adds the given TraceExportCloser.
func StartWithViewExportCloser(viewExportCloser ViewExportCloser) StartOption {
	return func(startOptions *startOptions) {
		startOptions.viewExportClosers = append(startOptions.viewExportClosers, viewExportCloser)
	}
}

// StartWithTraceViewExportCloser returns a new StartOption that adds the given TraceViewExportCloser.
func StartWithTraceViewExportCloser(traceViewExportCloser TraceViewExportCloser) StartOption {
	return func(startOptions *startOptions) {
		startOptions.traceViewExportClosers = append(startOptions.traceViewExportClosers, traceViewExportCloser)
	}
}

// NewHTTPTransport returns a HTTP transport instrumented with OpenCensus traces and metrics.
func NewHTTPTransport(base http.RoundTripper) http.RoundTripper {
	return &ochttp.Transport{
		NewClientTrace: ochttp.NewSpanAnnotatingClientTrace,
		Base:           base,
	}
}

// NewHTTPClient returns a HTTP client instrumented with OpenCensus traces and metrics.
func NewHTTPClient() *http.Client {
	return &http.Client{
		Transport: NewHTTPTransport(http.DefaultTransport),
	}
}

type startOptions struct {
	traceExportClosers     []TraceExportCloser
	viewExportClosers      []ViewExportCloser
	traceViewExportClosers []TraceViewExportCloser
}

func newStartOptions() *startOptions {
	return &startOptions{}
}
