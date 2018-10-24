// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metric

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/graphite"
)

var errNoEndpoint = errors.New("external.graphite.endpoint is not set")

// GraphiteExporter scrapes PrometheusExporter for metrics and pushes
// them to a Graphite or Carbon server.
type GraphiteExporter struct {
	pm *PrometheusExporter
}

// MakeGraphiteExporter returns an initialized graphite exporter.
func MakeGraphiteExporter(pm *PrometheusExporter) GraphiteExporter {
	return GraphiteExporter{pm: pm}
}

type loggerFunc func(...interface{})

// Println implements graphite.Logger.
func (lf loggerFunc) Println(v ...interface{}) {
	lf(v...)
}

// Push metrics scraped from registry to Graphite or Carbon server.
// It converts the same metrics that are pulled by Prometheus into Graphite-format.
func (ge *GraphiteExporter) Push(ctx context.Context, endpoint string) error {
	if endpoint == "" {
		return errNoEndpoint
	}
	h, err := os.Hostname()
	if err != nil {
		return err
	}
	// Make the bridge.
	var b *graphite.Bridge
	if b, err = graphite.NewBridge(&graphite.Config{
		URL:           endpoint,
		Gatherer:      ge.pm,
		Prefix:        fmt.Sprintf("%s.cockroach", h),
		Timeout:       10 * time.Second,
		ErrorHandling: graphite.AbortOnError,
		Logger: loggerFunc(func(args ...interface{}) {
			log.InfoDepth(ctx, 1, args...)
		}),
	}); err != nil {
		return err
	}
	// Regardless of whether Push() errors, clear metrics. Only latest metrics
	// are pushed. If there is an error, this will cause a gap in receiver. The
	// receiver associates timestamp with when metric arrived, so storing missed
	// metrics has no benefit.
	defer ge.pm.clearMetrics()
	return b.Push()
}
