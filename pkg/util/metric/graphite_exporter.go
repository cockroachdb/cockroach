// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
			log.InfofDepth(ctx, 1, "", args...)
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
