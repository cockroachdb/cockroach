// Copyright 2016 The Cockroach Authors.
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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/graphite"
	prometheusgo "github.com/prometheus/client_model/go"
)

var (
	// GraphiteURL is host:port, if any, of Graphite metrics server
	GraphiteURL = settings.RegisterStringSetting(
		"external.graphite.url",
		"if nonempty, push server metrics to the Graphite or Carbon server at the specified host:port",
		"",
	)
	// GraphiteFrequency is how often metrics are pushed to Graphite, if enabled
	GraphiteFrequency = settings.RegisterDurationSetting(
		"external.graphite.frequency",
		"the frequency with which metrics are pushed to Graphite (if enabled)",
		10*time.Second,
	)
	errNoURL = errors.New("external.graphite.url is not set")
)

// GraphiteExporter implements prometheus.Gatherer and pushes metrics to Graphite or Carbon server.
// It scrapes prometheus exporter for metrics.
type GraphiteExporter struct {
	pm  *PrometheusExporter
	ctx context.Context
	st  *cluster.Settings
}

// MakeGraphiteExporter returns an initialized graphite exporter.
func MakeGraphiteExporter(pm *PrometheusExporter, st *cluster.Settings) GraphiteExporter {
	return GraphiteExporter{pm: pm, st: st}
}

func (ge *GraphiteExporter) makeBridge(ctx context.Context) (*graphite.Bridge, error) {
	url := GraphiteURL.Get(&ge.st.SV)
	if url == "" {
		return nil, errNoURL
	}
	ge.ctx = ctx
	h, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	var b *graphite.Bridge
	if b, err = graphite.NewBridge(&graphite.Config{
		URL:           url, // TODO (neeral) will remove this comment  "localhost:2003"
		Gatherer:      ge,
		Prefix:        fmt.Sprintf("%s.cockroach", h),
		Timeout:       10 * time.Second,
		ErrorHandling: graphite.AbortOnError,
		Logger:        ge,
	}); err != nil {
		return nil, err
	}
	return b, nil
}

// Gather implements prometheus.Gatherer
func (ge *GraphiteExporter) Gather() ([]*prometheusgo.MetricFamily, error) {
	v := make([]*prometheusgo.MetricFamily, len(ge.pm.families))
	i := 0
	for _, family := range ge.pm.families {
		v[i] = family
		i++
	}
	return v, nil
}

// Verify GraphiteExporter implements Gatherer interface
var _ prometheus.Gatherer = (*GraphiteExporter)(nil)

// Println implements graphite.Logger.
func (ge *GraphiteExporter) Println(v ...interface{}) {
	log.Info(ge.ctx, v...)
}

// Push metrics scraped from registry to Graphite or Carbon server.
// It converts the same metrics that are pulled by Prometheus into Graphite-format.
func (ge *GraphiteExporter) Push(ctx context.Context) error {
	ge.ctx = ctx
	defer func() {
		// Clear metrics for reuse.
		for _, family := range ge.pm.families {
			family.Metric = []*prometheusgo.Metric{}
		}
	}()
	var b *graphite.Bridge
	var err error
	if b, err = ge.makeBridge(ctx); err != nil {
		return err
	}
	return b.Push()
}
