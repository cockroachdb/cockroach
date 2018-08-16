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
	"bufio"
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/graphite"
	prometheusgo "github.com/prometheus/client_model/go"
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
func (ge *GraphiteExporter) Push(ctx context.Context, endpoint string, whitelistPath string) error {
	log.Errorf(ctx, "neeral push endpoint=%s whitelistpath=%s", endpoint, whitelistPath)
	if endpoint == "" {
		return errNoEndpoint
	}
	h, err := os.Hostname()
	if err != nil {
		return err
	}
	var names []string
	if whitelistPath != "" {
		// Skip whitelisting if there are any errors.
		if f, err := os.Open(whitelistPath); err == nil {
			defer f.Close()

			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				names = append(names, strings.TrimSpace(scanner.Text()))
			}
			if err = scanner.Err(); err == nil {
				log.Errorf(ctx, "neeral reading file %s to give %v of size %d", whitelistPath, names, len(names))
			} else {
				log.Errorf(ctx, "neeral error reading from file %s: %v", whitelistPath, err)
			}
		}
	}
	inWhitelist := func(m *prometheusgo.MetricFamily) bool {
		for _, name := range names {
			if b, err := regexp.MatchString(name, *m.Name); err == nil && b {
				log.Errorf(ctx, "neeral match name=%s regex=%s", *m.Name, name)
				return true
			}
		}
		return false
	}
	// Apply whitelist. We can do this because GraphiteExporter has its own
	// instance of a PrometheusExporter.
	if names != nil {
		for k, family := range ge.pm.families {
			if inWhitelist(family) {
				delete(ge.pm.families, k)
			}
		}
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
	return b.Push()
}
