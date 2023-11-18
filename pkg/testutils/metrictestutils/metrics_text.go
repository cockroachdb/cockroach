// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metrictestutils

import (
	"bufio"
	"bytes"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/prometheus/common/expfmt"
)

// GetMetricsText scrapes a metrics registry, filters out the metrics according
// to the given regexp, sorts them, and returns them in a multi-line string.
func GetMetricsText(registry *metric.Registry, re *regexp.Regexp) (string, error) {
	ex := metric.MakePrometheusExporter()
	scrape := func(ex *metric.PrometheusExporter) {
		ex.ScrapeRegistry(registry, true /* includeChildMetrics */)
	}
	var in bytes.Buffer
	if err := ex.ScrapeAndPrintAsText(&in, expfmt.FmtText, scrape); err != nil {
		return "", err
	}
	sc := bufio.NewScanner(&in)
	var outLines []string
	for sc.Scan() {
		if bytes.HasPrefix(sc.Bytes(), []byte{'#'}) || !re.Match(sc.Bytes()) {
			continue
		}
		outLines = append(outLines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return "", err
	}
	sort.Strings(outLines)
	metricsText := strings.Join(outLines, "\n")
	return metricsText, nil
}
