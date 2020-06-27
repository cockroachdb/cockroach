// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import "sort"

var walk func(ChartSection, func(chart *IndividualChart))

func init() {
	walk = func(section ChartSection, visit func(chart *IndividualChart)) {
		for _, sub := range section.Subsections {
			walk(*sub, visit)
		}
		for _, chart := range section.Charts {
			visit(chart)
		}
	}
}

// AllMetricsNames returns all of the possible metric names used by the
// sections.
//
// Note that it will return some metric names that don't exist since the
// sections do not indicate whether metrics are per-node or per-store (so both
// are returned).
func AllMetricsNames(sections ...ChartSection) []string {
	m := map[string]struct{}{}
	Walk(func(chart *IndividualChart) {
		for _, metric := range chart.Metrics {
			// TODO(lunevalex): these prefixes come from pkg/server/status/recorder.go.
			// Unfortunately, the chart catalog does not tell us which ones are per-node
			// or per-store, so we simply tag both (one of them will be empty).
			for _, prefix := range []string{"cr.store.", "cr.node."} {
				m[prefix+metric.Name] = struct{}{}
			}
		}
	}, sections...)
	sl := make([]string, len(m))
	for k := range m {
		sl = append(sl, k)
	}
	sort.Strings(sl)
	return sl
}

// Walk invokes the visitor with every Metric recursively found in the sections.
// It does not deduplicate.
func Walk(visit func(chart *IndividualChart), sections ...ChartSection) {
	for _, section := range sections {
		walk(section, visit)

	}
}
