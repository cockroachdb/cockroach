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

var metricsNames []string

func init() {
	metricsNames = allMetricsNames()
}

// AllMetricsNames returns all of the possible metric names used by the
// sections.
//
// Note that it will return some metric names that don't exist since the
// sections do not indicate whether metrics are per-node or per-store (so both
// are returned).
func AllMetricsNames() []string {
	return metricsNames
}

func allMetricsNames() []string {
	m := map[string]struct{}{}
	for _, section := range charts {
		for _, chart := range section.Charts {
			for _, metric := range chart.Metrics {
				m[metric] = struct{}{}
			}
		}
	}
	// TODO(tbg): these prefixes come from pkg/server/status/recorder.go.
	// Unfortunately, the chart catalog does not tell us which ones are per-node
	// or per-store, so we simply tag both (one of them will be empty).
	names := make([]string, 0, 2*len(m))
	for name := range m {
		names = append(names, "cr.store."+name, "cr.node."+name)
	}
	sort.Strings(names)
	return names
}
