// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metricscan

import (
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/yamlutil"
)

// MetricOwners maps metric names directly to their owning team.
//
// All entries live in a single map. Key conventions distinguish the
// match type:
//
//   - Contains %s or %d → pattern (compiled to regexp via
//     formatToPromRegexp)
//   - Starts with ^ → prefix match (^ is stripped, remainder used
//     with strings.HasPrefix)
//   - Otherwise → exact match
type MetricOwners struct {
	Owners map[string]string `yaml:"owners"`

	// Populated by LoadMetricOwners. Not serialized.
	exact    map[string]string
	prefixes map[string]string
	patterns []compiledOwnerPattern
}

type compiledOwnerPattern struct {
	re    *regexp.Regexp
	owner string
}

// Resolve looks up the owning team for a scraped Prometheus metric
// name. The resolution logic mirrors Result.Resolve but returns
// the owner string directly.
func (mo *MetricOwners) Resolve(promName string) (string, bool) {
	if owner, ok := mo.exact[promName]; ok {
		return owner, true
	}

	baseName := StripHistogramSuffix(promName)
	if baseName != promName {
		if owner, ok := mo.exact[baseName]; ok {
			return owner, true
		}
	}

	for prefix, owner := range mo.prefixes {
		if strings.HasPrefix(promName, prefix) {
			return owner, true
		}
		if baseName != promName && strings.HasPrefix(baseName, prefix) {
			return owner, true
		}
	}

	for _, p := range mo.patterns {
		if p.re.MatchString(promName) {
			return p.owner, true
		}
		if baseName != promName && p.re.MatchString(baseName) {
			return p.owner, true
		}
	}

	// Progressively strip trailing underscore-delimited segments to find a
	// parent metric definition. We require the candidate to retain at least
	// two segments (i.e. contain an underscore) to avoid false-matching an
	// unrelated short metric name.
	for i := len(promName) - 1; i >= 0; i-- {
		if promName[i] == '_' {
			candidate := promName[:i]
			if !strings.Contains(candidate, "_") {
				break
			}
			if owner, ok := mo.exact[candidate]; ok {
				return owner, true
			}
		}
	}

	if strings.HasSuffix(baseName, "_internal") {
		stripped := strings.TrimSuffix(baseName, "_internal")
		if owner, ok := mo.exact[stripped]; ok {
			return owner, true
		}
	}

	return "", false
}

// BuildMetricOwners constructs a MetricOwners from a scan Result by
// applying resolveOwner to each source file to determine the team.
func BuildMetricOwners(r *Result, resolveOwner func(file string) string) *MetricOwners {
	mo := &MetricOwners{
		Owners: make(map[string]string),
	}

	for name, src := range r.Exact {
		if owner := resolveOwner(src.File); owner != "" {
			mo.Owners[name] = owner
		}
	}

	for _, p := range r.Patterns {
		if owner := resolveOwner(p.source.File); owner != "" {
			mo.Owners[p.format] = owner
		}
	}

	for _, p := range r.Prefixes {
		if owner := resolveOwner(p.Source.File); owner != "" {
			mo.Owners["^"+p.Prefix] = owner
		}
	}

	return mo
}

// DefaultMetricOwners returns a MetricOwners loaded from the
// generated data embedded by gen-metric-owners. This makes the
// metric-to-team mapping available at runtime without reading an
// external file.
func DefaultMetricOwners() (*MetricOwners, error) {
	return LoadMetricOwners(metricOwnersData)
}

// LoadMetricOwners deserializes a MetricOwners from YAML data and
// classifies entries into exact matches, prefixes, and patterns.
func LoadMetricOwners(data []byte) (*MetricOwners, error) {
	var mo MetricOwners
	if err := yamlutil.UnmarshalStrict(data, &mo); err != nil {
		return nil, err
	}
	mo.exact = make(map[string]string)
	mo.prefixes = make(map[string]string)

	// Sort keys for deterministic pattern resolution order.
	keys := make([]string, 0, len(mo.Owners))
	for k := range mo.Owners {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		owner := mo.Owners[k]
		if formatVerbRE.MatchString(k) {
			re := formatToPromRegexp(k)
			if re != nil {
				mo.patterns = append(mo.patterns, compiledOwnerPattern{
					re:    re,
					owner: owner,
				})
			}
		} else if strings.HasPrefix(k, "^") {
			mo.prefixes[k[1:]] = owner
		} else {
			mo.exact[k] = owner
		}
	}
	return &mo, nil
}
