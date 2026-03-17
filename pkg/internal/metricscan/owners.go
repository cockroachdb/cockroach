// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metricscan

import (
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v2"
)

// MetricOwners maps metric names directly to their owning team,
// bypassing the intermediate source-file→CODEOWNERS resolution.
// The structure mirrors Result but stores owner strings instead of
// Source locations.
type MetricOwners struct {
	// Exact maps a Prometheus-format metric name to the owning team.
	Exact map[string]string `yaml:"exact"`
	// Patterns holds format-string-based dynamic metric names with
	// their owning team. The regexp is compiled on load.
	Patterns []OwnerPattern `yaml:"patterns,omitempty"`
	// Prefixes holds metric name prefixes with their owning team.
	Prefixes []OwnerPrefix `yaml:"prefixes,omitempty"`
}

// OwnerPattern pairs a fmt.Sprintf format string (used as a regex
// source) with the owning team.
type OwnerPattern struct {
	Format string `yaml:"format"`
	Owner  string `yaml:"owner"`
	re     *regexp.Regexp
}

// OwnerPrefix pairs a metric name prefix with the owning team.
type OwnerPrefix struct {
	Prefix string `yaml:"prefix"`
	Owner  string `yaml:"owner"`
}

// Resolve looks up the owning team for a scraped Prometheus metric
// name. The resolution logic mirrors Result.Resolve but returns
// the owner string directly.
func (mo *MetricOwners) Resolve(promName string) (string, bool) {
	if owner, ok := mo.Exact[promName]; ok {
		return owner, true
	}

	baseName := StripHistogramSuffix(promName)
	if baseName != promName {
		if owner, ok := mo.Exact[baseName]; ok {
			return owner, true
		}
	}

	for _, p := range mo.Prefixes {
		if strings.HasPrefix(promName, p.Prefix) {
			return p.Owner, true
		}
		if baseName != promName && strings.HasPrefix(baseName, p.Prefix) {
			return p.Owner, true
		}
	}

	for _, p := range mo.Patterns {
		if p.re != nil && p.re.MatchString(promName) {
			return p.Owner, true
		}
		if baseName != promName && p.re != nil && p.re.MatchString(baseName) {
			return p.Owner, true
		}
	}

	for i := len(promName) - 1; i >= 0; i-- {
		if promName[i] == '_' {
			candidate := promName[:i]
			if owner, ok := mo.Exact[candidate]; ok {
				return owner, true
			}
		}
	}

	if strings.HasSuffix(baseName, "_internal") {
		stripped := strings.TrimSuffix(baseName, "_internal")
		if owner, ok := mo.Exact[stripped]; ok {
			return owner, true
		}
	}

	return "", false
}

// BuildMetricOwners constructs a MetricOwners from a scan Result by
// applying resolveOwner to each source file to determine the team.
// The output is sorted for deterministic YAML serialization.
func BuildMetricOwners(r *Result, resolveOwner func(file string) string) *MetricOwners {
	mo := &MetricOwners{
		Exact: make(map[string]string, len(r.Exact)),
	}

	for name, src := range r.Exact {
		if owner := resolveOwner(src.File); owner != "" {
			mo.Exact[name] = owner
		}
	}

	for _, p := range r.Patterns {
		if owner := resolveOwner(p.source.File); owner != "" {
			mo.Patterns = append(mo.Patterns, OwnerPattern{
				Format: p.format,
				Owner:  owner,
			})
		}
	}
	sort.Slice(mo.Patterns, func(i, j int) bool {
		return mo.Patterns[i].Format < mo.Patterns[j].Format
	})

	for _, p := range r.Prefixes {
		if owner := resolveOwner(p.Source.File); owner != "" {
			mo.Prefixes = append(mo.Prefixes, OwnerPrefix{
				Prefix: p.Prefix,
				Owner:  owner,
			})
		}
	}
	sort.Slice(mo.Prefixes, func(i, j int) bool {
		return mo.Prefixes[i].Prefix < mo.Prefixes[j].Prefix
	})

	return mo
}

// LoadMetricOwners deserializes a MetricOwners from YAML data and
// compiles the pattern regexps.
func LoadMetricOwners(data []byte) (*MetricOwners, error) {
	var mo MetricOwners
	if err := yaml.Unmarshal(data, &mo); err != nil {
		return nil, err
	}
	for i := range mo.Patterns {
		re := formatToPromRegexp(mo.Patterns[i].Format)
		if re == nil {
			continue
		}
		mo.Patterns[i].re = re
	}
	return &mo, nil
}
