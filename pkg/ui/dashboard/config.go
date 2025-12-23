// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package dashboard provides configuration structures and generation
// logic for CockroachDB dashboards across multiple platforms.
package dashboard

// DashboardConfig represents the top-level configuration for a dashboard.
type DashboardConfig struct {
	Name        string  `yaml:"name"`
	Description string  `yaml:"description"`
	Charts      []Chart `yaml:"charts"`
}

// Chart represents a single chart/graph within a dashboard.
type Chart struct {
	Title       string         `yaml:"title"`
	Type        string         `yaml:"type"`
	Axis        Axis           `yaml:"axis"`
	Description string         `yaml:"description"`
	Tooltip     any            `yaml:"tooltip"`
	Metrics     []Metric       `yaml:"metrics"`
	Options     map[string]any `yaml:"options"` // arbitrary platform-specific options
}

// Axis represents the axis configuration for a chart.
type Axis struct {
	Label string `yaml:"label"`
	Units string `yaml:"units"`
}

// Metric represents a single metric within a chart.
type Metric struct {
	Name    string         `yaml:"name"`
	Title   string         `yaml:"title"`
	Options map[string]any `yaml:"options"` // arbitrary platform-specific options (rate, aggregation, etc.)
}
