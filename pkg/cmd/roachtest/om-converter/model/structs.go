// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package model

type FileInfo struct {
	Path            string
	Content         []byte
	DirectoryPrefix string
}

// Source represents the source configuration
type Source struct {
	Type      string `yaml:"type"`
	Directory string `yaml:"directory"` // directory of the metrics source
	Bucket    string `yaml:"bucket"`    // bucket path in case of gcp based Source
	Project   string `yaml:"project"`   // gcp project
}

// Sink represents the sink configuration
type Sink struct {
	Type      string `yaml:"type"`
	Directory string `yaml:"directory"` // directory of the metrics sink
	Bucket    string `yaml:"bucket"`    // bucket path in case of gcp based Sink
	Project   string `yaml:"project"`   // gcp project
}

// Label represents a label in the metrics configuration
type Label struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type MetricLabel struct {
	Name   string  `yaml:"name"`
	Labels []Label `yaml:"labels"`
}

// Metric represents a metric configuration
type Metric struct {
	Name                string        `yaml:"name"`
	Type                string        `yaml:"type"`
	Labels              []Label       `yaml:"labels"`
	HigherBetterMetrics []string      `yaml:"higher-better-metrics"`
	LabelFromPath       []string      `yaml:"labels-from-path"`
	MetricLabels        []MetricLabel `yaml:"metric-labels"`
	SkipPostProcess     bool          `yaml:"skip-post-process"`
}

// Config represents the overall configuration
type Config struct {
	Source  Source   `yaml:"source"`
	Sink    Sink     `yaml:"sink"`
	Metrics []Metric `yaml:"metrics"`
	Global  []Label  `yaml:"global-labels"`
}
