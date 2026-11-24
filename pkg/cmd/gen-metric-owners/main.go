// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// gen-metric-owners scans Go source files for metric.Metadata
// definitions, resolves each to its owning team via CODEOWNERS, and
// writes a metric_owners.yaml consumed by `cockroach gen metric-list
// --metric-owners`.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/internal/codeowners"
	"github.com/cockroachdb/cockroach/pkg/internal/metricscan"
	"github.com/cockroachdb/cockroach/pkg/internal/reporoot"
	"gopkg.in/yaml.v2"
)

var out = flag.String("out", "", "path to write the metric owners file")

func main() {
	flag.Parse()
	if *out == "" {
		fmt.Fprintln(os.Stderr, "usage: gen-metric-owners -out=PATH")
		os.Exit(1)
	}
	// Prefer BUILD_WORKSPACE_DIRECTORY (set by `bazel run`) over
	// reporoot.Get(), because the latter resolves to the Bazel
	// execroot which does not contain the Go source files.
	root := os.Getenv("BUILD_WORKSPACE_DIRECTORY")
	if root == "" {
		root = reporoot.Get()
	}
	if root == "" {
		fmt.Fprintln(os.Stderr, "could not determine repo root")
		os.Exit(1)
	}
	result, err := metricscan.Scan(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "metricscan: %v\n", err)
		os.Exit(1)
	}
	owners, err := codeowners.DefaultLoadCodeOwners()
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading CODEOWNERS: %v\n", err)
		os.Exit(1)
	}
	mo := metricscan.BuildMetricOwners(result, func(file string) string {
		teams := owners.Match(file)
		if len(teams) > 0 {
			return string(teams[0].Name())
		}
		return ""
	})
	data, err := yaml.Marshal(mo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshaling YAML: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, data, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "writing output: %v\n", err)
		os.Exit(1)
	}
}
