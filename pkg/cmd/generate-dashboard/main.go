// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"log"
	"os"

	"github.com/cockroachdb/cockroach/pkg/ui/dashboard"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: generate-dashboard <metrics-yaml-path> <output-dir>")
	}

	metricsYAMLPath := os.Args[1]
	outputDir := os.Args[2]

	generator := dashboard.NewGenerator()

	// Load metrics.yaml for metadata lookup
	if err := generator.LoadMetricsYAML(metricsYAMLPath); err != nil {
		log.Fatalf("Error loading metrics.yaml: %v", err)
	}

	if err := generator.GenerateAllTypeScriptDashboards(outputDir); err != nil {
		log.Fatalf("Error generating dashboards: %v", err)
	}
}
