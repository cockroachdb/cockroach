// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/spf13/cobra"
)

var (
	suite            string
	owner            string
	onlyBenchmarks   bool
	forceCloudCompat bool
)

// addTestFilterFlags adds the flags related to test filtering to the given
// command.
//
// If includeCloudBench is false, the command is assumed to set up the cloud and
// onlyBenchmark variables separately.
func addTestFilterFlags(cmd *cobra.Command, includeCloudAndBench bool) {
	if includeCloudAndBench {
		cmd.Flags().BoolVar(
			&onlyBenchmarks, "bench", false, "Restricts to benchmarks")
		cmd.Flags().StringVar(
			&cloud, "cloud", spec.GCE, "Restricts tests to those compatible with the given cloud (local, aws, azure, gce, or all)")
	} else {
		cmd.Flags().BoolVar(
			&forceCloudCompat, "force-cloud-compat", false, "Includes tests that are not marked as compatible with the cloud used")
	}
	cmd.Flags().StringVar(
		&suite, "suite", "",
		"Restrict tests to only those in the given suite (e.g. nightly)",
	)
	cmd.Flags().StringVar(
		&owner, "owner", "",
		"Restrict tests to only those with the given owner (e.g. kv)",
	)
}

// makeTestFilter creates a registry.TestFilter based on the current flags and
// the given regexps.
func makeTestFilter(regexps []string) *registry.TestFilter {
	var options []registry.TestFilterOption
	if !forceCloudCompat && cloud != "all" && cloud != "" {
		options = append(options, registry.WithCloud(cloud))
	}
	if suite != "" {
		options = append(options, registry.WithSuite(suite))
	}
	if owner != "" {
		options = append(options, registry.WithOwner(registry.Owner(owner)))
	}
	if onlyBenchmarks {
		options = append(options, registry.OnlyBenchmarks())
	}
	return registry.NewTestFilter(regexps, options...)
}
