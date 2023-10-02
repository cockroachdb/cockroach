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
	"github.com/spf13/cobra"
)

var (
	suite            string
	owner            string
	onlyBenchmarks   bool
	forceCloudCompat bool
)

func addSuiteAndOwnerFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(
		&suite, "suite", "",
		"Restrict tests to those in the given suite (e.g. nightly)",
	)
	cmd.Flags().StringVar(
		&owner, "owner", "",
		"Restrict tests to those with the given owner (e.g. kv)",
	)
}

// makeTestFilter creates a registry.TestFilter based on the current flags and
// the given regexps.
func makeTestFilter(regexps []string) (*registry.TestFilter, error) {
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
