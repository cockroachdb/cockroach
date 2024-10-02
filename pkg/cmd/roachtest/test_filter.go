// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
)

// makeTestFilter creates a registry.TestFilter based on the current flags and
// the given regexps.
func makeTestFilter(regexps []string) (*registry.TestFilter, error) {
	var options []registry.TestFilterOption
	if !roachtestflags.ForceCloudCompat {
		if cloud := roachtestflags.Cloud; cloud.IsSet() {
			options = append(options, registry.WithCloud(cloud))
		}
	}
	if roachtestflags.Owner != "" {
		options = append(options, registry.WithOwner(registry.Owner(roachtestflags.Owner)))
	}
	if roachtestflags.OnlyBenchmarks {
		options = append(options, registry.OnlyBenchmarks())
	}
	if roachtestflags.Suite != "" {
		options = append(options, registry.WithSuite(roachtestflags.Suite))
	}

	// Tags no longer exist, but we provide some basic backward compatibility: if
	// we see a single tag which matches a known suite, we convert it to a suite.
	suite := roachtestflags.Suite
	var args []string
	for _, v := range regexps {
		if tagPrefix := "tag:"; strings.HasPrefix(v, tagPrefix) {
			tag := strings.TrimPrefix(v, tagPrefix)
			if suite == "" && registry.AllSuites.Contains(tag) {
				suite = tag
				continue
			}
			return nil, fmt.Errorf("tags are no longer supported; use --suite, --owner instead")
		}
		args = append(args, v)
	}
	if suite != "" {
		options = append(options, registry.WithSuite(suite))
	}
	return registry.NewTestFilter(args, options...)
}
