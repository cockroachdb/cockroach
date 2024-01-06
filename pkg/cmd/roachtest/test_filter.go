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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
)

// makeTestFilter creates a registry.TestFilter based on the current flags and
// the given regexps.
func makeTestFilter(regexps []string) (*registry.TestFilter, error) {
	var options []registry.TestFilterOption
	if !roachtestflags.ForceCloudCompat {
		if cloud := roachtestflags.Cloud; cloud != "all" && cloud != "" {
			options = append(options, registry.WithCloud(cloud))
		}
	}
	if roachtestflags.Suite != "" {
		options = append(options, registry.WithSuite(roachtestflags.Suite))
	}
	if roachtestflags.Owner != "" {
		options = append(options, registry.WithOwner(registry.Owner(roachtestflags.Owner)))
	}
	if roachtestflags.OnlyBenchmarks {
		options = append(options, registry.OnlyBenchmarks())
	}
	return registry.NewTestFilter(regexps, options...)
}
