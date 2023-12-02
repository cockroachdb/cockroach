// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type testRegistryImpl struct {
	m                map[string]*registry.TestSpec
	snapshotPrefixes map[string]struct{}

	promRegistry *prometheus.Registry
}

var _ registry.Registry = (*testRegistryImpl)(nil)

// makeTestRegistry constructs a testRegistryImpl and configures it with opts.
func makeTestRegistry() testRegistryImpl {
	return testRegistryImpl{
		m:                make(map[string]*registry.TestSpec),
		snapshotPrefixes: make(map[string]struct{}),
		promRegistry:     prometheus.NewRegistry(),
	}
}

// Add adds a test to the registry.
func (r *testRegistryImpl) Add(spec registry.TestSpec) {
	if _, ok := r.m[spec.Name]; ok {
		fmt.Fprintf(os.Stderr, "test %s already registered\n", spec.Name)
		os.Exit(1)
	}
	if spec.SnapshotPrefix != "" {
		for existingPrefix := range r.snapshotPrefixes {
			if strings.HasPrefix(existingPrefix, spec.SnapshotPrefix) {
				fmt.Fprintf(os.Stderr, "snapshot prefix %s shares prefix with another registered prefix %s\n",
					spec.SnapshotPrefix, existingPrefix)
				os.Exit(1)
			}
		}
		r.snapshotPrefixes[spec.SnapshotPrefix] = struct{}{}
	}
	if err := r.prepareSpec(&spec); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
	r.m[spec.Name] = &spec
}

// MakeClusterSpec makes a cluster spec. It should be used over `spec.MakeClusterSpec`
// because this method also adds options baked into the registry.
func (r *testRegistryImpl) MakeClusterSpec(nodeCount int, opts ...spec.Option) spec.ClusterSpec {
	return spec.MakeClusterSpec(nodeCount, opts...)
}

const testNameRE = "^[a-zA-Z0-9-_=/,]+$"

// prepareSpec validates a spec and does minor massaging of its fields.
func (r *testRegistryImpl) prepareSpec(spec *registry.TestSpec) error {
	if matched, err := regexp.MatchString(testNameRE, spec.Name); err != nil || !matched {
		return fmt.Errorf("%s: Name must match this regexp: %s", spec.Name, testNameRE)
	}

	spec.CompatibleClouds.AssertInitialized()
	spec.Suites.AssertInitialized()

	if spec.Run == nil {
		return fmt.Errorf("%s: must specify Run", spec.Name)
	}

	if spec.Cluster.ReusePolicy == nil {
		return fmt.Errorf("%s: must specify a ClusterReusePolicy", spec.Name)
	}

	// All tests must have an owner so the release team knows who signs off on
	// failures and so the github issue poster knows who to assign it to.
	if spec.Owner == `` {
		return fmt.Errorf(`%s: unspecified owner`, spec.Name)
	}
	if !spec.Owner.IsValid() {
		return fmt.Errorf(`%s: unknown owner %q`, spec.Name, spec.Owner)
	}

	// At the time of writing, we expect the roachtest job to finish within 24h
	// and have corresponding timeouts set up in CI. Since each individual test
	// may not be scheduled until a few hours in due to the CPU quota, individual
	// tests should expect to take "less time". Longer-running tests have to be in
	// the weekly suite.
	const maxTimeout = 18 * time.Hour
	if spec.Timeout > maxTimeout {
		if !spec.Suites.Contains(registry.Weekly) {
			return fmt.Errorf(
				"%s: timeout %s exceeds the maximum allowed of %s", spec.Name, spec.Timeout, maxTimeout,
			)
		}
	}

	return nil
}
func (r *testRegistryImpl) PromFactory() promauto.Factory {
	return promauto.With(r.promRegistry)
}

// AllTests returns all the tests specs, sorted by name.
func (r testRegistryImpl) AllTests() []registry.TestSpec {
	var tests []registry.TestSpec
	for _, t := range r.m {
		tests = append(tests, *t)
	}
	sort.Slice(tests, func(i, j int) bool {
		return tests[i].Name < tests[j].Name
	})
	return tests
}
