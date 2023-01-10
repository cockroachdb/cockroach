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
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var loadTeams = func() (team.Map, error) {
	return team.DefaultLoadTeams()
}

func ownerToAlias(o registry.Owner) team.Alias {
	return team.Alias(fmt.Sprintf("cockroachdb/%s", o))
}

type testRegistryImpl struct {
	m            map[string]*registry.TestSpec
	cloud        string
	instanceType string // optional
	zones        string
	preferSSD    bool
	// buildVersion is the version of the Cockroach binary that tests will run against.
	buildVersion version.Version

	promRegistry *prometheus.Registry
}

// makeTestRegistry constructs a testRegistryImpl and configures it with opts.
func makeTestRegistry(
	cloud string, instanceType string, zones string, preferSSD bool,
) (testRegistryImpl, error) {
	r := testRegistryImpl{
		cloud:        cloud,
		instanceType: instanceType,
		zones:        zones,
		preferSSD:    preferSSD,
		m:            make(map[string]*registry.TestSpec),
		promRegistry: prometheus.NewRegistry(),
	}
	v := buildTag
	if v == "" {
		var err error
		v, err = loadBuildVersion()
		if err != nil {
			return testRegistryImpl{}, err
		}
	}
	if err := r.setBuildVersion(v); err != nil {
		return testRegistryImpl{}, err
	}
	return r, nil
}

// Add adds a test to the registry.
func (r *testRegistryImpl) Add(spec registry.TestSpec) {
	if _, ok := r.m[spec.Name]; ok {
		fmt.Fprintf(os.Stderr, "test %s already registered\n", spec.Name)
		os.Exit(1)
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
	// NB: we need to make sure that `opts` is appended at the end, so that it
	// overrides the SSD and zones settings from the registry.
	var finalOpts []spec.Option
	if r.preferSSD {
		finalOpts = append(finalOpts, spec.PreferSSD())
	}
	if r.zones != "" {
		finalOpts = append(finalOpts, spec.Zones(r.zones))
	}
	finalOpts = append(finalOpts, opts...)
	return spec.MakeClusterSpec(r.cloud, r.instanceType, nodeCount, finalOpts...)
}

const testNameRE = "^[a-zA-Z0-9-_=/,]+$"

// prepareSpec validates a spec and does minor massaging of its fields.
func (r *testRegistryImpl) prepareSpec(spec *registry.TestSpec) error {
	if matched, err := regexp.MatchString(testNameRE, spec.Name); err != nil || !matched {
		return fmt.Errorf("%s: Name must match this regexp: %s", spec.Name, testNameRE)
	}

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
	teams, err := loadTeams()
	if err != nil {
		return err
	}
	if _, ok := teams[ownerToAlias(spec.Owner)]; !ok {
		return fmt.Errorf(`%s: unknown owner [%s]`, spec.Name, spec.Owner)
	}
	if len(spec.Tags) == 0 {
		spec.Tags = []string{registry.DefaultTag}
	}
	spec.Tags = append(spec.Tags, "owner-"+string(spec.Owner))

	// At the time of writing, we expect the roachtest job to finish within 24h
	// and have corresponding timeouts set up in CI. Since each individual test
	// may not be scheduled until a few hours in due to the CPU quota, individual
	// tests should expect to take "less time". Longer-running tests require the
	// weekly tag.
	const maxTimeout = 18 * time.Hour
	if spec.Timeout > maxTimeout {
		var weekly bool
		for _, tag := range spec.Tags {
			if tag == "weekly" {
				weekly = true
			}
		}
		if !weekly {
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

// GetTests returns all the tests that match the given regexp.
// Skipped tests are included, and tests that don't match their minVersion spec
// are also included but marked as skipped.
func (r testRegistryImpl) GetTests(
	ctx context.Context, filter *registry.TestFilter,
) ([]registry.TestSpec, []registry.TestSpec) {
	var tests []registry.TestSpec
	var tagMismatch []registry.TestSpec
	for _, t := range r.m {
		switch t.Match(filter) {
		case registry.Matched:
			tests = append(tests, *t)
		case registry.FailedTags:
			tagMismatch = append(tagMismatch, *t)
		case registry.FailedFilter:
		}
	}
	sort.Slice(tests, func(i, j int) bool {
		return tests[i].Name < tests[j].Name
	})
	sort.Slice(tagMismatch, func(i, j int) bool {
		return tagMismatch[i].Name < tagMismatch[j].Name
	})
	return tests, tagMismatch
}

// List lists tests that match one of the filters.
func (r testRegistryImpl) List(ctx context.Context, filters []string) []registry.TestSpec {
	filter := registry.NewTestFilter(filters, true)
	tests, _ := r.GetTests(ctx, filter)
	sort.Slice(tests, func(i, j int) bool { return tests[i].Name < tests[j].Name })
	return tests
}

func (r *testRegistryImpl) setBuildVersion(buildTag string) error {
	v, err := version.Parse(buildTag)
	if err != nil {
		return err
	}
	r.buildVersion = *v
	return err
}

func loadBuildVersion() (string, error) {
	cmd := exec.Command("git", "describe", "--abbrev=0", "--tags", "--match=v[0-9]*")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", errors.Wrapf(
			err, "failed to get version tag from git. Are you running in the "+
				"cockroach repo directory? err=%s, out=%s",
			err, out)
	}
	return strings.TrimSpace(string(out)), nil
}
