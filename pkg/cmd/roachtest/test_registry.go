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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
)

// Owner is a valid entry for the Owners field of a roachtest. They should be
// teams, not individuals.
type Owner string

// The allowable values of Owner.
const (
	OwnerSQLExperience Owner = `sql-experience`
	OwnerBulkIO        Owner = `bulk-io`
	OwnerCDC           Owner = `cdc`
	OwnerKV            Owner = `kv`
	OwnerMultiRegion   Owner = `multiregion`
	OwnerServer        Owner = `server`
	OwnerSQLQueries    Owner = `sql-queries`
	OwnerSQLSchema     Owner = `sql-schema`
	OwnerStorage       Owner = `storage`
)

func ownerToAlias(o Owner) team.Alias {
	return team.Alias(fmt.Sprintf("cockroachdb/%s", o))
}

const defaultTag = "default"

type testRegistryImpl struct {
	m            map[string]*TestSpec
	cloud        string
	instanceType string // optional
	zones        string
	preferSSD    bool
	// buildVersion is the version of the Cockroach binary that tests will run against.
	buildVersion version.Version
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
		m:            make(map[string]*TestSpec),
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
func (r *testRegistryImpl) Add(spec TestSpec) {
	if _, ok := r.m[spec.Name]; ok {
		fmt.Fprintf(os.Stderr, "test %s already registered\n", spec.Name)
		os.Exit(1)
	}
	if err := r.prepareSpec(&spec); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
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
func (r *testRegistryImpl) prepareSpec(spec *TestSpec) error {
	if matched, err := regexp.MatchString(testNameRE, spec.Name); err != nil || !matched {
		return fmt.Errorf("%s: Name must match this regexp: %s", spec.Name, testNameRE)
	}

	if spec.Run == nil {
		return fmt.Errorf("%s: must specify Run", spec.Name)
	}

	if spec.Cluster.ReusePolicy == nil {
		return fmt.Errorf("%s: must specify a ClusterReusePolicy", spec.Name)
	}

	if spec.MinVersion != "" {
		v, err := version.Parse(spec.MinVersion)
		if err != nil {
			return fmt.Errorf("%s: unable to parse min-version: %s", spec.Name, err)
		}
		if v.PreRelease() != "" {
			// Specifying a prerelease version as a MinVersion is too confusing
			// to be useful. The comparison is not straightforward.
			return fmt.Errorf("invalid version %s, cannot specify a prerelease (-xxx)", v)
		}
		// We append "-0" to the min-version spec so that we capture all
		// prereleases of the specified version. Otherwise, "v2.1.0" would compare
		// greater than "v2.1.0-alpha.x".
		spec.minVersion = version.MustParse(spec.MinVersion + "-0")
	}

	// All tests must have an owner so the release team knows who signs off on
	// failures and so the github issue poster knows who to assign it to.
	if spec.Owner == `` {
		return fmt.Errorf(`%s: unspecified owner`, spec.Name)
	}
	teams, err := team.DefaultLoadTeams()
	if err != nil {
		return err
	}
	if _, ok := teams[ownerToAlias(spec.Owner)]; !ok {
		return fmt.Errorf(`%s: unknown owner [%s]`, spec.Name, spec.Owner)
	}
	if len(spec.Tags) == 0 {
		spec.Tags = []string{defaultTag}
	}
	spec.Tags = append(spec.Tags, "owner-"+string(spec.Owner))

	return nil
}

// GetTests returns all the tests that match the given regexp.
// Skipped tests are included, and tests that don't match their minVersion spec
// are also included but marked as skipped.
func (r testRegistryImpl) GetTests(ctx context.Context, filter *testFilter) []TestSpec {
	var tests []TestSpec
	for _, t := range r.m {
		if !t.matchOrSkip(filter) {
			continue
		}
		if t.Skip == "" && t.minVersion != nil {
			if !r.buildVersion.AtLeast(t.minVersion) {
				t.Skip = fmt.Sprintf("build-version (%s) < min-version (%s)",
					r.buildVersion, t.minVersion)
			}
		}
		tests = append(tests, *t)
	}
	sort.Slice(tests, func(i, j int) bool {
		return tests[i].Name < tests[j].Name
	})
	return tests
}

// List lists tests that match one of the filters.
func (r testRegistryImpl) List(ctx context.Context, filters []string) []TestSpec {
	filter := newFilter(filters)
	tests := r.GetTests(ctx, filter)
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

// testFilter holds the name and tag filters for filtering tests.
type testFilter struct {
	name *regexp.Regexp
	tag  *regexp.Regexp
	// rawTag is the string representation of the regexps in tag
	rawTag []string
}

func newFilter(filter []string) *testFilter {
	var name []string
	var tag []string
	var rawTag []string
	for _, v := range filter {
		if strings.HasPrefix(v, "tag:") {
			tag = append(tag, strings.TrimPrefix(v, "tag:"))
			rawTag = append(rawTag, v)
		} else {
			name = append(name, v)
		}
	}

	if len(tag) == 0 {
		tag = []string{defaultTag}
		rawTag = []string{"tag:" + defaultTag}
	}

	makeRE := func(strs []string) *regexp.Regexp {
		switch len(strs) {
		case 0:
			return regexp.MustCompile(`.`)
		case 1:
			return regexp.MustCompile(strs[0])
		default:
			for i := range strs {
				strs[i] = "(" + strs[i] + ")"
			}
			return regexp.MustCompile(strings.Join(strs, "|"))
		}
	}

	return &testFilter{
		name:   makeRE(name),
		tag:    makeRE(tag),
		rawTag: rawTag,
	}
}
