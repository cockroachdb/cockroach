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

	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/pkg/errors"
)

type testRegistry struct {
	m map[string]*testSpec
	// buildVersion is the version of the Cockroach binary that tests will run against.
	buildVersion version.Version
}

// makeTestRegistry constructs a testRegistry and configures it with opts.
func makeTestRegistry() (testRegistry, error) {
	r := testRegistry{
		m: make(map[string]*testSpec),
	}
	v := buildTag
	if v == "" {
		var err error
		v, err = loadBuildVersion()
		if err != nil {
			return testRegistry{}, err
		}
	}
	if err := r.setBuildVersion(v); err != nil {
		return testRegistry{}, err
	}
	return r, nil
}

// Add adds a test to the registry.
func (r *testRegistry) Add(spec testSpec) {
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

// prepareSpec validates a spec and does minor massaging of its fields.
func (r *testRegistry) prepareSpec(spec *testSpec) error {
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
	return nil
}

// GetTests returns all the tests that match the given regexp.
// Skipped tests are included, and tests that don't match their minVersion spec
// are also included but marked as skipped.
func (r testRegistry) GetTests(ctx context.Context, filter *testFilter) []testSpec {
	var tests []testSpec
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
func (r testRegistry) List(ctx context.Context, filters []string) []string {
	filter := newFilter(filters)
	tests := r.GetTests(ctx, filter)
	var names []string
	for _, t := range tests {
		name := t.Name
		if t.Skip != "" {
			name += " (skipped: " + t.Skip + ")"
		}

		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *testRegistry) setBuildVersion(buildTag string) error {
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
		tag = []string{"default"}
		rawTag = []string{"tag:default"}
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
