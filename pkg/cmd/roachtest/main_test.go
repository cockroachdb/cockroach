// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
)

func init() {
	registry.OverrideTeams(team.Map{
		OwnerUnitTest.ToTeamAlias(): {},
	})
}

func makeRegistry(names ...string) testRegistryImpl {
	r := makeTestRegistry()
	dummyRun := func(context.Context, test.Test, cluster.Cluster) {}

	for _, name := range names {
		r.Add(registry.TestSpec{
			Name:             name,
			Owner:            OwnerUnitTest,
			Run:              dummyRun,
			Cluster:          spec.MakeClusterSpec(0),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
		})
	}

	return r
}

func TestSampleSpecs(t *testing.T) {
	r := makeRegistry("abc/1234", "abc/5678", "abc/9292", "abc/2313", "abc/5656", "abc/2233", "abc/1893", "def/1234", "ghi", "jkl/1234")
	filter, err := registry.NewTestFilter([]string{})
	if err != nil {
		t.Fatal(err)
	}

	for _, f := range []float64{0.01, 0.5, 1.0} {
		t.Run(fmt.Sprintf("Sample-%.3f", f), func(t *testing.T) {
			specs, _ := testsToRun(r, filter, false /* runSkipped */, f /* selectProbability */, false /* print */)

			matched := map[string]int{"abc": 0, "def": 0, "ghi": 0, "jkl": 0}
			for _, s := range specs {
				prefix := strings.Split(s.Name, "/")[0]
				matched[prefix]++
			}

			for prefix, count := range matched {
				if count == 0 {
					t.Errorf("expected match but none found for prefix %s", prefix)
				}
			}
		})
	}

	filter, err = registry.NewTestFilter([]string{"abc"})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []float64{0.01, 0.5, 1.0} {
		t.Run(fmt.Sprintf("Sample-abc-%.3f", f), func(t *testing.T) {
			specs, _ := testsToRun(r, filter, false /* runSkipped */, f /* selectProbability */, false /* print */)

			matched := map[string]int{"abc": 0, "def": 0, "ghi": 0, "jkl": 0}
			for _, s := range specs {
				prefix := strings.Split(s.Name, "/")[0]
				matched[prefix]++
			}

			for prefix, count := range matched {
				if prefix == "abc" {
					if count == 0 {
						t.Errorf("expected match but none found for prefix %s", prefix)
					}
				} else if count > 0 {
					t.Errorf("unexpected match for prefix %s", prefix)
				}
			}
		})
	}
}
