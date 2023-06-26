// Copyright 2022 The Cockroach Authors.
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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
)

func init() {
	loadTeams = func() (team.Map, error) {
		return map[team.Alias]team.Team{
			ownerToAlias(OwnerUnitTest): {},
		}, nil
	}
}

func makeRegistry(names ...string) testRegistryImpl {
	r := makeTestRegistry(spec.GCE, "", "", false /* preferSSD */, false /* benchOnly */)
	dummyRun := func(context.Context, test.Test, cluster.Cluster) {}

	for _, name := range names {
		r.Add(registry.TestSpec{
			Name:    name,
			Owner:   OwnerUnitTest,
			Run:     dummyRun,
			Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
		})
	}

	return r
}

func TestSampleSpecs(t *testing.T) {
	r := makeRegistry("abc/1234", "abc/5678", "abc/9292", "abc/2313", "abc/5656", "abc/2233", "abc/1893", "def/1234", "ghi", "jkl/1234")
	filter := registry.NewTestFilter([]string{}, false)

	for _, f := range []float64{0.01, 0.5, 1.0} {
		t.Run(fmt.Sprintf("Sample-%.3f", f), func(t *testing.T) {
			specs := testsToRun(r, filter, f, false)

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

	filter = registry.NewTestFilter([]string{"abc"}, false)
	for _, f := range []float64{0.01, 0.5, 1.0} {
		t.Run(fmt.Sprintf("Sample-abc-%.3f", f), func(t *testing.T) {
			specs := testsToRun(r, filter, f, false)

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
