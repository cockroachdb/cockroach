// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package registry

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/cockroachdb/datadriven"
)

func init() {
	OverrideTeams(team.Map{
		OwnerCDC.ToTeamAlias(): {},
		OwnerKV.ToTeamAlias():  {},
	})
}

func TestTestFilter(t *testing.T) {
	// Create a library of tests.
	var tests []TestSpec

	for _, name1 := range []string{"component_foo", "component_bar"} {
		for _, name2 := range []string{"test_foo", "bench_bar"} {
			for _, owner := range []Owner{OwnerCDC, OwnerKV} {
				for _, suites := range []SuiteSet{ManualOnly, Suites(Nightly), Suites(Nightly, Weekly)} {
					cloudSets := []CloudSet{AllClouds, AllExceptAWS}
					if name2 == "bench_bar" {
						cloudSets = []CloudSet{OnlyGCE}
					}
					for _, clouds := range cloudSets {
						suite := suites.String()
						if suite == "<none>" {
							suite = ""
						} else {
							suite = "-" + suite
						}
						t := TestSpec{
							Name:             fmt.Sprintf("%s/%s-%s%s-%s", name1, name2, owner, suite, clouds),
							Owner:            owner,
							CompatibleClouds: clouds,
							Suites:           suites,
						}
						if name2 == "bench_bar" {
							t.Benchmark = true
						}
						tests = append(tests, t)
					}
				}
			}
		}
	}

	datadriven.Walk(t, "testdata/", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			filters := strings.Fields(d.Input)
			var options []TestFilterOption
			var testName string
			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "cloud":
					options = append(options, WithCloud(spec.CloudFromString(arg.Vals[0])))
				case "suite":
					options = append(options, WithSuite(arg.Vals[0]))
				case "owner":
					options = append(options, WithOwner(Owner(arg.Vals[0])))
				case "benchmarks":
					options = append(options, OnlyBenchmarks())
				case "test":
					testName = arg.Vals[0]
				default:
					d.Fatalf(t, "unknown parameter %s", arg)
				}
			}

			filter, err := NewTestFilter(filters, options...)
			if err != nil {
				return fmt.Sprintf("error: %s", err)
			}

			switch d.Cmd {
			case "filter":
				matches, h := filter.FilterWithHint(tests)
				if len(matches) == 0 {
					return fmt.Sprintf("error: %s", filter.NoMatchesHintString(h))
				}
				lines := make([]string, len(matches))
				for i := range matches {
					lines[i] = matches[i].Name
				}
				return strings.Join(lines, "\n")

			case "test-matches":
				var spec *TestSpec
				for i := range tests {
					if tests[i].Name == testName {
						spec = &tests[i]
					}
				}
				if spec == nil {
					d.Fatalf(t, "no such test %q", testName)
				}
				matches, r := filter.Matches(spec)
				reason := "matches"
				if !matches {
					reason = filter.MatchFailReasonString(r)
				}
				return fmt.Sprintf("%s %s", testName, reason)

			case "describe":
				return filter.String()

			default:
				d.Fatalf(t, "unknown command %s", d.Cmd)
				return ""
			}
		})
	})
}
