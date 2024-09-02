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
	gosql "database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/testselector"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/stretchr/testify/require"
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

func Test_updateSpecForSelectiveTests(t *testing.T) {
	ctx := context.Background()
	var mock sqlmock.Sqlmock
	var db *gosql.DB
	var err error
	_ = os.Setenv("SFUSER", "dummy_user")
	_ = os.Setenv("SFPASSWORD", "dummy_password")
	testselector.SqlConnectorFunc = func(_, _ string) (*gosql.DB, error) {
		return db, err
	}
	t.Run("expect CategoriseTests to fail", func(t *testing.T) {
		db, mock, err = sqlmock.New()
		require.Nil(t, err)
		specs, _ := getMockData()
		mock.ExpectPrepare(regexp.QuoteMeta(testselector.PreparedQuery)).WillReturnError(fmt.Errorf("failed to prepare"))
		updateSpecForSelectiveTests(ctx, specs)
		for _, s := range specs {
			if !strings.Contains(s.Name, "skipped") {
				require.Empty(t, s.Skip)
			} else {
				require.Equal(t, "dummy skip", s.Skip)
			}
		}
	})
	t.Run("expect no failure", func(t *testing.T) {
		db, mock, err = sqlmock.New()
		require.Nil(t, err)
		specs, rows := getMockData()
		mock.ExpectPrepare(regexp.QuoteMeta(testselector.PreparedQuery))
		mock.ExpectQuery(regexp.QuoteMeta(testselector.PreparedQuery)).WillReturnRows(rows)
		oldSuite := roachtestflags.Suite
		roachtestflags.Suite = registry.Nightly
		defer func() {
			roachtestflags.Suite = oldSuite
		}()
		roachtestflags.SuccessfulTestsSelectPct = 0.30
		specsLengthBefore := len(specs)
		updateSpecForSelectiveTests(ctx, specs)
		require.Equal(t, specsLengthBefore, len(specs))
		for _, s := range specs {
			if strings.Contains(s.Name, "success_skip_selector") {
				require.Equal(t, "test selector", s.Skip, s.Name)
				require.Equal(t, "test skipped because it is stable and selective-tests is set.", s.SkipDetails, s.Name)
			} else if strings.Contains(s.Name, "skipped") {
				require.Equal(t, "dummy skip", s.Skip, s.Name)
				require.Equal(t, "dummy skip test", s.SkipDetails, s.Name)
			} else {
				require.Empty(t, s.Skip, s.Name)
			}
			if s.Name == "t2_selected" {
				require.True(t, s.IsLastFailurePreempt(), s.Name)
			} else {
				require.False(t, s.IsLastFailurePreempt(), s.Name)
			}
		}
	})
}

// getMockData returns the mock data as:
// 1. List of test specs.
// 2. List of rows to be returned by snowflake.
// The data follows a convention:
// all tests which contains "_skipped" are skipped in teh spec
// all tests which contains "_selected" are selected="yes"
// all tests which contains "_skip_selector" are skipped based on the criteria
// all tests which contains "_missing" are missing in the test spec
// all tests which contains "_success" are selected="no"
// all tests which contains "new_test" are present in specs, but missing in snowflake rows
// all tests which contains "_randomized" are set in spec as Randomized=true
func getMockData() ([]registry.TestSpec, *sqlmock.Rows) {
	specs := []registry.TestSpec{
		{Name: "t2_selected"},
		{Name: "t_skipped_selected", Skip: "dummy skip", SkipDetails: "dummy skip test"},
		{Name: "t1_success"},
		{Name: "t_randomized", Randomized: true},
		{Name: "t_new_test_included_1"},
		{Name: "t2_success"},
		{Name: "t3_success"},
		{Name: "t_opt_out", TestSelectionOptOutSuites: registry.Suites(registry.Nightly)},
		{Name: "t6_success_skip_selector", TestSelectionOptOutSuites: registry.Suites(registry.Weekly)},
		{Name: "t_skipped_new_test", Skip: "dummy skip", SkipDetails: "dummy skip test"},
		{Name: "t_skipped_not_selected", Skip: "dummy skip", SkipDetails: "dummy skip test"},
		{Name: "t5_success_skip_selector"},
		{Name: "t1_selected"},
		{Name: "t8_success_skip_selector"},
		{Name: "t_new_test_3"},
		{Name: "t10_success_skip_selector"},
	}
	for _, s := range specs {
		s.Suites = registry.Suites(registry.Nightly, registry.Weekly)
	}
	data := [][]string{
		{"t1_selected", "yes", "1", "no"},
		{"t2_selected", "yes", "2", "yes"},
		{"t_skipped_selected", "yes", "3", "no"},
		// tests from here are selected under percent criteria
		{"t1_success", "no", "4", "no"},
		{"t2_success", "no", "6", "no"},
		{"t3_success", "no", "7", "no"},
		// test is opted out from test selection
		{"t_opt_out", "no", "8", "no"},
		{"t4_missing", "no", "9", "no"},
		// tests from here are skipped as these are beyond the percentage criteria
		{"t5_success_skip_selector", "no", "10", "no"},
		{"t6_success_skip_selector", "no", "11", "no"},
		// the test is skipped by test selector, but is already skipped in spec
		{"t_skipped_not_selected", "no", "12", "no"},
		{"t7_missing", "no", "13", "no"},
		{"t8_success_skip_selector", "no", "14", "no"},
		{"t9_missing", "no", "15", "no"},
		{"t10_success_skip_selector", "no", "16", "no"},
		// test is Randomized=true, so, will always be selected even being at the last in the list
		{"t_randomized", "no", "5", "no"},
	}
	rows := sqlmock.NewRows([]string{
		"name", "selected", "avg_duration", "last_failure_is_preempt",
	})

	for _, ds := range data {
		rows.FromCSVString(strings.Join(ds, ","))
	}
	return specs, rows
}
