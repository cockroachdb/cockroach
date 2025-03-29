// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSimpleQueryGeneration shows how to use debug mode in a unit test
// without needing the full roachtest framework.
func TestSimpleQueryGeneration(t *testing.T) {
	// Skip test during automated test runs
	t.Skip("This test is an example and requires manual running")

	// Create a mock API directly for testing
	ctx := context.Background()
	api := metrics.CreateMockMetricsAPIForTesting(ctx)

	// Enable debug mode
	api.EnableDebugMode(true)

	// Test various query patterns
	testCases := []struct {
		name          string
		buildQuery    func() error
		expectedQuery string
	}{
		{
			name: "basic metric",
			buildQuery: func() error {
				return api.AssertThat("sql_txn_count").HasValueAtLeast(10)
			},
			expectedQuery: "sql_txn_count",
		},
		{
			name: "filtered by node",
			buildQuery: func() error {
				return api.AssertThat("liveness_heartbeats_successful").ForNode("1").HasValueAtLeast(1)
			},
			expectedQuery: `liveness_heartbeats_successful{node="1"}`,
		},
		{
			name: "rate query",
			buildQuery: func() error {
				return api.AssertThat("sql_select_count").OverLast("30s").Rate().HasRateAtLeast(0.1)
			},
			expectedQuery: "rate(sql_select_count[30s])",
		},
	}

	// Run each test case
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Execute the query builder
			err := tc.buildQuery()
			require.NoError(t, err)

			// Verify the query matches expected
			lastQuery := api.GetLastQuery()
			assert.True(t, strings.Contains(lastQuery, tc.expectedQuery),
				"Query %s should contain %s", lastQuery, tc.expectedQuery)
		})
	}
}

/*
// Example usage of the metrics API debug mode to verify query generation:

func verifyPrometheusQuery() {
	// Create a context
	ctx := context.Background()

	// Create a metrics API (in real usage you'd use NewMetricsAPI with proper context)
	api := metrics.CreateMockMetricsAPIForTesting(ctx)
	api.EnableDebugMode(true)

	// Build a query for a metric
	metric := "sql_txn_count"
	query := api.AssertThat(metric).
		ForNode("1").
		OverLast("30s").
		Rate()

	// Execute the query with an assertion
	_ = query.HasRateAtLeast(0.1)

	// Get the query that was executed
	lastQuery := api.GetLastQuery()
	fmt.Printf("Query executed: %s\n", lastQuery)

	// Get the result
	result := api.GetLastQueryResult()
	fmt.Printf("Result: %.2f\n", result)
}

// Example usage for comparing metrics over time:

func compareMetricsOverTime() {
	// Create a context
	ctx := context.Background()

	// Create a metrics API
	api := metrics.CreateMockMetricsAPIForTesting(ctx)
	api.EnableDebugMode(true)

	// Define time points for comparison
	startTime := time.Now().Add(-1 * time.Hour)
	endTime := time.Now()

	// Create comparison and assert
	comparison := api.CompareValuesOverTime("sql_txn_count", startTime, endTime)
	_ = comparison.HasIncreasedByLessThan(20) // Assert it increased by less than 20%

	// Get the executed query
	lastQuery := api.GetLastQuery()
	fmt.Printf("Comparison query: %s\n", lastQuery)
}
*/
