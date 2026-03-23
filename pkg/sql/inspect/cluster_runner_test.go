// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/inspect/inspectpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

type mockInspectClusterCheck struct {
	started bool
	pos     int
	closed  bool
	issues  []inspectIssue
}

var _ inspectClusterCheck = &mockInspectClusterCheck{}

func (m *mockInspectClusterCheck) StartedCluster() bool {
	return m.started
}

func (m *mockInspectClusterCheck) StartCluster(
	context.Context, *inspectpb.InspectSpanCheckData,
) error {
	if m.started {
		return errors.Newf("cluster inspect check already started")
	}
	m.started = true
	return nil
}

// NextCluster returns the next inspect error, if any.
// Returns (nil, nil) when there are no errors remaining.
func (m *mockInspectClusterCheck) NextCluster(ctx context.Context) (*inspectIssue, error) {
	if m.DoneCluster(ctx) {
		return nil, errors.Newf("cluster check is already done")
	}
	issue := &m.issues[m.pos]
	m.pos++
	return issue, nil
}

// DoneCluster reports whether the check has produced all results.
func (m *mockInspectClusterCheck) DoneCluster(context.Context) bool {
	return m.pos >= len(m.issues)
}

// CloseCluster cleans up resources for the check.
func (m *mockInspectClusterCheck) CloseCluster(context.Context) error {
	if !m.started {
		return errors.Newf("cluster inspect check hasn't been started")
	}
	m.closed = true
	return nil
}

func TestClusterRunnerStep(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, tc := range []struct {
		desc   string
		checks [][]inspectIssue
	}{
		{desc: "no checks"},
		{desc: "one check with no errors", checks: [][]inspectIssue{{}}},
		{desc: "one check with 1 error", checks: [][]inspectIssue{
			{
				{
					ErrorType:  "secondary_index_dangling",
					DatabaseID: 1,
					SchemaID:   2,
					ObjectID:   3,
					PrimaryKey: "/1/3",
					Details: map[redact.RedactableString]interface{}{
						"index_name": "foo_idx",
					},
				},
			},
		}},
		{desc: "two checks with 1 error each", checks: [][]inspectIssue{
			{
				{
					ErrorType:  "secondary_index_missing",
					DatabaseID: 10,
					SchemaID:   11,
					ObjectID:   12,
					PrimaryKey: "/10/12",
					Details: map[redact.RedactableString]interface{}{
						"index_name": "bar_idx",
					},
				},
			},
			{
				{
					ErrorType:  "internal_error",
					DatabaseID: 20,
					SchemaID:   21,
					ObjectID:   22,
					PrimaryKey: "/20/22",
					Details: map[redact.RedactableString]interface{}{
						"error": "unexpected data corruption",
					},
				},
			},
		}},
		{desc: "two checks with multiple errors", checks: [][]inspectIssue{
			{
				{
					ErrorType:  "secondary_index_dangling",
					DatabaseID: 100,
					SchemaID:   101,
					ObjectID:   102,
					PrimaryKey: "/100/1",
					Details: map[redact.RedactableString]interface{}{
						"index_name": "baz_idx",
					},
				},
				{
					ErrorType:  "secondary_index_missing",
					DatabaseID: 100,
					SchemaID:   101,
					ObjectID:   102,
					PrimaryKey: "/100/2",
					Details: map[redact.RedactableString]interface{}{
						"index_name": "baz_idx",
					},
				},
			},
			{
				{
					ErrorType:  "secondary_index_missing",
					DatabaseID: 200,
					SchemaID:   201,
					ObjectID:   202,
					PrimaryKey: "/200/1",
					Details: map[redact.RedactableString]interface{}{
						"index_name": "qux_idx",
					},
				},
			},
		}},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			logger := &testIssueCollector{}
			// The checks are consumed once they are processed in clusterRunner. So,
			// save them outside the runner first so we can refer back to them later.
			checks := make([]*mockInspectClusterCheck, len(tc.checks))
			for i := range tc.checks {
				checks[i] = &mockInspectClusterCheck{issues: tc.checks[i]}
			}

			progressTracker := &inspectProgressTracker{}
			progressTracker.mu.cachedProgress = &jobspb.Progress{
				Details: &jobspb.Progress_Inspect{
					Inspect: &jobspb.InspectProgress{},
				},
			}

			loggerBundle := newInspectLoggerBundle(1 /* jobID */, logger)
			runner := clusterRunner{
				logger:          loggerBundle,
				progressTracker: progressTracker,
			}
			for i := range tc.checks {
				runner.checks = append(runner.checks, checks[i])
			}

			issuesFound := 0
			for {
				foundIssue, err := runner.Step(ctx)
				require.NoError(t, err)
				if !foundIssue {
					break
				}
				issuesFound++
			}

			expectedIssuesFound := 0
			for _, check := range tc.checks {
				expectedIssuesFound += len(check)
			}
			require.Equal(t, expectedIssuesFound, issuesFound)
			require.Equal(t, issuesFound, logger.numIssuesFound())

			flatExpected := make([]inspectIssue, 0, expectedIssuesFound)
			for _, check := range tc.checks {
				flatExpected = append(flatExpected, check...)
			}
			for i := range flatExpected {
				require.Equal(t, flatExpected[i], logger.issue(i))
			}

			for i := range checks {
				require.True(t, checks[i].closed)
			}
		})
	}
}

func TestClusterRunnerStepError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	t.Run("error on start", func(t *testing.T) {
		logger := &testIssueCollector{}
		progressTracker := &inspectProgressTracker{}
		progressTracker.mu.cachedProgress = &jobspb.Progress{
			Details: &jobspb.Progress_Inspect{
				Inspect: &jobspb.InspectProgress{},
			},
		}

		loggerBundle := newInspectLoggerBundle(1 /* jobID */, logger)

		// Create a check that returns an error on StartCluster
		errorCheck := &errorInspectClusterCheck{
			startErr: errors.Newf("start error"),
		}

		runner := clusterRunner{
			checks:          []inspectClusterCheck{errorCheck},
			logger:          loggerBundle,
			progressTracker: progressTracker,
		}

		_, err := runner.Step(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "error starting cluster inspect check")
		require.Contains(t, err.Error(), "start error")
	})

	t.Run("error on next", func(t *testing.T) {
		logger := &testIssueCollector{}
		progressTracker := &inspectProgressTracker{}
		progressTracker.mu.cachedProgress = &jobspb.Progress{
			Details: &jobspb.Progress_Inspect{
				Inspect: &jobspb.InspectProgress{},
			},
		}

		loggerBundle := newInspectLoggerBundle(1 /* jobID */, logger)

		// Create a check that returns an error on NextCluster
		errorCheck := &errorInspectClusterCheck{
			nextErr: errors.Newf("next error"),
		}

		runner := clusterRunner{
			checks:          []inspectClusterCheck{errorCheck},
			logger:          loggerBundle,
			progressTracker: progressTracker,
		}

		_, err := runner.Step(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "error running cluster inspect check")
		require.Contains(t, err.Error(), "next error")
	})

	t.Run("error on close", func(t *testing.T) {
		logger := &testIssueCollector{}
		progressTracker := &inspectProgressTracker{}
		progressTracker.mu.cachedProgress = &jobspb.Progress{
			Details: &jobspb.Progress_Inspect{
				Inspect: &jobspb.InspectProgress{},
			},
		}

		loggerBundle := newInspectLoggerBundle(1 /* jobID */, logger)

		// Create a check that returns an error on CloseCluster
		errorCheck := &errorInspectClusterCheck{
			closeErr: errors.Newf("close error"),
		}

		runner := clusterRunner{
			checks:          []inspectClusterCheck{errorCheck},
			logger:          loggerBundle,
			progressTracker: progressTracker,
		}

		// First step: starts and runs the check
		foundIssue, err := runner.Step(ctx)
		require.NoError(t, err)
		require.True(t, foundIssue)

		// Second step: check is done, so it tries to close and returns error
		_, err = runner.Step(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "error closing cluster inspect check")
		require.Contains(t, err.Error(), "close error")
	})
}

func TestClusterRunnerClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	t.Run("close all checks", func(t *testing.T) {
		logger := &testIssueCollector{}
		progressTracker := &inspectProgressTracker{}
		progressTracker.mu.cachedProgress = &jobspb.Progress{
			Details: &jobspb.Progress_Inspect{
				Inspect: &jobspb.InspectProgress{},
			},
		}

		loggerBundle := newInspectLoggerBundle(1 /* jobID */, logger)

		checks := []*mockInspectClusterCheck{
			{started: true, issues: []inspectIssue{}},
			{started: true, issues: []inspectIssue{}},
		}

		runner := clusterRunner{
			logger:          loggerBundle,
			progressTracker: progressTracker,
		}
		for i := range checks {
			runner.checks = append(runner.checks, checks[i])
		}

		err := runner.Close(ctx)
		require.NoError(t, err)

		for i, check := range checks {
			require.True(t, check.closed, fmt.Sprintf("check %d should be closed", i))
		}
	})

	t.Run("close with errors", func(t *testing.T) {
		logger := &testIssueCollector{}
		progressTracker := &inspectProgressTracker{}
		progressTracker.mu.cachedProgress = &jobspb.Progress{
			Details: &jobspb.Progress_Inspect{
				Inspect: &jobspb.InspectProgress{},
			},
		}

		loggerBundle := newInspectLoggerBundle(1 /* jobID */, logger)

		checks := []inspectClusterCheck{
			&errorInspectClusterCheck{started: true, closeErr: errors.Newf("close error 1")},
			&errorInspectClusterCheck{started: true, closeErr: errors.Newf("close error 2")},
		}

		runner := clusterRunner{
			checks:          checks,
			logger:          loggerBundle,
			progressTracker: progressTracker,
		}

		err := runner.Close(ctx)
		require.ErrorContains(t, err, "close error")
	})
}

// errorInspectClusterCheck is a mock cluster check that can return errors at various stages.
type errorInspectClusterCheck struct {
	started  bool
	startErr error
	nextErr  error
	closeErr error
	done     bool
}

var _ inspectClusterCheck = &errorInspectClusterCheck{}

func (e *errorInspectClusterCheck) StartedCluster() bool {
	return e.started
}

func (e *errorInspectClusterCheck) StartCluster(
	context.Context, *inspectpb.InspectSpanCheckData,
) error {
	if e.startErr != nil {
		return e.startErr
	}
	e.started = true
	return nil
}

func (e *errorInspectClusterCheck) NextCluster(context.Context) (*inspectIssue, error) {
	if e.nextErr != nil {
		return nil, e.nextErr
	}
	e.done = true
	return nil, nil
}

func (e *errorInspectClusterCheck) DoneCluster(context.Context) bool {
	return e.done
}

func (e *errorInspectClusterCheck) CloseCluster(context.Context) error {
	return e.closeErr
}
