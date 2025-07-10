// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

type mockInspectCheck struct {
	started bool
	pos     int
	closed  bool
	issues  []inspectIssue
}

var _ inspectCheck = &mockInspectCheck{}

func (m *mockInspectCheck) Started() bool {
	return m.started
}

func (m *mockInspectCheck) Start(
	context.Context, *execinfra.ServerConfig, roachpb.Span, int,
) error {
	if m.started {
		return errors.Newf("inspect check already started")
	}
	m.started = true
	return nil
}

// Next returns the next inspect error, if any.
// Returns (nil, nil) when there are no errors for the current row.
func (m *mockInspectCheck) Next(
	ctx context.Context, _ *execinfra.ServerConfig,
) (*inspectIssue, error) {
	if m.Done(ctx) {
		return nil, errors.Newf("check is already done")
	}
	issue := &m.issues[m.pos]
	m.pos++
	return issue, nil
}

// Done reports whether the check has produced all results.
func (m *mockInspectCheck) Done(context.Context) bool {
	return m.pos >= len(m.issues)
}

// Close cleans up resources for the check.
func (m *mockInspectCheck) Close(context.Context) error {
	if !m.started {
		return errors.Newf("inspect check hasn't been started")
	}
	m.closed = true
	return nil
}

type mockLogger struct {
	issuesFound []inspectIssue
}

func (m *mockLogger) logIssue(_ context.Context, issue *inspectIssue) error {
	m.issuesFound = append(m.issuesFound, *issue)
	return nil
}

func TestRunnerStep(t *testing.T) {
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
		{desc: "two check with 1 error", checks: [][]inspectIssue{
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
			{},
		}},
		{desc: "two check with multiple errors", checks: [][]inspectIssue{
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
			logger := &mockLogger{}
			// The checks are consumed once they are processed in inspectRunner. So,
			// save them outside the runner first so we can refer back to them later.
			checks := make([]*mockInspectCheck, len(tc.checks))
			for i := range tc.checks {
				checks[i] = &mockInspectCheck{issues: tc.checks[i]}
			}
			runner := inspectRunner{logger: logger}
			for i := range tc.checks {
				runner.checks = append(runner.checks, checks[i])
			}

			issuesFound := 0
			for {
				foundIssue, err := runner.Step(ctx, nil, roachpb.Span{}, 0)
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
			require.Equal(t, issuesFound, len(logger.issuesFound))

			flatExpected := make([]inspectIssue, 0, expectedIssuesFound)
			for _, check := range tc.checks {
				flatExpected = append(flatExpected, check...)
			}
			for i := range flatExpected {
				require.Equal(t, flatExpected[i], logger.issuesFound[i])
			}

			for i := range checks {
				require.True(t, checks[i].closed)
			}
		})
	}
}
