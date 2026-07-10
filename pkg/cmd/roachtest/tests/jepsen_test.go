// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClassifyJepsenFailure(t *testing.T) {
	type testCase struct {
		name              string
		testErr           error
		outcome           string
		workloadCompleted bool
		expectedCause     string
		expectedBehavior  jepsenFailureBehavior
	}
	testCases := []testCase{
		{
			name: "no error",
		},
		{
			name: "monotonic setup sqlliveness race",
			outcome: `ERROR [2026-06-25 08:04:38,816] main - jepsen.cli Oh jeez, I'm sorry, Jepsen broke. Here's why:
org.postgresql.util.PSQLException: ERROR: active-schema-leases-by-region: sqlliveness subsystem has not yet been started
	at jepsen.cockroach.monotonic$monotonic_create_tables_BANG_.invoke(monotonic.clj:32)
	at jepsen.cockroach.monotonic.MonotonicClient.setup_BANG_(monotonic.clj:102)`,
			expectedCause:    "jepsen monotonic setup hit unstarted sqlliveness",
			expectedBehavior: jepsenFailureTransient,
		},
		{
			name: "sqlliveness error during workload",
			outcome: `ERROR [2026-06-25 08:04:38,816] main - jepsen.cli Oh jeez, I'm sorry, Jepsen broke. Here's why:
org.postgresql.util.PSQLException: ERROR: active-schema-leases-by-region: sqlliveness subsystem has not yet been started
	at jepsen.cockroach.monotonic.MonotonicClient.invoke_BANG_(monotonic.clj:120)`,
		},
		{
			name: "sqlliveness error is not the root exception",
			outcome: `ERROR [2026-06-25 08:04:38,816] main - jepsen.cli Oh jeez, I'm sorry, Jepsen broke. Here's why:
java.lang.RuntimeException: worker failed
Caused by: org.postgresql.util.PSQLException: ERROR: active-schema-leases-by-region: sqlliveness subsystem has not yet been started
	at jepsen.cockroach.monotonic.MonotonicClient.setup_BANG_(monotonic.clj:102)`,
		},
		{
			name: "different setup failure",
			outcome: `ERROR [2026-06-25 08:04:38,816] main - jepsen.cli Oh jeez, I'm sorry, Jepsen broke. Here's why:
java.lang.IllegalStateException: boom
	at jepsen.cockroach.monotonic.MonotonicClient.setup_BANG_(monotonic.clj:102)`,
		},
		{
			name:              "completed workload analysis timeout",
			testErr:           errJepsenTimedOut,
			workloadCompleted: true,
			expectedCause:     "jepsen workload completed, but analysis timed out",
			expectedBehavior:  jepsenFailureTransient,
		},
		{
			name:              "wrapped completed workload analysis timeout",
			testErr:           fmt.Errorf("wrapped: %w", errJepsenTimedOut),
			workloadCompleted: true,
			expectedCause:     "jepsen workload completed, but analysis timed out",
			expectedBehavior:  jepsenFailureTransient,
		},
		{
			name:    "workload did not complete before timeout",
			testErr: errJepsenTimedOut,
		},
		{
			name:              "completed workload without timeout",
			testErr:           errors.New("exit status 254"),
			workloadCompleted: true,
		},
		{
			name:             "successful analysis with nonzero exit",
			outcome:          "INFO jepsen.checker Everything looks good! ✓",
			expectedCause:    "jepsen succeeded but exited nonzero",
			expectedBehavior: jepsenFailureSkip,
		},
		{
			name:    "unknown outcome",
			outcome: jepsenFailureOutcomeMarker + "\njava.lang.IllegalStateException: boom",
		},
		{
			name: "known nested cause",
			outcome: jepsenFailureOutcomeMarker + `
java.lang.RuntimeException: worker failed
Caused by: java.lang.NullPointerException: null`,
			expectedCause:    "jepsen null pointer",
			expectedBehavior: jepsenFailureSkip,
		},
	}

	for _, knownFailure := range []struct {
		match string
		cause string
	}{
		{"BrokenBarrierException", "jepsen broken barrier"},
		{"InterruptedException", "jepsen interrupted"},
		{"ArrayIndexOutOfBoundsException", "jepsen array index out of bounds"},
		{"NullPointerException", "jepsen null pointer"},
		{"clojure.lang.ExceptionInfo: clj-ssh scp failure", "jepsen scp failure"},
		{"RuntimeException: Connection to", "jepsen connection timeout"},
	} {
		testCases = append(testCases, testCase{
			name:             knownFailure.cause,
			outcome:          jepsenFailureOutcomeMarker + "\n" + knownFailure.match,
			expectedCause:    knownFailure.cause,
			expectedBehavior: jepsenFailureSkip,
		})
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cause, behavior, ok := classifyJepsenFailure(
				tc.testErr, tc.outcome, tc.workloadCompleted,
			)
			require.Equal(t, tc.expectedCause != "", ok)
			require.Equal(t, tc.expectedCause, cause)
			require.Equal(t, tc.expectedBehavior, behavior)
		})
	}
}
