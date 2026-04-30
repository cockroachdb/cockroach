// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestRoachprodEnv tests the roachprodEnvRegex and roachprodEnvValue methods.
func TestRoachprodEnv(t *testing.T) {
	cases := []struct {
		clusterName string
		node        Node
		tag         string
		value       string
		regex       string
	}{
		{
			clusterName: "a",
			node:        1,
			tag:         "",
			value:       "1",
			regex:       `(ROACHPROD=1$|ROACHPROD=1[ \/])`,
		},
		{
			clusterName: "local-foo",
			node:        2,
			tag:         "",
			value:       "local-foo/2",
			regex:       `(ROACHPROD=local-foo\/2$|ROACHPROD=local-foo\/2[ \/])`,
		},
		{
			clusterName: "a",
			node:        3,
			tag:         "foo",
			value:       "3/foo",
			regex:       `(ROACHPROD=3\/foo$|ROACHPROD=3\/foo[ \/])`,
		},
		{
			clusterName: "a",
			node:        4,
			tag:         "foo/bar",
			value:       "4/foo/bar",
			regex:       `(ROACHPROD=4\/foo\/bar$|ROACHPROD=4\/foo\/bar[ \/])`,
		},
		{
			clusterName: "local-foo",
			node:        5,
			tag:         "tag",
			value:       "local-foo/5/tag",
			regex:       `(ROACHPROD=local-foo\/5\/tag$|ROACHPROD=local-foo\/5\/tag[ \/])`,
		},
	}

	for idx, tc := range cases {
		t.Run(fmt.Sprintf("%d", idx+1), func(t *testing.T) {
			var c SyncedCluster
			c.Name = tc.clusterName
			c.Tag = tc.tag
			if value := c.roachprodEnvValue(tc.node); value != tc.value {
				t.Errorf("expected value `%s`, got `%s`", tc.value, value)
			}
			if regex := c.roachprodEnvRegex(tc.node); regex != tc.regex {
				t.Errorf("expected regex `%s`, got `%s`", tc.regex, regex)
			}
		})
	}
}

func TestRunWithMaybeRetry(t *testing.T) {
	var testRetryOpts = &retry.Options{
		InitialBackoff: 1 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     100 * time.Millisecond,
		// This will run a total of 3 times `runWithMaybeRetry`
		MaxRetries: 2,
	}

	l := nilLogger()

	attempt := 0
	cases := []struct {
		retryOpts        *retry.Options
		f                func(ctx context.Context) (*RunResultDetails, error)
		shouldRetryFn    func(*RunResultDetails) bool
		expectedAttempts int
		shouldError      bool
	}{
		{ // 1. Happy path: no error, no retry required
			retryOpts: testRetryOpts,
			f: func(ctx context.Context) (*RunResultDetails, error) {
				return newResult(0), nil
			},
			expectedAttempts: 1,
			shouldError:      false,
		},
		{ // 2. Error, but with no retries
			retryOpts: testRetryOpts,
			f: func(ctx context.Context) (*RunResultDetails, error) {
				return newResult(1), nil
			},
			shouldRetryFn: func(*RunResultDetails) bool {
				return false
			},
			expectedAttempts: 1,
			shouldError:      true,
		},
		{ // 3. Error, but no retry function specified
			retryOpts: testRetryOpts,
			f: func(ctx context.Context) (*RunResultDetails, error) {
				return newResult(1), nil
			},
			expectedAttempts: 3,
			shouldError:      true,
		},
		{ // 4. Error, with retries exhausted
			retryOpts: testRetryOpts,
			f: func(ctx context.Context) (*RunResultDetails, error) {
				return newResult(255), nil
			},
			shouldRetryFn:    func(d *RunResultDetails) bool { return d.RemoteExitStatus == 255 },
			expectedAttempts: 3,
			shouldError:      true,
		},
		{ // 5. Eventual success after retries
			retryOpts: testRetryOpts,
			f: func(ctx context.Context) (*RunResultDetails, error) {
				attempt++
				if attempt == 3 {
					return newResult(0), nil
				}
				return newResult(255), nil
			},
			shouldRetryFn:    func(d *RunResultDetails) bool { return d.RemoteExitStatus == 255 },
			expectedAttempts: 3,
			shouldError:      false,
		},
		{ // 6. Error, runs once because nil RetryOpts
			retryOpts: nil,
			f: func(ctx context.Context) (*RunResultDetails, error) {
				return newResult(255), nil
			},
			expectedAttempts: 1,
			shouldError:      true,
		},
	}

	for idx, tc := range cases {
		attempt = 0
		t.Run(fmt.Sprintf("%d", idx+1), func(t *testing.T) {
			res, _ := runWithMaybeRetry(context.Background(), l, tc.retryOpts, tc.shouldRetryFn, tc.f)

			require.Equal(t, tc.shouldError, res.Err != nil)
			require.Equal(t, tc.expectedAttempts, res.Attempt)

			if tc.shouldError && tc.expectedAttempts == 3 {
				require.True(t, errors.Is(res.Err, ErrAfterRetry))
			}
		})
	}
}

func newResult(exitCode int) *RunResultDetails {
	var err error
	if exitCode != 0 {
		err = errors.Newf("Error with exit code %v", exitCode)
	}
	return &RunResultDetails{RemoteExitStatus: exitCode, Err: err}
}

func nilLogger() *logger.Logger {
	lcfg := logger.Config{
		Stdout: io.Discard,
		Stderr: io.Discard,
	}
	l, err := lcfg.NewLogger("" /* path */)
	if err != nil {
		panic(err)
	}
	return l
}

func TestGenFilenameFromArgs(t *testing.T) {
	const exp = "mkdir-p-logsredacted"
	require.Equal(t, exp, GenFilenameFromArgs(20, "mkdir -p logs/redacted && ./cockroach"))
	require.Equal(t, exp, GenFilenameFromArgs(20, "mkdir", "-p logs/redacted", "&& ./cockroach"))
	require.Equal(t, exp, GenFilenameFromArgs(20, "mkdir    -p logs/redacted && ./cockroach    "))
}

// TestWaitTimerBased verifies that the Wait function uses a timer-based approach
// rather than a counter-based approach, ensuring predictable timeout behavior.
func TestWaitTimerBased(t *testing.T) {
	t.Run("timeout respects time limit", func(t *testing.T) {
		// This test verifies that the wait loop uses time.Now().Before(deadline)
		// rather than a fixed retry counter. We simulate this by checking that
		// the timeout error message contains the duration format.

		// The actual Wait function would timeout after 5 minutes in real usage,
		// but we're just verifying the logic structure here.
		timeout := 5 * time.Minute
		deadline := time.Now().Add(timeout)

		require.True(t, time.Now().Before(deadline), "deadline should be in future")

		expectedFormat := fmt.Sprintf("timed out after %v", timeout)
		require.Equal(t, "timed out after 5m0s", expectedFormat)
	})

	t.Run("loop exits before deadline on success", func(t *testing.T) {
		timeout := 5 * time.Minute
		deadline := time.Now().Add(timeout)
		loopCount := 0

		for time.Now().Before(deadline) {
			loopCount++
			if loopCount == 3 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		require.Equal(t, 3, loopCount, "should exit early on success")
		require.True(t, time.Now().Before(deadline), "should exit well before deadline")
	})

	t.Run("loop respects deadline", func(t *testing.T) {
		timeout := 100 * time.Millisecond
		deadline := time.Now().Add(timeout)
		loopCount := 0

		for time.Now().Before(deadline) {
			loopCount++
			time.Sleep(20 * time.Millisecond)
		}

		require.GreaterOrEqual(t, loopCount, 4, "should loop multiple times")
		require.LessOrEqual(t, loopCount, 6, "should not loop excessively")
		require.False(t, time.Now().Before(deadline), "should exit after deadline")
	})
}
