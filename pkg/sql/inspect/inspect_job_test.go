// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect_test

import (
	"context"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestInspectJobImplicitTxnSemantics validates how inspect jobs behave when
// triggered by statements that run in implicit transactions. It verifies that the job
// starts correctly, that errors or timeouts propagate to the user, and that
// client-visible semantics (like statement timeout or job failure) behave as expected.
//
// Note: This test currently uses SCRUB to trigger a job, but is not testing SCRUB
// itself. The goal is to verify general execution semantics for async job statements.
func TestInspectJobImplicitTxnSemantics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var onInspectErrorToReturn atomic.Pointer[error]
	var pauseJobStart atomic.Bool
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Inspect: &sql.InspectTestingKnobs{
				OnInspectJobStart: func() error {
					// Use a timeout so we aren't stuck in here forever in case something bad happens.
					const maxPause = 30 * time.Second
					deadline := time.After(maxPause)
					for {
						if !pauseJobStart.Load() {
							break
						}
						select {
						case <-deadline:
							return errors.Newf("test timed out after %s while waiting for pause to clear", maxPause)
						default:
							time.Sleep(10 * time.Millisecond)
						}
					}
					if errPtr := onInspectErrorToReturn.Load(); errPtr != nil {
						return *errPtr
					}
					return nil
				},
			},
		},
	})
	defer s.Stopper().Stop(context.Background())
	runner := sqlutils.MakeSQLRunner(db)

	runner.Exec(t, `
		CREATE DATABASE db;
		SET enable_scrub_job = true;
		CREATE TABLE db.t (
			id INT PRIMARY KEY,
			val INT
		);
		CREATE INDEX i1 on db.t (val);
		INSERT INTO db.t VALUES (1, 2), (2,3);`)

	for _, tc := range []struct {
		desc              string
		setupSQL          string
		tearDownSQL       string
		pauseAtStart      bool
		onStartError      error
		expectedErrRegex  string
		expectedJobStatus string
	}{
		{desc: "inspect success", expectedJobStatus: "succeeded"},
		{desc: "inspect failure", onStartError: errors.Newf("inspect validation error"),
			expectedErrRegex: "inspect validation error", expectedJobStatus: "failed"},
		// Note: avoiding small statement timeouts, as this can impact the ability to reset.
		{desc: "statement timeout", setupSQL: "SET statement_timeout = '1s'", tearDownSQL: "RESET statement_timeout",
			pauseAtStart: true, expectedErrRegex: "canceled", expectedJobStatus: "succeeded"},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			// Run in a closure so that we run teardown before verifying job status
			func() {
				if tc.setupSQL != "" {
					runner.Exec(t, tc.setupSQL)
				}
				if tc.tearDownSQL != "" {
					defer func() { runner.Exec(t, tc.tearDownSQL) }()
				}
				if tc.pauseAtStart {
					pauseJobStart.Store(true)
				}
				if tc.onStartError != nil {
					onInspectErrorToReturn.Store(&tc.onStartError)
					defer func() { onInspectErrorToReturn.Store(nil) }()
				}
				_, err := db.Exec("EXPERIMENTAL SCRUB TABLE db.t")
				pauseJobStart.Store(false)
				if tc.expectedErrRegex != "" {
					require.Error(t, err)
					re := regexp.MustCompile(tc.expectedErrRegex)
					match := re.MatchString(err.Error())
					require.True(t, match, "Error text %q doesn't match the expected regexp of %q",
						err.Error(), tc.expectedErrRegex)
				} else {
					require.NoError(t, err)
				}
			}()

			// Wait for the job to finish.
			var status string
			var fractionCompleted float64
			testutils.SucceedsSoon(t, func() error {
				row := db.QueryRow(`SELECT status, fraction_completed FROM [SHOW JOBS] WHERE job_type = 'INSPECT' ORDER BY job_id DESC LIMIT 1`)
				if err := row.Scan(&status, &fractionCompleted); err != nil {
					return err
				}
				if status == "succeeded" || status == "failed" {
					return nil
				}
				return errors.Newf("job is not in the succeeded or failed state: %q", status)
			})
			require.Equal(t, tc.expectedJobStatus, status)
			if tc.expectedJobStatus == "succeeded" {
				require.InEpsilon(t, 1.0, fractionCompleted, 1e-9, "expected fraction_completed ≈ 1.0")
			}
		})
	}
}
