// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobutils

import (
	"context"
	gosql "database/sql"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// WaitForJobToSucceed waits for the specified job ID to succeed.
func WaitForJobToSucceed(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StateSucceeded)
}

// WaitForJobToPause waits for the specified job ID to be paused.
func WaitForJobToPause(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StatePaused)
}

// WaitForJobToCancel waits for the specified job ID to be in a cancelled state.
func WaitForJobToCancel(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StateCanceled)
}

// WaitForJobToRun waits for the specified job ID to be in a running state.
func WaitForJobToRun(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StateRunning)
}

// WaitForJobToFail waits for the specified job ID to be in a failed state.
func WaitForJobToFail(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StateFailed)
}

// WaitForJobReverting waits for the specified job ID to be in a reverting state.
func WaitForJobReverting(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StateReverting)
}

const (
	// InternalSystemJobsBaseQuery runs the query against an empty database string.
	// Since crdb_internal.system_jobs is a virtual table, by default, the query
	// will take a lease on the current database the SQL session is connected to. If
	// this database has been dropped or is unavailable then the query on the
	// virtual table will fail. The "" prefix prevents this lease acquisition.
	//
	// NB: Until the crdb_internal.system_jobs is turned into a
	// view structured such that unnecessary joins can be elided,
	// some users may prefer JobPayloadByIDQuery or
	// JobPayloadByIDQuery
	InternalSystemJobsBaseQuery = `SELECT status, payload, progress FROM "".crdb_internal.system_jobs WHERE id = $1`
	JobProgressByIDQuery        = "SELECT value FROM system.job_info WHERE job_id = $1 AND info_key::string = 'legacy_progress' ORDER BY written DESC LIMIT 1"
	JobPayloadByIDQuery         = "SELECT value FROM system.job_info WHERE job_id = $1 AND info_key::string = 'legacy_payload' ORDER BY written DESC LIMIT 1"
)

func waitForJobToHaveStatus(
	t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID, expectedStatus jobs.State,
) {
	t.Helper()
	const duration = 2 * time.Minute
	err := retry.ForDuration(duration, func() error {
		var status string
		db.QueryRow(t, "SELECT status FROM system.jobs WHERE id = $1", jobID).Scan(&status)
		if jobs.State(status) == jobs.StateFailed {
			if expectedStatus == jobs.StateFailed {
				return nil
			}
			payload := GetJobPayload(t, db, jobID)
			t.Fatalf("job failed: %s", payload.Error)
		}
		if e, a := expectedStatus, jobs.State(status); e != a {
			return errors.Errorf("expected job status %s, but got %s", e, a)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("condition failed to evaluate within %s: %s", duration, err)
	}
}

// BulkOpResponseFilter creates a blocking response filter for the responses
// related to bulk IO/backup/restore/import: Export, Import and AddSSTable. See
// discussion on RunJob for where this might be useful.
func BulkOpResponseFilter(allowProgressIota *chan struct{}) kvserverbase.ReplicaResponseFilter {
	return func(ctx context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse) *kvpb.Error {
		for _, ru := range br.Responses {
			switch ru.GetInner().(type) {
			case *kvpb.ExportResponse, *kvpb.AddSSTableResponse:
				select {
				case <-*allowProgressIota:
				case <-ctx.Done():
				}
			}
		}
		return nil
	}
}

func verifySystemJob(
	t testing.TB,
	db *sqlutils.SQLRunner,
	offset int,
	filterType jobspb.Type,
	expectedState string,
	expectedStatus string,
	expected jobs.Record,
) error {
	var actual jobs.Record
	var stateString string
	var status gosql.NullString
	var statusString string
	var usernameString string
	// We have to query for the nth job created rather than filtering by ID,
	// because job-generating SQL queries (e.g. BACKUP) do not currently return
	// the job ID.
	db.QueryRow(t, `
		SELECT description, user_name, status, running_status
		FROM crdb_internal.jobs WHERE job_type = $1 ORDER BY created LIMIT 1 OFFSET $2`,
		filterType.String(),
		offset,
	).Scan(
		&actual.Description, &usernameString,
		&stateString, &status,
	)
	actual.Username = username.MakeSQLUsernameFromPreNormalizedString(usernameString)
	if status.Valid {
		statusString = status.String
	}

	expected.DescriptorIDs = nil
	expected.Details = nil
	require.Equal(t, expected.Description, actual.Description)
	require.Equal(t, expected.Username, actual.Username)
	if expectedState != stateString {
		return errors.Errorf("job %d: expected state %v, got %v", offset, expectedState, stateString)
	}
	if expectedStatus != "" && expectedStatus != statusString {
		return errors.Errorf("job %d: expected status %v, got %v",
			offset, expectedStatus, statusString)
	}

	return nil
}

// VerifyRunningSystemJob checks that job description, user, and running status
// are created as expected and is marked as running.
func VerifyRunningSystemJob(
	t testing.TB,
	db *sqlutils.SQLRunner,
	offset int,
	filterType jobspb.Type,
	expectedStatus jobs.StatusMessage,
	expected jobs.Record,
) error {
	return verifySystemJob(t, db, offset, filterType, "running", string(expectedStatus), expected)
}

// VerifySystemJob checks that job description, user, and running status are
// created as expected.
func VerifySystemJob(
	t testing.TB,
	db *sqlutils.SQLRunner,
	offset int,
	filterType jobspb.Type,
	expectedStatus jobs.State,
	expected jobs.Record,
) error {
	return verifySystemJob(t, db, offset, filterType, string(expectedStatus), "", expected)
}

// GetJobID gets a particular job's ID.
func GetJobID(t testing.TB, db *sqlutils.SQLRunner, offset int) jobspb.JobID {
	var jobID jobspb.JobID
	db.QueryRow(t,
		"SELECT id FROM system.jobs ORDER BY created LIMIT 1 OFFSET $1", offset,
	).Scan(&jobID)
	return jobID
}

// GetJobProgress loads the Progress message associated with the job.
func GetJobProgress(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) *jobspb.Progress {
	ret := &jobspb.Progress{}
	var buf []byte
	db.QueryRow(t, JobProgressByIDQuery, jobID).Scan(&buf)
	if err := protoutil.Unmarshal(buf, ret); err != nil {
		t.Fatal(err)
	}
	return ret
}

// GetJobPayload loads the Payload message associated with the job.
func GetJobPayload(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) *jobspb.Payload {
	ret := &jobspb.Payload{}
	var buf []byte
	db.QueryRow(t, JobPayloadByIDQuery, jobID).Scan(&buf)
	if err := protoutil.Unmarshal(buf, ret); err != nil {
		t.Fatal(err)
	}
	return ret
}
