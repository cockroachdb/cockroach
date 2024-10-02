// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobutils

import (
	"context"
	gosql "database/sql"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

// WaitForJobToSucceed waits for the specified job ID to succeed.
func WaitForJobToSucceed(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StatusSucceeded)
}

// WaitForJobToPause waits for the specified job ID to be paused.
func WaitForJobToPause(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StatusPaused)
}

// WaitForJobToCancel waits for the specified job ID to be in a cancelled state.
func WaitForJobToCancel(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StatusCanceled)
}

// WaitForJobToRun waits for the specified job ID to be in a running state.
func WaitForJobToRun(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StatusRunning)
}

// WaitForJobToFail waits for the specified job ID to be in a failed state.
func WaitForJobToFail(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StatusFailed)
}

// WaitForJobReverting waits for the specified job ID to be in a reverting state.
func WaitForJobReverting(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StatusReverting)
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
	t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID, expectedStatus jobs.Status,
) {
	t.Helper()
	testutils.SucceedsWithin(t, func() error {
		var status string
		db.QueryRow(t, "SELECT status FROM system.jobs WHERE id = $1", jobID).Scan(&status)
		if jobs.Status(status) == jobs.StatusFailed {
			if expectedStatus == jobs.StatusFailed {
				return nil
			}
			payload := GetJobPayload(t, db, jobID)
			t.Fatalf("job failed: %s", payload.Error)
		}
		if e, a := expectedStatus, jobs.Status(status); e != a {
			return errors.Errorf("expected job status %s, but got %s", e, a)
		}
		return nil
	}, 2*time.Minute)
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

type logT struct{ testing.TB }

func (n logT) Errorf(format string, args ...interface{}) { n.Logf(format, args...) }
func (n logT) FailNow()                                  {}

func verifySystemJob(
	t testing.TB,
	db *sqlutils.SQLRunner,
	offset int,
	filterType jobspb.Type,
	expectedStatus string,
	expectedRunningStatus string,
	expected jobs.Record,
) error {
	var actual jobs.Record
	var rawDescriptorIDs pq.Int64Array
	var statusString string
	var runningStatus gosql.NullString
	var runningStatusString string
	var usernameString string
	// We have to query for the nth job created rather than filtering by ID,
	// because job-generating SQL queries (e.g. BACKUP) do not currently return
	// the job ID.
	db.QueryRow(t, `
		SELECT description, user_name, descriptor_ids, status, running_status
		FROM crdb_internal.jobs WHERE job_type = $1 ORDER BY created LIMIT 1 OFFSET $2`,
		filterType.String(),
		offset,
	).Scan(
		&actual.Description, &usernameString, &rawDescriptorIDs,
		&statusString, &runningStatus,
	)
	actual.Username = username.MakeSQLUsernameFromPreNormalizedString(usernameString)
	if runningStatus.Valid {
		runningStatusString = runningStatus.String
	}

	if len(expected.DescriptorIDs) > 0 {
		for _, id := range rawDescriptorIDs {
			actual.DescriptorIDs = append(actual.DescriptorIDs, descpb.ID(id))
		}
		sort.Sort(actual.DescriptorIDs)
		sort.Sort(expected.DescriptorIDs)
	}
	expected.Details = nil
	if e, a := expected, actual; !assert.Equal(logT{t}, e, a) {
		return errors.Errorf("job %d did not match:\n%s",
			offset, sqlutils.MatrixToStr(db.QueryStr(t, "SELECT * FROM crdb_internal.jobs")))
	}
	if expectedStatus != statusString {
		return errors.Errorf("job %d: expected status %v, got %v", offset, expectedStatus, statusString)
	}
	if expectedRunningStatus != "" && expectedRunningStatus != runningStatusString {
		return errors.Errorf("job %d: expected running status %v, got %v",
			offset, expectedRunningStatus, runningStatusString)
	}

	return nil
}

// VerifyRunningSystemJob checks that job records are created as expected
// and is marked as running.
func VerifyRunningSystemJob(
	t testing.TB,
	db *sqlutils.SQLRunner,
	offset int,
	filterType jobspb.Type,
	expectedRunningStatus jobs.RunningStatus,
	expected jobs.Record,
) error {
	return verifySystemJob(t, db, offset, filterType, "running", string(expectedRunningStatus), expected)
}

// VerifySystemJob checks that job records are created as expected.
func VerifySystemJob(
	t testing.TB,
	db *sqlutils.SQLRunner,
	offset int,
	filterType jobspb.Type,
	expectedStatus jobs.Status,
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
