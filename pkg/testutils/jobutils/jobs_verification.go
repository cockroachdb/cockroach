// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobutils

import (
	"context"
	gosql "database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/lib/pq"
)

// WaitForJob waits for the specified job ID to terminate.
func WaitForJob(t testing.TB, db *sqlutils.SQLRunner, jobID jobspb.JobID) {
	t.Helper()
	if err := retry.ForDuration(time.Minute*2, func() error {
		var status string
		var payloadBytes []byte
		db.QueryRow(
			t, `SELECT status, payload FROM system.jobs WHERE id = $1`, jobID,
		).Scan(&status, &payloadBytes)
		if jobs.Status(status) == jobs.StatusFailed {
			payload := &jobspb.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err == nil {
				t.Fatalf("job failed: %s", payload.Error)
			}
			t.Fatalf("job failed")
		}
		if e, a := jobs.StatusSucceeded, jobs.Status(status); e != a {
			return errors.Errorf("expected job status %s, but got %s", e, a)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// RunJob runs the provided job control statement, intializing, notifying and
// closing the chan at the passed pointer (see below for why) and returning the
// jobID and error result. PAUSE JOB and CANCEL JOB are racy in that it's hard
// to guarantee that the job is still running when executing a PAUSE or
// CANCEL--or that the job has even started running. To synchronize, we can
// install a store response filter which does a blocking receive for one of the
// responses used by our job (for example, Export for a BACKUP). Later, when we
// want to guarantee the job is in progress, we do exactly one blocking send.
// When this send completes, we know the job has started, as we've seen one
// expected response. We also know the job has not finished, because we're
// blocking all future responses until we close the channel, and our operation
// is large enough that it will generate more than one of the expected response.
func RunJob(
	t *testing.T,
	db *sqlutils.SQLRunner,
	allowProgressIota *chan struct{},
	ops []string,
	query string,
	args ...interface{},
) (jobspb.JobID, error) {
	*allowProgressIota = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := db.DB.ExecContext(context.TODO(), query, args...)
		errCh <- err
	}()
	select {
	case *allowProgressIota <- struct{}{}:
	case err := <-errCh:
		return 0, errors.Wrapf(err, "query returned before expected: %s", query)
	}
	var jobID jobspb.JobID
	db.QueryRow(t, `SELECT id FROM system.jobs ORDER BY created DESC LIMIT 1`).Scan(&jobID)
	for _, op := range ops {
		db.Exec(t, fmt.Sprintf("%s JOB %d", op, jobID))
		*allowProgressIota <- struct{}{}
	}
	close(*allowProgressIota)
	return jobID, <-errCh
}

// BulkOpResponseFilter creates a blocking response filter for the responses
// related to bulk IO/backup/restore/import: Export, Import and AddSSTable. See
// discussion on RunJob for where this might be useful.
func BulkOpResponseFilter(allowProgressIota *chan struct{}) kvserverbase.ReplicaResponseFilter {
	return func(_ context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
		for _, ru := range br.Responses {
			switch ru.GetInner().(type) {
			case *roachpb.ExportResponse, *roachpb.AddSSTableResponse:
				<-*allowProgressIota
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
	actual.Username = security.MakeSQLUsernameFromPreNormalizedString(usernameString)
	if runningStatus.Valid {
		runningStatusString = runningStatus.String
	}

	for _, id := range rawDescriptorIDs {
		actual.DescriptorIDs = append(actual.DescriptorIDs, descpb.ID(id))
	}
	sort.Sort(actual.DescriptorIDs)
	sort.Sort(expected.DescriptorIDs)
	expected.Details = nil
	if e, a := expected, actual; !reflect.DeepEqual(e, a) {
		return errors.Errorf("job %d did not match:\n%s",
			offset, strings.Join(pretty.Diff(e, a), "\n"))
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
	db.QueryRow(t, `
	SELECT job_id FROM crdb_internal.jobs ORDER BY created LIMIT 1 OFFSET $1`, offset,
	).Scan(&jobID)
	return jobID
}

// GetLastJobID gets the most recent job's ID.
func GetLastJobID(t testing.TB, db *sqlutils.SQLRunner) jobspb.JobID {
	var jobID jobspb.JobID
	db.QueryRow(
		t, `SELECT id FROM system.jobs ORDER BY created DESC LIMIT 1`,
	).Scan(&jobID)
	return jobID
}

// GetJobProgress loads the Progress message associated with the job.
func GetJobProgress(t *testing.T, db *sqlutils.SQLRunner, jobID jobspb.JobID) *jobspb.Progress {
	ret := &jobspb.Progress{}
	var buf []byte
	db.QueryRow(t, `SELECT progress FROM system.jobs WHERE id = $1`, jobID).Scan(&buf)
	if err := protoutil.Unmarshal(buf, ret); err != nil {
		t.Fatal(err)
	}
	return ret
}
