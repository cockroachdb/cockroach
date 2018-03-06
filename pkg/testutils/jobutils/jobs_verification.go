// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package jobutils

import (
	gosql "database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/kr/pretty"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// WaitForJob waits for the specified job ID to terminate.
func WaitForJob(db *gosql.DB, jobID int64) error {
	var jobFailedErr error
	err := retry.ForDuration(time.Minute*2, func() error {
		var status string
		var payloadBytes []byte
		if err := db.QueryRow(
			`SELECT status, payload FROM system.jobs WHERE id = $1`, jobID,
		).Scan(&status, &payloadBytes); err != nil {
			return err
		}
		if jobs.Status(status) == jobs.StatusFailed {
			jobFailedErr = errors.New("job failed")
			payload := &jobs.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err == nil {
				jobFailedErr = errors.Errorf("job failed: %s", payload.Error)
			}
			return nil
		}
		if e, a := jobs.StatusSucceeded, jobs.Status(status); e != a {
			return errors.Errorf("expected job status %s, but got %s", e, a)
		}
		return nil
	})
	if jobFailedErr != nil {
		return jobFailedErr
	}
	return err
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
	op, query string,
	args ...interface{},
) (int64, error) {
	*allowProgressIota = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := db.DB.Exec(query, args...)
		errCh <- err
	}()
	select {
	case *allowProgressIota <- struct{}{}:
	case err := <-errCh:
		return 0, errors.Wrapf(err, "query returned before expected: %s", query)
	}
	var jobID int64
	db.QueryRow(t, `SELECT id FROM system.jobs ORDER BY created DESC LIMIT 1`).Scan(&jobID)
	db.Exec(t, fmt.Sprintf("%s JOB %d", op, jobID))
	close(*allowProgressIota)
	return jobID, <-errCh
}

// BulkOpResponseFilter creates a blocking response filter for the responses
// related to bulk IO/backup/restore/import: Export, Import and AddSSTable. See
// discussion on RunJob for where this might be useful.
func BulkOpResponseFilter(allowProgressIota *chan struct{}) storagebase.ReplicaResponseFilter {
	return func(ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
		for _, res := range br.Responses {
			if res.Export != nil || res.Import != nil || res.AddSstable != nil {
				<-*allowProgressIota
			}
		}
		return nil
	}
}

// GetSystemJobsCount queries the number of entries in the jobs table.
func GetSystemJobsCount(t testing.TB, db *sqlutils.SQLRunner) int {
	var jobCount int
	db.QueryRow(t, `SELECT COUNT(*) FROM crdb_internal.jobs`).Scan(&jobCount)
	return jobCount
}

// VerifySystemJob checks that that job records are created as expected.
func VerifySystemJob(
	t testing.TB, db *sqlutils.SQLRunner, offset int, expectedType jobs.Type, expected jobs.Record,
) error {
	var actual jobs.Record
	var rawDescriptorIDs pq.Int64Array
	var actualType string
	var statusString string
	// We have to query for the nth job created rather than filtering by ID,
	// because job-generating SQL queries (e.g. BACKUP) do not currently return
	// the job ID.
	db.QueryRow(t, `
		SELECT type, description, username, descriptor_ids, status
		FROM crdb_internal.jobs ORDER BY created LIMIT 1 OFFSET $1`,
		offset,
	).Scan(
		&actualType, &actual.Description, &actual.Username, &rawDescriptorIDs,
		&statusString,
	)

	for _, id := range rawDescriptorIDs {
		actual.DescriptorIDs = append(actual.DescriptorIDs, sqlbase.ID(id))
	}
	sort.Sort(actual.DescriptorIDs)
	sort.Sort(expected.DescriptorIDs)
	expected.Details = nil
	if e, a := expected, actual; !reflect.DeepEqual(e, a) {
		return errors.Errorf("job %d did not match:\n%s",
			offset, strings.Join(pretty.Diff(e, a), "\n"))
	}

	if e, a := jobs.StatusSucceeded, jobs.Status(statusString); e != a {
		return errors.Errorf("job %d: expected status %v, got %v", offset, e, a)
	}
	if e, a := expectedType.String(), actualType; e != a {
		return errors.Errorf("job %d: expected type %v, got type %v", offset, e, a)
	}

	return nil
}
