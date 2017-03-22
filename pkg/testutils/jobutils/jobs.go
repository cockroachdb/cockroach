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
//
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

package jobutils

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/lib/pq"
)

// JobExpectation defines the information necessary to determine the validity of
// a job in the system.jobs table. Exposed for testing only.
type JobExpectation struct {
	Offset int
	Job    sql.JobRecord
	Type   string
	Before time.Time
	Error  string
}

// VerifyJobRecord verifies that the JobExpectation matches the job record
// stored in the system.jobs table. Exposed for testing only.
func VerifyJobRecord(
	t *testing.T, db *sqlutils.SQLRunner, expectedStatus sql.JobStatus, expected JobExpectation,
) {
	var typ string
	var description string
	var username string
	var descriptorArray pq.Int64Array
	var statusString string
	var created pq.NullTime
	var started pq.NullTime
	var finished pq.NullTime
	var modified pq.NullTime
	var err string
	// We have to query for the nth job created rather than filtering by ID,
	// because job-generating SQL queries (e.g. BACKUP) do not currently return
	// the job ID.
	db.QueryRow(`
		SELECT type, description, username, descriptor_ids, status,
				   created, started, finished, modified, error
		FROM crdb_internal.jobs ORDER BY created LIMIT 1 OFFSET $1`,
		expected.Offset,
	).Scan(
		&typ, &description, &username, &descriptorArray, &statusString,
		&created, &started, &finished, &modified, &err,
	)

	status := sql.JobStatus(statusString)
	if e, a := expectedStatus, status; e != a {
		t.Errorf("job %d: expected status %v, got %v", expected.Offset, e, a)
		return
	}
	if e, a := expected.Type, typ; e != a {
		t.Errorf("job %d: expected type %v, got type %v", expected.Offset, e, a)
	}
	if e, a := expected.Job.Description, description; e != a {
		t.Errorf("job %d: expected description %v, got %v", expected.Offset, e, a)
	}
	if e, a := expected.Job.Username, username; e != a {
		t.Errorf("job %d: expected user %v, got %v", expected.Offset, e, a)
	}

	descriptors := make([]int, len(descriptorArray))
	for _, id := range descriptorArray {
		descriptors = append(descriptors, int(id))
	}
	expectedDescriptors := make([]int, len(expected.Job.DescriptorIDs))
	for _, id := range expected.Job.DescriptorIDs {
		expectedDescriptors = append(expectedDescriptors, int(id))
	}
	sort.Ints(expectedDescriptors)
	sort.Ints(descriptors)
	if e, a := expectedDescriptors, descriptors; !reflect.DeepEqual(e, a) {
		t.Errorf("job %d: expected descriptors %v, got %v", expected.Offset, e, a)
	}

	verifyModifiedAgainst := func(name string, time time.Time) {
		if modified.Time.Before(time) {
			t.Errorf("job %d: modified time %v before %s time %v", expected.Offset, modified, name, time)
		}
		if now := timeutil.Now(); modified.Time.After(now) {
			t.Errorf("job %d: modified time %v after current time %v", expected.Offset, modified, now)
		}
	}

	if expected.Before.After(created.Time) {
		t.Errorf(
			"job %d: created time %v is before expected created time %v",
			expected.Offset, created, expected.Before,
		)
	}
	if status == sql.JobStatusPending {
		verifyModifiedAgainst("created", created.Time)
		return
	}

	if !started.Valid && status == sql.JobStatusSucceeded {
		t.Errorf("job %d: started time is NULL but job claims to be successful", expected.Offset)
	}
	if started.Valid && created.Time.After(started.Time) {
		t.Errorf("job %d: created time %v is after started time %v", expected.Offset, created, started)
	}
	if status == sql.JobStatusRunning {
		verifyModifiedAgainst("started", started.Time)
		return
	}

	if started.Time.After(finished.Time) {
		t.Errorf("job %d: started time %v is after finished time %v", expected.Offset, started, finished)
	}
	verifyModifiedAgainst("finished", finished.Time)
	if e, a := expected.Error, err; e != a {
		t.Errorf("job %d: expected error %v, got %v", expected.Offset, e, a)
	}
}
