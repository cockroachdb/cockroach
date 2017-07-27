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

package jobs_test

import (
	gosql "database/sql"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
)

// expectation defines the information necessary to determine the validity of
// a job in the system.jobs table.
type expectation struct {
	DB                *gosql.DB
	Record            jobs.Record
	Type              jobs.Type
	Before            time.Time
	FractionCompleted float32
	Error             string
}

func (expected *expectation) verify(id *int64, expectedStatus jobs.Status) error {
	var statusString string
	var created time.Time
	var payloadBytes []byte
	if err := expected.DB.QueryRow(
		`SELECT status, created, payload FROM system.jobs WHERE id = $1`, id,
	).Scan(
		&statusString, &created, &payloadBytes,
	); err != nil {
		return err
	}

	var payload jobs.Payload
	if err := payload.Unmarshal(payloadBytes); err != nil {
		return err
	}

	// Verify the upstream-provided fields.
	details, err := payload.UnwrapDetails()
	if err != nil {
		return err
	}
	if e, a := expected.Record, (jobs.Record{
		Description:   payload.Description,
		Details:       details,
		DescriptorIDs: payload.DescriptorIDs,
		Username:      payload.Username,
	}); !reflect.DeepEqual(e, a) {
		diff := strings.Join(pretty.Diff(e, a), "\n")
		return errors.Errorf("Records do not match:\n%s", diff)
	}

	// Verify internally-managed fields.
	status := jobs.Status(statusString)
	if e, a := expectedStatus, status; e != a {
		return errors.Errorf("expected status %v, got %v", e, a)
	}
	if e, a := expected.Type, payload.Type(); e != a {
		return errors.Errorf("expected type %v, got type %v", e, a)
	}
	if e, a := expected.FractionCompleted, payload.FractionCompleted; e != a {
		return errors.Errorf("expected fraction completed %f, got %f", e, a)
	}

	// Check internally-managed timestamps for sanity.
	started := timeutil.FromUnixMicros(payload.StartedMicros)
	modified := timeutil.FromUnixMicros(payload.ModifiedMicros)
	finished := timeutil.FromUnixMicros(payload.FinishedMicros)

	verifyModifiedAgainst := func(name string, ts time.Time) error {
		if modified.Before(ts) {
			return errors.Errorf("modified time %v before %s time %v", modified, name, ts)
		}
		if now := timeutil.Now().Round(time.Microsecond); modified.After(now) {
			return errors.Errorf("modified time %v after current time %v", modified, now)
		}
		return nil
	}

	if expected.Before.After(created) {
		return errors.Errorf(
			"created time %v is before expected created time %v",
			created, expected.Before,
		)
	}
	if status == jobs.StatusPending {
		return verifyModifiedAgainst("created", created)
	}

	if started == timeutil.UnixEpoch && status == jobs.StatusSucceeded {
		return errors.Errorf("started time is empty but job claims to be successful")
	}
	if started != timeutil.UnixEpoch && created.After(started) {
		return errors.Errorf("created time %v is after started time %v", created, started)
	}
	if status == jobs.StatusRunning {
		return verifyModifiedAgainst("started", started)
	}

	if started.After(finished) {
		return errors.Errorf("started time %v is after finished time %v", started, finished)
	}
	if e, a := expected.Error, payload.Error; e != a {
		return errors.Errorf("expected error %v, got %v", e, a)
	}
	return verifyModifiedAgainst("finished", finished)
}

func TestJobLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	registry := s.JobRegistry().(*jobs.Registry)

	t.Run("valid job lifecycles succeed", func(t *testing.T) {
		// Woody is a successful job.
		woodyRecord := jobs.Record{
			Description:   "There's a snake in my boot!",
			Username:      "Woody Pride",
			DescriptorIDs: []sqlbase.ID{1, 2, 3},
			Details:       jobs.RestoreDetails{},
		}
		woodyExpectation := expectation{
			DB:     sqlDB,
			Record: woodyRecord,
			Type:   jobs.TypeRestore,
			Before: timeutil.Now(),
		}
		woodyJob := registry.NewJob(woodyRecord)

		if err := woodyJob.Created(ctx, jobs.WithoutCancel); err != nil {
			t.Fatal(err)
		}
		if err := woodyExpectation.verify(woodyJob.ID(), jobs.StatusPending); err != nil {
			t.Fatal(err)
		}

		if err := woodyJob.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := woodyExpectation.verify(woodyJob.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		// This fraction completed progression tests that calling Progressed with a
		// fractionCompleted that is less than the last-recorded fractionCompleted
		// is silently ignored.
		progresses := []struct {
			actual   float32
			expected float32
		}{
			{0.0, 0.0}, {0.5, 0.5}, {0.5, 0.5}, {0.4, 0.5}, {0.8, 0.8}, {1.0, 1.0},
		}
		for _, f := range progresses {
			if err := woodyJob.Progressed(ctx, f.actual, jobs.Noop); err != nil {
				t.Fatal(err)
			}
			woodyExpectation.FractionCompleted = f.expected
			if err := woodyExpectation.verify(woodyJob.ID(), jobs.StatusRunning); err != nil {
				t.Fatal(err)
			}
		}

		// Test Progressed callbacks.
		if err := woodyJob.Progressed(ctx, 1.0, func(_ context.Context, details interface{}) {
			details.(*jobs.Payload_Restore).Restore.LowWaterMark = roachpb.Key("mariana")
		}); err != nil {
			t.Fatal(err)
		}
		woodyExpectation.Record.Details = jobs.RestoreDetails{LowWaterMark: roachpb.Key("mariana")}
		if err := woodyExpectation.verify(woodyJob.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		if err := woodyJob.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := woodyExpectation.verify(woodyJob.ID(), jobs.StatusSucceeded); err != nil {
			t.Fatal(err)
		}

		// Buzz fails after it starts running.
		buzzRecord := jobs.Record{
			Description:   "To infinity and beyond!",
			Username:      "Buzz Lightyear",
			DescriptorIDs: []sqlbase.ID{3, 2, 1},
		}
		buzzExpectation := expectation{
			DB:     sqlDB,
			Record: buzzRecord,
			Type:   jobs.TypeBackup,
			Before: timeutil.Now(),
			Error:  "Buzz Lightyear can't fly",
		}
		buzzJob := registry.NewJob(buzzRecord)

		// Test modifying the job details before calling `Created`.
		buzzJob.Record.Details = jobs.BackupDetails{}
		buzzExpectation.Record.Details = jobs.BackupDetails{}
		if err := buzzJob.Created(ctx, jobs.WithoutCancel); err != nil {
			t.Fatal(err)
		}
		if err := buzzExpectation.verify(buzzJob.ID(), jobs.StatusPending); err != nil {
			t.Fatal(err)
		}

		if err := buzzJob.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := buzzExpectation.verify(buzzJob.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		if err := buzzJob.Progressed(ctx, .42, jobs.Noop); err != nil {
			t.Fatal(err)
		}
		buzzExpectation.FractionCompleted = .42
		if err := buzzExpectation.verify(buzzJob.ID(), jobs.StatusRunning); err != nil {
			t.Fatal(err)
		}

		buzzJob.Failed(ctx, errors.New("Buzz Lightyear can't fly"))
		if err := buzzExpectation.verify(buzzJob.ID(), jobs.StatusFailed); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Buzz didn't corrupt Woody.
		if err := woodyExpectation.verify(woodyJob.ID(), jobs.StatusSucceeded); err != nil {
			t.Fatal(err)
		}

		// Sid fails before it starts running.
		sidRecord := jobs.Record{
			Description:   "The toys! The toys are alive!",
			Username:      "Sid Phillips",
			DescriptorIDs: []sqlbase.ID{6, 6, 6},
			Details:       jobs.RestoreDetails{},
		}
		sidExpectation := expectation{
			DB:     sqlDB,
			Record: sidRecord,
			Type:   jobs.TypeRestore,
			Before: timeutil.Now(),
			Error:  "Sid is a total failure",
		}
		sidJob := registry.NewJob(sidRecord)

		if err := sidJob.Created(ctx, jobs.WithoutCancel); err != nil {
			t.Fatal(err)
		}
		if err := sidExpectation.verify(sidJob.ID(), jobs.StatusPending); err != nil {
			t.Fatal(err)
		}

		sidJob.Failed(ctx, errors.New("Sid is a total failure"))
		if err := sidExpectation.verify(sidJob.ID(), jobs.StatusFailed); err != nil {
			t.Fatal(err)
		}

		// Ensure that logging Sid didn't corrupt Woody or Buzz.
		if err := woodyExpectation.verify(woodyJob.ID(), jobs.StatusSucceeded); err != nil {
			t.Fatal(err)
		}
		if err := buzzExpectation.verify(buzzJob.ID(), jobs.StatusFailed); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("bad job details fail", func(t *testing.T) {
		defer func() {
			if r, ok := recover().(string); !ok || !strings.Contains(r, "unknown details type int") {
				t.Fatalf("expected 'unknown details type int', but got: %v", r)
			}
		}()

		job := registry.NewJob(jobs.Record{
			Details: 42,
		})
		_ = job.Created(ctx, jobs.WithoutCancel)
	})

	t.Run("update before create fails", func(t *testing.T) {
		job := registry.NewJob(jobs.Record{})
		if err := job.Started(ctx); !testutils.IsError(err, "job not created") {
			t.Fatalf("expected 'job not created' error, but got %v", err)
		}
	})

	t.Run("same state transition twice succeeds silently", func(t *testing.T) {
		job := registry.NewJob(jobs.Record{
			Details: jobs.BackupDetails{},
		})
		if err := job.Created(ctx, jobs.WithoutCancel); err != nil {
			t.Fatal(err)
		}
		if err := job.Created(ctx, jobs.WithoutCancel); err != nil {
			t.Fatal(err)
		}
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("out of bounds progress fails", func(t *testing.T) {
		job := registry.NewJob(jobs.Record{
			Details: jobs.BackupDetails{},
		})
		if err := job.Created(ctx, jobs.WithoutCancel); err != nil {
			t.Fatal(err)
		}
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Progressed(ctx, -0.1, jobs.Noop); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
		if err := job.Progressed(ctx, 1.1, jobs.Noop); !testutils.IsError(err, "outside allowable range") {
			t.Fatalf("expected 'outside allowable range' error, but got %v", err)
		}
	})

	t.Run("progress on non-started job fails", func(t *testing.T) {
		job := registry.NewJob(jobs.Record{
			Details: jobs.BackupDetails{},
		})
		if err := job.Created(ctx, jobs.WithoutCancel); err != nil {
			t.Fatal(err)
		}
		if err := job.Progressed(ctx, 0.5, jobs.Noop); !testutils.IsError(err, `job \d+ not started`) {
			t.Fatalf("expected 'job not started' error, but got %v", err)
		}
	})

	t.Run("progress on finished job fails", func(t *testing.T) {
		job := registry.NewJob(jobs.Record{
			Details: jobs.BackupDetails{},
		})
		if err := job.Created(ctx, jobs.WithoutCancel); err != nil {
			t.Fatal(err)
		}
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Progressed(ctx, 0.5, jobs.Noop); !testutils.IsError(err, `job \d+ already finished`) {
			t.Fatalf("expected 'job already finished' error, but got %v", err)
		}
	})

	t.Run("succeeded forces fraction completed to 1.0", func(t *testing.T) {
		record := jobs.Record{Details: jobs.BackupDetails{}}
		expect := expectation{
			DB:                sqlDB,
			Record:            record,
			Type:              jobs.TypeBackup,
			Before:            timeutil.Now(),
			FractionCompleted: 1.0,
		}
		job := registry.NewJob(record)
		if err := job.Created(ctx, jobs.WithoutCancel); err != nil {
			t.Fatal(err)
		}
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Progressed(ctx, 0.2, jobs.Noop); err != nil {
			t.Fatal(err)
		}
		if err := job.Succeeded(ctx); err != nil {
			t.Fatal(err)
		}
		if err := expect.verify(job.ID(), jobs.StatusSucceeded); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("set details works", func(t *testing.T) {
		record := jobs.Record{Details: jobs.RestoreDetails{}}
		expect := expectation{
			DB:     sqlDB,
			Record: record,
			Type:   jobs.TypeRestore,
			Before: timeutil.Now(),
		}
		job := registry.NewJob(record)
		if err := job.Created(ctx, jobs.WithoutCancel); err != nil {
			t.Fatal(err)
		}
		if err := expect.verify(job.ID(), jobs.StatusPending); err != nil {
			t.Fatal(err)
		}
		newDetails := jobs.RestoreDetails{LowWaterMark: []byte{42}}
		expect.Record.Details = newDetails
		if err := job.SetDetails(ctx, newDetails); err != nil {
			t.Fatal(err)
		}
		if err := expect.verify(job.ID(), jobs.StatusPending); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRoundtripJob(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	registry := s.JobRegistry().(*jobs.Registry)
	defer s.Stopper().Stop(ctx)

	storedJob := registry.NewJob(jobs.Record{
		Description:   "beep boop",
		Username:      "robot",
		DescriptorIDs: sqlbase.IDs{42},
		Details:       jobs.RestoreDetails{},
	})
	if err := storedJob.Created(ctx, jobs.WithoutCancel); err != nil {
		t.Fatal(err)
	}
	retrievedJob, err := registry.LoadJob(ctx, *storedJob.ID())
	if err != nil {
		t.Fatal(err)
	}
	if e, a := storedJob, retrievedJob; !reflect.DeepEqual(e, a) {
		diff := strings.Join(pretty.Diff(e, a), "\n")
		t.Fatalf("stored job did not match retrieved job:\n%s", diff)
	}
}

func TestRegistryResume(t *testing.T) {
	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	db := s.KVClient().(*client.DB)
	ex := sql.InternalExecutor{LeaseManager: s.LeaseManager().(*sql.LeaseManager)}
	gossip := s.Gossip()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	nodeID := &base.NodeIDContainer{}

	registry := jobs.MakeRegistry(clock, db, ex, gossip, nodeID, jobs.DummyClusterID)
	nodeLiveness := jobs.NewMockNodeLiveness(clock, 4)

	const cancelInterval = time.Duration(math.MaxInt64)
	const adoptInterval = time.Nanosecond
	if err := registry.Start(s.Stopper(), nodeLiveness, cancelInterval, adoptInterval); err != nil {
		t.Fatal(err)
	}

	wait := func() {
		<-nodeLiveness.MapCh
		<-nodeLiveness.MapCh
	}

	jobMap := make(map[int64]roachpb.NodeID)
	for i := 0; i < 3; i++ {
		nodeID.Reset(roachpb.NodeID(i + 1))
		job := registry.NewJob(jobs.Record{Details: jobs.BackupDetails{}})
		if err := job.Created(ctx, func() {}); err != nil {
			t.Fatal(err)
		}
		if err := job.Started(ctx); err != nil {
			t.Fatal(err)
		}
		jobMap[*job.ID()] = nodeID.Get()
	}

	nodeID.Reset(4)

	hookCallCount := 0
	resumeCounts := make(map[roachpb.NodeID]int)
	var newJobs []*jobs.Job
	jobs.AddResumeHook(func(_ jobs.Type) func(context.Context, *jobs.Job) error {
		hookCallCount++
		return func(_ context.Context, job *jobs.Job) error {
			resumeCounts[jobMap[*job.ID()]]++
			newJobs = append(newJobs, job)
			return nil
		}
	})

	wait()
	if e, a := 0, hookCallCount; e != a {
		t.Fatalf("expected hookCallCount to be %d, but got %d", e, a)
	}

	wait()
	if e, a := 0, hookCallCount; e != a {
		t.Fatalf("expected hookCallCount to be %d, but got %d", e, a)
	}

	nodeLiveness.SetExpiration(1, hlc.MinTimestamp)
	wait()
	if hookCallCount == 0 {
		t.Fatalf("expected hookCallCount to be non-zero, but got %d", hookCallCount)
	}

	wait()
	if e, a := 1, resumeCounts[1]; e != a {
		t.Fatalf("expected resumeCount to be %d, but got %d", e, a)
	}

	nodeLiveness.IncrementEpoch(3)
	wait()
	if e, a := 1, resumeCounts[3]; e != a {
		t.Fatalf("expected resumeCount to be %d, but got %d", e, a)
	}

	if e, a := 0, resumeCounts[2]; e != a {
		t.Fatalf("expected resumeCount to be %d, but got %d", e, a)
	}

	for _, newJob := range newJobs {
		if e, a := roachpb.NodeID(4), newJob.Payload().Lease.NodeID; e != a {
			t.Errorf("expected job %d to have been adopted by node %d, but was adopted by node %d",
				*newJob.ID(), e, a)
		}
	}
}
