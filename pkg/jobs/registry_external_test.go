// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRoundtripJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	registry := s.JobRegistry().(*jobs.Registry)
	defer s.Stopper().Stop(ctx)

	jobID := registry.MakeJobID()
	storedJob := registry.NewJob(jobs.Record{
		Description:   "beep boop",
		Username:      security.MakeSQLUsernameFromPreNormalizedString("robot"),
		DescriptorIDs: descpb.IDs{42},
		Details:       jobspb.RestoreDetails{},
		Progress:      jobspb.RestoreProgress{},
	}, jobID)
	if err := storedJob.Created(ctx); err != nil {
		t.Fatal(err)
	}
	retrievedJob, err := registry.LoadJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if e, a := storedJob, retrievedJob; !reflect.DeepEqual(e, a) {
		//diff := strings.Join(pretty.Diff(e, a), "\n")
		t.Fatalf("stored job did not match retrieved job:\n%+v\n%+v", e, a)
	}
}

func TestRegistryResumeExpiredLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	ctx := context.Background()

	ver201 := cluster.MakeTestingClusterSettingsWithVersions(
		roachpb.Version{Major: 20, Minor: 1},
		roachpb.Version{Major: 20, Minor: 1},
		true)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{Settings: ver201})
	defer s.Stopper().Stop(ctx)

	// Disable leniency for instant expiration
	jobs.LeniencySetting.Override(&s.ClusterSettings().SV, 0)
	const cancelInterval = time.Duration(math.MaxInt64)
	const adoptInterval = time.Microsecond
	slinstance.DefaultTTL.Override(&s.ClusterSettings().SV, 2*adoptInterval)
	slinstance.DefaultHeartBeat.Override(&s.ClusterSettings().SV, adoptInterval)

	db := s.DB()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	nodeLiveness := jobs.NewFakeNodeLiveness(4)
	newRegistry := func(id roachpb.NodeID) *jobs.Registry {
		var c base.NodeIDContainer
		c.Set(ctx, id)
		idContainer := base.NewSQLIDContainer(0, &c)
		ac := log.AmbientContext{Tracer: tracing.NewTracer()}
		sqlStorage := slstorage.NewStorage(
			s.Stopper(), clock, db, keys.SystemSQLCodec, s.ClusterSettings(),
		)
		sqlInstance := slinstance.NewSQLInstance(s.Stopper(), clock, sqlStorage, s.ClusterSettings())
		r := jobs.MakeRegistry(
			ac, s.Stopper(), clock, optionalnodeliveness.MakeContainer(nodeLiveness), db,
			s.InternalExecutor().(sqlutil.InternalExecutor), idContainer, sqlInstance,
			s.ClusterSettings(), base.DefaultHistogramWindowInterval(), jobs.FakePHS, "",
			nil, /* knobs */
		)
		if err := r.Start(ctx, s.Stopper(), cancelInterval, adoptInterval); err != nil {
			t.Fatal(err)
		}
		return r
	}

	const jobCount = 3

	drainAdoptionLoop := func() {
		// Every turn of the registry's adoption loop will generate exactly one call
		// to nodeLiveness.GetLivenesses. Only after we've witnessed one call for
		// each job, plus one more call, can we be sure that all work has been
		// completed.
		//
		// Waiting for only jobCount calls to nodeLiveness.GetLivenesses is racy, as
		// we might perform our assertions just as the last turn of registry loop
		// observes our injected liveness failure, if any.
		for i := 0; i < jobCount+1; i++ {
			<-nodeLiveness.GetLivenessesCalledCh
		}
	}

	// jobMap maps node IDs to job IDs.
	jobMap := make(map[roachpb.NodeID]jobspb.JobID)
	hookCallCount := 0
	// resumeCounts maps jobs IDs to number of start/resumes.
	resumeCounts := make(map[jobspb.JobID]int)
	// done prevents jobs from finishing.
	done := make(chan struct{})
	// resumeCalled does a locked, blocking send when a job is started/resumed. A
	// receive on it will block until a job is running.
	resumeCalled := make(chan struct{})
	var lock syncutil.Mutex
	jobs.RegisterConstructor(jobspb.TypeBackup, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		lock.Lock()
		hookCallCount++
		lock.Unlock()
		return jobs.FakeResumer{
			OnResume: func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case resumeCalled <- struct{}{}:
				case <-done:
				}
				lock.Lock()
				resumeCounts[job.ID()]++
				lock.Unlock()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-done:
					return nil
				}
			},
		}
	})

	for i := 0; i < jobCount; i++ {
		nodeid := roachpb.NodeID(i + 1)
		rec := jobs.Record{
			Details:  jobspb.BackupDetails{},
			Progress: jobspb.BackupProgress{},
		}
		job, err := jobs.TestingCreateAndStartJob(ctx, newRegistry(nodeid), db, rec)
		if err != nil {
			t.Fatal(err)
		}
		// Wait until the job is running.
		<-resumeCalled
		lock.Lock()
		jobMap[nodeid] = job.ID()
		lock.Unlock()
	}

	drainAdoptionLoop()
	if e, a := jobCount, hookCallCount; e != a {
		t.Fatalf("expected hookCallCount to be %d, but got %d", e, a)
	}

	drainAdoptionLoop()
	if e, a := jobCount, hookCallCount; e != a {
		t.Fatalf("expected hookCallCount to be %d, but got %d", e, a)
	}

	nodeLiveness.FakeSetExpiration(1, hlc.MinTimestamp)
	drainAdoptionLoop()
	<-resumeCalled
	testutils.SucceedsSoon(t, func() error {
		lock.Lock()
		defer lock.Unlock()
		if hookCallCount <= jobCount {
			return errors.Errorf("expected hookCallCount to be > %d, but got %d", jobCount, hookCallCount)
		}
		return nil
	})

	testutils.SucceedsSoon(t, func() error {
		lock.Lock()
		defer lock.Unlock()
		if e, a := 2, resumeCounts[jobMap[1]]; e != a {
			return errors.Errorf("expected resumeCount to be %d, but got %d", e, a)
		}
		return nil
	})

	// We want to verify that simply incrementing the epoch does not
	// result in the job being rescheduled.
	nodeLiveness.FakeIncrementEpoch(3)
	drainAdoptionLoop()
	select {
	case <-resumeCalled:
		t.Fatal("Incrementing an epoch should not reschedule a job")
	default:
	}

	// When we reset the liveness of the node, though, we should get
	// a reschedule.
	nodeLiveness.FakeSetExpiration(3, hlc.MinTimestamp)
	drainAdoptionLoop()
	<-resumeCalled
	close(done)

	testutils.SucceedsSoon(t, func() error {
		lock.Lock()
		defer lock.Unlock()
		if e, a := 1, resumeCounts[jobMap[3]]; e > a {
			return errors.Errorf("expected resumeCount to be > %d, but got %d", e, a)
		}
		if e, a := 1, resumeCounts[jobMap[2]]; e > a {
			return errors.Errorf("expected resumeCount to be > %d, but got %d", e, a)
		}
		count := 0
		for _, ct := range resumeCounts {
			count += ct
		}

		if e, a := 4, count; e > a {
			return errors.Errorf("expected total jobs to be > %d, but got %d", e, a)
		}
		return nil
	})
}

func TestRegistryResumeActiveLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer jobs.TestingSetAdoptAndCancelIntervals(10*time.Millisecond, 10*time.Millisecond)()

	resumeCh := make(chan jobspb.JobID)
	defer jobs.ResetConstructors()()
	jobs.RegisterConstructor(jobspb.TypeBackup, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{
			OnResume: func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case resumeCh <- job.ID():
					return nil
				}
			},
		}
	})

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	payload, err := protoutil.Marshal(&jobspb.Payload{
		Lease:   &jobspb.Lease{NodeID: 1, Epoch: 1},
		Details: jobspb.WrapPayloadDetails(jobspb.BackupDetails{}),
	})
	if err != nil {
		t.Fatal(err)
	}

	progress, err := protoutil.Marshal(&jobspb.Progress{
		Details: jobspb.WrapProgressDetails(jobspb.BackupProgress{}),
	})
	if err != nil {
		t.Fatal(err)
	}

	var id jobspb.JobID
	sqlutils.MakeSQLRunner(sqlDB).QueryRow(t,
		`INSERT INTO system.jobs (status, payload, progress) VALUES ($1, $2, $3) RETURNING id`,
		jobs.StatusRunning, payload, progress).Scan(&id)

	if e, a := id, <-resumeCh; e != a {
		t.Fatalf("expected job %d to be resumed, but got %d", e, a)
	}
}

// TestExpiringSessionsDoesNotTouchTerminalJobs will ensure that we do not
// update the claim_session_id field of jobs when expiring sessions or claiming
// jobs.
func TestExpiringSessionsAndClaimJobsDoesNotTouchTerminalJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Don't adopt, cancel rapidly.
	defer jobs.TestingSetAdoptAndCancelIntervals(10*time.Hour, 10*time.Millisecond)()

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	payload, err := protoutil.Marshal(&jobspb.Payload{
		Details: jobspb.WrapPayloadDetails(jobspb.BackupDetails{}),
	})
	if err != nil {
		t.Fatal(err)
	}

	progress, err := protoutil.Marshal(&jobspb.Progress{
		Details: jobspb.WrapProgressDetails(jobspb.BackupProgress{}),
	})
	if err != nil {
		t.Fatal(err)
	}

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	const insertQuery = `
   INSERT
     INTO system.jobs (
                        status,
                        payload,
                        progress,
                        claim_session_id,
                        claim_instance_id
                      )
   VALUES ($1, $2, $3, $4, $5)
RETURNING id;
`
	// Disallow clean up of claimed jobs
	jobs.CancellationsUpdateLimitSetting.Override(&s.ClusterSettings().SV, 0)
	terminalStatuses := []jobs.Status{jobs.StatusSucceeded, jobs.StatusCanceled, jobs.StatusFailed}
	terminalIDs := make([]jobspb.JobID, len(terminalStatuses))
	terminalClaims := make([][]byte, len(terminalStatuses))
	for i, s := range terminalStatuses {
		terminalClaims[i] = uuid.MakeV4().GetBytes() // bogus claim
		tdb.QueryRow(t, insertQuery, s, payload, progress, terminalClaims[i], 42).
			Scan(&terminalIDs[i])
	}
	var nonTerminalID jobspb.JobID
	tdb.QueryRow(t, insertQuery, jobs.StatusRunning, payload, progress, uuid.MakeV4().GetBytes(), 42).
		Scan(&nonTerminalID)

	checkClaimEqual := func(id jobspb.JobID, exp []byte) error {
		const getClaimQuery = `SELECT claim_session_id FROM system.jobs WHERE id = $1`
		var claim []byte
		tdb.QueryRow(t, getClaimQuery, id).Scan(&claim)
		if !bytes.Equal(claim, exp) {
			return errors.Errorf("expected nil, got %s", hex.EncodeToString(exp))
		}
		return nil
	}

	getClaimCount := func(id jobspb.JobID) int {
		const getClaimQuery = `SELECT count(claim_session_id) FROM system.jobs WHERE id = $1`
		count := 0
		tdb.QueryRow(t, getClaimQuery, id).Scan(&count)
		return count
	}
	// Validate the claims were not cleaned up.
	claimCount := getClaimCount(nonTerminalID)
	if claimCount == 0 {
		require.FailNowf(t, "unexpected claim sessions",
			"claim session ID's were removed some how %d", claimCount)
	}
	// Allow clean up of claimed jobs
	jobs.CancellationsUpdateLimitSetting.Override(&s.ClusterSettings().SV, 1000)
	testutils.SucceedsSoon(t, func() error {
		return checkClaimEqual(nonTerminalID, nil)
	})
	for i, id := range terminalIDs {
		require.NoError(t, checkClaimEqual(id, terminalClaims[i]))
	}
	// Update the terminal jobs to set them to have a NULL claim.
	for _, id := range terminalIDs {
		tdb.Exec(t, `UPDATE system.jobs SET claim_session_id = NULL WHERE id = $1`, id)
	}
	// At this point, all of the jobs should have a NULL claim.
	// Assert that.
	for _, id := range append(terminalIDs, nonTerminalID) {
		require.NoError(t, checkClaimEqual(id, nil))
	}

	// Nudge the adoption queue and ensure that only the non-terminal job gets
	// claimed.
	s.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()

	sess, err := s.SQLLivenessProvider().(sqlliveness.Provider).Session(ctx)
	require.NoError(t, err)
	testutils.SucceedsSoon(t, func() error {
		return checkClaimEqual(nonTerminalID, sess.ID().UnsafeBytes())
	})
	// Ensure that the terminal jobs still have a nil claim.
	for _, id := range terminalIDs {
		require.NoError(t, checkClaimEqual(id, nil))
	}
}
