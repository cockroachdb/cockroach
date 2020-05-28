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
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	"github.com/cockroachdb/errors"
)

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
		Details:       jobspb.RestoreDetails{},
		Progress:      jobspb.RestoreProgress{},
	})
	if err := storedJob.Created(ctx); err != nil {
		t.Fatal(err)
	}
	retrievedJob, err := registry.LoadJob(ctx, *storedJob.ID())
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
	defer jobs.ResetConstructors()()

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Disable leniency for instant expiration
	jobs.LeniencySetting.Override(&s.ClusterSettings().SV, 0)

	db := s.DB()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	nodeLiveness := jobs.NewFakeNodeLiveness(4)
	newRegistry := func(id roachpb.NodeID) *jobs.Registry {
		const cancelInterval = time.Duration(math.MaxInt64)
		const adoptInterval = time.Nanosecond

		var c base.NodeIDContainer
		c.Set(ctx, id)
		idContainer := base.NewSQLIDContainer(0, &c, true /* exposed */)
		ac := log.AmbientContext{Tracer: tracing.NewTracer()}
		r := jobs.MakeRegistry(
			ac, s.Stopper(), clock, sqlbase.MakeOptionalNodeLiveness(nodeLiveness), db, s.InternalExecutor().(sqlutil.InternalExecutor),
			idContainer, s.ClusterSettings(), base.DefaultHistogramWindowInterval(), jobs.FakePHS, "",
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
	jobMap := make(map[roachpb.NodeID]int64)
	hookCallCount := 0
	// resumeCounts maps jobs IDs to number of start/resumes.
	resumeCounts := make(map[int64]int)
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
			OnResume: func(ctx context.Context, _ chan<- tree.Datums) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case resumeCalled <- struct{}{}:
				case <-done:
				}
				lock.Lock()
				resumeCounts[*job.ID()]++
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
		job, _, err := newRegistry(nodeid).CreateAndStartJob(ctx, nil, rec)
		if err != nil {
			t.Fatal(err)
		}
		// Wait until the job is running.
		<-resumeCalled
		lock.Lock()
		jobMap[nodeid] = *job.ID()
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

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	resumeCh := make(chan int64)
	defer jobs.ResetConstructors()()
	jobs.RegisterConstructor(jobspb.TypeBackup, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{
			OnResume: func(ctx context.Context, _ chan<- tree.Datums) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case resumeCh <- *job.ID():
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

	var id int64
	sqlutils.MakeSQLRunner(sqlDB).QueryRow(t,
		`INSERT INTO system.jobs (status, payload, progress) VALUES ($1, $2, $3) RETURNING id`,
		jobs.StatusRunning, payload, progress).Scan(&id)

	if e, a := id, <-resumeCh; e != a {
		t.Fatalf("expected job %d to be resumed, but got %d", e, a)
	}
}
