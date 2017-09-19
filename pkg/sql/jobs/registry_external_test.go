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

package jobs_test

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/kr/pretty"
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

func TestRegistryResumeExpiredLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer jobs.ResetResumeHooks()()

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	db := s.KVClient().(*client.DB)
	ex := sql.InternalExecutor{LeaseManager: s.LeaseManager().(*sql.LeaseManager)}
	gossip := s.Gossip()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	nodeID := &base.NodeIDContainer{}

	registry := jobs.MakeRegistry(clock, db, ex, gossip, nodeID, jobs.FakeClusterID)
	nodeLiveness := jobs.NewFakeNodeLiveness(clock, 4)

	const cancelInterval = time.Duration(math.MaxInt64)
	const adoptInterval = time.Nanosecond
	if err := registry.Start(
		ctx, s.Stopper(), nodeLiveness, cancelInterval, adoptInterval,
	); err != nil {
		t.Fatal(err)
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

	jobMap := make(map[int64]roachpb.NodeID)
	for i := 0; i < jobCount; i++ {
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

	nodeID.Reset(jobCount + 1)

	hookCallCount := 0
	resumeCounts := make(map[roachpb.NodeID]int)
	resumeCalled := make(chan struct{})
	var newJobs []*jobs.Job
	jobs.AddResumeHook(func(_ jobs.Type) func(context.Context, *jobs.Job) error {
		hookCallCount++
		return func(_ context.Context, job *jobs.Job) error {
			resumeCounts[jobMap[*job.ID()]]++
			newJobs = append(newJobs, job)
			resumeCalled <- struct{}{}
			return nil
		}
	})

	drainAdoptionLoop()
	if e, a := 0, hookCallCount; e != a {
		t.Fatalf("expected hookCallCount to be %d, but got %d", e, a)
	}

	drainAdoptionLoop()
	if e, a := 0, hookCallCount; e != a {
		t.Fatalf("expected hookCallCount to be %d, but got %d", e, a)
	}

	nodeLiveness.FakeSetExpiration(1, hlc.MinTimestamp)
	drainAdoptionLoop()
	if hookCallCount == 0 {
		t.Fatalf("expected hookCallCount to be non-zero, but got %d", hookCallCount)
	}

	<-resumeCalled
	if e, a := 1, resumeCounts[1]; e != a {
		t.Fatalf("expected resumeCount to be %d, but got %d", e, a)
	}

	nodeLiveness.FakeIncrementEpoch(3)
	<-resumeCalled
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

func TestRegistryResumeActiveLease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	resumeCh := make(chan int64)
	defer jobs.ResetResumeHooks()()
	jobs.AddResumeHook(func(_ jobs.Type) func(context.Context, *jobs.Job) error {
		return func(ctx context.Context, job *jobs.Job) error {
			resumeCh <- *job.ID()
			return nil
		}
	})

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	payload, err := (&jobs.Payload{
		Lease:   &jobs.Lease{NodeID: 1, Epoch: 1},
		Details: jobs.WrapPayloadDetails(jobs.BackupDetails{}),
	}).Marshal()
	if err != nil {
		t.Fatal(err)
	}

	var id int64
	sqlutils.MakeSQLRunner(t, sqlDB).QueryRow(
		`INSERT INTO system.jobs (status, payload) VALUES ($1, $2) RETURNING id`,
		jobs.StatusRunning, payload).Scan(&id)

	if e, a := id, <-resumeCh; e != a {
		t.Fatalf("expected job %d to be resumed, but got %d", e, a)
	}
}
