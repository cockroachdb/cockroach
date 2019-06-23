// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func FakePHS(opName, user string) (interface{}, func()) {
	return nil, func() {}
}

func TestRegistryCancelation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, stopper := context.Background(), stop.NewStopper()
	defer stopper.Stop(ctx)

	// Not using the server.DefaultHistogramWindowInterval constant because
	// of a dep cycle.
	const histogramWindowInterval = 60 * time.Second

	var db *client.DB
	// Insulate this test from wall time.
	mClock := hlc.NewManualClock(hlc.UnixNano())
	clock := hlc.NewClock(mClock.UnixNano, time.Nanosecond)
	registry := MakeRegistry(
		log.AmbientContext{}, stopper, clock, db, nil /* ex */, FakeNodeID, cluster.NoSettings,
		histogramWindowInterval, FakePHS)

	const nodeCount = 1
	nodeLiveness := NewFakeNodeLiveness(nodeCount)

	const cancelInterval = time.Nanosecond
	const adoptInterval = time.Duration(math.MaxInt64)
	if err := registry.Start(ctx, stopper, nodeLiveness, cancelInterval, adoptInterval); err != nil {
		t.Fatal(err)
	}

	wait := func() {
		// Every turn of the registry's liveness poll loop will generate exactly one
		// call to nodeLiveness.Self. Only after we've witnessed two calls can we be
		// sure that the first turn of the registry's loop has completed.
		//
		// Waiting for only the first call to nodeLiveness.Self is racy, as we'd
		// perform our assertions concurrently with the registry loop's observation
		// of our injected liveness failure, if any.
		<-nodeLiveness.SelfCalledCh
		<-nodeLiveness.SelfCalledCh
	}

	cancelCount := 0
	didRegister := false
	jobID := int64(1)
	const nodeID = roachpb.NodeID(1)

	register := func() {
		didRegister = true
		jobID++
		registry.register(jobID, func() { cancelCount++ })
	}
	unregister := func() {
		registry.unregister(jobID)
		didRegister = false
	}
	expectCancel := func(expect bool) {
		t.Helper()

		wait()
		var e int
		if expect {
			e = 1
		}
		if a := cancelCount; e != a {
			t.Errorf("expected cancelCount of %d, but got %d", e, a)
		}
	}
	check := func(fn func()) {
		fn()
		if didRegister {
			unregister()
			wait()
		}
		cancelCount = 0
	}
	// inWindow slews the expiration time of the node's expiration.
	inWindow := func(in bool) {
		nanos := -defaultLeniencySetting.Nanoseconds()
		if in {
			nanos = nanos / 2
		} else {
			nanos = nanos * 2
		}
		nodeLiveness.FakeSetExpiration(nodeID, clock.Now().Add(nanos, 0))
	}

	// Jobs that complete while the node is live should be canceled once.
	check(func() {
		register()
		expectCancel(false)
		unregister()
		expectCancel(true)
	})

	// Jobs that are in-progress when the liveness epoch is incremented
	// should not be canceled.
	check(func() {
		register()
		nodeLiveness.FakeIncrementEpoch(nodeID)
		expectCancel(false)
		unregister()
		expectCancel(true)
	})

	// Jobs started in the new epoch that complete while the new epoch is live
	// should be canceled once.
	check(func() {
		register()
		expectCancel(false)
		unregister()
		expectCancel(true)
	})

	// Jobs **alive** within the leniency period should not be canceled.
	check(func() {
		register()
		inWindow(true)
		expectCancel(false)
		unregister()
		expectCancel(true)
	})

	// Jobs **started** within the leniency period should not be canceled.
	check(func() {
		inWindow(true)
		register()
		expectCancel(false)
	})

	// Jobs **alive** outside of the leniency period should be canceled.
	check(func() {
		register()
		inWindow(false)
		expectCancel(true)
	})

	// Jobs **started** outside of the leniency period should be canceled.
	check(func() {
		inWindow(false)
		register()
		expectCancel(true)
	})
}

func TestRegistryGC(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)

	ts := timeutil.Now()
	earlier := ts.Add(-1 * time.Hour)
	muchEarlier := ts.Add(-2 * time.Hour)

	writeJob := func(created, finished time.Time, status Status) string {
		ft := timeutil.ToUnixMicros(finished)
		payload, err := protoutil.Marshal(&jobspb.Payload{
			Lease:          &jobspb.Lease{NodeID: 1, Epoch: 1},
			Details:        jobspb.WrapPayloadDetails(jobspb.BackupDetails{}),
			FinishedMicros: ft,
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
		db.QueryRow(t,
			`INSERT INTO system.jobs (status, payload, progress, created) VALUES ($1, $2, $3, $4) RETURNING id`,
			status, payload, progress, created).Scan(&id)
		return strconv.Itoa(int(id))
	}

	j1 := writeJob(muchEarlier, time.Time{}, StatusRunning)
	j2 := writeJob(muchEarlier, muchEarlier.Add(time.Minute), StatusSucceeded)

	j3 := writeJob(earlier, time.Time{}, StatusRunning)
	j4 := writeJob(earlier, earlier.Add(time.Minute), StatusSucceeded)

	db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{{j1}, {j2}, {j3}, {j4}})
	if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, earlier); err != nil {
		t.Fatal(err)
	}
	db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{{j1}, {j3}, {j4}})
	if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, earlier); err != nil {
		t.Fatal(err)
	}
	db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{{j1}, {j3}, {j4}})
	if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, ts.Add(time.Minute*-10)); err != nil {
		t.Fatal(err)
	}
	db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{{j1}, {j3}})
}
