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
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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

	const nodeCount = 1
	nodeLiveness := NewFakeNodeLiveness(nodeCount)

	var db *kv.DB
	// Insulate this test from wall time.
	mClock := hlc.NewManualClock(hlc.UnixNano())
	clock := hlc.NewClock(mClock.UnixNano, time.Nanosecond)
	registry := MakeRegistry(
		log.AmbientContext{},
		stopper,
		clock,
		sqlbase.MakeOptionalNodeLiveness(nodeLiveness),
		db,
		nil, /* ex */
		base.TestingIDContainer,
		cluster.NoSettings,
		histogramWindowInterval,
		FakePHS,
		"",
	)

	const cancelInterval = time.Nanosecond
	const adoptInterval = time.Duration(math.MaxInt64)
	if err := registry.Start(ctx, stopper, cancelInterval, adoptInterval); err != nil {
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
		if err := registry.register(jobID, func() { cancelCount++ }); err != nil {
			t.Fatal(err)
		}
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
	t.Skip("")
	// TODO (lucy): This test probably shouldn't continue to exist in its current
	// form if GCMutations will cease to be used. Refactor or get rid of it.

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)

	type mutationOptions struct {
		// Set if the desc should have any mutations of any sort.
		hasMutation bool
		// Set if the mutation being inserted is a GCMutation.
		hasGCMutation bool
		// Set if the desc should have a job that is dropping it.
		hasDropJob bool
	}

	ts := timeutil.Now()
	earlier := ts.Add(-1 * time.Hour)
	muchEarlier := ts.Add(-2 * time.Hour)

	setMutations := func(mutations []sqlbase.DescriptorMutation) sqlbase.ID {
		desc := sqlbase.TestingGetMutableExistingTableDescriptor(
			kvDB, keys.SystemSQLCodec, "t", "to_be_mutated")
		desc.Mutations = mutations
		if err := kvDB.Put(
			context.Background(),
			sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, desc.GetID()),
			desc.DescriptorProto(),
		); err != nil {
			t.Fatal(err)
		}
		return desc.GetID()
	}

	setGCMutations := func(gcMutations []sqlbase.TableDescriptor_GCDescriptorMutation) sqlbase.ID {
		desc := sqlbase.TestingGetMutableExistingTableDescriptor(
			kvDB, keys.SystemSQLCodec, "t", "to_be_mutated")
		desc.GCMutations = gcMutations
		if err := kvDB.Put(
			context.Background(),
			sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, desc.GetID()),
			desc.DescriptorProto(),
		); err != nil {
			t.Fatal(err)
		}
		return desc.GetID()
	}

	setDropJob := func(shouldDrop bool) sqlbase.ID {
		desc := sqlbase.TestingGetMutableExistingTableDescriptor(
			kvDB, keys.SystemSQLCodec, "t", "to_be_mutated")
		if shouldDrop {
			desc.DropJobID = 123
		} else {
			// Set it back to the default val.
			desc.DropJobID = 0
		}
		if err := kvDB.Put(
			context.Background(),
			sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, desc.GetID()),
			desc.DescriptorProto(),
		); err != nil {
			t.Fatal(err)
		}
		return desc.GetID()
	}

	writeJob := func(name string, created, finished time.Time, status Status, mutOptions mutationOptions) string {
		if _, err := sqlDB.Exec(`
CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.to_be_mutated AS SELECT 1`); err != nil {
			t.Fatal(err)
		}
		descriptorID := setDropJob(mutOptions.hasDropJob)
		if mutOptions.hasMutation {
			descriptorID = setMutations([]sqlbase.DescriptorMutation{{}})
		}
		if mutOptions.hasGCMutation {
			descriptorID = setGCMutations([]sqlbase.TableDescriptor_GCDescriptorMutation{{}})
		}

		payload, err := protoutil.Marshal(&jobspb.Payload{
			Description: name,
			Lease:       &jobspb.Lease{NodeID: 1, Epoch: 1},
			// register a mutation on the table so that jobs that reference
			// the table are not considered orphaned
			DescriptorIDs: []sqlbase.ID{
				descriptorID,
				sqlbase.InvalidID, // invalid id to test handling of missing descriptors.
			},
			Details:        jobspb.WrapPayloadDetails(jobspb.SchemaChangeDetails{}),
			StartedMicros:  timeutil.ToUnixMicros(created),
			FinishedMicros: timeutil.ToUnixMicros(finished),
		})
		if err != nil {
			t.Fatal(err)
		}
		progress, err := protoutil.Marshal(&jobspb.Progress{
			Details: jobspb.WrapProgressDetails(jobspb.SchemaChangeProgress{}),
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

	// Test the descriptor when any of the following are set.
	// 1. Mutations
	// 2. GC Mutations
	// 3. A drop job
	for _, hasMutation := range []bool{true, false} {
		for _, hasGCMutation := range []bool{true, false} {
			for _, hasDropJob := range []bool{true, false} {
				if !hasMutation && !hasGCMutation && !hasDropJob {
					continue
				}
				mutOptions := mutationOptions{
					hasMutation:   hasMutation,
					hasGCMutation: hasGCMutation,
					hasDropJob:    hasDropJob,
				}
				oldRunningJob := writeJob("old_running", muchEarlier, time.Time{}, StatusRunning, mutOptions)
				oldSucceededJob := writeJob("old_succeeded", muchEarlier, muchEarlier.Add(time.Minute), StatusSucceeded, mutOptions)
				oldSucceededJob2 := writeJob("old_succeeded2", muchEarlier, muchEarlier.Add(time.Minute), StatusSucceeded, mutOptions)
				newRunningJob := writeJob("new_running", earlier, time.Time{}, StatusRunning, mutOptions)
				newSucceededJob := writeJob("new_succeeded", earlier, earlier.Add(time.Minute), StatusSucceeded, mutOptions)

				db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{
					{oldRunningJob}, {oldSucceededJob}, {oldSucceededJob2}, {newRunningJob}, {newSucceededJob}})

				if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, earlier); err != nil {
					t.Fatal(err)
				}
				db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{
					{oldRunningJob}, {newRunningJob}, {newSucceededJob}})

				if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, earlier); err != nil {
					t.Fatal(err)
				}
				db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{
					{oldRunningJob}, {newRunningJob}, {newSucceededJob}})

				if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, ts.Add(time.Minute*-10)); err != nil {
					t.Fatal(err)
				}
				db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{
					{oldRunningJob}, {newRunningJob}})

				// force the running jobs to become orphaned
				_ = setMutations(nil)
				_ = setGCMutations(nil)
				_ = setDropJob(false)
				if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, ts.Add(time.Minute*-10)); err != nil {
					t.Fatal(err)
				}
				db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{})
			}
		}
	}
}
