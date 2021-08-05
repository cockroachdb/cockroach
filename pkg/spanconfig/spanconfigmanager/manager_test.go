// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigmanager_test

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigmanager"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

// TestManagerConcurrentJobCreation ensures that only one of two concurrent
// attempts to create the auto span config reconciliation job are successful. We
// also ensure that the created job is what we expect.
// Sketch:
// - The first goroutine checks and ensures that the auto span config
// reconciliation job does not exists. Blocks after checking.
// - The second goroutine checks and ensures the auto span config reconciliation
// job does not exists and creates it. Unblocks the first goroutine.
// - The first goroutine tries to create the job but gets restarted. It
// subsequently notices that the job does indeed exist so ends up not creating
// one.
func TestManagerConcurrentJobCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // disable the automatic job creation
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	var mu syncutil.Mutex
	blocker := make(chan struct{})
	var unblocker chan struct{} = nil

	ts := tc.Server(0)
	manager := spanconfigmanager.New(
		ts.DB(),
		ts.JobRegistry().(*jobs.Registry),
		ts.InternalExecutor().(*sql.InternalExecutor),
		tc.Stopper(),
		&spanconfig.TestingKnobs{
			ManagerCreatedJobInterceptor: func(jobI interface{}) {
				job := jobI.(*jobs.Job)
				require.True(t, job.Payload().Noncancelable)
				require.Equal(t, job.Payload().Description, "reconciling span configurations")
			},
			ManagerAfterCheckedReconciliationJobExistsInterceptor: func(exists bool) {
				// First entrant blocks, second entrant does not.
				mu.Lock()
				if blocker != nil {
					require.False(t, exists)
					unblocker = blocker
					blocker = nil
					mu.Unlock()
					<-unblocker
					return
				}
				mu.Unlock()
			},
		},
	)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		started := manager.StartJobIfNoneExists(ctx)
		if started {
			t.Errorf("expected no job to start, but it did")
		}
	}()

	go func() {
		defer wg.Done()
		// Only try to start the job once the main testing goroutine has reached the
		// blocker and is waiting.
		for {
			mu.Lock()
			if blocker == nil {
				mu.Unlock()
				break
			}
			mu.Unlock()
		}
		started := manager.StartJobIfNoneExists(ctx)
		if !started {
			t.Errorf("expected job to start, but it did not")
		}
		close(unblocker)
	}()
	wg.Wait()
}
