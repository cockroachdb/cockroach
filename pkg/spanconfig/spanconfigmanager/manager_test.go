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
	"errors"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigmanager"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/testsqlwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestManagerJobCreation checks that the span config manager creates the
// reconciliation job idempotently, and that the created job is what we expect.
func TestManagerJobCreation(t *testing.T) {
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

	ts := tc.Server(0)
	var createdCount int32
	manager := spanconfigmanager.New(
		ts.DB(),
		ts.JobRegistry().(*jobs.Registry),
		ts.InternalExecutor().(*sql.InternalExecutor),
		ts.Node().(*server.Node),
		testsqlwatcher.New(),
		tc.Stopper(),
		&spanconfig.TestingKnobs{
			ManagerCreatedJobInterceptor: func(jobI interface{}) {
				job := jobI.(*jobs.Job)
				if atomic.AddInt32(&createdCount, 1) > 1 {
					t.Errorf("expected single reconciliation job to be created")
				}
				require.True(t, job.Payload().Noncancelable)
				require.Equal(t, job.Payload().Description, "reconciling span configurations")
			},
		},
	)

	// Queue up concurrent attempts to create and start the reconciliation job.
	manager.StartJobIfNoneExist(ctx)
	manager.StartJobIfNoneExist(ctx)
	manager.StartJobIfNoneExist(ctx)

	testutils.SucceedsSoon(t, func() error {
		if createdCount == 0 {
			return errors.New("reconciliation job not created")
		}
		return nil
	})
}
