// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigmanagerccl

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigmanager"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils/spanconfigtestcluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestManagerCheckJobConditions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	scKnobs := &spanconfig.TestingKnobs{
		ManagerDisableJobCreation: true,
	}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: scKnobs,
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	spanConfigTestCluster := spanconfigtestcluster.NewHandle(t, tc, scKnobs)
	defer spanConfigTestCluster.Cleanup()

	systemTenant := spanConfigTestCluster.InitializeTenant(ctx, roachpb.SystemTenantID)
	secondaryTenant := spanConfigTestCluster.InitializeTenant(ctx, roachpb.MakeTenantID(10))
	secondaryTenant.Exec(`SET CLUSTER SETTING spanconfig.reconciliation_job.enabled = false;`)

	var interceptCount int32
	checkInterceptCountGreaterThan := func(min int32) int32 {
		var currentCount int32
		testutils.SucceedsSoon(t, func() error {
			if currentCount = atomic.LoadInt32(&interceptCount); !(currentCount > min) {
				return errors.Errorf("expected intercept count(=%d) > min(=%d)",
					currentCount, min)
			}
			return nil
		})
		return currentCount
	}
	manager := spanconfigmanager.New(
		secondaryTenant.ExecutorConfig().(sql.ExecutorConfig).DB,
		secondaryTenant.JobRegistry().(*jobs.Registry),
		secondaryTenant.ExecutorConfig().(sql.ExecutorConfig).InternalExecutor,
		secondaryTenant.ExecutorConfig().(sql.ExecutorConfig).Codec,
		secondaryTenant.Stopper(),
		secondaryTenant.ClusterSettings(),
		secondaryTenant.SpanConfigReconciler().(spanconfig.Reconciler),
		&spanconfig.TestingKnobs{
			ManagerDisableJobCreation: true,
			ManagerCheckJobInterceptor: func() {
				atomic.AddInt32(&interceptCount, 1)
			},
		},
	)

	var currentCount int32
	require.NoError(t, manager.Start(ctx))
	currentCount = checkInterceptCountGreaterThan(currentCount) // wait for an initial check

	secondaryTenant.Exec(`SET CLUSTER SETTING spanconfig.reconciliation_job.enabled = true;`)
	currentCount = checkInterceptCountGreaterThan(currentCount) // the job enablement setting triggers a check

	systemTenant.Exec(`ALTER TENANT 10 SET CLUSTER SETTING spanconfig.tenant_reconciliation_job.enabled = true;`)
	currentCount = checkInterceptCountGreaterThan(currentCount) // the tenant job enablement setting triggers a check

	secondaryTenant.Exec(`SET CLUSTER SETTING spanconfig.reconciliation_job.check_interval = '25m'`)
	_ = checkInterceptCountGreaterThan(currentCount) // the job check interval setting triggers a check
}
