// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"
	"math/rand"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func testMergeProcessors(
	t *testing.T,
	s serverutils.ApplicationLayerInterface,
	expectedTaskCount, expectedInstanceCount int,
) {

	ctx := context.Background()

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	jobExecCtx, cleanup := sql.MakeJobExecContext(ctx, "test", username.RootUserName(), &sql.MemoryMetrics{}, &execCfg)
	defer cleanup()

	plan, planCtx, err := newBulkMergePlan(ctx, jobExecCtx, expectedTaskCount)
	require.NoError(t, err)
	defer plan.Release()

	require.Equal(t, plan.GetResultTypes(), mergeCoordinatorOutputTypes)

	var result execinfrapb.BulkMergeSpec_Output
	rowWriter := sql.NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		require.NoError(t, protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &result))
		return nil
	})

	sqlReciever := sql.MakeDistSQLReceiver(
		ctx,
		rowWriter,
		tree.Rows,
		execCfg.RangeDescriptorCache,
		nil,
		nil,
		jobExecCtx.ExtendedEvalContext().Tracing)
	defer sqlReciever.Release()

	jobExecCtx.DistSQLPlanner().Run(
		ctx,
		planCtx,
		nil,
		plan,
		sqlReciever,
		jobExecCtx.ExtendedEvalContext(),
		nil,
	)

	require.NoError(t, rowWriter.Err())

	foundInstances := make(map[string]bool)
	foundTasks := []int64{}
	// ssts are expected to have the uri format nodelocal://<instance_id>/<job_id>/merger/<task_id>.sst
	for _, sst := range result.Ssts {
		parsed, err := url.Parse(sst.Uri)
		require.NoError(t, err)

		filename := strings.Split(parsed.Path, "/")[3]

		var taskID int64
		taskID, err = strconv.ParseInt(strings.Split(filename, ".")[0], 10, 64)
		require.NoError(t, err)
		foundTasks = append(foundTasks, taskID)

		foundInstances[parsed.Host] = true
	}

	slices.Sort(foundTasks)
	require.Len(t, foundTasks, expectedTaskCount)
	for _, taskID := range foundTasks {
		require.GreaterOrEqual(t, taskID, int64(0))
		require.Less(t, taskID, int64(expectedTaskCount))
	}

	require.Equal(t, len(foundInstances), expectedInstanceCount)
}

func TestDistributedMergeOneNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	testMergeProcessors(t, srv.ApplicationLayer(), 5 /*taskCount*/, 1 /*instanceCount*/)
}

func TestDistributedMergeThreeNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	instanceCount := 3
	taskCount := 100
	tc := testcluster.StartTestCluster(t, instanceCount, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// Pick a random node to connect to
	nodeIdx := rand.Intn(instanceCount)
	testMergeProcessors(t, tc.Server(nodeIdx).ApplicationLayer(), taskCount, instanceCount)
}
