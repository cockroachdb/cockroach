// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestMergeProcessors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(jeffswenson): start a three node instance to ensure each instances
	// gets a processor.
	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	jobExecCtx, cleanup := sql.MakeJobExecContext(ctx, "test", username.RootUserName(), &sql.MemoryMetrics{}, &execCfg)
	defer cleanup()

	plan, planCtx, err := newBulkMergePlan(ctx, jobExecCtx, 5)
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

	var uri []string
	for _, sst := range result.Ssts {
		uri = append(uri, sst.Uri)
	}
	sort.Strings(uri)

	require.Equal(t, uri, []string{
		"nodelocal://0.sst",
		"nodelocal://1.sst",
		"nodelocal://2.sst",
		"nodelocal://3.sst",
		"nodelocal://4.sst",
	})
}
