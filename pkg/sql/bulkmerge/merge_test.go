// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestDistributedMergeOneNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		ExternalIODir: dir,
	})
	defer srv.Stopper().Stop(ctx)

	testMergeProcessors(t, srv.ApplicationLayer(), srv.Engines())
}

func TestDistributedMergeThreeNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	instanceCount := 3
	tc := testcluster.StartTestCluster(t, instanceCount, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// Pick a random node to connect to
	nodeIdx := rand.Intn(instanceCount)
	srv := tc.Server(nodeIdx)
	testMergeProcessors(t, srv, srv.Engines())
}

func randIntSlice(n int) []int {
	ls := make([]int, n)
	for i := range ls {
		ls[i] = i
	}
	for i := range ls {
		r := rand.Intn(n)
		ls[i], ls[r] = ls[r], ls[i]
	}
	return ls
}

func encodeKey(strKey string) storage.MVCCKey {
	key := storage.MVCCKey{
		Key: []byte(strKey),
	}
	return storage.MVCCKey{
		Timestamp: hlc.Timestamp{WallTime: 1},
		Key:       storage.EncodeMVCCKeyToBuf(nil, key),
	}
}

func writeSSTs(t *testing.T, ctx context.Context, b *bulksst.Writer, n int) {
	ls := randIntSlice(n)
	for _, i := range ls {
		k := encodeKey(fmt.Sprintf("key-%d", i))
		v := []byte(fmt.Sprintf("value-%d", i))
		require.NoError(t, b.AddMVCCKey(ctx, k, v))
	}
}

type testServerAllocator struct {
	es        cloud.ExternalStorage
	prefixUri string
}

func newTestServerAllocator(
	t *testing.T, ctx context.Context, app serverutils.ApplicationLayerInterface,
) *testServerAllocator {
	execCfg := app.ExecutorConfig().(sql.ExecutorConfig)
	prefixURI := "nodelocal://1/merge/"
	store, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, prefixURI, username.RootUserName())
	require.NoError(t, err)
	return &testServerAllocator{
		es:        store,
		prefixUri: prefixURI,
	}
}

func testMergeProcessors(
	t *testing.T, s serverutils.ApplicationLayerInterface, engines []storage.Engine,
) {
	ctx := context.Background()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	tsa := newTestServerAllocator(t, ctx, s)

	jobExecCtx, cleanup := sql.MakeJobExecContext(ctx, "test", username.RootUserName(), &sql.MemoryMetrics{}, &execCfg)
	defer cleanup()

	bulksst.BatchKeyCount.Override(ctx, &s.ClusterSettings().SV, 1)
	fileAllocator := bulksst.NewExternalFileAllocator(tsa.es, tsa.prefixUri)
	batcher := bulksst.NewUnsortedSSTBatcher(s.ClusterSettings(), fileAllocator)
	writeSSTs(t, ctx, batcher, 11)
	outputURI := "nodelocal://1/merge/out"
	spec := execinfrapb.BulkMergeSpec{
		Ssts:      fileAllocator.GetFileList(),
		OutputUri: &outputURI,
	}
	plan, planCtx, err := newBulkMergePlan(ctx, jobExecCtx, spec)
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

	// Since we have 0 splits (for now)
	// TODO(annie): Generate splits (random sample of the keys we wrote).
	require.Equal(t, len(result.Ssts), 1)
	// Read that all merge uris contain what we expect.
	require.Equal(t, "nodelocal://1/merge/out/0.sst", result.Ssts[0].Uri)
}
