// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/sql/bulkutil"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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

	testMergeProcessors(t, srv.ApplicationLayer())
}

func TestDistributedMergeThreeNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	instanceCount := 3

	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	args := base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{},
	}
	for i := 0; i < 3; i++ {
		args.ServerArgsPerNode[i] = base.TestServerArgs{
			ExternalIODir: fmt.Sprintf("%s/node-%d", dir, i),
		}
	}

	tc := serverutils.StartCluster(t, instanceCount, args)
	defer tc.Stopper().Stop(ctx)

	// Pick a random node to connect to
	nodeIdx := rand.Intn(instanceCount)
	srv := tc.Server(nodeIdx)
	testMergeProcessors(t, srv)
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

func writeSSTs(t *testing.T, ctx context.Context, b *bulksst.Writer, n int) []int {
	ls := randIntSlice(n)
	for idx, i := range ls {
		k := encodeKey(fmt.Sprintf("key-%d", idx))
		v := []byte(fmt.Sprintf("value-%d", i))
		require.NoError(t, b.AddMVCCKey(ctx, k, v))
	}
	return ls
}

type testServerAllocator struct {
	es        cloud.ExternalStorage
	prefixUri string
}

func newTestServerAllocator(
	t *testing.T, ctx context.Context, execCfg sql.ExecutorConfig,
) *testServerAllocator {
	prefixURI := "nodelocal://1/merge/"
	store, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, prefixURI, username.RootUserName())
	require.NoError(t, err)
	return &testServerAllocator{
		es:        store,
		prefixUri: prefixURI,
	}
}

func importToMerge(mapFiles *bulksst.SSTFiles) []execinfrapb.BulkMergeSpec_SST {
	ssts := make([]execinfrapb.BulkMergeSpec_SST, 0, len(mapFiles.SST))
	for i := range mapFiles.SST {
		ssts = append(ssts, execinfrapb.BulkMergeSpec_SST{
			Uri:      mapFiles.SST[i].URI,
			StartKey: []byte(mapFiles.SST[i].StartKey),
			EndKey:   []byte(mapFiles.SST[i].EndKey),
		})
	}
	return ssts
}

func testMergeProcessors(t *testing.T, s serverutils.ApplicationLayerInterface) {
	ctx := context.Background()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	tsa := newTestServerAllocator(t, ctx, execCfg)

	jobExecCtx, cleanup := sql.MakeJobExecContext(ctx, "test", username.RootUserName(), &sql.MemoryMetrics{}, &execCfg)
	defer cleanup()

	bulksst.BatchKeyCount.Override(ctx, &s.ClusterSettings().SV, 1)
	// Override the target size of our merged SSTs to ensure we are flushing
	// correctly.
	targetFileSize.Override(ctx, &s.ClusterSettings().SV, 30)
	fileAllocator := bulksst.NewExternalFileAllocator(tsa.es, tsa.prefixUri)
	batcher := bulksst.NewUnsortedSSTBatcher(s.ClusterSettings(), fileAllocator)
	ls := writeSSTs(t, ctx, batcher, 13)
	ssts := importToMerge(fileAllocator.GetFileList())
	plan, planCtx, err := newBulkMergePlan(ctx, jobExecCtx, ssts, []roachpb.Span{{
		Key:    nil,
		EndKey: roachpb.KeyMax,
	}}, func(instanceID base.SQLInstanceID) string {
		return fmt.Sprintf("nodelocal://%d/merge/out/", instanceID)
	})
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

	// Verify the SSTs

	verifySSTs(t, ctx, execCfg, result.Ssts, ls, 30)
}

// verifySSTs reads and verifies the contents of the generated SSTs.
// It checks that:
// 1. The number of SSTs is correct
// 2. Each SST's size is within the target size
// 3. Each k,v is correct
// 4. No key ranges overlap between SSTs
func verifySSTs(
	t *testing.T,
	ctx context.Context,
	execCfg sql.ExecutorConfig,
	ssts []execinfrapb.BulkMergeSpec_SST,
	ls []int,
	maxSize int64,
) {
	t.Helper()

	// Since we have 0 splits (for now).
	// Each k,v pair equates to 1 sst; we are just checking that we have flushed
	// some merged ssts for now.
	// TODO(annie): Generate splits (random sample of the keys we wrote).
	require.True(t, len(ls) > len(ssts), "no SSTs were merged")

	var prevEndKey roachpb.Key
	cloudMux := bulkutil.NewCloudStorageMux(execCfg.DistSQLSrv.ExternalStorageFromURI, username.RootUserName())
	for i, sst := range ssts {
		// Read that all merge uris contain what we expect.
		require.Regexp(t, "nodelocal://.*/merge/out/[0-9]+.sst", sst.Uri)

		// Create store file for the SST
		file, err := cloudMux.StoreFile(ctx, sst.Uri)

		iterOpts := storage.IterOptions{
			LowerBound: sst.StartKey,
			UpperBound: sst.EndKey,
		}

		iter, err := storageccl.ExternalSSTReader(ctx, []storageccl.StoreFile{file}, nil, iterOpts)
		require.NoError(t, err)
		defer iter.Close()

		// Verify key ordering
		if i > 0 {
			require.True(t, bytes.Compare(prevEndKey, sst.StartKey) <= 0,
				"SST %d start key <= previous end key. Got start: %v, prev end: %v",
				i, sst.StartKey, prevEndKey)
		}
		prevEndKey = sst.EndKey

		// Verify SST contents
		var sstSize int64
		for iter.SeekGE(storage.MVCCKey{Key: sst.StartKey}); ; iter.NextKey() {
			ok, err := iter.Valid()
			require.NoError(t, err)
			if !ok {
				break
			}

			key := iter.UnsafeKey()
			val, err := iter.UnsafeValue()
			require.NoError(t, err)

			// Add debug logging for key and value sizes
			t.Logf("SST %d - Key: %q (size=%d), Value: %q (size=%d)",
				i, key.Key, len(key.Key), val, len(val))

			// Verify key is within SST bounds
			require.True(t, bytes.Compare(key.Key, sst.StartKey) >= 0,
				"key before SST start key. Got key: %v, start: %v", key.Key, sst.StartKey)
			require.True(t, bytes.Compare(key.Key, sst.EndKey) <= 0,
				"key after SST end key. Got key: %v, end: %v", key.Key, sst.EndKey)

			// Decode the MVCC key first
			decodedKey, err := storage.DecodeMVCCKey(key.Key)
			require.NoError(t, err)

			// Now trim the prefix from the decoded key
			keyStr := string(bytes.TrimPrefix(decodedKey.Key, []byte("key-")))
			keyIdx, err := strconv.Atoi(keyStr)
			require.NoError(t, err)
			expectedVal := []byte(fmt.Sprintf("value-%d", ls[keyIdx]))
			require.Equal(t, expectedVal, val, fmt.Sprintf("value mismatch - got: %x, want: %x for key: %s",
				val, expectedVal, keyStr))

			sstSize += int64(len(key.Key) + len(val))
			t.Logf("SST %d - Running size: %d", i, sstSize)
		}

		// Verify SST size
		require.True(t, sstSize <= maxSize*10,
			"SST %d exceeds max size. Got %d, max %d", i, sstSize, maxSize)

		t.Logf("SST %d: size=%d, range=[%v, %v]", i, sstSize, sst.StartKey, sst.EndKey)
	}
}
