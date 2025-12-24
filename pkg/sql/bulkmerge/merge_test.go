// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/sql/bulkutil"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/taskset"
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

	testMergeProcessors(t, srv.ApplicationLayer(), 1)
}

func TestDistributedMergeThreeNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "test caused OOM")

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
	testMergeProcessors(t, srv, instanceCount)
}

func TestDistributedMergeProcessorFailurePropagates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	injectedErr := errors.New("injected merge processor failure")

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		ExternalIODir: dir,
		Knobs: base.TestingKnobs{
			DistSQL: &execinfra.TestingKnobs{
				BulkMergeTestingKnobs: &TestingKnobs{
					RunBeforeMergeTask: func(
						ctx context.Context, flowID execinfrapb.FlowID, taskID taskset.TaskID,
					) error {
						return injectedErr
					},
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	execCfg := srv.ExecutorConfig().(sql.ExecutorConfig)
	jobExecCtx, cleanupJob := sql.MakeJobExecContext(
		ctx, "test-merge-failure", username.RootUserName(), &sql.MemoryMetrics{}, &execCfg,
	)
	defer cleanupJob()

	spans := []roachpb.Span{{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax}}
	ssts := []execinfrapb.BulkMergeSpec_SST{{
		URI:      "nodelocal://1/unused",
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
	}}

	writeTS := hlc.Timestamp{WallTime: 1}
	_, err := Merge(ctx, jobExecCtx, ssts, spans, func(instanceID base.SQLInstanceID) string {
		return fmt.Sprintf("nodelocal://%d/merge/out/", instanceID)
	}, 1 /* iteration */, 1 /* maxIterations */, &writeTS)
	require.ErrorIs(t, err, injectedErr)
}

func TestDistributedMergeMultiPassIngestsIntoKV(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		ExternalIODir: dir,
		// These tests write directly into KV, so they must run against the system
		// tenant.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	tsa := newTestServerAllocator(t, ctx, execCfg)
	jobExecCtx, jobCleanup := sql.MakeJobExecContext(
		ctx, "test", username.RootUserName(), &sql.MemoryMetrics{}, &execCfg,
	)
	defer jobCleanup()

	targetFileSize.Override(ctx, &s.ClusterSettings().SV, 1<<20)
	fileAllocator := bulksst.NewExternalFileAllocator(tsa.es, tsa.prefixUri, s.Clock())
	batcher := bulksst.NewUnsortedSSTBatcher(s.ClusterSettings(), fileAllocator)

	prefix := "merge-multi/"
	const keyCount = 10
	expected := make(map[string]string, keyCount)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%skey-%d", prefix, i)
		val := fmt.Sprintf("value-%d", i)
		expected[key] = val

		mvccVal := roachpb.MakeValueFromBytes([]byte(val))
		encMVCCVal, err := storage.EncodeMVCCValue(storage.MVCCValue{Value: mvccVal})
		require.NoError(t, err)
		mvccKey := storage.MVCCKey{
			Key:       []byte(key),
			Timestamp: hlc.Timestamp{WallTime: 1},
		}
		require.NoError(t, batcher.AddMVCCKey(ctx, mvccKey, encMVCCVal))
	}
	require.NoError(t, batcher.CloseWithError(ctx))

	inputSSTs := importToMerge(fileAllocator.GetFileList())
	start := roachpb.Key(prefix)
	end := start.PrefixEnd()
	spans := []roachpb.Span{{Key: start, EndKey: end}}

	// First iteration produces merged SSTs to external storage.
	iter1Out, err := Merge(
		ctx,
		jobExecCtx,
		inputSSTs,
		spans,
		func(instanceID base.SQLInstanceID) string {
			return fmt.Sprintf("nodelocal://%d/merge/iter-1/", instanceID)
		},
		1, /* iteration */
		2, /* maxIterations */
		nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, iter1Out)

	// Second (final) iteration ingests directly into KV.
	writeTS := execCfg.Clock.Now()
	iter2Out, err := Merge(
		ctx,
		jobExecCtx,
		iter1Out,
		spans,
		func(instanceID base.SQLInstanceID) string {
			return fmt.Sprintf("nodelocal://%d/merge/iter-2/", instanceID)
		},
		2, /* iteration */
		2, /* maxIterations */
		&writeTS,
	)
	require.NoError(t, err)
	require.Nil(t, iter2Out, "final iteration should not produce SST outputs")

	rows, err := s.DB().Scan(ctx, start, end, 0 /* maxRows */)
	require.NoError(t, err)
	require.Len(t, rows, keyCount)
	for _, kv := range rows {
		val, err := kv.Value.GetBytes()
		require.NoError(t, err)
		require.Equal(t, expected[string(kv.Key)], string(val))
	}
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
			URI:      mapFiles.SST[i].URI,
			StartKey: []byte(mapFiles.SST[i].StartKey),
			EndKey:   []byte(mapFiles.SST[i].EndKey),
		})
	}
	return ssts
}

func testMergeProcessors(
	t *testing.T, s serverutils.ApplicationLayerInterface, expectedTaskInstanceCount int,
) {
	ctx := context.Background()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	tsa := newTestServerAllocator(t, ctx, execCfg)

	jobExecCtx, cleanup := sql.MakeJobExecContext(
		ctx, "test", username.RootUserName(), &sql.MemoryMetrics{}, &execCfg,
	)

	// Wait for all nodes to be ready for DistSQL before planning.
	// SetupAllNodesPlanning filters out unhealthy nodes, so if it returns the
	// expected count, all nodes passed the NodeVitality.IsLive(DistSQL) check.
	require.Eventually(t, func() bool {
		_, sqlInstanceIDs, err := execCfg.DistSQLPlanner.SetupAllNodesPlanning(
			ctx, jobExecCtx.ExtendedEvalContext(), &execCfg,
		)
		return err == nil && len(sqlInstanceIDs) >= expectedTaskInstanceCount
	}, 30*time.Second, 100*time.Millisecond, "timed out waiting for %d nodes to be ready for DistSQL", expectedTaskInstanceCount)
	defer cleanup()

	// Override the target size of our merged SSTs to ensure we are flushing
	// correctly.
	targetFileSize.Override(ctx, &s.ClusterSettings().SV, 30)
	fileAllocator := bulksst.NewExternalFileAllocator(tsa.es, tsa.prefixUri, s.Clock())
	batcher := bulksst.NewUnsortedSSTBatcher(s.ClusterSettings(), fileAllocator)
	ls := writeSSTs(t, ctx, batcher, 13)

	ssts := importToMerge(fileAllocator.GetFileList())

	// Generate splits by sampling keys from the SSTs. We'll create splits every 3 keys
	// to divide the work into multiple tasks.
	var splitKeys []roachpb.Key
	for i := 3; i < 13; i += 3 {
		splitKey := encodeKey(fmt.Sprintf("key-%d", i)).Key
		splitKeys = append(splitKeys, splitKey)
	}

	// Convert split keys into spans for each task
	spans := make([]roachpb.Span, len(splitKeys)+1)
	for i := range spans {
		if i == 0 {
			spans[i] = roachpb.Span{Key: roachpb.KeyMin, EndKey: splitKeys[0]}
		} else if i == len(splitKeys) {
			spans[i] = roachpb.Span{Key: splitKeys[i-1], EndKey: roachpb.KeyMax}
		} else {
			spans[i] = roachpb.Span{Key: splitKeys[i-1], EndKey: splitKeys[i]}
		}
	}

	plan, planCtx, err := newBulkMergePlan(
		ctx,
		jobExecCtx,
		ssts,
		spans,
		func(instanceID base.SQLInstanceID) string {
			return fmt.Sprintf("nodelocal://%d/merge/out/", instanceID)
		},
		1, /* iteration */
		2, /* maxIterations */
		nil /* writeTS */)
	require.NoError(t, err)
	defer plan.Release()

	fmt.Printf("plan: %+v\n", plan)
	require.Equal(t, plan.GetResultTypes(), mergeCoordinatorOutputTypes)

	var result execinfrapb.BulkMergeSpec_Output
	rowWriter := sql.NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		require.NoError(t, protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &result))
		return nil
	})

	sqlReceiver := sql.MakeDistSQLReceiver(
		ctx,
		rowWriter,
		tree.Rows,
		execCfg.RangeDescriptorCache,
		nil,
		nil,
		jobExecCtx.ExtendedEvalContext().Tracing)
	defer sqlReceiver.Release()

	evalCtxCopy := jobExecCtx.ExtendedEvalContext().Context.Copy()
	jobExecCtx.DistSQLPlanner().Run(
		ctx,
		planCtx,
		nil,
		plan,
		sqlReceiver,
		evalCtxCopy,
		nil,
	)

	require.NoError(t, rowWriter.Err())

	// Verify the SSTs

	verifySSTs(t, ctx, execCfg, result.SSTs, ls, 30)
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

	// Verify that merging occurred - we should have fewer output SSTs than input
	// k,v pairs since the merge process combines them.
	require.True(t, len(ls) > len(ssts), "no SSTs were merged")

	var prevEndKey roachpb.Key
	cloudMux := bulkutil.NewExternalStorageMux(execCfg.DistSQLSrv.ExternalStorageFromURI, username.RootUserName())
	for i, sst := range ssts {
		// Read that all merge uris contain what we expect.
		require.Regexp(t, "nodelocal://.*/merge/out/[0-9]+.sst", sst.URI)

		// Create store file for the SST
		file, err := cloudMux.StoreFile(ctx, sst.URI)
		require.NoError(t, err)

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
				"SST %d start key < previous end key. Got prev end: %v, start: %v",
				i, prevEndKey, sst.StartKey)
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
		require.True(t, sstSize <= maxSize,
			"SST %d exceeds max size. Got %d, max %d", i, sstSize, maxSize)

		t.Logf("SST %d: size=%d, range=[%v, %v]", i, sstSize, sst.StartKey, sst.EndKey)
	}
}

// TestMergeSSTsSplitsAtRowBoundaries tests that when SSTs are split due to
// size constraints, the splits occur at row boundaries (not mid-row, splitting
// column families apart). It verifies that the endKey calculation uses
// EnsureSafeSplitKey() + PrefixEnd() to ensure proper row boundary handling.
func TestMergeSSTsSplitsAtRowBoundaries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		ExternalIODir: dir,
	})
	defer srv.Stopper().Stop(ctx)

	execCfg := srv.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)

	// Set a small target size to force splits mid-processing.
	// With 3 rows × 2 column families × ~60 byte values = ~360 bytes of data,
	// a 150 byte target should create at least 2 SSTs.
	targetFileSize.Override(ctx, &srv.ClusterSettings().SV, 150)

	tsa := newTestServerAllocator(t, ctx, execCfg)
	fileAllocator := bulksst.NewExternalFileAllocator(tsa.es, tsa.prefixUri, srv.Clock())
	batcher := bulksst.NewUnsortedSSTBatcher(srv.ClusterSettings(), fileAllocator)

	ts := hlc.Timestamp{WallTime: 1}
	rowPrefix := keys.SystemSQLCodec.IndexPrefix(7, 1)

	// Create 3 rows, each with 2 column families
	// Row structure: /Table/7/1/<rowID>/0/<familyID>
	for rowID := 1; rowID <= 3; rowID++ {
		rowKey := encoding.EncodeUvarintAscending(rowPrefix, uint64(rowID))
		cf1 := roachpb.Key(keys.MakeFamilyKey(append([]byte(nil), rowKey...), 1))
		cf2 := roachpb.Key(keys.MakeFamilyKey(append([]byte(nil), rowKey...), 2))

		// Use large values to ensure we exceed target size
		value := make([]byte, 60)
		for i := range value {
			value[i] = byte('a' + rowID - 1)
		}

		require.NoError(t, batcher.AddMVCCKey(ctx, storage.MVCCKey{Key: cf1, Timestamp: ts}, value))
		require.NoError(t, batcher.AddMVCCKey(ctx, storage.MVCCKey{Key: cf2, Timestamp: ts}, value))
	}

	require.NoError(t, batcher.Flush(ctx))

	ssts := importToMerge(fileAllocator.GetFileList())

	// Create a merge spec with a single span covering all data
	spans := []roachpb.Span{{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax}}

	jobExecCtx, cleanup := sql.MakeJobExecContext(
		ctx, "test", username.RootUserName(), &sql.MemoryMetrics{}, &execCfg,
	)
	defer cleanup()

	writeTS := hlc.Timestamp{WallTime: 1}
	plan, planCtx, err := newBulkMergePlan(
		ctx,
		jobExecCtx,
		ssts,
		spans,
		func(instanceID base.SQLInstanceID) string {
			return fmt.Sprintf("nodelocal://%d/merge/out/", instanceID)
		},
		1,        /* iteration */
		2,        /* maxIterations */
		&writeTS, /* writeTS */
	)
	require.NoError(t, err)
	defer plan.Release()

	var result execinfrapb.BulkMergeSpec_Output
	rowWriter := sql.NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		require.NoError(t, protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &result))
		return nil
	})

	sqlReceiver := sql.MakeDistSQLReceiver(
		ctx,
		rowWriter,
		tree.Rows,
		execCfg.RangeDescriptorCache,
		nil,
		nil,
		jobExecCtx.ExtendedEvalContext().Tracing)
	defer sqlReceiver.Release()

	evalCtxCopy := jobExecCtx.ExtendedEvalContext().Context.Copy()
	jobExecCtx.DistSQLPlanner().Run(
		ctx,
		planCtx,
		nil,
		plan,
		sqlReceiver,
		evalCtxCopy,
		nil,
	)

	require.NoError(t, rowWriter.Err())

	// Verify that splitting occurred (we should have at least 2 SSTs)
	require.GreaterOrEqual(t, len(result.SSTs), 2, "expected at least 2 output SSTs due to size-based splitting")

	// Verify each SST's contents: column families of the same row must stay together
	cloudMux := bulkutil.NewExternalStorageMux(execCfg.DistSQLSrv.ExternalStorageFromURI, username.RootUserName())

	for i, sst := range result.SSTs {
		file, err := cloudMux.StoreFile(ctx, sst.URI)
		require.NoError(t, err)

		iterOpts := storage.IterOptions{
			KeyTypes:   storage.IterKeyTypePointsAndRanges,
			LowerBound: sst.StartKey,
			UpperBound: sst.EndKey,
		}

		iter, err := storageccl.ExternalSSTReader(ctx, []storageccl.StoreFile{file}, nil, iterOpts)
		require.NoError(t, err)
		defer iter.Close()

		// Track column families per row
		type rowInfo struct {
			familyIDs map[uint32]bool
			firstKey  roachpb.Key
		}
		rows := make(map[string]*rowInfo)

		var lastKey roachpb.Key
		for iter.SeekGE(storage.MVCCKey{Key: sst.StartKey}); ; iter.NextKey() {
			ok, err := iter.Valid()
			require.NoError(t, err)
			if !ok {
				break
			}

			key := iter.UnsafeKey()
			lastKey = key.Key.Clone()

			// Get the safe split key (row boundary) for this key
			rowStart, err := keys.EnsureSafeSplitKey(key.Key)
			require.NoError(t, err)

			rowStartStr := string(rowStart)
			if rows[rowStartStr] == nil {
				rows[rowStartStr] = &rowInfo{
					familyIDs: make(map[uint32]bool),
					firstKey:  key.Key.Clone(),
				}
			}

			// Extract family ID from the key by parsing from the row start
			// The key format is: <rowStart><familyID>
			// We can decode the family ID by removing the row prefix
			remainder := bytes.TrimPrefix(key.Key, rowStart)
			if len(remainder) > 0 {
				// Decode the family ID (it's the next varint encoded value)
				// DecodeUvarintAscending returns (remaining bytes, decoded value, error)
				_, familyIDUint64, err := encoding.DecodeUvarintAscending(remainder)
				if err == nil {
					rows[rowStartStr].familyIDs[uint32(familyIDUint64)] = true
				}
			}
		}

		t.Logf("SST %d: [%v, %v), contains %d rows", i, sst.StartKey, sst.EndKey, len(rows))

		// Verify that each row has exactly 2 column families (no partial rows)
		for rowKey, info := range rows {
			require.Len(t, info.familyIDs, 2,
				"row %q in SST %d should have both column families (got %v)", rowKey, i, info.familyIDs)
		}

		// Verify that the SST endKey is at a row boundary (for non-final SSTs)
		if i < len(result.SSTs)-1 && !bytes.Equal(sst.EndKey, roachpb.KeyMax) {
			// The endKey should be the PrefixEnd of a safe split key
			// This means the endKey should be one byte past a valid row start
			safeKey, err := keys.EnsureSafeSplitKey(lastKey)
			require.NoError(t, err)
			expectedEndKey := safeKey.PrefixEnd()
			require.Equal(t, expectedEndKey, sst.EndKey,
				"SST %d endKey should be at row boundary (safeKey.PrefixEnd())", i)
		}
	}
}
