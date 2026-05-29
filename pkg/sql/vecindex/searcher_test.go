// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"context"
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecencoding"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore/vecstorepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSearcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	internalDB := srv.ApplicationLayer().InternalDB().(descs.DB)
	codec := srv.ApplicationLayer().Codec()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	runner.Exec(t, "CREATE TABLE t (id INT PRIMARY KEY, prefix INT NOT NULL, v VECTOR(2))")

	baseTableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "defaultdb", "t")
	vCol, err := catalog.MustFindColumnByName(baseTableDesc, "v")
	require.NoError(t, err)
	prefixCol, err := catalog.MustFindColumnByName(baseTableDesc, "prefix")
	require.NoError(t, err)

	indexDesc := descpb.IndexDescriptor{
		ID:                  43,
		Name:                "t_idx",
		Type:                idxtype.VECTOR,
		KeyColumnIDs:        []descpb.ColumnID{prefixCol.GetID(), vCol.GetID()},
		KeyColumnNames:      []string{prefixCol.GetName(), vCol.GetName()},
		KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
		KeySuffixColumnIDs:  []descpb.ColumnID{baseTableDesc.GetPrimaryIndex().GetKeyColumnID(0)},
		Version:             descpb.LatestIndexDescriptorVersion,
		EncodingType:        catenumpb.SecondaryIndexEncoding,
	}

	// Now edit our fake index into the table descriptor.
	rawTableDesc := baseTableDesc.TableDesc()
	rawTableDesc.Indexes = append(rawTableDesc.Indexes, indexDesc)
	tableDesc := tabledesc.NewBuilder(rawTableDesc).BuildImmutableTable()

	quantizer := quantize.NewUnQuantizer(2, vecpb.L2SquaredDistance)
	store, err := vecstore.NewWithLeasedDesc(
		ctx,
		internalDB,
		quantizer,
		codec,
		tableDesc,
		indexDesc.ID,
	)
	require.NoError(t, err)

	options := cspann.IndexOptions{
		RotAlgorithm:     vecpb.RotGivens,
		MinPartitionSize: 2,
		MaxPartitionSize: 4,
		BaseBeamSize:     1,
		IsDeterministic:  true,
		// Disable adaptive search until it's extended to work with vecstore.
		DisableAdaptiveSearch: true,
	}
	idx, err := cspann.NewIndex(ctx, store, quantizer, 42 /* seed */, &options, srv.Stopper())
	require.NoError(t, err)

	tx := internalDB.KV().NewTxn(ctx, "searcher")

	// Insert two vectors into root partition.
	var mutator MutationSearcher
	evalCtx := eval.NewTestingEvalContext(srv.ApplicationLayer().ClusterSettings())
	defer evalCtx.Stop(ctx)

	var index catalog.Index
	for _, idx := range tableDesc.NonPrimaryIndexes() {
		if idx.GetID() == indexDesc.ID {
			index = idx
			break
		}
	}
	require.NotNil(t, index)
	var fullVecFetchSpec vecstorepb.GetFullVectorsFetchSpec
	err = vecstore.InitGetFullVectorsFetchSpec(&fullVecFetchSpec, evalCtx, tableDesc, index, tableDesc.GetPrimaryIndex())
	require.NoError(t, err)
	mutator.Init(evalCtx, idx, tx, &fullVecFetchSpec)

	// Reuse prefix, key bytes, value bytes and vector memory, to ensure it's
	// allowed.
	var prefix, keyBytes, valueBytes []byte
	var original, randomized vector.T

	insertVector := func(vec vector.T, key int64, val cspann.ValueBytes) vector.T {
		prefix = prefix[:0]
		keyBytes = keyBytes[:0]
		randomized = randomized[:0]

		prefix = encoding.EncodeVarintAscending(prefix, 100)
		require.NoError(t, mutator.SearchForInsert(ctx, prefix, vec))

		partitionKey := cspann.PartitionKey(*mutator.PartitionKey().(*tree.DInt))
		keyBytes = keys.MakeFamilyKey(encoding.EncodeVarintAscending(keyBytes, key), 0 /* famID */)

		randomized = slices.Grow(randomized, len(vec))[:len(vec)]
		idx.TransformVector(vec, randomized)
		err = mutator.txn.AddToPartition(ctx, cspann.TreeKey(prefix), partitionKey, cspann.LeafLevel,
			randomized, cspann.ChildKey{KeyBytes: keyBytes}, val)
		require.NoError(t, err)
		return randomized
	}

	// Reuse vector and value memory, to ensure it's allowed.
	original = vector.T{1, 2}
	valueBytes = []byte{1, 2}
	insertVector(original, 1, cspann.ValueBytes(valueBytes))
	original[0] = 5
	original[1] = 3
	valueBytes[0] = 3
	valueBytes[1] = 4
	randomized = insertVector(original, 2, cspann.ValueBytes(valueBytes))

	// Validate that search vector was correctly encoded and quantized.
	encodedVec := mutator.EncodedVector()
	vecSet := quantize.UnQuantizedVectorSet{Vectors: vector.MakeSet(2)}
	remainder, err := vecencoding.DecodeUnquantizerVectorToSet(
		[]byte(*encodedVec.(*tree.DBytes)), &vecSet)
	require.NoError(t, err)
	require.Empty(t, remainder)
	require.Equal(t, randomized, vecSet.Vectors.At(0))

	// Search for a vector that doesn't exist in the tree (reuse memory).
	prefix = prefix[:0]
	prefix = encoding.EncodeVarintAscending(prefix, 200)
	original[0] = 1
	original[1] = 1
	var searcher Searcher
	searcher.Init(evalCtx, idx, tx, &fullVecFetchSpec,
		8 /* baseBeamSize */, 2 /* maxResults */, 5 /* rerankMultiplier */)
	require.NoError(t, searcher.Search(ctx, prefix, original))
	require.Nil(t, searcher.NextResult())

	// Search for a vector that does exist (reuse memory).
	prefix = prefix[:0]
	prefix = encoding.EncodeVarintAscending(prefix, 100)
	require.NoError(t, searcher.Search(ctx, prefix, original))
	res := searcher.NextResult()
	require.InDelta(t, float32(1), res.QueryDistance, 0.01)
	res = searcher.NextResult()
	require.InDelta(t, float32(20), res.QueryDistance, 0.01)
	require.Nil(t, searcher.NextResult())

	// Search again to ensure search state is reset.
	require.NoError(t, searcher.Search(ctx, prefix, original))
	res = searcher.NextResult()
	require.InDelta(t, float32(1), res.QueryDistance, 0.01)
	res = searcher.NextResult()
	require.InDelta(t, float32(20), res.QueryDistance, 0.01)
	require.Nil(t, searcher.NextResult())

	// Search for a vector to delete that doesn't exist (reuse memory).
	keyBytes = keyBytes[:0]
	original[0] = 1
	original[1] = 2
	keyBytes = keys.MakeFamilyKey(encoding.EncodeVarintAscending(keyBytes, 123), 0 /* famID */)
	require.NoError(t, mutator.SearchForDelete(ctx, prefix, original, keyBytes))
	require.Equal(t, tree.DNull, mutator.PartitionKey())
	require.Nil(t, mutator.EncodedVector())

	// Search for a vector to delete that doesn't exist in the tree (reuse memory).
	prefix = prefix[:0]
	prefix = encoding.EncodeVarintAscending(prefix, 200)
	require.NoError(t, mutator.SearchForDelete(ctx, prefix, original, keyBytes))
	require.Equal(t, tree.DNull, mutator.PartitionKey())
	require.Nil(t, mutator.EncodedVector())

	// Search for a vector to delete (reuse memory).
	prefix = prefix[:0]
	prefix = encoding.EncodeVarintAscending(prefix, 100)
	keyBytes = keyBytes[:0]
	keyBytes = keys.MakeFamilyKey(encoding.EncodeVarintAscending(keyBytes, 1), 0 /* famID */)
	require.NoError(t, mutator.SearchForDelete(ctx, prefix, original, keyBytes))
	require.Equal(t, tree.NewDInt(tree.DInt(1)), mutator.PartitionKey())
	require.Nil(t, mutator.EncodedVector())

	require.NoError(t, tx.Commit(ctx))
}

// TestVectorIndexPanicCaught verifies that panics originating in
// pkg/sql/vecindex on the SQL executor path are caught by the colexecerror
// allow-list and returned to the SQL client as internal errors, rather than
// crashing the node. Subtests cover the read and mutation executor paths.
//
// The fact that the test process is still alive between subtests is the
// implicit allow-list assertion: without the allow-list entry covering
// pkg/sql/vecindex, the panic would re-propagate out of
// CatchVectorizedRuntimeError and tear down the test binary.
//
// Regression check: commenting out the sqlVecindexPackagesPrefix line in
// pkg/sql/colexecerror/error.go's shouldCatchPanic causes all subtests to
// fail with uncaught panics (exit code 2). The cspann.Index.Search subtest
// specifically panics from a subpackage, locking down the breadth of the
// prefix match: that the allow-list covers pkg/sql/vecindex/... subpackages,
// not just the top-level package.
func TestVectorIndexPanicCaught(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// crdb_test builds default to re-panicking from CatchVectorizedRuntimeError
	// so that bugs in the vectorized engine fail loudly in tests. This test
	// specifically exercises the production panic-catching path, so restore
	// release-build behavior for its scope.
	defer colexecerror.ProductionBehaviorForTests()()

	ctx := context.Background()
	knobs := &VecIndexTestingKnobs{}
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			VecIndexTestingKnobs: knobs,
		},
	})
	defer srv.Stopper().Stop(ctx)
	runner := sqlutils.MakeSQLRunner(sqlDB)

	runner.Exec(t, "SET CLUSTER SETTING feature.vector_index.enabled = true")
	runner.Exec(t, "CREATE TABLE t (k INT PRIMARY KEY, v VECTOR(2), VECTOR INDEX (v))")
	// Seed rows so the SELECT subtests have something for the optimizer to
	// plan a vector index scan against. This INSERT runs before any knob is
	// set, so it completes normally.
	runner.Exec(t, "INSERT INTO t VALUES (1, '[1, 2]'), (2, '[3, 4]')")

	t.Run("MutationSearcher panic on INSERT", func(t *testing.T) {
		knobs.PanicDuringMutationSearch = func() {
			panic(errors.AssertionFailedf("injected MutationSearcher panic"))
		}
		defer func() { knobs.PanicDuringMutationSearch = nil }()

		_, err := sqlDB.ExecContext(ctx, "INSERT INTO t VALUES (99, '[7, 8]')")
		require.Error(t, err)
		require.Contains(t, err.Error(), "injected MutationSearcher panic")
		// Sanity: panic fires at the top of SearchForInsert, before any KV
		// write, so the seed rows are intact.
		runner.CheckQueryResults(t, "SELECT count(*) FROM t", [][]string{{"2"}})
	})

	t.Run("Searcher panic on SELECT", func(t *testing.T) {
		knobs.PanicDuringSearch = func() {
			panic(errors.AssertionFailedf("injected Searcher.Search panic"))
		}
		defer func() { knobs.PanicDuringSearch = nil }()

		_, err := sqlDB.ExecContext(ctx, "SELECT k FROM t ORDER BY v <-> '[1, 2]' LIMIT 1")
		require.Error(t, err)
		require.Contains(t, err.Error(), "injected Searcher.Search panic")
	})

	t.Run("cspann.Index.Search panic on SELECT", func(t *testing.T) {
		knobs.PanicDuringCspannSearch = func() {
			panic(errors.AssertionFailedf("injected cspann.Index.Search panic"))
		}
		defer func() { knobs.PanicDuringCspannSearch = nil }()

		_, err := sqlDB.ExecContext(ctx, "SELECT k FROM t ORDER BY v <-> '[1, 2]' LIMIT 1")
		require.Error(t, err)
		require.Contains(t, err.Error(), "injected cspann.Index.Search panic")
	})
}
