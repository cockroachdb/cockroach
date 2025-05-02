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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecencoding"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
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

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "defaultdb", "t")
	vCol, err := catalog.MustFindColumnByName(tableDesc, "v")
	require.NoError(t, err)
	prefixCol, err := catalog.MustFindColumnByName(tableDesc, "prefix")
	require.NoError(t, err)

	indexDesc := descpb.IndexDescriptor{
		ID:                  43,
		Name:                "t_idx",
		Type:                idxtype.VECTOR,
		KeyColumnIDs:        []descpb.ColumnID{prefixCol.GetID(), vCol.GetID()},
		KeyColumnNames:      []string{prefixCol.GetName(), vCol.GetName()},
		KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
		KeySuffixColumnIDs:  []descpb.ColumnID{tableDesc.GetPrimaryIndex().GetKeyColumnID(0)},
		Version:             descpb.LatestIndexDescriptorVersion,
		EncodingType:        catenumpb.SecondaryIndexEncoding,
	}

	quantizer := quantize.NewUnQuantizer(2)
	store, err := vecstore.NewWithColumnID(
		ctx,
		internalDB,
		quantizer,
		codec,
		tableDesc,
		indexDesc.ID,
		vCol.GetID(),
	)
	require.NoError(t, err)

	options := cspann.IndexOptions{
		MinPartitionSize: 2,
		MaxPartitionSize: 4,
		BaseBeamSize:     1,
		IsDeterministic:  true,
	}
	idx, err := cspann.NewIndex(ctx, store, quantizer, 42 /* seed */, &options, srv.Stopper())
	require.NoError(t, err)

	tx := internalDB.KV().NewTxn(ctx, "searcher")

	// Insert two vectors into root partition.
	var mutator MutationSearcher
	mutator.Init(idx, tx)

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
		idx.RandomizeVector(vec, randomized)
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
	searcher.Init(idx, tx, 8 /* baseBeamSize */, 2 /* maxResults */)
	require.NoError(t, searcher.Search(ctx, prefix, original))
	require.Nil(t, searcher.NextResult())

	// Search for a vector that does exist (reuse memory).
	prefix = prefix[:0]
	prefix = encoding.EncodeVarintAscending(prefix, 100)
	require.NoError(t, searcher.Search(ctx, prefix, original))
	res := searcher.NextResult()
	require.InDelta(t, float32(1), res.QuerySquaredDistance, 0.01)
	res = searcher.NextResult()
	require.InDelta(t, float32(20), res.QuerySquaredDistance, 0.01)
	require.Nil(t, searcher.NextResult())

	// Search again to ensure search state is reset.
	require.NoError(t, searcher.Search(ctx, prefix, original))
	res = searcher.NextResult()
	require.InDelta(t, float32(1), res.QuerySquaredDistance, 0.01)
	res = searcher.NextResult()
	require.InDelta(t, float32(20), res.QuerySquaredDistance, 0.01)
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
