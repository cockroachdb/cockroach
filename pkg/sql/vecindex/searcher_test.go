// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"context"
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
	prefix := keys.MakeFamilyKey(encoding.EncodeVarintAscending([]byte{}, 100), 0 /* famID */)

	insertVector := func(vec vector.T, key int64, val cspann.ValueBytes) vector.T {
		require.NoError(t, mutator.SearchForInsert(ctx, prefix, vec))
		partitionKey := cspann.PartitionKey(*mutator.PartitionKey().(*tree.DInt))
		keyBytes := keys.MakeFamilyKey(encoding.EncodeVarintAscending([]byte{}, key), 0 /* famID */)
		randomizedVec := make(vector.T, len(vec))
		idx.RandomizeVector(vec, randomizedVec)
		err = mutator.txn.AddToPartition(ctx, cspann.TreeKey(prefix), partitionKey, cspann.LeafLevel,
			randomizedVec, cspann.ChildKey{KeyBytes: keyBytes}, val)
		require.NoError(t, err)
		return randomizedVec
	}

	insertVector(vector.T{1, 2}, 1, cspann.ValueBytes{1, 2})
	randomizedVec := insertVector(vector.T{5, 3}, 2, cspann.ValueBytes{3, 4})

	// Validate that search vector was correctly encoded and quantized.
	encodedVec := mutator.EncodedVector()
	vecSet := quantize.UnQuantizedVectorSet{Vectors: vector.MakeSet(2)}
	remainder, err := vecencoding.DecodeUnquantizedVectorToSet(
		[]byte(*encodedVec.(*tree.DBytes)), &vecSet)
	require.NoError(t, err)
	require.Empty(t, remainder)
	require.Equal(t, randomizedVec, vecSet.Vectors.At(0))

	// Use the Searcher.
	var searcher Searcher
	searcher.Init(idx, tx)
	require.NoError(t, searcher.Search(ctx, prefix, vector.T{1, 1}, 2))
	res := searcher.NextResult()
	require.InDelta(t, float32(20), res.QuerySquaredDistance, 0.01)
	res = searcher.NextResult()
	require.InDelta(t, float32(1), res.QuerySquaredDistance, 0.01)
	require.Nil(t, searcher.NextResult())

	// Search for a vector to delete.
	keyBytes := keys.MakeFamilyKey(encoding.EncodeVarintAscending([]byte{}, 1), 0 /* famID */)
	require.NoError(t, mutator.SearchForDelete(ctx, prefix, vector.T{1, 2}, keyBytes))
	require.Equal(t, tree.NewDInt(tree.DInt(1)), mutator.PartitionKey())
	require.Nil(t, mutator.EncodedVector())

	// Search for a vector to delete that doesn't exist.
	keyBytes = keys.MakeFamilyKey(encoding.EncodeVarintAscending([]byte{}, 123), 0 /* famID */)
	require.NoError(t, mutator.SearchForDelete(ctx, prefix, vector.T{1, 2}, keyBytes))
	require.Equal(t, tree.DNull, mutator.PartitionKey())
	require.Nil(t, mutator.EncodedVector())

	require.NoError(t, tx.Commit(ctx))
}
