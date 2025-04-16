// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/commontest"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// testStore implements the commontest.TestStore interface.
type testStore struct {
	*Store

	inserted  int64
	usePrefix bool
	runner    *sqlutils.SQLRunner
}

func (ts *testStore) AllowMultipleTrees() bool {
	return ts.usePrefix
}

func (ts *testStore) MakeTreeKey(t *testing.T, treeID int) cspann.TreeKey {
	if !ts.usePrefix {
		return nil
	}
	return keys.MakeFamilyKey(encoding.EncodeVarintAscending([]byte{}, int64(treeID)), 0 /* famID */)
}

func (ts *testStore) InsertVector(t *testing.T, treeID int, vec vector.T) cspann.KeyBytes {
	// TODO(andyk): For now, don't actually insert anything, since the execution
	// engine doesn't yet support insertion into a table with a vector index. As
	// a workaround, the vectors are pre-inserted when the test store is created,
	// below.
	ts.inserted++
	return keys.MakeFamilyKey(encoding.EncodeVarintAscending([]byte{}, ts.inserted), 0 /* famID */)
}

func TestStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	internalDB := srv.ApplicationLayer().InternalDB().(descs.DB)
	codec := srv.ApplicationLayer().Codec()
	runner := sqlutils.MakeSQLRunner(sqlDB)
	defer srv.Stopper().Stop(ctx)

	tbl := 0
	usePrefix := false
	makeStore := func(quantizer quantize.Quantizer) commontest.TestStore {
		tbl++
		tblName := fmt.Sprintf("t%d", tbl)

		runner.Exec(t, "CREATE TABLE "+tblName+" (id INT PRIMARY KEY, prefix INT NOT NULL, v VECTOR(2))")

		// TODO(andyk): Pre-insert the values that the common tests will insert
		// via InsertVector. These can be removed once the execution engine
		// supports insertion into a table with a vector index.
		runner.Exec(t, "INSERT INTO "+tblName+" (id, prefix, v) VALUES ($1, $2, $3)", 1, 0, "[1, 2]")
		runner.Exec(t, "INSERT INTO "+tblName+" (id, prefix, v) VALUES ($1, $2, $3)", 2, 0, "[7, 4]")
		runner.Exec(t, "INSERT INTO "+tblName+" (id, prefix, v) VALUES ($1, $2, $3)", 3, 0, "[4, 3]")
		runner.Exec(t, "INSERT INTO "+tblName+" (id, prefix, v) VALUES ($1, $2, $3)", 4, 1, "[1, 2]")
		runner.Exec(t, "INSERT INTO "+tblName+" (id, prefix, v) VALUES ($1, $2, $3)", 5, 1, "[7, 4]")
		runner.Exec(t, "INSERT INTO "+tblName+" (id, prefix, v) VALUES ($1, $2, $3)", 6, 1, "[4, 3]")

		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "defaultdb", tblName)
		vCol, err := catalog.MustFindColumnByName(tableDesc, "v")
		require.NoError(t, err)
		prefixCol, err := catalog.MustFindColumnByName(tableDesc, "prefix")
		require.NoError(t, err)

		indexDesc1 := descpb.IndexDescriptor{
			ID: 42, Name: "t_idx1",
			Type:                idxtype.VECTOR,
			KeyColumnIDs:        []descpb.ColumnID{vCol.GetID()},
			KeyColumnNames:      []string{vCol.GetName()},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			KeySuffixColumnIDs:  []descpb.ColumnID{tableDesc.GetPrimaryIndex().GetKeyColumnID(0)},
			Version:             descpb.LatestIndexDescriptorVersion,
			EncodingType:        catenumpb.SecondaryIndexEncoding,
		}

		indexDesc2 := descpb.IndexDescriptor{
			ID: 43, Name: "t_idx2",
			Type:                idxtype.VECTOR,
			KeyColumnIDs:        []descpb.ColumnID{prefixCol.GetID(), vCol.GetID()},
			KeyColumnNames:      []string{prefixCol.GetName(), vCol.GetName()},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
			KeySuffixColumnIDs:  []descpb.ColumnID{tableDesc.GetPrimaryIndex().GetKeyColumnID(0)},
			Version:             descpb.LatestIndexDescriptorVersion,
			EncodingType:        catenumpb.SecondaryIndexEncoding,
		}

		indexID := indexDesc1.ID
		if usePrefix {
			indexID = indexDesc2.ID
		}

		store, err := NewWithColumnID(
			internalDB,
			quantizer,
			codec,
			tableDesc,
			indexID,
			vCol.GetID(),
		)
		require.NoError(t, err)

		// Use CONSISTENT reads to ensure test is deterministic.
		store.SetMinimumConsistency(kvpb.CONSISTENT)

		return &testStore{Store: store, usePrefix: usePrefix, runner: runner}
	}

	// Run tests with a non-prefixed index.
	suite.Run(t, commontest.NewStoreTestSuite(ctx, makeStore))

	// Re-run the tests with a prefixed index.
	usePrefix = true
	suite.Run(t, commontest.NewStoreTestSuite(ctx, makeStore))

	// Ensure that races to create partition metadata either do not error, or
	// they error with WriteTooOldError.
	t.Run("race to create partition metadata", func(t *testing.T) {
		store := makeStore(quantize.NewUnQuantizer(2)).(*testStore)

		var done atomic.Int64
		getMetadata := func(treeKey cspann.TreeKey) {
			_ = store.RunTransaction(ctx, func(txn cspann.Txn) error {
				// Enable stepping in the txn, which is what SQL does.
				txn.(*Txn).kv.ConfigureStepping(ctx, kv.SteppingEnabled)

				_, err := txn.GetPartitionMetadata(ctx, treeKey, cspann.RootKey, true /* forUpdate */)
				if err != nil {
					// Returning the error will cause the transaction to abort.
					require.ErrorContains(t, err, "WriteTooOldError")
					done.Store(1)
					return err
				}

				// Run GetPartitionMetadata again, to ensure that it succeeds, as a
				// way of simulating multiple vectors being inserted in the same
				// SQL statement.
				_, err = txn.GetPartitionMetadata(ctx, treeKey, cspann.RootKey, true /* forUpdate */)
				require.NoError(t, err)
				return nil
			})
		}

		for i := range 100 {
			var wait sync.WaitGroup
			wait.Add(2)
			treeKey := store.MakeTreeKey(t, i)
			go func() {
				defer func() {
					wait.Done()
				}()
				getMetadata(treeKey)
			}()
			go func() {
				defer func() {
					wait.Done()
				}()
				getMetadata(treeKey)
			}()
			wait.Wait()

			// Fail on foreground goroutine if a background goroutine failed.
			if t.Failed() {
				t.FailNow()
			}

			// End the test once we find at least one WriteTooOldError.
			if done.Load() == 1 {
				break
			}
		}
	})

}

func TestQuantizeAndEncode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dims := 4
	seed := int64(42)

	// Create test vectors.
	vec1 := vector.T{1.0, 2.0, 3.0, 4.0}
	vec2 := vector.T{-1.0, -2.0, -3.0, -4.0}
	centroid := vector.T{0.0, 0.0, 0.0, 0.0}

	// Create quantizers.
	rootQuantizer := quantize.NewUnQuantizer(dims)
	quantizer := quantize.NewRaBitQuantizer(dims, seed)

	// Create a transaction and metadata.
	tx := &Txn{codec: makePartitionCodec(rootQuantizer, quantizer)}

	// Test encoding with non-root partition key (uses RaBitQuantizer).
	partitionKey := cspann.PartitionKey(123)
	encoded1, err := tx.QuantizeAndEncode(partitionKey, centroid, vec1)
	require.NoError(t, err)
	require.NotEmpty(t, encoded1)

	// Verify we can decode the encoded vector.
	codec := makeStoreCodec(quantizer)
	codec.Init(centroid, 1)
	_, err = codec.DecodeVector(encoded1)
	require.NoError(t, err)
	vs := codec.GetVectorSet().(*quantize.RaBitQuantizedVectorSet)
	require.Equal(t, 1, vs.GetCount())

	// Encode a different vector with root partition key (uses UnQuantizer).
	encoded2, err := tx.QuantizeAndEncode(cspann.RootKey, centroid, vec2)
	require.NoError(t, err)
	require.NotEmpty(t, encoded2)

	// Verify we can decode the encoded vector.
	codec = makeStoreCodec(rootQuantizer)
	codec.Init(centroid, 1)
	_, err = codec.DecodeVector(encoded2)
	require.NoError(t, err)
	vs2 := codec.GetVectorSet().(*quantize.UnQuantizedVectorSet)
	require.Equal(t, 1, vs2.GetCount())
}
