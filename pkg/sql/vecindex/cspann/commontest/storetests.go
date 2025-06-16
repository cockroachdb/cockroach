// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package commontest

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"
)

var vec1 = vector.T{1, 2}
var vec2 = vector.T{7, 4}
var vec3 = vector.T{4, 3}
var vec4 = vector.T{6, -2}
var vec5 = vector.T{0, 5}

var primaryKey1 = cspann.ChildKey{KeyBytes: cspann.KeyBytes{1, 136}}
var primaryKey2 = cspann.ChildKey{KeyBytes: cspann.KeyBytes{2, 136}}
var primaryKey3 = cspann.ChildKey{KeyBytes: cspann.KeyBytes{3, 136}}
var primaryKey5 = cspann.ChildKey{KeyBytes: cspann.KeyBytes{5, 136}}

var partitionKey1 = cspann.ChildKey{PartitionKey: 10}
var partitionKey2 = cspann.ChildKey{PartitionKey: 20}
var partitionKey3 = cspann.ChildKey{PartitionKey: 30}
var partitionKey4 = cspann.ChildKey{PartitionKey: 40}
var partitionKey5 = cspann.ChildKey{PartitionKey: 50}

var valueBytes1 = cspann.ValueBytes{1, 2}
var valueBytes2 = cspann.ValueBytes{3, 4}
var valueBytes3 = cspann.ValueBytes{5, 6}
var valueBytes4 = cspann.ValueBytes{7, 8}
var valueBytes5 = cspann.ValueBytes{9, 10}

// TestStore wraps a Store interface so that it can be tested by a common set of
// tests.
type TestStore interface {
	cspann.Store

	// AllowMultipleTrees is true if the Store supports storing vectors in
	// multiple distinct trees that are identified by the cspann.TreeKey
	// parameter. This is used by prefixed vector indexes, e.g. where non-vector
	// column(s) precede the vector column in the index definition.
	AllowMultipleTrees() bool

	// MakeTreeKey converts a tree identifier into a tree key. The treeID is
	// convenient for testing, but it must be converted into a TreeKey in a way
	// that is specific to each store.
	MakeTreeKey(t *testing.T, treeID int) cspann.TreeKey

	// InsertVector inserts a vector into the store and returns the primary key
	// bytes that can be used to retrieve that vector via GetFullVectors.
	InsertVector(t *testing.T, treeID int, vec vector.T) cspann.KeyBytes

	// Close gives the store a chance to perform any validation or cleanup once
	// the store is no longer needed.
	Close(t *testing.T)
}

// MakeStoreFunc defines a function that creates a new TestStore instance for
// use by the common Store tests.
type MakeStoreFunc func(quantizer quantize.Quantizer) TestStore

type StoreTestSuite struct {
	suite.Suite

	ctx              context.Context
	makeStore        MakeStoreFunc
	rootQuantizer    quantize.Quantizer
	quantizer        quantize.Quantizer
	nextPartitionKey cspann.PartitionKey
}

// NewStoreTestSuite constructs a new suite of tests that run against
// implementations of the cspann.Store interface. Implementations do not need to
// have their own tests; instead, they can be validated using a common set of
// tests.
func NewStoreTestSuite(ctx context.Context, makeStore MakeStoreFunc) *StoreTestSuite {
	return &StoreTestSuite{
		ctx:              ctx,
		makeStore:        makeStore,
		rootQuantizer:    quantize.NewUnQuantizer(2, vecpb.L2SquaredDistance),
		quantizer:        quantize.NewRaBitQuantizer(2, 42, vecpb.L2SquaredDistance),
		nextPartitionKey: cspann.RootKey + 1,
	}
}

func (suite *StoreTestSuite) TestRunTransaction() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	rootVec := vector.T{1, 2}
	rootChildKey := cspann.ChildKey{KeyBytes: cspann.KeyBytes{10, 20, 136}}
	treeKey := store.MakeTreeKey(suite.T(), 0)

	// No error, should commit.
	suite.NoError(store.RunTransaction(suite.ctx, func(tx cspann.Txn) error {
		val := cspann.ValueBytes{100, 200}
		return tx.AddToPartition(
			suite.ctx, treeKey, cspann.RootKey, cspann.LeafLevel, rootVec, rootChildKey, val)
	}))

	// Return error and abort.
	suite.ErrorContains(store.RunTransaction(suite.ctx, func(tx cspann.Txn) error {
		toSearch := []cspann.PartitionToSearch{{Key: cspann.RootKey}}
		searchSet := cspann.SearchSet{MaxResults: 1}
		err := tx.SearchPartitions(suite.ctx, treeKey, toSearch, vector.T{1, -1}, &searchSet)
		suite.NoError(err)
		suite.Equal(1, toSearch[0].Count)
		return errors.New("abort")
	}), "abort")
}

func (suite *StoreTestSuite) TestGetPartitionMetadata() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	doTest := func(treeID int) {
		var metadata cspann.PartitionMetadata
		var err error
		var partitionKey cspann.PartitionKey
		treeKey := store.MakeTreeKey(suite.T(), treeID)

		RunTransaction(suite.ctx, suite.T(), store, func(txn cspann.Txn) {
			// Root partition does not yet exist, expect synthesized metadata.
			metadata, err = txn.GetPartitionMetadata(
				suite.ctx, treeKey, cspann.RootKey, false /* forUpdate */)
			suite.NoError(err)
			CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, vector.T{0, 0},
				cspann.PartitionStateDetails{State: cspann.ReadyState})

			// Non-root partition does not yet exist, expect error.
			_, err = txn.GetPartitionMetadata(
				suite.ctx, treeKey, cspann.PartitionKey(99), false /* forUpdate */)
			suite.ErrorIs(err, cspann.ErrPartitionNotFound)
		})

		// Create non-root partition with some vectors in it.
		partitionKey, _ = suite.createTestPartition(store, treeKey)

		RunTransaction(suite.ctx, suite.T(), store, func(txn cspann.Txn) {
			// Check metadata of new partition.
			metadata, err = txn.GetPartitionMetadata(
				suite.ctx, treeKey, partitionKey, true /* forUpdate */)
			CheckPartitionMetadata(suite.T(), metadata, cspann.SecondLevel, vector.T{4, 3},
				cspann.PartitionStateDetails{State: cspann.ReadyState})
		})

		// Update the partition state to DrainingForSplit.
		expected := metadata
		metadata.StateDetails.MakeDrainingForSplit(20, 30)
		suite.NoError(store.TryUpdatePartitionMetadata(
			suite.ctx, treeKey, partitionKey, metadata, expected))

		RunTransaction(suite.ctx, suite.T(), store, func(txn cspann.Txn) {
			// If forUpdate = false, GetPartitionMetadata should not error.
			metadata, err := txn.GetPartitionMetadata(
				suite.ctx, treeKey, partitionKey, false /* forUpdate */)
			suite.NoError(err)
			details := cspann.PartitionStateDetails{
				State: cspann.DrainingForSplitState, Target1: 20, Target2: 30}
			CheckPartitionMetadata(suite.T(), metadata, cspann.SecondLevel, vector.T{4, 3}, details)

			// If forUpdate = true, GetPartitionMetadata should error.
			var errConditionFailed *cspann.ConditionFailedError
			_, err = txn.GetPartitionMetadata(suite.ctx, treeKey, partitionKey, true /* forUpdate */)
			suite.ErrorAs(err, &errConditionFailed)
			CheckPartitionMetadata(suite.T(), errConditionFailed.Actual, cspann.SecondLevel,
				vector.T{4, 3}, details)
		})
		suite.NoError(err)
	}

	suite.Run("default tree", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		// Ensure that vectors are independent across trees.
		suite.Run("different tree", func() {
			doTest(1)
		})
	}
}

func (suite *StoreTestSuite) TestAddToPartition() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)

		RunTransaction(suite.ctx, suite.T(), store, func(txn cspann.Txn) {
			// Non-root partition does not exist, expect error.
			err := txn.AddToPartition(suite.ctx, treeKey, cspann.PartitionKey(99), cspann.LeafLevel,
				vec1, primaryKey1, valueBytes1)
			suite.ErrorIs(err, cspann.ErrPartitionNotFound)

			// Root partition does not exist, expect it to be lazily created.
			err = txn.AddToPartition(suite.ctx, treeKey, cspann.RootKey, cspann.LeafLevel,
				vec1, primaryKey1, valueBytes1)
			suite.NoError(err)
		})
		CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, cspann.RootKey, 1)

		RunTransaction(suite.ctx, suite.T(), store, func(txn cspann.Txn) {
			// Add another vector to root partition.
			err := txn.AddToPartition(suite.ctx, treeKey, cspann.RootKey, cspann.LeafLevel,
				vec2, primaryKey2, valueBytes2)
			suite.NoError(err)
		})
		CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, cspann.RootKey, 2)

		RunTransaction(suite.ctx, suite.T(), store, func(txn cspann.Txn) {
			// Add a vector with a duplicate child key.
			err := txn.AddToPartition(suite.ctx, treeKey, cspann.RootKey, cspann.LeafLevel,
				vec3, primaryKey2, valueBytes3)
			suite.NoError(err)
		})
		CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, cspann.RootKey, 2)

		// Fetch back the root partition and validate it.
		partition, err := store.TryGetPartition(suite.ctx, treeKey, cspann.RootKey)
		suite.NoError(err)
		CheckPartitionMetadata(suite.T(), *partition.Metadata(), cspann.LeafLevel,
			vector.T{0, 0}, cspann.PartitionStateDetails{State: cspann.ReadyState})
		suite.Equal([]cspann.ChildKey{primaryKey1, primaryKey2}, partition.ChildKeys())
		suite.Equal([]cspann.ValueBytes{valueBytes1, valueBytes3}, partition.ValueBytes())

		// Create non-root partition.
		partitionKey, partition := suite.createTestPartition(store, treeKey)

		RunTransaction(suite.ctx, suite.T(), store, func(txn cspann.Txn) {
			// Add a vector to the non-root partition.
			err := txn.AddToPartition(suite.ctx, treeKey, partitionKey, cspann.SecondLevel,
				vec4, partitionKey4, valueBytes4)
			suite.NoError(err)
		})
		CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, partitionKey, 4)

		// Update the partition state to DrainingForMerge.
		expected := *partition.Metadata()
		metadata := expected
		metadata.StateDetails.MakeDrainingForMerge(20)
		suite.NoError(store.TryUpdatePartitionMetadata(
			suite.ctx, treeKey, partitionKey, metadata, expected))

		RunTransaction(suite.ctx, suite.T(), store, func(txn cspann.Txn) {
			// Try to add to partition, expect error due to its state.
			var errConditionFailed *cspann.ConditionFailedError
			err = txn.AddToPartition(suite.ctx, treeKey, partitionKey, cspann.SecondLevel,
				vec4, partitionKey4, valueBytes4)
			suite.ErrorAs(err, &errConditionFailed)
			details := cspann.PartitionStateDetails{State: cspann.DrainingForMergeState, Target1: 20}
			CheckPartitionMetadata(suite.T(), errConditionFailed.Actual, cspann.SecondLevel,
				vector.T{4, 3}, details)
		})
	}

	suite.Run("default tree", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		// Ensure that vectors are independent across trees.
		suite.Run("different tree", func() {
			doTest(1)
		})
	}
}

func (suite *StoreTestSuite) TestRemoveFromPartition() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)

		RunTransaction(suite.ctx, suite.T(), store, func(txn cspann.Txn) {
			// Non-root partition does not exist, expect error.
			err := txn.RemoveFromPartition(suite.ctx, treeKey, cspann.PartitionKey(99),
				cspann.LeafLevel, primaryKey1)
			suite.ErrorIs(err, cspann.ErrPartitionNotFound)

			// Root partition does not exist, expect remove to be no-op.
			err = txn.RemoveFromPartition(suite.ctx, treeKey, cspann.RootKey,
				cspann.LeafLevel, primaryKey1)
			suite.NoError(err)

			// Add vector to root partition.
			err = txn.AddToPartition(suite.ctx, treeKey, cspann.RootKey, cspann.LeafLevel,
				vec1, primaryKey1, valueBytes1)
			suite.NoError(err)
		})
		CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, cspann.RootKey, 1)

		// Remove the vector that was added to the root.
		RunTransaction(suite.ctx, suite.T(), store, func(txn cspann.Txn) {
			err := txn.RemoveFromPartition(suite.ctx, treeKey, cspann.RootKey,
				cspann.LeafLevel, primaryKey1)
			suite.NoError(err)
		})
		CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, cspann.RootKey, 0)

		// Create non-root partition.
		partitionKey, partition := suite.createTestPartition(store, treeKey)

		RunTransaction(suite.ctx, suite.T(), store, func(txn cspann.Txn) {
			// Remove vector from non-root partition.
			err := txn.RemoveFromPartition(suite.ctx, treeKey, partitionKey, cspann.SecondLevel,
				partitionKey2)
			suite.NoError(err)
		})
		CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, partitionKey, 2)

		// Update the partition state to DrainingForSplit.
		expected := *partition.Metadata()
		metadata := expected
		metadata.StateDetails.MakeDrainingForSplit(20, 30)
		suite.NoError(store.TryUpdatePartitionMetadata(
			suite.ctx, treeKey, partitionKey, metadata, expected))

		RunTransaction(suite.ctx, suite.T(), store, func(txn cspann.Txn) {
			// Try to remove from partition, expect error due to its state.
			var errConditionFailed *cspann.ConditionFailedError
			err := txn.RemoveFromPartition(suite.ctx, treeKey, partitionKey, cspann.SecondLevel,
				partitionKey3)
			suite.ErrorAs(err, &errConditionFailed)
			details := cspann.PartitionStateDetails{
				State: cspann.DrainingForSplitState, Target1: 20, Target2: 30}
			CheckPartitionMetadata(suite.T(), errConditionFailed.Actual, cspann.SecondLevel,
				vector.T{4, 3}, details)
		})
	}

	suite.Run("default tree", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		// Ensure that vectors are independent across trees.
		suite.Run("different tree", func() {
			doTest(1)
		})
	}
}

// TestSearchMultiplePartitions tests the store's SearchPartitions method.
func (suite *StoreTestSuite) TestSearchPartitions() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)

		// Create non-root partition with some vectors in it.
		testPartitionKey, testPartition := suite.createTestPartition(store, treeKey)

		// Create another partition.
		testPartitionKey2 := cspann.PartitionKey(20)
		metadata := cspann.PartitionMetadata{
			Level:    cspann.SecondLevel,
			Centroid: vector.T{2, 4},
		}
		metadata.StateDetails.MakeSplitting(10, 20)
		suite.NoError(store.TryCreateEmptyPartition(suite.ctx, treeKey, testPartitionKey2, metadata))
		vectors := vector.MakeSet(2)
		vectors.Add(vec4)
		vectors.Add(vec5)
		childKeys := []cspann.ChildKey{partitionKey4, partitionKey5}
		valueBytes := []cspann.ValueBytes{valueBytes4, valueBytes5}
		added, err := store.TryAddToPartition(
			suite.ctx, treeKey, testPartitionKey2, vectors, childKeys, valueBytes, metadata)
		suite.NoError(err)
		suite.True(added)

		suite.NoError(store.RunTransaction(suite.ctx, func(txn cspann.Txn) error {
			searchSet := cspann.SearchSet{MaxResults: 2}
			toSearch := []cspann.PartitionToSearch{
				{Key: testPartitionKey},
				{Key: cspann.PartitionKey(99)}, // Partition does not exist.
				{Key: testPartitionKey2},
			}
			err := txn.SearchPartitions(suite.ctx, treeKey, toSearch, vector.T{6, 1}, &searchSet)
			suite.NoError(err)

			// Validate partition info.
			suite.Equal(cspann.SecondLevel, toSearch[0].Level)
			suite.Equal(testPartition.Metadata().StateDetails, toSearch[0].StateDetails)
			suite.Equal(3, toSearch[0].Count)

			suite.Equal(cspann.InvalidLevel, toSearch[1].Level)
			suite.Equal(cspann.PartitionStateDetails{}, toSearch[1].StateDetails)
			suite.Equal(0, toSearch[1].Count)

			suite.Equal(cspann.SecondLevel, toSearch[2].Level)
			suite.Equal(metadata.StateDetails, toSearch[2].StateDetails)
			suite.Equal(2, toSearch[2].Count)

			// Validate search results.
			result1 := cspann.SearchResult{
				QueryDistance: 4.2, ErrorBound: 50.99,
				ParentPartitionKey: testPartitionKey2, ChildKey: partitionKey4, ValueBytes: valueBytes4}
			result2 := cspann.SearchResult{
				QueryDistance: 8, ErrorBound: 0,
				ParentPartitionKey: testPartitionKey, ChildKey: partitionKey3, ValueBytes: valueBytes3}
			suite.Equal(cspann.SearchResults{result1, result2}, RoundResults(searchSet.PopResults(), 2))

			return nil
		}))
	}

	suite.Run("default tree", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		// Ensure that different tree is independent.
		suite.Run("different tree", func() {
			doTest(1)
		})
	}
}

// TestGetFullVectors tests the GetFullVectors method on the store, fetching
// vectors by primary key and centroids by partition key.
func (suite *StoreTestSuite) TestGetFullVectors() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	doTest := func(treeID int) {
		// Create partitions.
		treeKey := store.MakeTreeKey(suite.T(), treeID)
		metadata := cspann.PartitionMetadata{
			Level:    cspann.SecondLevel,
			Centroid: vector.T{0, 0},
		}
		metadata.StateDetails.MakeSplitting(20, 30)
		suite.NoError(store.TryCreateEmptyPartition(suite.ctx, treeKey, cspann.RootKey, metadata))
		partitionKey, _ := suite.createTestPartition(store, treeKey)

		RunTransaction(suite.ctx, suite.T(), store, func(txn cspann.Txn) {
			// Empty request set.
			err := txn.GetFullVectors(suite.ctx, treeKey, []cspann.VectorWithKey{})
			suite.NoError(err)

			// Insert some full vectors into the test store.
			key1 := store.InsertVector(suite.T(), treeID, vec1)
			key2 := store.InsertVector(suite.T(), treeID, vec2)
			key3 := store.InsertVector(suite.T(), treeID, vec3)

			// Start by fetching partition keys, both that exist and that do not.
			results := []cspann.VectorWithKey{
				{Key: cspann.ChildKey{PartitionKey: cspann.RootKey}},
				{Key: cspann.ChildKey{PartitionKey: cspann.PartitionKey(99)}}, // No such partition.
				{Key: cspann.ChildKey{PartitionKey: partitionKey}},
			}
			err = txn.GetFullVectors(suite.ctx, treeKey, results)
			suite.NoError(err)
			suite.Equal(vector.T{0, 0}, results[0].Vector)
			suite.Nil(results[1].Vector)
			suite.Equal(vector.T{4, 3}, results[2].Vector)

			// Next fetch primary keys that reference vectors that exist and that
			// do not exist.
			results = []cspann.VectorWithKey{
				{Key: cspann.ChildKey{KeyBytes: key1}},
				{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
				{Key: cspann.ChildKey{KeyBytes: key2}},
				{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
				{Key: cspann.ChildKey{KeyBytes: key3}},
			}
			err = txn.GetFullVectors(suite.ctx, treeKey, results)
			suite.NoError(err)
			suite.Equal(vec1, results[0].Vector)
			suite.Nil(results[1].Vector)
			suite.Equal(vec2, results[2].Vector)
			suite.Nil(results[3].Vector)
			suite.Equal(vec3, results[4].Vector)

			// Grab another set of vectors to ensure that saved state is properly reset.
			results = []cspann.VectorWithKey{
				{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
				{Key: cspann.ChildKey{KeyBytes: key3}},
				{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
				{Key: cspann.ChildKey{KeyBytes: key2}},
				{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
				{Key: cspann.ChildKey{KeyBytes: key1}},
			}
			err = txn.GetFullVectors(suite.ctx, treeKey, results)
			suite.NoError(err)
			suite.Nil(results[0].Vector)
			suite.Equal(vec3, results[1].Vector)
			suite.Nil(results[2].Vector)
			suite.Equal(vec2, results[3].Vector)
			suite.Nil(results[4].Vector)
			suite.Equal(vec1, results[5].Vector)
		})
	}

	suite.Run("default tree", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		// Ensure that vectors are independent across trees.
		suite.Run("different tree", func() {
			doTest(1)
		})
	}
}

func (suite *StoreTestSuite) TestEstimatePartitionCount() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)

		// Partition does not yet exist.
		count, err := store.EstimatePartitionCount(suite.ctx, treeKey, cspann.PartitionKey(99))
		suite.NoError(err)
		suite.Equal(0, count)

		// Create partition with some vectors in it.
		partitionKey, partition := suite.createTestPartition(store, treeKey)
		count, err = store.EstimatePartitionCount(suite.ctx, treeKey, partitionKey)
		suite.NoError(err)
		suite.Equal(partition.Count(), count)
	}

	suite.Run("default tree", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		// Ensure that vectors are independent across trees.
		suite.Run("different tree", func() {
			doTest(1)
		})
	}
}

func (suite *StoreTestSuite) TestTryCreateEmptyPartition() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)
		partitionKey := cspann.PartitionKey(10)
		centroid := vector.T{4, 3}

		// Create empty partition.
		metadata := cspann.PartitionMetadata{
			Level:    cspann.SecondLevel,
			Centroid: centroid,
		}
		metadata.StateDetails.MakeSplitting(20, 30)
		suite.NoError(store.TryCreateEmptyPartition(suite.ctx, treeKey, partitionKey, metadata))

		// Fetch back the partition and validate it.
		partition, err := store.TryGetPartition(suite.ctx, treeKey, partitionKey)
		suite.NoError(err)
		suite.Equal(0, partition.Count())
		suite.True(partition.Metadata().Equal(&metadata))

		// Update partition metadata.
		expected := metadata
		metadata.StateDetails.State = cspann.ReadyState
		suite.NoError(store.TryUpdatePartitionMetadata(
			suite.ctx, treeKey, partitionKey, metadata, expected))

		// Try to create empty partition when it already exists. Expect to get
		// a ConditionFailedError and the updated metadata.
		var errConditionFailed *cspann.ConditionFailedError
		err = store.TryCreateEmptyPartition(
			suite.ctx, treeKey, partitionKey, cspann.PartitionMetadata{})
		suite.ErrorAs(err, &errConditionFailed)
		suite.True(errConditionFailed.Actual.Equal(&metadata))
	}

	suite.Run("default tree", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		// Ensure that vectors are independent across trees.
		suite.Run("different tree", func() {
			doTest(1)
		})
	}
}

func (suite *StoreTestSuite) TestTryDeletePartition() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)

		// Partition does not yet exist.
		err := store.TryDeletePartition(suite.ctx, treeKey, cspann.PartitionKey(99))
		suite.ErrorIs(err, cspann.ErrPartitionNotFound)

		// Create partition with some vectors in it.
		partitionKey, partition := suite.createTestPartition(store, treeKey)

		// Delete the partition.
		suite.NoError(store.TryDeletePartition(suite.ctx, treeKey, partitionKey))

		// Ensure the partition is really deleted.
		_, err = store.TryGetPartition(suite.ctx, treeKey, partitionKey)
		suite.ErrorIs(err, cspann.ErrPartitionNotFound)

		// Add should not work either.
		added, err := store.TryAddToPartition(suite.ctx, treeKey, partitionKey,
			vector.MakeSet(2), []cspann.ChildKey{}, []cspann.ValueBytes{}, *partition.Metadata())
		suite.ErrorIs(err, cspann.ErrPartitionNotFound)
		suite.False(added)
	}

	suite.Run("default tree", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		// Ensure that vectors are independent across trees.
		suite.Run("different tree", func() {
			doTest(1)
		})
	}
}

func (suite *StoreTestSuite) TestTryGetPartition() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)
		partitionKey := cspann.PartitionKey(10)
		centroid := vector.T{4, 3}

		// Partition does not yet exist.
		_, err := store.TryGetPartition(suite.ctx, treeKey, partitionKey)
		suite.ErrorIs(err, cspann.ErrPartitionNotFound)

		// Create partition with some vectors in it.
		metadata := cspann.PartitionMetadata{
			Level:    cspann.LeafLevel,
			Centroid: centroid,
		}
		metadata.StateDetails.MakeUpdating(20)
		suite.NoError(store.TryCreateEmptyPartition(suite.ctx, treeKey, partitionKey, metadata))
		vectors := vector.MakeSet(2)
		vectors.Add(vec1)
		vectors.Add(vec2)
		vectors.Add(vec3)
		childKeys := []cspann.ChildKey{primaryKey1, primaryKey2, primaryKey3}
		valueBytes := []cspann.ValueBytes{valueBytes1, valueBytes2, valueBytes3}
		added, err := store.TryAddToPartition(
			suite.ctx, treeKey, partitionKey, vectors, childKeys, valueBytes, metadata)
		suite.NoError(err)
		suite.True(added)

		// Fetch back the partition and validate it.
		partition, err := store.TryGetPartition(suite.ctx, treeKey, partitionKey)
		suite.NoError(err)
		suite.True(partition.Metadata().Equal(&metadata))
		suite.Equal(cspann.LeafLevel, partition.Level())
		suite.Equal(childKeys, partition.ChildKeys())
		suite.Equal(valueBytes, partition.ValueBytes())
		suite.Equal(centroid, partition.Centroid())
	}

	suite.Run("default tree", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		// Ensure that vectors are independent across trees.
		suite.Run("different tree", func() {
			doTest(1)
		})
	}
}

// TestTryGetPartitionMetadata tests the Store's TryGetPartitionMetadata method.
func (suite *StoreTestSuite) TestTryGetPartitionMetadata() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)

		// Create two partition with vectors in them.
		partitionKey1, partition1 := suite.createTestPartition(store, treeKey)
		partitionKey2, partition2 := suite.createTestPartition(store, treeKey)

		// Fetch metadata for the partitions, along with one that doesn't exist.
		toGet := []cspann.PartitionMetadataToGet{
			{Key: partitionKey1},
			{Key: cspann.PartitionKey(9999)},
			{Key: partitionKey2},
		}
		err := store.TryGetPartitionMetadata(suite.ctx, treeKey, toGet)
		suite.NoError(err)

		// Validate that partition 9999 does not exist.
		suite.Equal(cspann.PartitionMetadata{}, toGet[1].Metadata)

		// Validate metadata for other partitions.
		suite.True(partition1.Metadata().Equal(&toGet[0].Metadata))
		suite.True(partition2.Metadata().Equal(&toGet[2].Metadata))

		// Update the metadata and verify we get the updated values.
		expected := toGet[0].Metadata
		metadata := expected
		metadata.StateDetails.MakeUpdating(30)
		suite.NoError(store.TryUpdatePartitionMetadata(
			suite.ctx, treeKey, partitionKey1, metadata, expected))

		// Fetch updated metadata and validate.
		err = store.TryGetPartitionMetadata(suite.ctx, treeKey, toGet[:1])
		suite.NoError(err)
		suite.True(toGet[0].Metadata.Equal(&metadata))
	}

	suite.Run("default tree", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		// Ensure that vectors are independent across trees.
		suite.Run("different tree", func() {
			doTest(1)
		})
	}
}

func (suite *StoreTestSuite) TestTryUpdatePartitionMetadata() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)

		err := store.TryUpdatePartitionMetadata(suite.ctx, treeKey, cspann.PartitionKey(99),
			cspann.PartitionMetadata{}, cspann.PartitionMetadata{})
		suite.ErrorIs(err, cspann.ErrPartitionNotFound)

		// Create partition with some vectors in it.
		partitionKey, partition := suite.createTestPartition(store, treeKey)

		// Try to update the metadata with mismatched expected metadata.
		var errConditionFailed *cspann.ConditionFailedError
		expected := *partition.Metadata()
		metadata := expected
		metadata.StateDetails.State = cspann.MergingState
		metadata.StateDetails.Source = 30
		err = store.TryUpdatePartitionMetadata(
			suite.ctx, treeKey, partitionKey, metadata, metadata)
		suite.ErrorAs(err, &errConditionFailed)
		suite.True(errConditionFailed.Actual.Equal(&expected))

		// Try again, this time with the right expected metadata.
		suite.NoError(store.TryUpdatePartitionMetadata(
			suite.ctx, treeKey, partitionKey, metadata, expected))

		// Validate that correct metadata was stored.
		partition, err = store.TryGetPartition(suite.ctx, treeKey, partitionKey)
		suite.NoError(err)
		suite.True(partition.Metadata().Equal(&metadata))
	}

	suite.Run("default tree", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		// Ensure that vectors are independent across trees.
		suite.Run("different tree", func() {
			doTest(1)
		})
	}
}

func (suite *StoreTestSuite) TestTryAddToPartition() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)
		partitionKey := cspann.PartitionKey(10)
		centroid := vector.T{4, 3}

		// Partition does not yet exist.
		metadata := cspann.PartitionMetadata{
			Level:    cspann.LeafLevel,
			Centroid: centroid,
		}
		metadata.StateDetails.MakeUpdating(20)
		addVectors := vector.MakeSet(2)
		addVectors.Add(vec1)
		addVectors.Add(vec2)
		addChildKeys := []cspann.ChildKey{primaryKey1, primaryKey2}
		addValueBytes := []cspann.ValueBytes{valueBytes1, valueBytes2}
		added, err := store.TryAddToPartition(
			suite.ctx, treeKey, partitionKey, addVectors, addChildKeys, addValueBytes, metadata)
		suite.ErrorIs(err, cspann.ErrPartitionNotFound)
		suite.False(added)

		// Create empty partition.
		suite.NoError(store.TryCreateEmptyPartition(suite.ctx, treeKey, partitionKey, metadata))

		// Now add should work.
		added, err = store.TryAddToPartition(
			suite.ctx, treeKey, partitionKey, addVectors, addChildKeys, addValueBytes, metadata)
		suite.NoError(err)
		suite.True(added)

		// Fetch back the partition and validate it.
		partition, err := store.TryGetPartition(suite.ctx, treeKey, partitionKey)
		suite.NoError(err)
		suite.True(partition.Metadata().Equal(&metadata))
		suite.Equal(cspann.LeafLevel, partition.Level())
		suite.Equal(addChildKeys, partition.ChildKeys())
		suite.Equal(addValueBytes, partition.ValueBytes())
		suite.Equal(centroid, partition.Centroid())

		// Try to add vectors, but with mismatched expected metadata.
		// Also, use duplicate keys, both within the batch and with previous
		// batch. Note that we're using primaryKey1, but with vec4 and valueBytes4,
		// to test that the primaryKey1 value is not overwritten.
		addVectors2 := vector.MakeSet(2)
		addVectors2.Add(vec3)
		addVectors2.Add(vec3)
		addVectors2.Add(vec4)
		addVectors2.Add(vec5)
		addChildKeys2 := []cspann.ChildKey{primaryKey3, primaryKey3, primaryKey1, primaryKey5}
		addValueBytes2 := []cspann.ValueBytes{valueBytes3, valueBytes3, valueBytes4, valueBytes5}

		var errConditionFailed *cspann.ConditionFailedError
		expected := metadata
		metadata.Level = cspann.SecondLevel
		added, err = store.TryAddToPartition(
			suite.ctx, treeKey, partitionKey, addVectors2, addChildKeys2, addValueBytes2, metadata)
		suite.ErrorAs(err, &errConditionFailed)
		suite.False(added)
		suite.True(errConditionFailed.Actual.Equal(&expected))

		// Try again, this time with correct expected metadata.
		added, err = store.TryAddToPartition(
			suite.ctx, treeKey, partitionKey, addVectors2, addChildKeys2, addValueBytes2, expected)
		suite.NoError(err)
		suite.True(added)

		// Fetch back the partition and validate it. The duplicate vector should
		// not have been added.
		expectVectors := vector.MakeSet(2)
		expectVectors.Add(vec1)
		expectVectors.Add(vec2)
		expectVectors.Add(vec3)
		expectVectors.Add(vec5)
		expectChildKeys := []cspann.ChildKey{primaryKey1, primaryKey2, primaryKey3, primaryKey5}
		expectValueBytes := []cspann.ValueBytes{valueBytes1, valueBytes2, valueBytes3, valueBytes5}
		partition, err = store.TryGetPartition(suite.ctx, treeKey, partitionKey)
		suite.NoError(err)
		suite.True(partition.Metadata().Equal(&expected))
		suite.Equal(cspann.LeafLevel, partition.Level())
		suite.Equal(expectChildKeys, partition.ChildKeys())
		suite.Equal(expectValueBytes, partition.ValueBytes())
		suite.Equal(centroid, partition.Centroid())

		// Try to add only duplicate vectors.
		addVectors3 := vector.MakeSet(2)
		addVectors3.Add(vec1)
		addVectors3.Add(vec3)
		addVectors3.Add(vec3)
		addVectors3.Add(vec5)
		addChildKeys3 := []cspann.ChildKey{primaryKey1, primaryKey3, primaryKey3, primaryKey5}
		addValueBytes3 := []cspann.ValueBytes{valueBytes1, valueBytes3, valueBytes3, valueBytes5}
		added, err = store.TryAddToPartition(
			suite.ctx, treeKey, partitionKey, addVectors3, addChildKeys3, addValueBytes3, expected)
		suite.NoError(err)
		suite.False(added)
	}

	suite.Run("default tree", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		// Ensure that vectors are independent across trees.
		suite.Run("different tree", func() {
			doTest(1)
		})
	}
}

func (suite *StoreTestSuite) TestTryRemoveFromPartition() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)

		// Partition does not yet exist.
		childKeys := []cspann.ChildKey{partitionKey1}
		removed, err := store.TryRemoveFromPartition(suite.ctx, treeKey, cspann.PartitionKey(99),
			childKeys, cspann.PartitionMetadata{})
		suite.ErrorIs(err, cspann.ErrPartitionNotFound)
		suite.False(removed)

		// Create partition with some vectors.
		partitionKey, partition := suite.createTestPartition(store, treeKey)

		// Now remove should work.
		expected := *partition.Metadata()
		removed, err = store.TryRemoveFromPartition(suite.ctx, treeKey, partitionKey,
			childKeys, expected)
		suite.NoError(err)
		suite.True(removed)

		// Fetch back the partition and validate it.
		partition, err = store.TryGetPartition(suite.ctx, treeKey, partitionKey)
		suite.NoError(err)
		suite.True(partition.Metadata().Equal(&expected))
		suite.Equal(cspann.SecondLevel, partition.Level())
		suite.Equal(vector.T{4, 3}, partition.Centroid())
		childKeys = partition.ChildKeys()
		suite.Len(childKeys, 2)
		valueBytes := partition.ValueBytes()
		suite.Len(valueBytes, 2)
		if childKeys[0].PartitionKey == partitionKey3.PartitionKey {
			// Sort the keys and values.
			childKeys[0], childKeys[1] = childKeys[1], childKeys[0]
			valueBytes[0], valueBytes[1] = valueBytes[1], valueBytes[0]
		}
		suite.Equal([]cspann.ChildKey{partitionKey2, partitionKey3}, childKeys)
		suite.Equal([]cspann.ValueBytes{valueBytes2, valueBytes3}, valueBytes)

		// Try to remove non-existent vector, but with mismatched expected metadata.
		var errConditionFailed *cspann.ConditionFailedError
		childKeys = []cspann.ChildKey{partitionKey1}
		metadata := expected
		metadata.StateDetails.State = cspann.DrainingForMergeState
		removed, err = store.TryRemoveFromPartition(suite.ctx, treeKey, partitionKey,
			childKeys, metadata)
		suite.ErrorAs(err, &errConditionFailed)
		suite.False(removed)
		suite.True(errConditionFailed.Actual.Equal(&expected))

		// Try again, this time with correct expected metadata.
		removed, err = store.TryRemoveFromPartition(suite.ctx, treeKey, partitionKey,
			childKeys, expected)
		suite.NoError(err)
		suite.False(removed) // no vectors removed

		// Remove the last remaining vectors in the partition.
		childKeys = []cspann.ChildKey{partitionKey2, partitionKey3}
		removed, err = store.TryRemoveFromPartition(suite.ctx, treeKey, partitionKey,
			childKeys, expected)
		suite.NoError(err)
		suite.True(removed)

		// Verify that partition contains no remaining vectors.
		partition, err = store.TryGetPartition(suite.ctx, treeKey, partitionKey)
		suite.NoError(err)
		suite.Equal(0, partition.Count())
	}

	suite.Run("default tree", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		// Ensure that vectors are independent across trees.
		suite.Run("different tree", func() {
			doTest(1)
		})
	}
}

func (suite *StoreTestSuite) TestTryClearPartition() {
	store := suite.makeStore(suite.quantizer)
	defer store.Close(suite.T())

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)

		// Partition does not yet exist.
		_, err := store.TryClearPartition(suite.ctx, treeKey, cspann.PartitionKey(99),
			cspann.PartitionMetadata{})
		suite.ErrorIs(err, cspann.ErrPartitionNotFound)

		// Create partition with some vectors.
		partitionKey, partition := suite.createTestPartition(store, treeKey)

		// Now clear should work.
		expected := *partition.Metadata()
		count, err := store.TryClearPartition(suite.ctx, treeKey, partitionKey, expected)
		suite.NoError(err)
		suite.Equal(3, count)

		// Fetch back the partition and validate it.
		partition, err = store.TryGetPartition(suite.ctx, treeKey, partitionKey)
		suite.NoError(err)
		suite.True(partition.Metadata().Equal(&expected))
		suite.Equal(cspann.SecondLevel, partition.Level())
		suite.Equal(vector.T{4, 3}, partition.Centroid())
		suite.Len(partition.ChildKeys(), 0)
		suite.Len(partition.ValueBytes(), 0)

		// Try to clear with mismatched expected metadata.
		var errConditionFailed *cspann.ConditionFailedError
		metadata := expected
		metadata.StateDetails.State = cspann.DrainingForMergeState
		_, err = store.TryClearPartition(suite.ctx, treeKey, partitionKey, metadata)
		suite.ErrorAs(err, &errConditionFailed)
		suite.True(errConditionFailed.Actual.Equal(&expected))

		// Try again, this time with correct expected metadata.
		count, err = store.TryClearPartition(suite.ctx, treeKey, partitionKey, expected)
		suite.NoError(err)
		suite.Equal(0, count)
	}

	suite.Run("default tree", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		// Ensure that vectors are independent across trees.
		suite.Run("different tree", func() {
			doTest(1)
		})
	}
}

// Create a test partition with some vectors in it.
func (suite *StoreTestSuite) createTestPartition(
	store TestStore, treeKey cspann.TreeKey,
) (cspann.PartitionKey, *cspann.Partition) {
	partitionKey := suite.nextPartitionKey
	suite.nextPartitionKey++
	metadata := cspann.MakeReadyPartitionMetadata(cspann.SecondLevel, vector.T{4, 3})
	suite.NoError(store.TryCreateEmptyPartition(suite.ctx, treeKey, partitionKey, metadata))
	vectors := vector.MakeSet(2)
	vectors.Add(vec1)
	vectors.Add(vec2)
	vectors.Add(vec3)
	childKeys := []cspann.ChildKey{partitionKey1, partitionKey2, partitionKey3}
	valueBytes := []cspann.ValueBytes{valueBytes1, valueBytes2, valueBytes3}
	added, err := store.TryAddToPartition(
		suite.ctx, treeKey, partitionKey, vectors, childKeys, valueBytes, metadata)
	suite.NoError(err)
	suite.True(added)

	// Get the partition.
	partition, err := store.TryGetPartition(suite.ctx, treeKey, partitionKey)
	suite.NoError(err)
	return partitionKey, partition
}
