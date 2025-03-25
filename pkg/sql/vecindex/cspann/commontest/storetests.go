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
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"
)

type equaler interface {
	Equal(that interface{}) bool
}

var vec1 = vector.T{1, 2}
var vec2 = vector.T{7, 4}
var vec3 = vector.T{4, 3}
var vec4 = vector.T{6, -2}

var primaryKey1 = cspann.ChildKey{KeyBytes: cspann.KeyBytes{1, 00}}
var primaryKey2 = cspann.ChildKey{KeyBytes: cspann.KeyBytes{2, 00}}
var primaryKey3 = cspann.ChildKey{KeyBytes: cspann.KeyBytes{3, 00}}
var primaryKey4 = cspann.ChildKey{KeyBytes: cspann.KeyBytes{4, 00}}

var partitionKey1 = cspann.ChildKey{PartitionKey: 10}
var partitionKey2 = cspann.ChildKey{PartitionKey: 20}
var partitionKey3 = cspann.ChildKey{PartitionKey: 30}

var valueBytes1 = cspann.ValueBytes{1, 2}
var valueBytes2 = cspann.ValueBytes{3, 4}
var valueBytes3 = cspann.ValueBytes{5, 6}
var valueBytes4 = cspann.ValueBytes{7, 8}

// TestStore wraps a Store interface so that it can be tested by a common set of
// tests.
type TestStore interface {
	cspann.Store

	// AllowMultipleTrees is true if the Store supports storing vectors in
	// multiple distinct trees that are identified by the cspann.TreeKey
	// parameter. This is used by prefixed vector indexes, e.g. where non-vector
	// column(s) precede the vector column in the index definition.
	AllowMultipleTrees() bool

	// SupportsTry is true if this Store supports the Try methods on cspann.Store.
	SupportsTry() bool

	// MakeTreeKey converts a tree identifier into a tree key. The treeID is
	// convenient for testing, but it must be converted into a TreeKey in a way
	// that is specific to each store.
	MakeTreeKey(t *testing.T, treeID int) cspann.TreeKey

	// InsertVector inserts a vector into the store and returns the primary key
	// bytes that can be used to retrieve that vector via GetFullVectors.
	InsertVector(t *testing.T, treeID int, vec vector.T) cspann.KeyBytes
}

// MakeStoreFunc defines a function that creates a new TestStore instance for
// use by the common Store tests.
type MakeStoreFunc func(quantizer quantize.Quantizer) TestStore

type StoreTestSuite struct {
	suite.Suite

	ctx           context.Context
	workspace     workspace.T
	makeStore     MakeStoreFunc
	rootQuantizer quantize.Quantizer
	quantizer     quantize.Quantizer
}

// NewStoreTestSuite constructs a new suite of tests that run against
// implementations of the cspann.Store interface. Implementations do not need to
// have their own tests; instead, they can be validated using a common set of
// tests.
func NewStoreTestSuite(ctx context.Context, makeStore MakeStoreFunc) *StoreTestSuite {
	return &StoreTestSuite{
		ctx:           ctx,
		makeStore:     makeStore,
		rootQuantizer: quantize.NewUnQuantizer(2),
		quantizer:     quantize.NewRaBitQuantizer(2, 42)}
}

func (suite *StoreTestSuite) TestRunTransaction() {
	store := suite.makeStore(suite.quantizer)

	rootVec := vector.T{1, 2}
	rootChildKey := cspann.ChildKey{KeyBytes: cspann.KeyBytes{10, 20}}
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
		_, err := tx.SearchPartitions(suite.ctx, treeKey, toSearch, vector.T{1, -1}, &searchSet)
		suite.NoError(err)
		suite.Equal(1, toSearch[0].Count)
		return errors.New("abort")
	}), "abort")
}

// TestRootPartition runs tests against the root partition, which has special
// rules as compared to other partitions.
func (suite *StoreTestSuite) TestRootPartition() {
	store := suite.makeStore(suite.quantizer)

	// Test missing root partition.
	suite.testEmptyOrMissingRoot(store, 0, "missing root partition")

	rootVec := vector.T{1, 2}
	rootChildKey := cspann.ChildKey{KeyBytes: cspann.KeyBytes{10, 20}}
	treeKey := store.MakeTreeKey(suite.T(), 0)

	suite.Run("add vector to root", func() {
		suite.runInTransaction(store, func(tx cspann.Txn) {
			val := cspann.ValueBytes{100, 200}
			err := tx.AddToPartition(
				suite.ctx, treeKey, cspann.RootKey, cspann.LeafLevel, rootVec, rootChildKey, val)
			suite.NoError(err)
		})
		CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, cspann.RootKey, 1)
	})

	if store.AllowMultipleTrees() {
		// Other trees' root partitions should still be missing.
		suite.testEmptyOrMissingRoot(store, 1, "missing root partition, different tree")
	}

	// Now remove the only vector from the root partition, making it empty, but
	// not missing.
	suite.Run("remove only vector from root", func() {
		suite.runInTransaction(store, func(tx cspann.Txn) {
			err := tx.RemoveFromPartition(
				suite.ctx, treeKey, cspann.RootKey, cspann.LeafLevel, rootChildKey)
			suite.NoError(err)
		})
		CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, cspann.RootKey, 0)
	})

	// Test empty root partition.
	suite.testEmptyOrMissingRoot(store, 0, "empty root partition")

	// Now add vectors to root partition and test it.
	suite.addToRoot(store, 0)
	suite.Run("test root partition", func() {
		suite.testLeafPartition(store, 0, cspann.RootKey, vector.T{0, 0})
	})

	if store.AllowMultipleTrees() {
		// Other root partitions should be independent.
		suite.addToRoot(store, 1)
		suite.Run("test root partition, different tree", func() {
			suite.testLeafPartition(store, 1, cspann.RootKey, vector.T{0, 0})
		})
	}

	// Replace the entire root partition with a new set of vectors.
	suite.Run("replace root partition", func() {
		suite.setRootPartition(store, 0)
	})

	if store.AllowMultipleTrees() {
		suite.Run("replace root partition, different tree", func() {
			suite.setRootPartition(store, 1)
		})
	}

	// Delete the root partition and re-test.
	suite.Run("deleted root partition", func() {
		suite.deletePartition(store, 0, cspann.RootKey)
		suite.testEmptyOrMissingRoot(store, 0, "deleted root partition")
	})

	if store.AllowMultipleTrees() {
		suite.Run("deleted root partition, different tree", func() {
			suite.deletePartition(store, 1, cspann.RootKey)
			suite.testEmptyOrMissingRoot(store, 1, "deleted root partition")
		})
	}
}

// TestNonRootPartition tests non-root partitions at interior and leaf levels.
func (suite *StoreTestSuite) TestNonRootPartition() {
	store := suite.makeStore(suite.quantizer)

	// Construct non-root leaf partition and add/remove vectors.
	suite.Run("test non-root leaf partition", func() {
		partitionKey := suite.insertLeafPartition(store, 0)
		suite.testLeafPartition(store, 0, partitionKey, vector.T{4, 3})
		suite.deletePartition(store, 0, partitionKey)
	})

	if store.AllowMultipleTrees() {
		suite.Run("test non-root leaf partition, different tree", func() {
			partitionKey := suite.insertLeafPartition(store, 1)
			suite.testLeafPartition(store, 1, partitionKey, vector.T{4, 3})
			suite.deletePartition(store, 1, partitionKey)
		})
	}

	doTest := func(treeID int) {
		tx := BeginTransaction(suite.ctx, suite.T(), store)
		defer CommitTransaction(suite.ctx, suite.T(), store, tx)
		treeKey := store.MakeTreeKey(suite.T(), treeID)

		vectors := vector.MakeSet(2)
		vectors.Add(vec1)
		vectors.Add(vec2)
		quantizedSet := suite.quantizer.Quantize(&suite.workspace, vectors)
		childKeys := []cspann.ChildKey{partitionKey1, partitionKey2}
		valueBytes := []cspann.ValueBytes{valueBytes1, valueBytes2}
		metadata := cspann.PartitionMetadata{
			Level: cspann.SecondLevel, Centroid: quantizedSet.GetCentroid()}
		partition := cspann.NewPartition(
			metadata, suite.quantizer, quantizedSet, childKeys, valueBytes)

		partitionKey, err := tx.InsertPartition(suite.ctx, treeKey, partition)
		suite.NoError(err)

		// Read back and verify the partition.
		readPartition, err := tx.GetPartition(suite.ctx, treeKey, partitionKey)
		suite.NoError(err)
		ValidatePartitionsEqual(suite.T(), partition, readPartition)

		// Add and remove vectors from partition.
		err = tx.AddToPartition(
			suite.ctx, treeKey, partitionKey, partition.Level(), vec3, partitionKey3, valueBytes3)
		suite.NoError(err)
		err = tx.RemoveFromPartition(suite.ctx, treeKey, partitionKey, partition.Level(), partitionKey1)
		suite.NoError(err)

		// Search partition.
		toSearch := []cspann.PartitionToSearch{{Key: partitionKey}}
		searchSet := cspann.SearchSet{MaxResults: 1}
		searchLevel, err := tx.SearchPartitions(suite.ctx, treeKey, toSearch, vector.T{5, -1}, &searchSet)
		suite.NoError(err)
		suite.Equal(cspann.SecondLevel, searchLevel)
		result1 := cspann.SearchResult{
			QuerySquaredDistance: 17, ErrorBound: 0, CentroidDistance: 0,
			ParentPartitionKey: partitionKey, ChildKey: partitionKey3, ValueBytes: valueBytes3}
		results := searchSet.PopResults()
		RoundResults(results, 4)
		suite.Equal(cspann.SearchResults{result1}, results)
		suite.Equal(2, toSearch[0].Count)
	}

	suite.Run("test non-root interior partition", func() {
		doTest(0)
	})

	if store.AllowMultipleTrees() {
		suite.Run("test non-root interior partition, different tree", func() {
			doTest(1)
		})
	}
}

// TestGetFullVectors tests the GetFullVectors method on the store, fetching
// vectors by primary key and centroids by partition key.
func (suite *StoreTestSuite) TestGetFullVectors() {
	store := suite.makeStore(suite.quantizer)

	doTest := func(treeID int) {
		// Create partitions.
		suite.addToRoot(store, treeID)
		partitionKey := suite.insertLeafPartition(store, treeID)

		tx := BeginTransaction(suite.ctx, suite.T(), store)
		defer CommitTransaction(suite.ctx, suite.T(), store, tx)
		treeKey := store.MakeTreeKey(suite.T(), treeID)

		// Insert some full vectors into the test store.
		key1 := store.InsertVector(suite.T(), treeID, vec1)
		key2 := store.InsertVector(suite.T(), treeID, vec2)
		key3 := store.InsertVector(suite.T(), treeID, vec3)

		// Include primary keys, partition keys, and keys that cannot be found.
		results := []cspann.VectorWithKey{
			{Key: cspann.ChildKey{KeyBytes: key1}},
			{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
			{Key: cspann.ChildKey{PartitionKey: cspann.RootKey}},
			{Key: cspann.ChildKey{KeyBytes: key2}},
			{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
			{Key: cspann.ChildKey{PartitionKey: partitionKey}},
			{Key: cspann.ChildKey{KeyBytes: key3}},
		}
		err := tx.GetFullVectors(suite.ctx, treeKey, results)
		suite.NoError(err)
		suite.Equal(vec1, results[0].Vector)
		suite.Nil(results[1].Vector)
		suite.Equal(vector.T{0, 0}, results[2].Vector)
		suite.Equal(vec2, results[3].Vector)
		suite.Nil(results[4].Vector)
		suite.Equal(vector.T{4, 3}, results[5].Vector)
		suite.Equal(vec3, results[6].Vector)

		// Grab another set of vectors to ensure that saved state is properly reset.
		results = []cspann.VectorWithKey{
			{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
			{Key: cspann.ChildKey{KeyBytes: key3}},
			{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
			{Key: cspann.ChildKey{KeyBytes: key2}},
			{Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes{0}}},
			{Key: cspann.ChildKey{KeyBytes: key1}},
		}
		err = tx.GetFullVectors(suite.ctx, treeKey, results)
		suite.NoError(err)
		suite.Nil(results[0].Vector)
		suite.Equal(vec3, results[1].Vector)
		suite.Nil(results[2].Vector)
		suite.Equal(vec2, results[3].Vector)
		suite.Nil(results[4].Vector)
		suite.Equal(vec1, results[5].Vector)
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

// TestSearchMultiplePartitions tests the store's SearchPartitions method across
// more than one partition.
func (suite *StoreTestSuite) TestSearchMultiplePartitions() {
	store := suite.makeStore(suite.quantizer)

	doTest := func(treeID int) {
		// Create some partitions to search.
		suite.addToRoot(store, treeID)
		partitionKey := suite.insertLeafPartition(store, treeID)

		tx := BeginTransaction(suite.ctx, suite.T(), store)
		defer CommitTransaction(suite.ctx, suite.T(), store, tx)
		treeKey := store.MakeTreeKey(suite.T(), treeID)

		// Remove a vector from the non-root partition so they are not the same.
		err := tx.RemoveFromPartition(suite.ctx, treeKey, partitionKey, cspann.LeafLevel, primaryKey3)
		suite.NoError(err)

		searchSet := cspann.SearchSet{MaxResults: 2}
		toSearch := []cspann.PartitionToSearch{{Key: cspann.RootKey}, {Key: partitionKey}}
		level, err := tx.SearchPartitions(suite.ctx, treeKey, toSearch, vec4, &searchSet)
		suite.NoError(err)
		suite.Equal(cspann.LeafLevel, level)
		result1 := cspann.SearchResult{
			QuerySquaredDistance: 24, ErrorBound: 24.08, CentroidDistance: 3.16,
			ParentPartitionKey: partitionKey, ChildKey: primaryKey1, ValueBytes: valueBytes1}
		result2 := cspann.SearchResult{
			QuerySquaredDistance: 29, ErrorBound: 0, CentroidDistance: 5,
			ParentPartitionKey: cspann.RootKey, ChildKey: primaryKey3, ValueBytes: valueBytes3}
		suite.Equal(cspann.SearchResults{result1, result2}, RoundResults(searchSet.PopResults(), 2))
		suite.Equal(3, toSearch[0].Count)
		suite.Equal(2, toSearch[1].Count)
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

func (suite *StoreTestSuite) TestTryCreateEmptyPartition() {
	store := suite.makeStore(suite.quantizer)
	if !store.SupportsTry() {
		return
	}

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)
		partitionKey := cspann.PartitionKey(10)
		centroid := vector.T{4, 3}
		timestamp := timeutil.Now()

		// Create empty partition.
		metadata := cspann.PartitionMetadata{
			Level:    cspann.SecondLevel,
			Centroid: centroid,
			StateDetails: cspann.PartitionStateDetails{
				State:     cspann.SplittingState,
				Target1:   20,
				Target2:   30,
				Timestamp: timestamp,
			},
		}
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
	if !store.SupportsTry() {
		return
	}

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
	if !store.SupportsTry() {
		return
	}

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)
		partitionKey := cspann.PartitionKey(10)
		centroid := vector.T{4, 3}
		timestamp := timeutil.Now()

		// Partition does not yet exist.
		_, err := store.TryGetPartition(suite.ctx, treeKey, partitionKey)
		suite.ErrorIs(err, cspann.ErrPartitionNotFound)

		// Create partition with some vectors in it.
		metadata := cspann.PartitionMetadata{
			Level:    cspann.LeafLevel,
			Centroid: centroid,
			StateDetails: cspann.PartitionStateDetails{
				State:     cspann.UpdatingState,
				Source:    20,
				Timestamp: timestamp,
			},
		}
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
	if !store.SupportsTry() {
		return
	}

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)
		partitionKey := cspann.PartitionKey(10)

		// Partition does not yet exist.
		_, err := store.TryGetPartitionMetadata(suite.ctx, treeKey, partitionKey)
		suite.ErrorIs(err, cspann.ErrPartitionNotFound)

		// Create partition with some vectors in it.
		partitionKey, partition := suite.createTestPartition(store, treeKey)

		// Fetch back only the metadata and validate it.
		partitionMetadata, err := store.TryGetPartitionMetadata(suite.ctx, treeKey, partitionKey)
		suite.NoError(err)
		suite.True(partitionMetadata.Equal(partition.Metadata()))

		// Update the metadata and verify we get the updated values.
		expected := *partition.Metadata()
		metadata := expected
		metadata.StateDetails = cspann.PartitionStateDetails{
			State:     cspann.UpdatingState,
			Source:    30,
			Timestamp: timeutil.Now(),
		}
		suite.NoError(store.TryUpdatePartitionMetadata(
			suite.ctx, treeKey, partitionKey, metadata, expected))

		// Fetch updated metadata and validate.
		partitionMetadata, err = store.TryGetPartitionMetadata(suite.ctx, treeKey, partitionKey)
		suite.NoError(err)
		suite.True(partitionMetadata.Equal(&metadata))
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
	if !store.SupportsTry() {
		return
	}

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
	if !store.SupportsTry() {
		return
	}

	doTest := func(treeID int) {
		treeKey := store.MakeTreeKey(suite.T(), treeID)
		partitionKey := cspann.PartitionKey(10)
		centroid := vector.T{4, 3}
		timestamp := timeutil.Now()

		// Partition does not yet exist.
		metadata := cspann.PartitionMetadata{
			Level:    cspann.LeafLevel,
			Centroid: centroid,
			StateDetails: cspann.PartitionStateDetails{
				State:     cspann.UpdatingState,
				Source:    20,
				Timestamp: timestamp,
			},
		}
		addVectors := vector.MakeSet(2)
		addVectors.Add(vec1)
		addVectors.Add(vec2)
		addChildKeys := []cspann.ChildKey{partitionKey1, partitionKey2}
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
		addVectors2 := vector.MakeSet(2)
		addVectors2.Add(vec3)
		addVectors2.Add(vec4)
		addChildKeys2 := []cspann.ChildKey{partitionKey3, partitionKey1} // Use duplicate key.
		addValueBytes2 := []cspann.ValueBytes{valueBytes3, valueBytes4}

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
		expectChildKeys := []cspann.ChildKey{partitionKey1, partitionKey2, partitionKey3}
		expectValueBytes := []cspann.ValueBytes{valueBytes1, valueBytes2, valueBytes3}
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
		addChildKeys3 := []cspann.ChildKey{partitionKey1, partitionKey3} // Use duplicate keys.
		addValueBytes3 := []cspann.ValueBytes{valueBytes1, valueBytes3}
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
	if !store.SupportsTry() {
		return
	}

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
		suite.Equal([]cspann.ChildKey{partitionKey3, partitionKey2}, partition.ChildKeys())
		suite.Equal([]cspann.ValueBytes{valueBytes3, valueBytes2}, partition.ValueBytes())

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

		// Try to remove the last remaining vectors in the partition.
		childKeys = []cspann.ChildKey{partitionKey2, partitionKey3}
		removed, err = store.TryRemoveFromPartition(suite.ctx, treeKey, partitionKey,
			childKeys, expected)
		suite.ErrorContains(err, "cannot remove last remaining vector from non-leaf partition")
		suite.True(removed)

		// Verify that partition contains one last remaining vector.
		partition, err = store.TryGetPartition(suite.ctx, treeKey, partitionKey)
		suite.NoError(err)
		suite.Equal(1, partition.Count())
		suite.Equal([]cspann.ChildKey{partitionKey3}, partition.ChildKeys())
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

func (suite *StoreTestSuite) runInTransaction(store TestStore, fn func(tx cspann.Txn)) {
	suite.NoError(store.RunTransaction(suite.ctx, func(tx cspann.Txn) error {
		fn(tx)
		return nil
	}))
}

// Create a test partition with some vectors in it.
func (suite *StoreTestSuite) createTestPartition(
	store TestStore, treeKey cspann.TreeKey,
) (cspann.PartitionKey, *cspann.Partition) {
	partitionKey := cspann.PartitionKey(10)
	metadata := cspann.PartitionMetadata{
		Level:    cspann.SecondLevel,
		Centroid: vector.T{4, 3},
		StateDetails: cspann.PartitionStateDetails{
			State:     cspann.ReadyState,
			Timestamp: timeutil.Now(),
		},
	}
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

// testEmptyOrMissingRoot includes tests against a missing or empty root
// partition. Operations against the root partition should not return "partition
// not found" errors, even if it is missing. The root partition needs to be
// lazily created when the first vector is added to it.
func (suite *StoreTestSuite) testEmptyOrMissingRoot(store TestStore, treeID int, desc string) {
	treeKey := store.MakeTreeKey(suite.T(), treeID)

	suite.Run("get metadata for "+desc, func() {
		suite.runInTransaction(store, func(tx cspann.Txn) {
			metadata, err := tx.GetPartitionMetadata(
				suite.ctx, treeKey, cspann.RootKey, false /* forUpdate */)
			suite.NoError(err)
			CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, vector.T{0, 0})
		})
		CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, cspann.RootKey, 0)
	})

	suite.Run("get "+desc, func() {
		suite.runInTransaction(store, func(tx cspann.Txn) {
			partition, err := tx.GetPartition(suite.ctx, treeKey, cspann.RootKey)
			suite.NoError(err)
			suite.Equal(cspann.Level(1), partition.Level())
			suite.Equal([]cspann.ChildKey(nil), testutils.NormalizeSlice(partition.ChildKeys()))
			suite.Equal([]cspann.ValueBytes(nil), testutils.NormalizeSlice(partition.ValueBytes()))
			suite.Equal(vector.T{0, 0}, partition.Centroid())
		})
	})

	suite.Run("search "+desc, func() {
		suite.runInTransaction(store, func(tx cspann.Txn) {
			searchSet := cspann.SearchSet{MaxResults: 2}
			toSearch := []cspann.PartitionToSearch{{Key: cspann.RootKey}}
			level, err := tx.SearchPartitions(suite.ctx, treeKey, toSearch, vector.T{1, 1}, &searchSet)
			suite.NoError(err)
			suite.Equal(cspann.LeafLevel, level)
			suite.Nil(searchSet.PopResults())
			suite.Equal(0, toSearch[0].Count)
		})
	})

	suite.Run("try to remove vector from "+desc, func() {
		suite.runInTransaction(store, func(tx cspann.Txn) {
			err := tx.RemoveFromPartition(suite.ctx, treeKey, cspann.RootKey,
				cspann.LeafLevel, cspann.ChildKey{KeyBytes: cspann.KeyBytes{1, 2, 3}})
			suite.NoError(err)
		})
		CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, cspann.RootKey, 0)
	})
}

// addToRoot inserts vec1, vec2, and vec3 into the root partition.
func (suite *StoreTestSuite) addToRoot(store TestStore, treeID int) {
	treeKey := store.MakeTreeKey(suite.T(), treeID)
	func() {
		tx := BeginTransaction(suite.ctx, suite.T(), store)
		defer CommitTransaction(suite.ctx, suite.T(), store, tx)

		// Get partition metadata with forUpdate = true before updates.
		metadata, err := tx.GetPartitionMetadata(suite.ctx, treeKey, cspann.RootKey, true /* forUpdate */)
		suite.NoError(err)
		CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, vector.T{0, 0})

		// Add vectors to partition.
		err = tx.AddToPartition(
			suite.ctx, treeKey, cspann.RootKey, metadata.Level, vec1, primaryKey1, valueBytes1)
		suite.NoError(err)
		err = tx.AddToPartition(
			suite.ctx, treeKey, cspann.RootKey, metadata.Level, vec2, primaryKey2, valueBytes2)
		suite.NoError(err)
		CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, vector.T{0, 0})
		err = tx.AddToPartition(
			suite.ctx, treeKey, cspann.RootKey, metadata.Level, vec3, primaryKey3, valueBytes3)
		suite.NoError(err)
	}()

	// Check partition count only after committing the transaction.
	CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, cspann.RootKey, 3)
}

// insertLeafPartition inserts a new leaf partition containing vec1, vec2, and
// vec3, and then validates it.
func (suite *StoreTestSuite) insertLeafPartition(store TestStore, treeID int) cspann.PartitionKey {
	tx := BeginTransaction(suite.ctx, suite.T(), store)
	defer CommitTransaction(suite.ctx, suite.T(), store, tx)
	treeKey := store.MakeTreeKey(suite.T(), treeID)

	vectors := vector.MakeSet(2)
	vectors.Add(vec1)
	vectors.Add(vec2)
	vectors.Add(vec3)
	quantizedSet := suite.quantizer.Quantize(&suite.workspace, vectors)
	childKeys := []cspann.ChildKey{primaryKey1, primaryKey2, primaryKey3}
	valueBytes := []cspann.ValueBytes{valueBytes1, valueBytes2, valueBytes3}
	metadata := cspann.PartitionMetadata{
		Level: cspann.LeafLevel, Centroid: quantizedSet.GetCentroid()}
	partition := cspann.NewPartition(
		metadata, suite.quantizer, quantizedSet, childKeys, valueBytes)

	partitionKey, err := tx.InsertPartition(suite.ctx, treeKey, partition)
	suite.NoError(err)

	// Read back and verify the partition.
	readPartition, err := tx.GetPartition(suite.ctx, treeKey, partitionKey)
	suite.NoError(err)
	ValidatePartitionsEqual(suite.T(), partition, readPartition)

	return partitionKey
}

// testLeafPartition assumes that the partition already has vec1, vec2, and vec3
// in it, either from calling addToRoot or insertLeafPartition. From that
// starting point, it adds, removes, and searches vectors in the partition.
func (suite *StoreTestSuite) testLeafPartition(
	store TestStore, treeID int, partitionKey cspann.PartitionKey, centroid vector.T,
) {
	treeKey := store.MakeTreeKey(suite.T(), treeID)
	CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, partitionKey, 3)

	suite.Run("add to leaf partition", func() {
		suite.runInTransaction(store, func(tx cspann.Txn) {
			// Get partition metadata with forUpdate = true before update.
			metadata, err := tx.GetPartitionMetadata(
				suite.ctx, treeKey, partitionKey, true /* forUpdate */)
			suite.NoError(err)
			CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, centroid)

			// Add duplicate and expect value to be overwritten. Centroid should not
			// change.
			dupVec := vector.T{5, 5}
			err = tx.AddToPartition(suite.ctx, treeKey, partitionKey,
				cspann.LeafLevel, dupVec, primaryKey3, valueBytes3)
			suite.NoError(err)

			metadata, err = tx.GetPartitionMetadata(
				suite.ctx, treeKey, partitionKey, false /* forUpdate */)
			suite.NoError(err)
			CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, centroid)

			// Search partition.
			searchSet := cspann.SearchSet{MaxResults: 2}
			toSearch := []cspann.PartitionToSearch{{Key: partitionKey}}
			searchLevel, err := tx.SearchPartitions(suite.ctx, treeKey, toSearch, vector.T{1, 1}, &searchSet)
			suite.NoError(err)
			suite.Equal(cspann.LeafLevel, searchLevel)
			result1 := cspann.SearchResult{
				QuerySquaredDistance: 1, ErrorBound: 0,
				CentroidDistance:   testutils.RoundFloat(num32.L2Distance(vec1, centroid), 4),
				ParentPartitionKey: partitionKey, ChildKey: primaryKey1, ValueBytes: valueBytes1}
			result2 := cspann.SearchResult{
				QuerySquaredDistance: 32, ErrorBound: 0,
				CentroidDistance:   testutils.RoundFloat(num32.L2Distance(dupVec, centroid), 4),
				ParentPartitionKey: partitionKey, ChildKey: primaryKey3, ValueBytes: valueBytes3}
			if partitionKey != cspann.RootKey {
				// Non-root partitions use RaBitQuantizer, where distances are approximate.
				result1.QuerySquaredDistance = 0
				result1.ErrorBound = 16.1245
				result2.QuerySquaredDistance = 34.6667
				result2.ErrorBound = 11.4018
			}
			results := searchSet.PopResults()
			RoundResults(results, 4)
			suite.Equal(cspann.SearchResults{result1, result2}, results)
			suite.Equal(3, toSearch[0].Count)

			// Ensure partition metadata is updated.
			metadata, err = tx.GetPartitionMetadata(
				suite.ctx, treeKey, partitionKey, true /* forUpdate */)
			suite.NoError(err)
			CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, centroid)
		})
		CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, partitionKey, 3)
	})

	suite.Run("remove from leaf partition", func() {
		suite.runInTransaction(store, func(tx cspann.Txn) {
			// Remove vector from partition.
			err := tx.RemoveFromPartition(suite.ctx, treeKey, partitionKey, cspann.LeafLevel, primaryKey1)
			suite.NoError(err)
		})
		CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, partitionKey, 2)

		suite.runInTransaction(store, func(tx cspann.Txn) {
			// Try to remove the same key again.
			err := tx.RemoveFromPartition(suite.ctx, treeKey, partitionKey, cspann.LeafLevel, primaryKey1)
			suite.NoError(err)
		})
		CheckPartitionCount(suite.ctx, suite.T(), store, treeKey, partitionKey, 2)
	})

	suite.Run("add again to leaf partition", func() {
		suite.runInTransaction(store, func(tx cspann.Txn) {
			// Add an alternate element and add duplicate, expecting value to be overwritten.
			err := tx.AddToPartition(
				suite.ctx, treeKey, partitionKey, cspann.LeafLevel, vec4, primaryKey4, valueBytes4)
			suite.NoError(err)

			// Search partition.
			searchSet := cspann.SearchSet{MaxResults: 1}
			toSearch := []cspann.PartitionToSearch{{Key: partitionKey}}
			searchLevel, err := tx.SearchPartitions(suite.ctx, treeKey, toSearch, vector.T{10, -5}, &searchSet)
			suite.NoError(err)
			suite.Equal(cspann.LeafLevel, searchLevel)
			result1 := cspann.SearchResult{
				QuerySquaredDistance: 25, ErrorBound: 0,
				CentroidDistance:   testutils.RoundFloat(num32.L2Distance(vec4, centroid), 4),
				ParentPartitionKey: partitionKey, ChildKey: primaryKey4, ValueBytes: valueBytes4}
			if partitionKey != cspann.RootKey {
				// Distances are approximate.
				result1.QuerySquaredDistance = 13
				result1.ErrorBound = 76.1577
			}
			results := searchSet.PopResults()
			RoundResults(results, 4)
			suite.Equal(cspann.SearchResults{result1}, results)
			suite.Equal(3, toSearch[0].Count)
		})
	})
}

// setRootPartition first validates that the root partition starts with 3
// vectors. It then replaces that with a new set of data and validates it.
func (suite *StoreTestSuite) setRootPartition(store TestStore, treeID int) {
	tx := BeginTransaction(suite.ctx, suite.T(), store)
	defer CommitTransaction(suite.ctx, suite.T(), store, tx)

	treeKey := store.MakeTreeKey(suite.T(), treeID)

	// Create new root partition that's at level 2 of the tree, pointing to leaf
	// partition centroids.
	vectors := vector.MakeSet(2)
	vectors.Add(vec1)
	vectors.Add(vec2)
	centroid := vector.T{4, 3}

	quantizedSet := suite.rootQuantizer.Quantize(&suite.workspace, vectors)
	childKeys := []cspann.ChildKey{partitionKey1, partitionKey2}
	valueBytes := []cspann.ValueBytes{valueBytes1, valueBytes2}
	metadata := cspann.PartitionMetadata{
		Level: cspann.SecondLevel, Centroid: quantizedSet.GetCentroid()}
	newRoot := cspann.NewPartition(
		metadata, suite.rootQuantizer, quantizedSet, childKeys, valueBytes)

	err := tx.SetRootPartition(suite.ctx, treeKey, newRoot)
	suite.NoError(err)

	// Check partition metadata.
	metadata, err = tx.GetPartitionMetadata(suite.ctx, treeKey, cspann.RootKey, false /* forUpdate */)
	suite.NoError(err)
	CheckPartitionMetadata(suite.T(), metadata, cspann.SecondLevel, centroid)

	// Read back and verify the partition.
	readRoot, err := tx.GetPartition(suite.ctx, treeKey, cspann.RootKey)
	suite.NoError(err)
	ValidatePartitionsEqual(suite.T(), newRoot, readRoot)

	// Search partition.
	searchSet := cspann.SearchSet{MaxResults: 1}
	toSearch := []cspann.PartitionToSearch{{Key: cspann.RootKey}}
	searchLevel, err := tx.SearchPartitions(suite.ctx, treeKey, toSearch, vector.T{5, 5}, &searchSet)
	suite.NoError(err)
	suite.Equal(cspann.SecondLevel, searchLevel)
	result1 := cspann.SearchResult{
		QuerySquaredDistance: 5, ErrorBound: 0, CentroidDistance: 3.1623,
		ParentPartitionKey: cspann.RootKey, ChildKey: partitionKey2, ValueBytes: valueBytes2}
	results := searchSet.PopResults()
	RoundResults(results, 4)
	suite.Equal(cspann.SearchResults{result1}, results)
	suite.Equal(2, toSearch[0].Count)
}

// deletePartition tests deleting the given partition.
func (suite *StoreTestSuite) deletePartition(
	store TestStore, treeID int, partitionKey cspann.PartitionKey,
) {
	tx := BeginTransaction(suite.ctx, suite.T(), store)
	defer CommitTransaction(suite.ctx, suite.T(), store, tx)

	treeKey := store.MakeTreeKey(suite.T(), treeID)

	// Delete the partition and validate it is really gone.
	err := tx.DeletePartition(suite.ctx, treeKey, partitionKey)
	suite.NoError(err)
	partition, err := tx.GetPartition(suite.ctx, treeKey, partitionKey)
	if err != nil {
		// Validate the error.
		suite.True(
			errors.Is(err, cspann.ErrPartitionNotFound) || errors.Is(err, cspann.ErrRestartOperation))
	} else {
		// The root partition is lazily materialized.
		suite.Equal(cspann.RootKey, partitionKey)
		suite.Equal(0, partition.Count())
	}

	// Now try to delete again, and ensure it's a no-op.
	err = tx.DeletePartition(suite.ctx, treeKey, partitionKey)
	suite.NoError(err)
}
