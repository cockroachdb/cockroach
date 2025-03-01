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

// TestRootPartition runs tests against the root partition, which has special
// rules as compared to other partitions.
func (suite *StoreTestSuite) TestRootPartition() {
	store := suite.makeStore(suite.quantizer)

	// Test missing root partition.
	suite.testEmptyOrMissingRoot(store, 0, "missing root partition", true /* isMissing */)

	// Add a vector to the root partition.
	rootVec := vector.T{1, 2}
	rootChildKey := cspann.ChildKey{KeyBytes: cspann.KeyBytes{10, 20}}
	suite.runInTransaction(store, 0, func(tx cspann.Txn, treeKey cspann.TreeKey) {
		val := cspann.ValueBytes{100, 200}
		metadata, err := tx.AddToPartition(
			suite.ctx, treeKey, cspann.RootKey, rootVec, rootChildKey, val)
		suite.NoError(err)
		CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, vector.T{0, 0}, 1)
	})

	if store.AllowMultipleTrees() {
		// Other trees' root partitions should still be missing.
		suite.testEmptyOrMissingRoot(
			store, 1, "missing root partition, different tree", true /* isMissing */)
	}

	// Now remove the only vector from the root partition, making it empty, but
	// not missing.
	suite.runInTransaction(store, 0, func(tx cspann.Txn, treeKey cspann.TreeKey) {
		metadata, err := tx.RemoveFromPartition(suite.ctx, treeKey, cspann.RootKey, rootChildKey)
		suite.NoError(err)
		CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, vector.T{0, 0}, 0)
	})

	// Test empty root partition.
	suite.testEmptyOrMissingRoot(store, 0, "empty root partition", false /* isMissing */)

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
		suite.testEmptyOrMissingRoot(store, 0, "deleted root partition", true /* isMissing */)
	})

	if store.AllowMultipleTrees() {
		suite.Run("deleted root partition, different tree", func() {
			suite.deletePartition(store, 1, cspann.RootKey)
			suite.testEmptyOrMissingRoot(store, 1, "deleted root partition", true /* isMissing */)
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
		partition := cspann.NewPartition(
			suite.quantizer, quantizedSet, childKeys, valueBytes, cspann.SecondLevel)

		partitionKey, err := tx.InsertPartition(suite.ctx, treeKey, partition)
		suite.NoError(err)

		// Read back and verify the partition.
		readPartition, err := tx.GetPartition(suite.ctx, treeKey, partitionKey)
		suite.NoError(err)
		ValidatePartitionsEqual(suite.T(), partition, readPartition)

		// Add and remove vectors from partition.
		_, err = tx.AddToPartition(suite.ctx, treeKey, partitionKey, vec3, partitionKey3, valueBytes3)
		suite.NoError(err)
		_, err = tx.RemoveFromPartition(suite.ctx, treeKey, partitionKey, partitionKey1)
		suite.NoError(err)

		// Search partition.
		partitionCounts := []int{0}
		searchSet := cspann.SearchSet{MaxResults: 1}
		searchLevel, err := tx.SearchPartitions(suite.ctx, treeKey,
			[]cspann.PartitionKey{partitionKey}, vector.T{5, -1}, &searchSet, partitionCounts)
		suite.NoError(err)
		suite.Equal(cspann.SecondLevel, searchLevel)
		result1 := cspann.SearchResult{
			QuerySquaredDistance: 17, ErrorBound: 0, CentroidDistance: 0,
			ParentPartitionKey: partitionKey, ChildKey: partitionKey3, ValueBytes: valueBytes3}
		results := searchSet.PopResults()
		RoundResults(results, 4)
		suite.Equal(cspann.SearchResults{result1}, results)
		suite.Equal(2, partitionCounts[0])
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
		_, err := tx.RemoveFromPartition(suite.ctx, treeKey, partitionKey, primaryKey3)
		suite.NoError(err)

		searchSet := cspann.SearchSet{MaxResults: 2}
		partitionCounts := []int{0, 0}
		level, err := tx.SearchPartitions(suite.ctx, treeKey,
			[]cspann.PartitionKey{cspann.RootKey, partitionKey}, vec4,
			&searchSet, partitionCounts)
		suite.NoError(err)
		suite.Equal(cspann.LeafLevel, level)
		result1 := cspann.SearchResult{
			QuerySquaredDistance: 24, ErrorBound: 24.08, CentroidDistance: 3.16,
			ParentPartitionKey: partitionKey, ChildKey: primaryKey1, ValueBytes: valueBytes1}
		result2 := cspann.SearchResult{
			QuerySquaredDistance: 29, ErrorBound: 0, CentroidDistance: 5,
			ParentPartitionKey: cspann.RootKey, ChildKey: primaryKey3, ValueBytes: valueBytes3}
		suite.Equal(cspann.SearchResults{result1, result2}, RoundResults(searchSet.PopResults(), 2))
		suite.Equal([]int{3, 2}, partitionCounts)
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

func (suite *StoreTestSuite) runInTransaction(
	store TestStore, treeID int, fn func(tx cspann.Txn, treeKey cspann.TreeKey),
) {
	tx := BeginTransaction(suite.ctx, suite.T(), store)
	defer CommitTransaction(suite.ctx, suite.T(), store, tx)
	fn(tx, store.MakeTreeKey(suite.T(), treeID))
}

// testEmptyOrMissingRoot includes tests against a missing or empty root
// partition. Operations against the root partition should not return "partition
// not found" errors, even if it is missing. The root partition needs to be
// lazily created when the first vector is added to it.
func (suite *StoreTestSuite) testEmptyOrMissingRoot(
	store TestStore, treeID int, desc string, isMissing bool,
) {
	tx := BeginTransaction(suite.ctx, suite.T(), store)
	defer CommitTransaction(suite.ctx, suite.T(), store, tx)

	treeKey := store.MakeTreeKey(suite.T(), treeID)

	suite.Run("get metadata for "+desc, func() {
		metadata, err := tx.GetPartitionMetadata(
			suite.ctx, treeKey, cspann.RootKey, false /* forUpdate */)
		suite.NoError(err)
		CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, vector.T{0, 0}, 0)
	})

	suite.Run("get "+desc, func() {
		partition, err := tx.GetPartition(suite.ctx, treeKey, cspann.RootKey)
		suite.NoError(err)
		suite.Equal(cspann.Level(1), partition.Level())
		suite.Equal([]cspann.ChildKey(nil), testutils.NormalizeSlice(partition.ChildKeys()))
		suite.Equal([]cspann.ValueBytes(nil), testutils.NormalizeSlice(partition.ValueBytes()))
		suite.Equal(vector.T{0, 0}, partition.Centroid())
	})

	suite.Run("search "+desc, func() {
		searchSet := cspann.SearchSet{MaxResults: 2}
		partitionCounts := []int{0}
		level, err := tx.SearchPartitions(suite.ctx, treeKey,
			[]cspann.PartitionKey{cspann.RootKey}, vector.T{1, 1}, &searchSet, partitionCounts)
		suite.NoError(err)
		suite.Equal(cspann.LeafLevel, level)
		suite.Nil(searchSet.PopResults())
		suite.Equal(0, partitionCounts[0])
	})

	suite.Run("try to remove vector from "+desc, func() {
		metadata, err := tx.RemoveFromPartition(
			suite.ctx, treeKey, cspann.RootKey, cspann.ChildKey{KeyBytes: cspann.KeyBytes{1, 2, 3}})
		if isMissing {
			suite.ErrorIs(err, cspann.ErrPartitionNotFound)
		} else {
			suite.NoError(err)
			CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, vector.T{0, 0}, 0)
		}
	})
}

// addToRoot inserts vec1, vec2, and vec3 into the root partition.
func (suite *StoreTestSuite) addToRoot(store TestStore, treeID int) {
	tx := BeginTransaction(suite.ctx, suite.T(), store)
	defer CommitTransaction(suite.ctx, suite.T(), store, tx)

	treeKey := store.MakeTreeKey(suite.T(), treeID)

	// Get partition metadata with forUpdate = true before updates.
	metadata, err := tx.GetPartitionMetadata(suite.ctx, treeKey, cspann.RootKey, true /* forUpdate */)
	suite.NoError(err)
	CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, vector.T{0, 0}, 0)

	// Add vectors to partition.
	metadata, err = tx.AddToPartition(
		suite.ctx, treeKey, cspann.RootKey, vec1, primaryKey1, valueBytes1)
	suite.NoError(err)
	CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, vector.T{0, 0}, 1)
	metadata, err = tx.AddToPartition(
		suite.ctx, treeKey, cspann.RootKey, vec2, primaryKey2, valueBytes2)
	suite.NoError(err)
	CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, vector.T{0, 0}, 2)
	metadata, err = tx.AddToPartition(
		suite.ctx, treeKey, cspann.RootKey, vec3, primaryKey3, valueBytes3)
	suite.NoError(err)
	CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, vector.T{0, 0}, 3)
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
	partition := cspann.NewPartition(
		suite.quantizer, quantizedSet, childKeys, valueBytes, cspann.LeafLevel)

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
	tx := BeginTransaction(suite.ctx, suite.T(), store)
	defer CommitTransaction(suite.ctx, suite.T(), store, tx)
	treeKey := store.MakeTreeKey(suite.T(), treeID)

	// Get partition metadata with forUpdate = true before update.
	metadata, err := tx.GetPartitionMetadata(suite.ctx, treeKey, partitionKey, true /* forUpdate */)
	suite.NoError(err)
	CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, centroid, 3)

	// Add duplicate and expect value to be overwritten. Centroid should not
	// change.
	dupVec := vector.T{5, 5}
	metadata, err = tx.AddToPartition(
		suite.ctx, treeKey, partitionKey, dupVec, primaryKey3, valueBytes3)
	suite.NoError(err)
	CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, centroid, 3)

	// Search partition.
	searchSet := cspann.SearchSet{MaxResults: 2}
	partitionCounts := []int{0}
	searchLevel, err := tx.SearchPartitions(suite.ctx, treeKey,
		[]cspann.PartitionKey{partitionKey}, vector.T{1, 1}, &searchSet, partitionCounts)
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
	suite.Equal(3, partitionCounts[0])

	// Ensure partition metadata is updated.
	metadata, err = tx.GetPartitionMetadata(suite.ctx, treeKey, partitionKey, true /* forUpdate */)
	suite.NoError(err)
	CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, centroid, 3)

	// Remove vector from partition.
	metadata, err = tx.RemoveFromPartition(suite.ctx, treeKey, partitionKey, primaryKey1)
	suite.NoError(err)
	CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, centroid, 2)

	// Try to remove the same key again.
	metadata, err = tx.RemoveFromPartition(suite.ctx, treeKey, partitionKey, primaryKey1)
	suite.NoError(err)
	CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, centroid, 2)

	// Add an alternate element and add duplicate, expecting value to be overwritten.
	metadata, err = tx.AddToPartition(
		suite.ctx, treeKey, partitionKey, vec4, primaryKey4, valueBytes4)
	suite.NoError(err)
	CheckPartitionMetadata(suite.T(), metadata, cspann.LeafLevel, centroid, 3)

	// Search partition.
	searchSet = cspann.SearchSet{MaxResults: 1}
	searchLevel, err = tx.SearchPartitions(suite.ctx, treeKey,
		[]cspann.PartitionKey{partitionKey}, vector.T{10, -5}, &searchSet, partitionCounts)
	suite.NoError(err)
	suite.Equal(cspann.LeafLevel, searchLevel)
	result1 = cspann.SearchResult{
		QuerySquaredDistance: 25, ErrorBound: 0,
		CentroidDistance:   testutils.RoundFloat(num32.L2Distance(vec4, centroid), 4),
		ParentPartitionKey: partitionKey, ChildKey: primaryKey4, ValueBytes: valueBytes4}
	if partitionKey != cspann.RootKey {
		// Distances are approximate.
		result1.QuerySquaredDistance = 13
		result1.ErrorBound = 76.1577
	}
	results = searchSet.PopResults()
	RoundResults(results, 4)
	suite.Equal(cspann.SearchResults{result1}, results)
	suite.Equal(3, partitionCounts[0])
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
	newRoot := cspann.NewPartition(
		suite.rootQuantizer, quantizedSet, childKeys, valueBytes, cspann.SecondLevel)

	err := tx.SetRootPartition(suite.ctx, treeKey, newRoot)
	suite.NoError(err)

	// Check partition metadata.
	metadata, err := tx.GetPartitionMetadata(suite.ctx, treeKey, cspann.RootKey, false /* forUpdate */)
	suite.NoError(err)
	CheckPartitionMetadata(suite.T(), metadata, cspann.SecondLevel, centroid, 2)

	// Read back and verify the partition.
	readRoot, err := tx.GetPartition(suite.ctx, treeKey, cspann.RootKey)
	suite.NoError(err)
	ValidatePartitionsEqual(suite.T(), newRoot, readRoot)

	// Search partition.
	searchSet := cspann.SearchSet{MaxResults: 1}
	partitionCounts := []int{0}
	searchLevel, err := tx.SearchPartitions(suite.ctx, treeKey,
		[]cspann.PartitionKey{cspann.RootKey}, vector.T{5, 5}, &searchSet, partitionCounts)
	suite.NoError(err)
	suite.Equal(cspann.SecondLevel, searchLevel)
	result1 := cspann.SearchResult{
		QuerySquaredDistance: 5, ErrorBound: 0, CentroidDistance: 3.1623,
		ParentPartitionKey: cspann.RootKey, ChildKey: partitionKey2, ValueBytes: valueBytes2}
	results := searchSet.PopResults()
	RoundResults(results, 4)
	suite.Equal(cspann.SearchResults{result1}, results)
	suite.Equal(2, partitionCounts[0])
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
