// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"bytes"
	"context"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/utils"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// RerankMultiplier is multiplied by MaxResults to calculate the maximum number
// of search results that will be reranked with the original full-size vectors.
const RerankMultiplier = 10

// DeletedMultiplier increases the number of results that will be reranked, in
// order to account for vectors that may have been deleted in the primary index.
const DeletedMultiplier = 1.2

// MaxQualitySamples specifies the max value of the QualitySamples index option.
const MaxQualitySamples = 32

// IndexOptions specifies options that control how the index will be built, as
// well as default options for how it will be searched. A given search operation
// can specify SearchOptions to override the default behavior.
type IndexOptions struct {
	// MinPartitionSize specifies the size below which a partition will be merged
	// into other partitions at the same level.
	MinPartitionSize int
	// MaxPartitionSize specifies the size above which a partition will be split.
	MaxPartitionSize int
	// BaseBeamSize is the default number of child partitions that will be
	// searched at each level during insert, delete, and search operations.
	// Adaptive search will automatically decrease or increase this value as the
	// search proceeds.
	BaseBeamSize int
	// QualitySamples is the number of search results that are used as samples for
	// calculating search quality statistics. Adaptive search uses these stats to
	// determine how many partitions to search at each level.
	QualitySamples int
	// DisableAdaptiveSearch can be set to true to disable adaptive search. This
	// is useful for testing and benchmarking.
	DisableAdaptiveSearch bool
	// IsDeterministic instructs the vector index to use a deterministic random
	// number generator for performing operations and fixups. As long as the
	// index is initialized with the same seed and fixups happen serially, the
	// index will behave deterministically. This is useful for testing.
	IsDeterministic bool
	// MaxWorkers specifies the maximum number of background workers that can be
	// created to process fixups for this vector index instance.
	MaxWorkers int
	// StalledOpTimeout can be called to determine how long a split/merge
	// operation can stay in the same state before another worker may attempt to
	// assist. If this is nil, then a default value is used.
	StalledOpTimeout func() time.Duration
}

// SearchOptions specifies options that apply to a particular search operation
// over the vector index.
type SearchOptions struct {
	// BaseBeamSize is the default number of child partitions that will be
	// searched at each level. Adaptive search will automatically decrease or
	// increase this value as the search proceeds. If zero, then it defaults to
	// the BaseBeamSize value from IndexOptions.
	BaseBeamSize int
	// SkipRerank does not rerank search results using the original full-size
	// vectors. While this speeds up the search, it can also significantly
	// reduce accuracy. It is currently only used for testing.
	SkipRerank bool
	// UpdateStats specifies whether index statistics will be modified by this
	// search. These stats are used for adaptive search.
	UpdateStats bool
}

// Context contains per-thread state needed during index operations. Callers
// must call Init before use.
//
// NOTE: This struct is *not* thread-safe and should only be used on one
// goroutine at a time. However, it can and should be re-used across multiple
// index operations.
type Context struct {
	// txn is the transaction in which the current operation runs. It's set by
	// the caller via the Init method.
	txn Txn
	// treeKey is the K-means tree which the current operation runs. It's set
	// when an operation begins and stays the same throughout.
	treeKey TreeKey
	// options specifies settings that modify searches over the vector index.
	options SearchOptions
	// level of the tree from which search results are returned. For the Search
	// operation, this is always LeafLevel, but inserts and splits/merges can
	// search at intermediate levels of the tree.
	level Level
	// original is the original, full-size vector that was passed to the top-level
	// Index method.
	original vector.T
	// randomized is the original vector after it has been randomized by applying
	// a random orthogonal transformation (ROT).
	randomized vector.T
	// forInsert indicates that this is an insert operation (or a search for
	// insert operation).
	forInsert bool
	// forDelete indicates that this is a delete operation (or a search for
	// delete operation).
	forDelete bool
	// search is used to iteratively search a K-means tree, both to find existing
	// vectors and to find partitions in which to insert new vectors.
	search searcher

	tempSearchSet       SearchSet
	tempResults         [2]SearchResult
	tempQualitySamples  [MaxQualitySamples]float64
	tempToSearch        []PartitionToSearch
	tempVectorsWithKeys []VectorWithKey
}

// Init sets up the context to operate in the scope of the given transaction.
// The caller needs to call this before passing the context to Index methods.
func (ic *Context) Init(txn Txn) {
	// Methods on the index are responsible for initializing other private fields.
	ic.txn = txn
}

// OriginalVector is the original, full-size vector that was passed to the last
// index operation.
func (ic *Context) OriginalVector() vector.T {
	return ic.original
}

// RandomizedVector is the randomized form of OriginalVector.
func (ic *Context) RandomizedVector() vector.T {
	return ic.randomized
}

// Index implements the C-SPANN algorithm, which adapts Microsoft's SPANN and
// SPFresh algorithms to work well with CockroachDB's unique distributed
// architecture. This enables CockroachDB to efficiently answer approximate
// nearest neighbor (ANN) queries with high accuracy, low latency, and fresh
// results, with millions or even billions of indexed vectors. In a departure
// from SPANN (and similar to Google's ScaNN), C-SPANN packs hundreds of vectors
// into each partition, and indexes partitions using a K-means tree. Each index
// can be composed of a forest of K-means trees in order to support partitioned
// indexes (e.g. partitioned across localities or tenants).
//
// Index is thread-safe. There should typically be only one Index instance in
// the process for each index.
type Index struct {
	// options specify how the index will be built and searched, by default.
	options IndexOptions
	// store is the interface with the component that transactionally stores
	// partitions and vectors.
	store Store
	// rootQuantizer quantizes vectors in the root partition.
	rootQuantizer quantize.Quantizer
	// quantizer quantizes vectors in every partition except the root.
	quantizer quantize.Quantizer
	// fixups runs index maintenance operations like split and merge on a
	// background goroutine.
	fixups FixupProcessor
	// stats maintains locally-cached statistics about the vector index that are
	// used by adaptive search to improve search accuracy.
	stats statsManager
	// rot is a square dims x dims matrix that performs random orthogonal
	// transformations on input vectors, in order to distribute skew more evenly.
	rot num32.Matrix
}

// NewIndex constructs a new vector index instance. Typically, only one Index
// instance should be created for each index in the process.
//
// NOTE: It's important that the index is always initialized with the same seed,
// first at the time of creation and then every time it's used.
//
// NOTE: If "stopper" is not nil, then the index will start a background
// goroutine to process index fixups. When the index is no longer needed, the
// caller must call Close to shut down the background goroutine.
func NewIndex(
	ctx context.Context,
	store Store,
	quantizer quantize.Quantizer,
	seed int64,
	options *IndexOptions,
	stopper *stop.Stopper,
) (*Index, error) {
	vi := &Index{
		options:       *options,
		store:         store,
		rootQuantizer: quantize.NewUnQuantizer(quantizer.GetDims()),
		quantizer:     quantizer,
	}
	if vi.options.MinPartitionSize == 0 {
		vi.options.MinPartitionSize = 16
	}
	if vi.options.MaxPartitionSize == 0 {
		vi.options.MaxPartitionSize = 128
	}
	if vi.options.BaseBeamSize == 0 {
		vi.options.BaseBeamSize = 8
	}
	if vi.options.QualitySamples == 0 {
		vi.options.QualitySamples = 16
	}
	if vi.options.StalledOpTimeout == nil {
		vi.options.StalledOpTimeout = func() time.Duration { return DefaultStalledOpTimeout }
	}

	if vi.options.MaxPartitionSize < 2 {
		return nil, errors.AssertionFailedf("MaxPartitionSize cannot be less than 2")
	}
	if vi.options.QualitySamples > MaxQualitySamples {
		return nil, errors.Errorf(
			"QualitySamples option %d exceeds max allowed value", vi.options.QualitySamples)
	}

	rng := rand.New(rand.NewSource(seed))

	// Generate dims x dims random orthogonal matrix to mitigate the impact of
	// skewed input data distributions:
	//
	//   1. Set skew: some dimensions can have higher variance than others. For
	//      example, perhaps all vectors in a set have similar values for one
	//      dimension but widely differing values in another dimension.
	//   2. Vector skew: Individual vectors can have internal skew, such that
	//      values higher than the mean are more spread out than values lower
	//      than the mean.
	//
	// Multiplying vectors by this matrix helps with both forms of skew. While
	// total skew does not change, the skew is more evenly distributed across
	// the dimensions. Now quantizing the vector will have more uniform
	// information loss across dimensions. Critically, none of this impacts
	// distance calculations, as orthogonal transformations do not change
	// distances or angles between vectors.
	//
	// Ultimately, performing a random orthogonal transformation (ROT) means that
	// the index will work more consistently across a diversity of input data
	// sets, even those with skewed data distributions. In addition, the RaBitQ
	// algorithm depends on the statistical properties that are granted by the
	// ROT.
	vi.rot = num32.MakeRandomOrthoMatrix(rng, quantizer.GetDims())

	// Initialize fixup processor.
	var fixupSeed int64
	if options.IsDeterministic {
		// Fixups need to be deterministic, so set the seed and worker count.
		fixupSeed = seed
		if vi.options.MaxWorkers > 1 {
			return nil, errors.AssertionFailedf(
				"multiple asynchronous workers cannot be used with the IsDeterministic option")
		}
		vi.options.MaxWorkers = 1
	}
	if vi.options.MaxWorkers == 0 {
		// Default to a max of one worker per processor.
		vi.options.MaxWorkers = runtime.GOMAXPROCS(-1)
	}
	vi.fixups.Init(ctx, stopper, vi, fixupSeed)

	if err := vi.stats.Init(ctx, store.MergeStats); err != nil {
		return nil, err
	}

	return vi, nil
}

// Store returns the underlying vector store for the index.
func (vi *Index) Store() Store {
	return vi.store
}

// Fixups returns the background fixup processor for the index.
func (vi *Index) Fixups() *FixupProcessor {
	return &vi.fixups
}

// Options returns the options that specify how the index should be built and
// searched.
func (vi *Index) Options() IndexOptions {
	return vi.options
}

// FormatStats returns index statistics as a formatted string.
func (vi *Index) FormatStats() string {
	return vi.stats.Format()
}

// RandomizeVector performs a random orthogonal transformation (ROT) on the
// "original" vector and writes it to the "randomized" vector. The caller is
// responsible for allocating the randomized vector with length equal to the
// index's dimensions.
//
// Randomizing vectors distributes skew more evenly across dimensions and
// across vectors in a set. Distance and angle between any two vectors
// remains unchanged, as long as the same ROT is applied to both.
func (vi *Index) RandomizeVector(original vector.T, randomized vector.T) vector.T {
	return num32.MulMatrixByVector(&vi.rot, original, randomized, num32.NoTranspose)
}

// UnRandomizeVector inverts the random orthogonal transformation performed by
// RandomizeVector, in order to recover the original vector from its randomized
// form. The caller is responsible for allocating the original vector with
// length equal to the index's dimensions.
func (vi *Index) UnRandomizeVector(randomized vector.T, original vector.T) vector.T {
	return num32.MulMatrixByVector(&vi.rot, randomized, original, num32.Transpose)
}

// Close shuts down any background fixup workers. While this also happens when
// the Init context is closed or the stopper is quiesced, this method can be
// used to perform the shutdown independently of those mechanisms.
func (vi *Index) Close() {
	vi.fixups.cancel()
}

// Insert adds a new vector with the given primary key to the index. This is
// called within the scope of a transaction so that index does not appear to
// change during the insert.
//
// NOTE: This can result in two vectors with the same primary key being inserted
// into the index. To minimize this possibility, callers should call Delete
// before Insert when a vector is updated. Even then, it's not guaranteed that
// Delete will find the old vector. The search set handles this rare case by
// filtering out results with duplicate key bytes.
func (vi *Index) Insert(
	ctx context.Context, idxCtx *Context, treeKey TreeKey, vec vector.T, key KeyBytes,
) error {
	// Potentially throttle insert operation if background work is falling behind.
	if err := vi.fixups.DelayInsertOrDelete(ctx); err != nil {
		return err
	}

	vi.setupInsertContext(idxCtx, treeKey, vec)

	// Insert the vector into the secondary index.
	childKey := ChildKey{KeyBytes: key}
	valueBytes := ValueBytes{}

	// When a candidate insert partition is found, add the vector to it.
	addFunc := func(ctx context.Context, idxCtx *Context, result *SearchResult) error {
		partitionKey := result.ChildKey.PartitionKey
		return vi.addToPartition(ctx, idxCtx.txn, idxCtx.treeKey,
			partitionKey, idxCtx.level-1, idxCtx.randomized, childKey, valueBytes)
	}

	_, err := vi.searchForUpdateHelper(ctx, idxCtx, addFunc, nil /* deleteKey */)
	return err
}

// Delete attempts to remove a vector from the index, given its value and
// primary key. This is called within the scope of a transaction so that the
// index does not appear to change during the delete.
//
// NOTE: Delete may not be able to locate the vector in the index, meaning a
// "dangling vector" reference will be left in the tree. Vector index methods
// handle this rare case by joining quantized vectors in the tree with their
// corresponding full vector from the primary index (which cannot "dangle")
// before returning search results. For details, see Index.getFullVectors.
func (vi *Index) Delete(
	ctx context.Context, idxCtx *Context, treeKey TreeKey, vec vector.T, key KeyBytes,
) error {
	// Potentially throttle operation if background work is falling behind.
	if err := vi.fixups.DelayInsertOrDelete(ctx); err != nil {
		return err
	}

	vi.setupDeleteContext(idxCtx, treeKey, vec)

	// When a candidate delete partition is found, remove the vector from it.
	removeFunc := func(ctx context.Context, idxCtx *Context, result *SearchResult) error {
		return vi.removeFromPartition(ctx, idxCtx.txn, idxCtx.treeKey,
			result.ParentPartitionKey, idxCtx.level, result.ChildKey)
	}

	_, err := vi.searchForUpdateHelper(ctx, idxCtx, removeFunc, key)
	return err
}

// Search finds vectors in the index that are closest to the given query vector
// and returns them in the search set. Set searchSet.MaxResults to limit the
// number of results. This is called within the scope of a transaction so that
// the index does not appear to change during the search.
func (vi *Index) Search(
	ctx context.Context,
	idxCtx *Context,
	treeKey TreeKey,
	vec vector.T,
	searchSet *SearchSet,
	options SearchOptions,
) error {
	vi.setupContext(idxCtx, treeKey, vec, options, LeafLevel)
	return vi.searchHelper(ctx, idxCtx, searchSet)
}

// SearchForInsert finds the best partition in which to insert the given vector.
// It always returns a single search result containing the key of that
// partition, as well as the centroid of the partition (in the Vector field).
// This is useful for callers that directly insert KV rows rather than using
// this library to do it.
func (vi *Index) SearchForInsert(
	ctx context.Context, idxCtx *Context, treeKey TreeKey, vec vector.T,
) (*SearchResult, error) {
	// Potentially throttle operation if background work is falling behind.
	if err := vi.fixups.DelayInsertOrDelete(ctx); err != nil {
		return nil, err
	}

	vi.setupInsertContext(idxCtx, treeKey, vec)

	// When a candidate insert partition is found, lock its metadata for update
	// and get its centroid.
	getFunc := func(ctx context.Context, idxCtx *Context, result *SearchResult) error {
		partitionKey := result.ChildKey.PartitionKey
		metadata, err := idxCtx.txn.GetPartitionMetadata(
			ctx, treeKey, partitionKey, true /* forUpdate */)
		if err != nil {
			return err
		}
		result.Vector = metadata.Centroid
		return nil
	}

	return vi.searchForUpdateHelper(ctx, idxCtx, getFunc, nil /* deleteKey */)
}

// SearchForDelete finds the leaf partition containing the vector to be deleted.
// It returns a single search result containing the key of that partition, or
// nil if the vector cannot be found. This is useful for callers that directly
// delete KV rows rather than using this library to do it.
func (vi *Index) SearchForDelete(
	ctx context.Context, idxCtx *Context, treeKey TreeKey, vec vector.T, key KeyBytes,
) (*SearchResult, error) {
	// Potentially throttle operation if background work is falling behind.
	if err := vi.fixups.DelayInsertOrDelete(ctx); err != nil {
		return nil, err
	}

	vi.setupDeleteContext(idxCtx, treeKey, vec)

	// When a candidate delete partition is found, lock its metadata for update.
	removeFunc := func(ctx context.Context, idxCtx *Context, result *SearchResult) error {
		partitionKey := result.ParentPartitionKey
		_, err := idxCtx.txn.GetPartitionMetadata(ctx, treeKey, partitionKey, true /* forUpdate */)
		return err
	}

	return vi.searchForUpdateHelper(ctx, idxCtx, removeFunc, key)
}

// SuspendFixups suspends background fixup processing until ProcessFixups is
// explicitly called. It is used for testing.
func (vi *Index) SuspendFixups() {
	vi.fixups.Suspend()
}

// DiscardFixups drops all pending fixups. It is used for testing.
func (vi *Index) DiscardFixups() {
	vi.fixups.Process(true /* discard */)
}

// ProcessFixups waits until all pending fixups have been processed by
// background workers. It is used for testing.
func (vi *Index) ProcessFixups() {
	vi.fixups.Process(false /* discard */)
}

// ForceSplit enqueues a split fixup. It is used for testing.
func (vi *Index) ForceSplit(
	ctx context.Context,
	treeKey TreeKey,
	parentPartitionKey PartitionKey,
	partitionKey PartitionKey,
	singleStep bool,
) {
	vi.fixups.AddSplit(ctx, treeKey, parentPartitionKey, partitionKey, singleStep)
}

// ForceMerge enqueues a merge fixup. It is used for testing.
func (vi *Index) ForceMerge(
	ctx context.Context,
	treeKey TreeKey,
	parentPartitionKey PartitionKey,
	partitionKey PartitionKey,
	singleStep bool,
) {
	vi.fixups.AddMerge(ctx, treeKey, parentPartitionKey, partitionKey, singleStep)
}

// setupInsertContext sets up the given context for an insert operation. Before
// performing an insert, we need to search for the best partition with the
// closest centroid to the query vector. The partition in which to insert the
// vector is at the parent of the leaf level.
func (vi *Index) setupInsertContext(idxCtx *Context, treeKey TreeKey, vec vector.T) {
	// Perform the search using quantized vectors rather than full vectors (i.e.
	// skip reranking).
	vi.setupContext(idxCtx, treeKey, vec, SearchOptions{
		BaseBeamSize: vi.options.BaseBeamSize,
		SkipRerank:   true,
		UpdateStats:  true,
	}, SecondLevel)
	idxCtx.forInsert = true
}

// setupDeleteContext sets up the given context for a delete operation.
func (vi *Index) setupDeleteContext(idxCtx *Context, treeKey TreeKey, vec vector.T) {
	// Perform the search using quantized vectors rather than full vectors (i.e.
	// skip reranking). Use a larger beam size to make it more likely that we'll
	// find the vector to delete.
	vi.setupContext(idxCtx, treeKey, vec, SearchOptions{
		BaseBeamSize: vi.options.BaseBeamSize * 2,
		SkipRerank:   true,
		UpdateStats:  true,
	}, LeafLevel)
	idxCtx.forDelete = true
}

// setupContext sets up the given context as an operation is beginning.
func (vi *Index) setupContext(
	idxCtx *Context, treeKey TreeKey, vec vector.T, options SearchOptions, level Level,
) {
	idxCtx.treeKey = treeKey
	idxCtx.level = level
	idxCtx.original = vec
	idxCtx.forInsert = false
	idxCtx.forDelete = false
	idxCtx.options = options
	if idxCtx.options.BaseBeamSize == 0 {
		idxCtx.options.BaseBeamSize = vi.options.BaseBeamSize
	}

	// Randomize the original vector.
	idxCtx.randomized = ensureSliceLen(idxCtx.randomized, len(vec))
	idxCtx.randomized = vi.RandomizeVector(vec, idxCtx.randomized)
}

// updateFunc is called by searchForUpdateHelper when it has a candidate
// partition to update, i.e. a partition that may allow a vector to be added to
// or removed from it. Different operations will take different actions; for
// example, the Insert operation will attempt to directly add the vector to the
// partition, while the SearchForInsert operation will only lock the partition
// for later update. If the partition is in a state that does not allow adds or
// removes, then the function should return ConditionFailedError with the latest
// metadata.
type updateFunc func(ctx context.Context, idxCtx *Context, result *SearchResult) error

// searchForUpdateHelper searches for the best partition in which to add or from
// which to remove a vector, and then calls the update function. If that returns
// ConditionFailedError, then the partition does not allow adds or removes, so
// searchForUpdateHelper tries again with another partition, and so on.
// Eventually, the search will succeed, and searchForUpdateHelper returns a
// search result containing the partition to update (or nil if no partition
// can be found in the Delete case).
//
// There are two cases to consider, depending on whether we're updating a root
// partition or a non-root partition:
//
//  1. Root partition: If the root partition does not allow updates, then we
//     need to instead update one of its target partitions. However, there are
//     race conditions where those can be splitting in turn, in which case we
//     need to retry the search.
//  2. Non-root partition: If the top search result does not allow updates, then
//     we try the next best result, and so on.
//
// Note that it's possible for retry to switch back and forth between #1 and #2,
// if we're racing with levels being added to or removed from the tree.
func (vi *Index) searchForUpdateHelper(
	ctx context.Context, idxCtx *Context, fn updateFunc, deleteKey KeyBytes,
) (*SearchResult, error) {
	const maxInsertAttempts = 16
	const maxDeleteAttempts = 3
	var maxAttempts int

	idxCtx.tempSearchSet.Clear()
	idxCtx.tempSearchSet.MaxExtraResults = 0
	if idxCtx.forInsert {
		// Insert case, so get extra candidate partitions in case initial
		// candidates don't allow inserts.
		idxCtx.tempSearchSet.MaxResults = vi.options.QualitySamples
		idxCtx.tempSearchSet.MatchKey = nil
		maxAttempts = maxInsertAttempts
	} else {
		// Delete case, so just get 1 result per batch that matches the key.
		// Fetch another batch if first batch doesn't find the vector.
		idxCtx.tempSearchSet.MaxResults = 1
		idxCtx.tempSearchSet.MatchKey = deleteKey
		maxAttempts = maxDeleteAttempts
	}
	idxCtx.search.Init(vi, idxCtx, &idxCtx.tempSearchSet)
	var result *SearchResult
	var lastError error

	// Loop until we find a partition to update or we've exhausted attempts.
	// Each "next batch" operation and each updateFunc callback count as an
	// "attempt", since each is separately expensive to do.
	attempts := 0
	for attempts < maxAttempts {
		// Get next partition to check.
		result = idxCtx.tempSearchSet.PopBestResult()
		if result == nil {
			// Get next batch of results from the searcher.
			attempts++
			ok, err := idxCtx.search.Next(ctx)
			if err != nil {
				log.Infof(ctx, "error during update: %v\n", err)
				return nil, errors.Wrapf(err, "searching for partition to update")
			}
			if !ok {
				log.Infof(ctx, "could not find result: %v\n", result)
				break
			}
			continue
		}

		// Check first result.
		attempts++
		err := fn(ctx, idxCtx, result)
		if err == nil {
			// This partition supports updates, so done.
			break
		}
		lastError = errors.Wrapf(err, "checking result: %+v", result)

		var errConditionFailed *ConditionFailedError
		if errors.Is(err, ErrRestartOperation) {
			// Redo search operation.
			log.VEventf(ctx, 2, "restarting search for update operation: %v", err)
			return vi.searchForUpdateHelper(ctx, idxCtx, fn, deleteKey)
		} else if errors.As(err, &errConditionFailed) {
			// This partition does not allow updates, so fallback to a target
			// partition if this is an insert operation. This is not necessary in
			// the delete case; it's OK if we end up leaving a dangling vector.
			if idxCtx.forInsert {
				partitionKey := result.ChildKey.PartitionKey
				metadata := errConditionFailed.Actual
				err = vi.fallbackOnTargets(ctx, idxCtx, &idxCtx.tempSearchSet,
					partitionKey, idxCtx.randomized, metadata.StateDetails)
				if err != nil {
					return nil, err
				}
			}
		} else if errors.Is(err, ErrPartitionNotFound) {
			// This partition does not exist, so try next partition. This can happen
			// when a DrainingForSplit target partition has itself been split and
			// deleted.
			log.VEventf(ctx, 2, "partition %d not found: %v", result.ChildKey.PartitionKey, err)
		} else {
			return nil, lastError
		}
	}

	if idxCtx.forDelete {
		// Don't perform the inconsistent scan for the delete case, since we've
		// already scanned the partition in order to find the vector to delete and
		// enqueued any needed split/merge fixup.
		return result, nil
	}

	if result == nil {
		// Inserts are expected to find a partition.
		err := errors.Errorf(
			"search failed to find a partition (level=%d) that allows inserts", idxCtx.level)
		return nil, errors.CombineErrors(err, lastError)
	}

	// Do an inconsistent scan of the partition to see if it might be ready to
	// split or merge. This is necessary because the search does not scan leaf
	// partitions. Unless we do this, we may never realize the partition is
	// oversized or undersized.
	// NOTE: The scan is not performed in the scope of the current transaction,
	// so it will not reflect the effects of this operation. That's OK, since it's
	// not necessary to split or merge at exactly the point where the partition
	// becomes oversized or undersized.
	partitionKey := result.ChildKey.PartitionKey
	count, err := vi.store.EstimatePartitionCount(ctx, idxCtx.treeKey, partitionKey)
	if err != nil {
		return nil, errors.Wrapf(err, "counting vectors in partition %d", partitionKey)
	}
	if count >= vi.options.MaxPartitionSize {
		vi.fixups.AddSplit(ctx, idxCtx.treeKey,
			result.ParentPartitionKey, partitionKey, false /* singleStep */)
	}

	return result, nil
}

// fallbackOnTargets is called when none of the partitions returned by a search
// allow inserting a vector, because they are in a Draining state. Instead, the
// search needs to continue with the target partitions of the split (or merge).
// fallbackOnTargets returns an ordered list of search results for the targets.
// NOTE: "tempResults" is overwritten within this method by results.
func (vi *Index) fallbackOnTargets(
	ctx context.Context,
	idxCtx *Context,
	searchSet *SearchSet,
	partitionKey PartitionKey,
	vec vector.T,
	state PartitionStateDetails,
) error {
	if state.State == DrainingForSplitState {
		// Synthesize one search result for each split target partition to pass
		// to getFullVectors.
		idxCtx.tempResults[0] = SearchResult{
			ParentPartitionKey: partitionKey,
			ChildKey:           ChildKey{PartitionKey: state.Target1},
		}
		idxCtx.tempResults[1] = SearchResult{
			ParentPartitionKey: partitionKey,
			ChildKey:           ChildKey{PartitionKey: state.Target2},
		}

		// Fetch the centroids of the target partitions.
		var err error
		tempResults, err := vi.getFullVectors(ctx, idxCtx, idxCtx.tempResults[:2])
		if err != nil {
			return errors.Wrapf(err,
				"fetching centroids for target partitions %d and %d, for splitting partition %d",
				state.Target1, state.Target2, partitionKey)
		}

		// Calculate the distance of the query vector to the centroids.
		for i := range tempResults {
			tempResults[i].QuerySquaredDistance = num32.L2SquaredDistance(vec, tempResults[i].Vector)
			searchSet.Add(&tempResults[i])
		}

		return nil
	}

	// TODO(andyk): Add support for DrainingForMergeState.
	return errors.AssertionFailedf(
		"expected state that disallows updates, not %s", state.String())
}

// addToPartition calls the store to add the given vector to an existing
// partition. If this causes the partition to exceed its maximum size, a split
// fixup will be enqueued.
func (vi *Index) addToPartition(
	ctx context.Context,
	txn Txn,
	treeKey TreeKey,
	partitionKey PartitionKey,
	level Level,
	vec vector.T,
	childKey ChildKey,
	valueBytes ValueBytes,
) error {
	err := txn.AddToPartition(ctx, treeKey, partitionKey, level, vec, childKey, valueBytes)
	if err != nil {
		return err
	}
	return vi.stats.OnAddOrRemoveVector(ctx)
}

// removeFromPartition calls the store to remove a vector, by its key, from an
// existing partition.
func (vi *Index) removeFromPartition(
	ctx context.Context,
	txn Txn,
	treeKey TreeKey,
	partitionKey PartitionKey,
	level Level,
	childKey ChildKey,
) error {
	err := txn.RemoveFromPartition(ctx, treeKey, partitionKey, level, childKey)
	if err != nil {
		return errors.Wrapf(err, "removing vector from partition %d", partitionKey)
	}
	if err = vi.stats.OnAddOrRemoveVector(ctx); err != nil {
		return err
	}
	return nil
}

// searchHelper searches the tree for nearest matches to the query vector and
// adds them to the given search set. The target search set is first cleared.
func (vi *Index) searchHelper(ctx context.Context, idxCtx *Context, searchSet *SearchSet) error {
	searchSet.Clear()
	idxCtx.search.Init(vi, idxCtx, searchSet)
	ok, err := idxCtx.search.Next(ctx)
	if err != nil {
		return errors.Wrapf(err, "searching K-means tree")
	}
	if !ok {
		return errors.AssertionFailedf("expected searcher.Next to return true")
	}

	return nil
}

// rerankSearchResults updates the given set of candidates with their exact
// distances from the query vector. It does this by fetching the original full
// size vectors from the store, in order to re-rank the top candidates for
// extra search result accuracy.
func (vi *Index) rerankSearchResults(
	ctx context.Context, idxCtx *Context, candidates []SearchResult,
) ([]SearchResult, error) {
	if len(candidates) == 0 {
		return candidates, nil
	}

	// Fetch the full vectors from the store.
	candidates, err := vi.getFullVectors(ctx, idxCtx, candidates)
	if err != nil {
		return candidates, err
	}

	queryVector := idxCtx.randomized
	if idxCtx.level == LeafLevel {
		// Leaf vectors haven't been randomized, so compare with the original query
		// vector if available, or un-randomize the randomized vector. The original
		// vector is not available in some cases where split/merge needs to move
		// vectors between partitions.
		if idxCtx.original == nil {
			idxCtx.original = ensureSliceLen(idxCtx.original, len(idxCtx.randomized))
			vi.UnRandomizeVector(idxCtx.randomized, idxCtx.original)
		}
		queryVector = idxCtx.original
	}

	// Compute exact distances for the vectors.
	for i := range candidates {
		candidate := &candidates[i]
		candidate.QuerySquaredDistance = num32.L2SquaredDistance(candidate.Vector, queryVector)
		candidate.ErrorBound = 0
	}

	return candidates, nil
}

// getFullVectors updates the given search candidates with the original full
// size vectors from the store. If a candidate's vector has been deleted from
// the primary index, that candidate is removed from the list of candidates
// that's returned.
func (vi *Index) getFullVectors(
	ctx context.Context, idxCtx *Context, candidates []SearchResult,
) ([]SearchResult, error) {
	// Prepare vector references.
	idxCtx.tempVectorsWithKeys = ensureSliceLen(idxCtx.tempVectorsWithKeys, len(candidates))
	for i := range candidates {
		idxCtx.tempVectorsWithKeys[i].Key = candidates[i].ChildKey
	}

	// The store is expected to fetch the vectors in parallel.
	err := idxCtx.txn.GetFullVectors(ctx, idxCtx.treeKey, idxCtx.tempVectorsWithKeys)
	if err != nil {
		return nil, err
	}

	i := 0
	for i < len(candidates) {
		candidates[i].Vector = idxCtx.tempVectorsWithKeys[i].Vector

		// Exclude deleted child keys from results.
		if candidates[i].Vector == nil {
			// TODO(andyk): Need to create an DeletePartitionKey fixup to handle
			// the case of a dangling partition key.
			if candidates[i].ChildKey.KeyBytes != nil {
				// Vector was deleted, so add fixup to delete it.
				vi.fixups.AddDeleteVector(
					ctx, candidates[i].ParentPartitionKey, candidates[i].ChildKey.KeyBytes)
			}

			// Move the last candidate to the current position and reduce size
			// of slice by one.
			idxCtx.tempVectorsWithKeys[i] = idxCtx.tempVectorsWithKeys[len(candidates)-1]
			candidates[i] = candidates[len(candidates)-1]
			candidates[len(candidates)-1] = SearchResult{} // for GC
			candidates = candidates[:len(candidates)-1]
		} else {
			i++
		}
	}

	return candidates, nil
}

// FormatOptions modifies the behavior of the Format method.
type FormatOptions struct {
	// PrimaryKeyStrings, if true, indicates that primary key bytes should be
	// interpreted as strings. This is used for testing scenarios.
	PrimaryKeyStrings bool
	// RootPartitionKey is the key of the partition that is used as the root of
	// the formatted output. If this is InvalidKey, then RootKey is used by
	// default.
	RootPartitionKey PartitionKey
}

// Format formats the vector index as a tree-formatted string similar to this,
// for testing and debugging purposes:
//
// • 1 (4, 3)
// │
// ├───• vec1 (1, 2)
// ├───• vec2 (7, 4)
// └───• vec3 (4, 3)
//
// Vectors with many dimensions are abbreviated like (5, -1, ..., 2, 8), and
// values are rounded to 4 decimal places. Centroids are printed next to
// partition keys.
func (vi *Index) Format(
	ctx context.Context, treeKey TreeKey, options FormatOptions,
) (str string, err error) {
	// Write formatted bytes to this buffer.
	var buf bytes.Buffer

	writePrimaryKey := func(key KeyBytes) {
		if options.PrimaryKeyStrings {
			buf.WriteString(string(key))
		} else {
			for i, b := range key {
				if i != 0 {
					buf.WriteByte(' ')
				}
				buf.WriteString(strconv.FormatUint(uint64(b), 10))
			}
		}
	}

	var helper func(partitionKey PartitionKey, parentPrefix string, childPrefix string) error
	helper = func(partitionKey PartitionKey, parentPrefix string, childPrefix string) error {
		partition, err := vi.store.TryGetPartition(ctx, treeKey, partitionKey)
		if err != nil {
			if errors.Is(err, ErrPartitionNotFound) {
				// Something else might be modifying the tree as we're trying to
				// print it. Fallback to showing MISSING.
				buf.WriteString(parentPrefix)
				buf.WriteString("• ")
				buf.WriteString(strconv.FormatInt(int64(partitionKey), 10))

				// If this is the root partition, then show synthesized empty
				// partition.
				if partitionKey == RootKey {
					centroid := make(vector.T, vi.quantizer.GetDims())
					buf.WriteByte(' ')
					utils.WriteVector(&buf, centroid, 4)
				} else {
					buf.WriteString(" (MISSING)\n")
				}
				return nil
			}
			return err
		}
		// Get centroid for the partition and un-randomize it so that it displays
		// the original vector.
		random := partition.Centroid()
		original := make(vector.T, len(random))
		vi.UnRandomizeVector(random, original)
		buf.WriteString(parentPrefix)
		buf.WriteString("• ")
		buf.WriteString(strconv.FormatInt(int64(partitionKey), 10))
		buf.WriteByte(' ')
		utils.WriteVector(&buf, original, 4)
		details := partition.Metadata().StateDetails
		if details.State != ReadyState {
			buf.WriteString(" [")
			buf.WriteString(details.String())
			buf.WriteByte(']')
		}
		buf.WriteByte('\n')

		if partition.Count() == 0 {
			return nil
		}

		buf.WriteString(childPrefix)
		buf.WriteString("│\n")

		for i, childKey := range partition.ChildKeys() {
			isLastChild := (i == partition.Count()-1)
			if isLastChild {
				parentPrefix = childPrefix + "└───"
			} else {
				parentPrefix = childPrefix + "├───"
			}

			if partition.Level() == LeafLevel {
				refs := []VectorWithKey{{Key: childKey}}
				err = vi.store.RunTransaction(ctx, func(tx Txn) error {
					return tx.GetFullVectors(ctx, treeKey, refs)
				})
				if err != nil {
					return err
				}
				buf.WriteString(parentPrefix)
				buf.WriteString("• ")
				writePrimaryKey(childKey.KeyBytes)
				if refs[0].Vector != nil {
					buf.WriteByte(' ')
					utils.WriteVector(&buf, refs[0].Vector, 4)
				} else {
					buf.WriteString(" (MISSING)")
				}
				buf.WriteByte('\n')

				if isLastChild && strings.TrimSpace(childPrefix) != "" {
					buf.WriteString(strings.TrimRight(childPrefix, " "))
					buf.WriteByte('\n')
				}
			} else {
				nextChildPrefix := childPrefix
				if isLastChild {
					nextChildPrefix += "    "
				} else {
					nextChildPrefix += "│   "
				}
				if err = helper(childKey.PartitionKey, parentPrefix, nextChildPrefix); err != nil {
					return err
				}
			}
		}

		return nil
	}

	rootKey := options.RootPartitionKey
	if rootKey == InvalidKey {
		rootKey = RootKey
	}
	if err = helper(rootKey, "", ""); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// ensureSliceCap returns a slice of length = 0, of the given capacity and
// generic type. If the existing slice has enough capacity, that slice is
// returned after setting its length to zero. Otherwise, a new, larger slice is
// allocated.
func ensureSliceCap[T any](s []T, c int) []T {
	if cap(s) < c {
		return make([]T, 0, c)
	}
	return s[:0]
}

// ensureSliceLen returns a slice of the given length and generic type. If the
// existing slice has enough capacity, that slice is returned after adjusting
// its length. Otherwise, a new, larger slice is allocated.
// NOTE: Every element of the new slice is uninitialized; callers are
// responsible for initializing the memory.
func ensureSliceLen[T any](s []T, l int) []T {
	// In test builds, always allocate new memory, to catch bugs where callers
	// assume existing slice elements will be copied.
	if cap(s) < l || buildutil.CrdbTestBuild {
		return make([]T, l)
	}
	return s[:l]
}
