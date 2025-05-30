// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"bytes"
	"context"
	"math"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/utils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/vecdist"
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

// DeletedMinCount sets a minimum number of results that will be reranked, in
// order to account for vectors that may have been deleted in the primary index.
const DeletedMinCount = 10

// DeletedMultiplier increases the number of results that will be reranked by
// this factor, in order to account for vectors that may have been deleted in
// the primary index.
const DeletedMultiplier = 1.2

// MaxQualitySamples specifies the max value of the QualitySamples index option.
const MaxQualitySamples = 32

// IncreaseRerankResults returns good values for maxResults and maxExtraResults
// that have a high probability of returning the desired number of results, even
// when there are deleted results. Deleted results will be filtered out by the
// rerank process, so we need to make sure there are additional results that can
// be returned instead.
//
// TODO(andyk): Switch the index to use a search iterator so the caller can keep
// requesting further results rather than guessing at how many additional
// results might be needed.
func IncreaseRerankResults(desiredMaxResults int) (maxResults, maxExtraResults int) {
	maxResults = max(int(math.Ceil(float64(desiredMaxResults)*DeletedMultiplier)), DeletedMinCount)
	maxExtraResults = desiredMaxResults * RerankMultiplier
	return maxResults, maxExtraResults
}

// IndexOptions specifies options that control how the index will be built, as
// well as default options for how it will be searched. A given search operation
// can specify SearchOptions to override the default behavior.
type IndexOptions struct {
	// RotAlgorithm specifies the type of random orthogonal transformation to
	// apply to vectors before indexing and search. See RotAlgorithm for details.
	RotAlgorithm RotAlgorithm
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
	// ReadOnly is true if the index cannot be modified. If true, the index will
	// not perform any fixups, even if a too-large or too-small partition is
	// discovered during a search.
	ReadOnly bool
	// MaxWorkers specifies the maximum number of background workers that can be
	// created to process fixups for this vector index instance.
	MaxWorkers int
	// StalledOpTimeout can be called to determine how long a split/merge
	// operation can stay in the same state before another worker may attempt to
	// assist. If this is nil, then a default value is used.
	StalledOpTimeout func() time.Duration
	// MaxInsertAttempts controls the number of times Insert or SearchForInsert
	// will retry after finding partitions that do not allow inserts.
	MaxInsertAttempts int
	// MaxDeleteAttempts controls the number of times Delete or SearchForDelete
	// will retry after failed attempts to find a requested deletion vector.
	MaxDeleteAttempts int
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
	// query manages the query vector that was passed to the top-level Index
	// method.
	query queryComparer
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

// RandomizedVector is the randomized form of OriginalVector.
func (ic *Context) RandomizedVector() vector.T {
	return ic.query.Randomized()
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
// Each partition contains a cluster of quantized vectors (typically ~100
// vectors) grouped around a centroid within the K-means tree. A metadata
// record stores the centroid and tracks the partition's current state. Leaf
// partitions reference primary index rows and store quantized versions of the
// full vectors. Interior partitions reference child partitions at the next
// level down and store quantized versions of those partitions' centroids.
//
// The index supports multiple distance metrics: L2Squared, Cosine, and
// InnerProduct. Normalization behavior varies by metric:
//
//   - Cosine: Both leaf and centroid vectors are normalized before quantization
//     since cosine similarity is magnitude-agnostic and more efficient with
//     unit vectors. Query vectors are also normalized for similar reasons.
//   - InnerProduct: Only centroid vectors are normalized before quantization.
//     This converts mean centroids to spherical centroids, preventing
//     high-magnitude centroids from attracting disproportionate numbers of
//     vectors to their partitions.
//   - L2Squared: No normalization is applied.
//
// All query and data vectors undergo random orthogonal transformation (ROT)
// before searching or storage. This redistributes skew more evenly across
// dimensions while preserving distances and angles between vectors.
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
	// rot computes random orthogonal transformations on query and data vectors
	// to more evenly distribute skew across dimensions.
	rot RandomOrthoTransformer
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
		rootQuantizer: quantize.NewUnQuantizer(quantizer.GetDims(), quantizer.GetDistanceMetric()),
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
	if vi.options.MaxInsertAttempts == 0 {
		vi.options.MaxInsertAttempts = 32
	}
	if vi.options.MaxDeleteAttempts == 0 {
		vi.options.MaxDeleteAttempts = 3
	}

	if vi.options.MaxPartitionSize < 2 {
		return nil, errors.AssertionFailedf("MaxPartitionSize cannot be less than 2")
	}
	if vi.options.QualitySamples > MaxQualitySamples {
		return nil, errors.Errorf(
			"QualitySamples option %d exceeds max allowed value", vi.options.QualitySamples)
	}

	// Initialize the random orthogonal transformer.
	vi.rot.Init(vi.options.RotAlgorithm, quantizer.GetDims(), seed)

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

// TransformVector performs a random orthogonal transformation (ROT) on the
// "original" vector and writes it to the "randomized" vector. If the index uses
// the Cosine distance metric, it also ensures that the original vector has been
// normalized into a unit vector (norm = 1). The caller is responsible for
// allocating the randomized vector with length equal to the index's dimensions.
//
// Randomizing vectors distributes skew more evenly across dimensions and
// across vectors in a set. Distance and angle between any two vectors
// remains unchanged, as long as the same ROT is applied to both.
//
// Query and data vectors are assumed to be normalized when calculating Cosine
// distances.
func (vi *Index) TransformVector(original vector.T, randomized vector.T) vector.T {
	vi.rot.RandomizeVector(original, randomized)
	if vi.quantizer.GetDistanceMetric() == vecdist.Cosine {
		num32.Normalize(randomized)
	}
	return randomized
}

// UnRandomizeVector inverts the random orthogonal transformation performed by
// TransformVector, in order to recover the normalized vector in the case of
// Cosine distance, or the original vector in the case of other distance
// functions. The caller is responsible for allocating the original vector with
// length equal to the index's dimensions.
func (vi *Index) UnRandomizeVector(randomized vector.T, normalized vector.T) vector.T {
	return vi.rot.UnRandomizeVector(randomized, normalized)
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
//
// NOTE: The caller is assumed to own the memory for all parameters and can
// reuse the memory after the call returns.
// TODO(andyk): This is not true of the MemStore.
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
		err := vi.addToPartition(ctx, idxCtx.txn, idxCtx.treeKey,
			partitionKey, idxCtx.level-1, idxCtx.query.Randomized(), childKey, valueBytes)
		if err != nil {
			return errors.Wrapf(err, "inserting vector into partition %d", partitionKey)
		}
		return nil
	}

	_, err := vi.searchForUpdateHelper(
		ctx, idxCtx, addFunc, nil /* deleteKey */, vi.options.MaxInsertAttempts)
	return err
}

// Delete attempts to remove a vector from the index, given its value and
// primary key. This is called within the scope of a transaction so that the
// index does not appear to change during the delete. It returns true if the
// vector was removed from the index.
//
// NOTE: Delete may not be able to locate the vector in the index, meaning a
// "dangling vector" reference will be left in the tree. Vector index methods
// handle this rare case by joining quantized vectors in the tree with their
// corresponding full vector from the primary index (which cannot "dangle")
// before returning search results. For details, see Index.findExactDistances.
//
// NOTE: Even if the vector is removed, there may still be duplicate dangling
// instances of the vector still remaining in the index.
//
// NOTE: The caller is assumed to own the memory for all parameters and can
// reuse the memory after the call returns.
// TODO(andyk): This is not true of the MemStore.
func (vi *Index) Delete(
	ctx context.Context, idxCtx *Context, treeKey TreeKey, vec vector.T, key KeyBytes,
) (deleted bool, err error) {
	// Potentially throttle operation if background work is falling behind.
	if err := vi.fixups.DelayInsertOrDelete(ctx); err != nil {
		return false, err
	}

	vi.setupDeleteContext(idxCtx, treeKey, vec)

	// When a candidate delete partition is found, remove the vector from it.
	removeFunc := func(ctx context.Context, idxCtx *Context, result *SearchResult) error {
		partitionKey := result.ParentPartitionKey
		err := vi.removeFromPartition(ctx, idxCtx.txn, idxCtx.treeKey,
			partitionKey, idxCtx.level, result.ChildKey)
		if err != nil {
			return errors.Wrapf(err, "deleting vector from partition %d", partitionKey)
		}
		return nil
	}

	result, err := vi.searchForUpdateHelper(
		ctx, idxCtx, removeFunc, key, vi.options.MaxDeleteAttempts)
	return result != nil, err
}

// Search finds vectors in the index that are closest to the given query vector
// and returns them in the search set. Set searchSet.MaxResults to limit the
// number of results. This is called within the scope of a transaction so that
// the index does not appear to change during the search.
//
// NOTE: The caller is assumed to own the memory for all parameters and can
// reuse the memory after the call returns.
// TODO(andyk): This is not true of the MemStore.
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
//
// NOTE: The caller is assumed to own the memory for all parameters and can
// reuse the memory after the call returns.
// TODO(andyk): This is not true of the MemStore.
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
			return errors.Wrapf(err, "locking metadata for insert into partition %d", partitionKey)
		}
		result.Vector = metadata.Centroid
		return nil
	}

	return vi.searchForUpdateHelper(
		ctx, idxCtx, getFunc, nil /* deleteKey */, vi.options.MaxInsertAttempts)
}

// SearchForDelete finds the leaf partition containing the vector to be deleted.
// It returns a single search result containing the key of that partition, or
// nil if the vector cannot be found. This is useful for callers that directly
// delete KV rows rather than using this library to do it.
//
// NOTE: The caller is assumed to own the memory for all parameters and can
// reuse the memory after the call returns.
// TODO(andyk): This is not true of the MemStore.
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
		if err != nil {
			return errors.Wrapf(err, "locking metadata for delete from partition %d", partitionKey)
		}
		return nil
	}

	return vi.searchForUpdateHelper(ctx, idxCtx, removeFunc, key, vi.options.MaxDeleteAttempts)
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
		UpdateStats:  !vi.options.DisableAdaptiveSearch,
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
		UpdateStats:  !vi.options.DisableAdaptiveSearch,
	}, LeafLevel)
	idxCtx.forDelete = true
}

// setupContext sets up the given context as an operation is beginning.
func (vi *Index) setupContext(
	idxCtx *Context, treeKey TreeKey, vec vector.T, options SearchOptions, level Level,
) {
	idxCtx.treeKey = treeKey
	idxCtx.level = level
	idxCtx.query.Init(vi.quantizer.GetDistanceMetric(), vec, &vi.rot)
	idxCtx.forInsert = false
	idxCtx.forDelete = false
	idxCtx.options = options
	if idxCtx.options.BaseBeamSize == 0 {
		idxCtx.options.BaseBeamSize = vi.options.BaseBeamSize
	}
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
	ctx context.Context, idxCtx *Context, fn updateFunc, deleteKey KeyBytes, remainingAttempts int,
) (*SearchResult, error) {
	idxCtx.tempSearchSet.Clear()
	idxCtx.tempSearchSet.MaxExtraResults = 0
	if idxCtx.forInsert {
		// Insert case, so get extra candidate partitions in case initial
		// candidates don't allow inserts.
		idxCtx.tempSearchSet.MaxResults = vi.options.QualitySamples
		idxCtx.tempSearchSet.MatchKey = nil
	} else {
		// Delete case, so just get 1 result per batch that matches the key.
		// Fetch another batch if first batch doesn't find the vector.
		idxCtx.tempSearchSet.MaxResults = 1
		idxCtx.tempSearchSet.MatchKey = deleteKey
	}
	idxCtx.search.Init(vi, idxCtx, &idxCtx.tempSearchSet)
	var result *SearchResult
	var lastError error

	// Loop until we find a partition to update or we've exhausted attempts.
	// Each "next batch" operation and each updateFunc callback count as an
	// "attempt", since each is separately expensive to do.
	for remainingAttempts > 0 {
		// Get next partition to check.
		result = idxCtx.tempSearchSet.PopBestResult()
		if result == nil {
			// Get next batch of results from the searcher.
			remainingAttempts--
			ok, err := idxCtx.search.Next(ctx)
			if err != nil {
				log.Infof(ctx, "error during update: %v", err)
				return nil, errors.Wrapf(err, "searching for partition to update")
			}
			if !ok {
				if idxCtx.forInsert {
					return vi.searchForUpdateHelper(ctx, idxCtx, fn, deleteKey, remainingAttempts)
				}
				break
			}
			continue
		}

		// Check first result.
		remainingAttempts--
		err := fn(ctx, idxCtx, result)
		if err == nil {
			// This partition supports updates, so done.
			break
		}
		lastError = errors.Wrapf(err, "failed to update (remaining attempts=%d)", remainingAttempts)

		var errConditionFailed *ConditionFailedError
		if errors.Is(err, ErrRestartOperation) {
			// Redo search operation.
			log.VEventf(ctx, 2, "restarting search for update operation: %v", lastError)
			return vi.searchForUpdateHelper(ctx, idxCtx, fn, deleteKey, remainingAttempts)
		} else if errors.As(err, &errConditionFailed) {
			state := errConditionFailed.Actual.StateDetails
			log.VEventf(ctx, 2, "updates not allowed in state %s: %v", state.String(), lastError)

			// This partition does not allow updates, so fallback to a target
			// partition if this is an insert operation. This is not necessary in
			// the delete case; it's OK if we end up leaving a dangling vector.
			if idxCtx.forInsert {
				// If splitting the root, the new parent will be the root.
				var parentPartitionKey PartitionKey
				sourcePartitionKey := result.ChildKey.PartitionKey
				if sourcePartitionKey == RootKey {
					// Splitting the root partition, so the parent of the target
					// partitions will be the root.
					parentPartitionKey = RootKey
				} else {
					// Splitting a non-root partition, so the parent of the target
					// partitions will be the parent of the splitting partition.
					parentPartitionKey = result.ParentPartitionKey
				}

				err = vi.fallbackOnTargets(ctx, idxCtx, &idxCtx.tempSearchSet,
					parentPartitionKey, sourcePartitionKey, state)
				if err != nil {
					return nil, err
				}
			}
		} else if errors.Is(err, ErrPartitionNotFound) {
			// This partition does not exist, so try next partition. This can happen
			// when a DrainingForSplit target partition has itself been split and
			// deleted.
			log.VEventf(ctx, 2, "partition %d not found: %v", result.ChildKey.PartitionKey, lastError)
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
		// TODO(andyk): Should we make this error retryable server-side and/or by
		// the user?
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
// allow inserting a vector, because they are in a Draining or Deleting state.
// Instead, the search needs to continue with the target partitions of the split
// (or merge). fallbackOnTargets returns an ordered list of search results for
// the targets.
// NOTE: "tempResults" is overwritten within this method by results.
func (vi *Index) fallbackOnTargets(
	ctx context.Context,
	idxCtx *Context,
	searchSet *SearchSet,
	parentPartitionKey, sourcePartitionKey PartitionKey,
	state PartitionStateDetails,
) error {
	switch state.State {
	case DrainingForSplitState:
		// Synthesize one search result for each split target partition to pass
		// to findExactDistances.
		idxCtx.tempResults[0] = SearchResult{
			ParentPartitionKey: parentPartitionKey,
			ChildKey:           ChildKey{PartitionKey: state.Target1},
		}
		idxCtx.tempResults[1] = SearchResult{
			ParentPartitionKey: parentPartitionKey,
			ChildKey:           ChildKey{PartitionKey: state.Target2},
		}

		// Get exact distance of the query vector from the centroids of the target
		// partitions.
		var err error
		tempResults, err := vi.findExactDistances(ctx, idxCtx, idxCtx.tempResults[:2])
		if err != nil {
			return errors.Wrapf(err,
				"finding exact distances from target partitions %d and %d, for splitting partition %d",
				state.Target1, state.Target2, sourcePartitionKey)
		}

		// Add the exact results to the search set.
		for i := range tempResults {
			searchSet.Add(&tempResults[i])
		}

		return nil

	case DeletingForSplitState:
		// The partition is ready for deletion; its target partitions have already
		// been built and may themselves been split or deleted, so don't use them.
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
	vi.validateVectorToAdd(level, vec)

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

// findExactDistances updates the given search candidates with the original full
// size vectors from the store and computes their exact distances from the query
// vector. It does this by fetching the original full-size vectors from the
// store. If a candidate vector cannot be found in the store, that candidate is
// removed from the list of candidates that's returned.
func (vi *Index) findExactDistances(
	ctx context.Context, idxCtx *Context, candidates []SearchResult,
) ([]SearchResult, error) {
	if len(candidates) == 0 {
		return candidates, nil
	}

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
		candidate := &candidates[i]
		candidate.Vector = idxCtx.tempVectorsWithKeys[i].Vector

		// Exclude deleted child keys from results.
		if candidate.Vector == nil {
			// TODO(andyk): Need to create an DeletePartitionKey fixup to handle
			// the case of a dangling partition key.
			if candidate.ChildKey.IsPrimaryIndexBytes() {
				// Vector was deleted, so add fixup to delete it.
				vi.fixups.AddDeleteVector(ctx, idxCtx.treeKey,
					candidate.ParentPartitionKey, candidate.ChildKey.KeyBytes)
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

	// Compute exact distance between query vector and the data vectors.
	idxCtx.query.ComputeExactDistances(idxCtx.level, candidates)

	return candidates, nil
}

// validateVectorToAdd ensures a vector being added to partitions is a unit
// vector when required by the distance metric: always for Cosine, and for
// InnerProduct only in interior partitions (not leaf partitions).
func (vi *Index) validateVectorToAdd(level Level, vec vector.T) {
	if buildutil.CrdbTestBuild {
		switch vi.quantizer.GetDistanceMetric() {
		case vecdist.InnerProduct:
			if level != LeafLevel {
				utils.ValidateUnitVector(vec)
			}

		case vecdist.Cosine:
			utils.ValidateUnitVector(vec)
		}
	}
}

// validateVectorsToAdd is similar to validateVectorToAdd, but works for a set
// of vectors.
func (vi *Index) validateVectorsToAdd(level Level, vectors vector.Set) {
	if buildutil.CrdbTestBuild {
		switch vi.quantizer.GetDistanceMetric() {
		case vecdist.InnerProduct:
			if level != LeafLevel {
				utils.ValidateUnitVectors(vectors)
			}

		case vecdist.Cosine:
			utils.ValidateUnitVectors(vectors)
		}
	}
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
