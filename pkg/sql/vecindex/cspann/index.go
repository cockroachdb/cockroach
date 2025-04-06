// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/utils"
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
	// DisableErrorBounds can be set to true to disable using error bounds to
	// limit the number of results that need to be reranked. This is useful for
	// testing and benchmarking.
	DisableErrorBounds bool
	// IsDeterministic instructs the vector index to use a deterministic random
	// number generator for performing operations and fixups. As long as the
	// index is initialized with the same seed and fixups happen serially, the
	// index will behave deterministically. This is useful for testing.
	IsDeterministic bool
	// MaxWorkers specifies the maximum number of background workers that can be
	// created to process fixups for this vector index instance.
	MaxWorkers int
	// UseNewFixups specifies that the background fixup processor should use new
	// split and merge fixup implementations that avoid the use of transactions.
	// TODO(andyk): Remove this once we've fully converted to the new fixups.
	UseNewFixups bool
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
	// ReturnVectors specifies whether to return the original full-size vectors
	// in search results. If this is a leaf-level search then the returned
	// vectors have not been randomized.
	ReturnVectors bool
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
	// ignoreLonelyVector, if true, prohibits searches from returning a vector
	// that is the last remaining in its partition. This is used to avoid moving
	// the last remaining vector to another partition, thereby creating an empty
	// non-leaf partition, which is not allowed in a balanced K-means tree.
	ignoreLonelyVector bool
	// forInsert indicates that this is an insert operation (or a search for
	// insert operation).
	forInsert bool
	// forDelete indicates that this is a delete operation (or a search for
	// delete operation).
	forDelete bool

	tempSearchSet       SearchSet
	tempSubSearchSet    SearchSet
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
	return vi.insertHelper(ctx, idxCtx, childKey, ValueBytes{})
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
	result, err := vi.SearchForDelete(ctx, idxCtx, treeKey, vec, key)
	if err != nil {
		return err
	}
	if result == nil {
		return nil
	}

	// Remove the vector from its partition in the store.
	return vi.removeFromPartition(
		ctx, idxCtx.txn, treeKey, result.ParentPartitionKey, LeafLevel, result.ChildKey)
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

	return vi.searchForInsertHelper(ctx, idxCtx, getFunc)
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

	// Don't rerank results, since we just need a key match.
	vi.setupContext(idxCtx, treeKey, vec, SearchOptions{
		SkipRerank:  true,
		UpdateStats: true,
	}, LeafLevel)
	idxCtx.forDelete = true

	idxCtx.tempSearchSet = SearchSet{MaxResults: 1, MatchKey: key}

	// Search with the base beam size. If that fails to find the vector, try again
	// with a larger beam size, in order to minimize the chance of dangling
	// vector references in the index.
	baseBeamSize := max(vi.options.BaseBeamSize, 1)
	for range 2 {
		idxCtx.options.BaseBeamSize = baseBeamSize

		err := vi.searchHelper(ctx, idxCtx, &idxCtx.tempSearchSet)
		if err != nil {
			return nil, err
		}
		results := idxCtx.tempSearchSet.PopResults()
		if len(results) == 0 {
			// Retry search with significantly higher beam size.
			baseBeamSize *= 8
		} else {
			return &results[0], nil
		}
	}

	return nil, nil
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
		SkipRerank:   vi.options.DisableErrorBounds,
		UpdateStats:  true,
	}, SecondLevel)
	idxCtx.forInsert = true
}

// setupContext sets up the given context as an operation is beginning.
func (vi *Index) setupContext(
	idxCtx *Context, treeKey TreeKey, vec vector.T, options SearchOptions, level Level,
) {
	idxCtx.treeKey = treeKey
	idxCtx.options = options
	idxCtx.level = level
	idxCtx.original = vec
	idxCtx.forInsert = false

	// Randomize the original vector.
	idxCtx.randomized = ensureSliceLen(idxCtx.randomized, len(vec))
	idxCtx.randomized = vi.RandomizeVector(vec, idxCtx.randomized)
}

// insertHelper looks for the best partition in which to add the vector and then
// adds the vector to that partition. This is an internal helper method that can
// be used by callers once they have set up a search context.
func (vi *Index) insertHelper(
	ctx context.Context, idxCtx *Context, childKey ChildKey, valueBytes ValueBytes,
) error {
	// When a candidate insert partition is found, add the vector to it.
	addFunc := func(ctx context.Context, idxCtx *Context, result *SearchResult) error {
		partitionKey := result.ChildKey.PartitionKey
		return vi.addToPartition(ctx, idxCtx.txn, idxCtx.treeKey,
			partitionKey, idxCtx.level-1, idxCtx.randomized, childKey, valueBytes)
	}

	_, err := vi.searchForInsertHelper(ctx, idxCtx, addFunc)
	return err
}

// insertFunc is called by searchForInsertHelper when it has a candidate insert
// partition, i.e. a partition that may allow a vector to be added to it. The
// Insert operation will attempt to directly add the vector, while the
// SearchForInsert operation will only lock the partition for later update. If
// the partition is in a state that does not allow adds or removes, then the
// function will return ConditionFailedError with the latest metadata.
type insertFunc func(ctx context.Context, idxCtx *Context, result *SearchResult) error

// searchForInsertHelper searches for the best partition in which to add a
// vector and calls the insert function. If that returns ConditionFailedError,
// then the partition does not allow adds or removes, so searchForInsertHelper
// tries again with another partition, and so on. Eventually, the search will
// succeed, and searchForInsertHelper returns a search result containing the
// insert partition (never nil).
//
// There are two cases to consider, depending on whether we're inserting into a
// root partition or a non-root partition:
//
//  1. Root partition: If the root partition does not allow inserts, then we
//     need to instead insert into one of its target partitions. However, there
//     are race conditions where those can be splitting in turn, in which case
//     we need to retry the search.
//  2. Non-root partition: If the top search result does not allow inserts, then
//     we need to try a different result. We start by fetching 2 results, so
//     that there's a backup. If both results do not allow inserts, we widen the
//     search to 4 results, then to 8, and so on (up to 32, upon which we return
//     an error).
//
// Note that it's possible for retry to switch back and forth between #1 and #2,
// if we're racing with levels being added to or removed from the tree.
func (vi *Index) searchForInsertHelper(
	ctx context.Context, idxCtx *Context, fn insertFunc,
) (*SearchResult, error) {
	// Loop until we find an insert partition. Fetch two candidate partitions to
	// start so that we rarely need to re-search, even in the case where the first
	// candidate partition does not allow inserts.
	var original, results []SearchResult
	var seen map[PartitionKey]struct{}
	maxResults := 2
	allowRetry := true
	for {
		// If there are no more results, get more now.
		if len(results) == 0 {
			if !allowRetry || maxResults > 32 {
				// This is an extreme edge case where none of the partitions we've
				// checked allow inserts.
				// TODO(andyk): Do we need to do something better here?
				return nil, errors.Newf(
					"search failed to find a partition (level=%d) that allows inserts: %v",
					idxCtx.level, original)
			}

			// In most cases, the top result is the best insert partition. However, if
			// that partition does not allow inserts, we need to fall back to another
			// partition. Set MaxResults in the same way as searchHelper does.
			idxCtx.tempSearchSet = SearchSet{MaxResults: maxResults}
			err := vi.searchHelper(ctx, idxCtx, &idxCtx.tempSearchSet)
			if err != nil {
				return nil, err
			}
			original = idxCtx.tempSearchSet.PopResults()
			if len(original) < 1 {
				return nil, errors.AssertionFailedf("SearchForInsert should return at least one result")
			}
			results = original
			allowRetry = false
		}

		// Check first result.
		partitionKey := results[0].ChildKey.PartitionKey
		if seen != nil {
			// Don't re-check partitions we've already checked.
			if _, ok := seen[partitionKey]; ok {
				results = results[1:]
				continue
			}
		}

		// Allow retry since there's at least one result we haven't yet seen.
		allowRetry = true

		err := fn(ctx, idxCtx, &results[0])
		if err == nil {
			// This partition supports inserts, so done.
			break
		}

		var errConditionFailed *ConditionFailedError
		if errors.Is(err, ErrRestartOperation) {
			// Redo search operation.
			results = results[:0]
			continue
		} else if errors.As(err, &errConditionFailed) {
			// This partition does not allow adds or removes, so fallback to
			// another partition.
			if partitionKey == RootKey {
				// This is the root partition, so fallback to its target partitions.
				metadata := errConditionFailed.Actual
				results, err = vi.fallbackOnTargets(ctx, idxCtx,
					partitionKey, idxCtx.randomized, metadata.StateDetails, results)
				if err != nil {
					return nil, err
				}
			} else {
				// This is a non-root partition, so check the next partition in the
				// result list. If there are no more partitions to check, then expand
				// the search to a wider set of partitions by increasing MaxResults.
				results = results[1:]
				if len(results) == 0 {
					if seen == nil {
						seen = make(map[PartitionKey]struct{})
					}
					seen[partitionKey] = struct{}{}
					maxResults *= 2
				}
			}
		} else {
			return nil, err
		}
	}

	// Do an inconsistent scan of the partition to see if it might be ready to
	// split. This is necessary because the search does not scan leaf partitions.
	// Unless we do this, we may never realize the partition is oversized.
	// NOTE: The scan is not performed in the scope of the current transaction,
	// so it will not reflect the effects of this operation. That's OK, since it's
	// not necessary to split at exactly the point where the partition becomes
	// oversized.
	partitionKey := results[0].ChildKey.PartitionKey
	count, err := vi.store.EstimatePartitionCount(ctx, idxCtx.treeKey, partitionKey)
	if err != nil {
		return nil, errors.Wrapf(err, "counting vectors in partition %d", partitionKey)
	}
	if count >= vi.options.MaxPartitionSize {
		vi.fixups.AddSplit(ctx, idxCtx.treeKey,
			results[0].ParentPartitionKey, partitionKey, false /* singleStep */)
	}

	return &results[0], nil
}

// fallbackOnTargets is called when none of the partitions returned by a search
// allow inserting a vector, because they are in a Draining state. Instead, the
// search needs to continue with the target partitions of the split (or merge).
// fallbackOnTargets returns an ordered list of search results for the targets.
// NOTE: "tempResults" is overwritten within this method by results.
func (vi *Index) fallbackOnTargets(
	ctx context.Context,
	idxCtx *Context,
	partitionKey PartitionKey,
	vec vector.T,
	state PartitionStateDetails,
	tempResults []SearchResult,
) ([]SearchResult, error) {
	if state.State == DrainingForSplitState {
		// Synthesize one search result for each split target partition to pass
		// to getFullVectors.
		tempResults = tempResults[:0]
		tempResults = append(tempResults, SearchResult{
			ParentPartitionKey: partitionKey,
			ChildKey:           ChildKey{PartitionKey: state.Target1},
		})
		tempResults = append(tempResults, SearchResult{
			ParentPartitionKey: partitionKey,
			ChildKey:           ChildKey{PartitionKey: state.Target2},
		})

		// Fetch the centroids of the target partitions.
		var err error
		tempResults, err = vi.getFullVectors(ctx, idxCtx, tempResults)
		if err != nil {
			return nil, errors.Wrapf(err,
				"fetching centroids for target partitions %d and %d, for splitting partition %d",
				state.Target1, state.Target2, partitionKey)
		}
		if len(tempResults) != 2 {
			return nil, errors.AssertionFailedf(
				"expected to get two centroids for state %s", state.String())
		}

		// Order by the distance of the centroids from the query vector.
		dist1 := num32.L2SquaredDistance(vec, tempResults[0].Vector)
		dist2 := num32.L2SquaredDistance(vec, tempResults[1].Vector)

		tempResults[0].QuerySquaredDistance = dist1
		tempResults[1].QuerySquaredDistance = dist2

		if dist1 > dist2 {
			// Swap results.
			tempResult := tempResults[0]
			tempResults[0] = tempResults[1]
			tempResults[1] = tempResult
		}
		return tempResults, nil
	}

	// TODO(andyk): Add support for DrainingForMergeState.
	return nil, errors.AssertionFailedf(
		"expected state that disallows adds/removes, not %s", state.String())
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
		return errors.Wrapf(err, "adding vector to partition %d", partitionKey)
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

// searchHelper contains the core search logic for the K-means tree. It begins
// at the root and proceeds downwards, breadth-first. At each level of the tree,
// it searches the subset of partitions that have centroids nearest to the query
// vector. Using estimated distance calculations, the search finds the nearest
// quantized data vectors within these partitions. If at an interior level,
// these data vectors are the quantized representation of centroids in the next
// level down, and the search continues there. If at the leaf level, then these
// data vectors are the quantized representation of the original vectors that
// were inserted into the tree. The original, full-size vectors are fetched from
// the primary index and used to re-rank candidate search results.
func (vi *Index) searchHelper(ctx context.Context, idxCtx *Context, searchSet *SearchSet) error {
	// Return enough search results to:
	// 1. Ensure that the number of results requested by the caller is respected.
	// 2. Ensure that there are enough samples for calculating stats.
	// 3. Ensure that there are enough results for adaptive querying to dynamically
	//    expand the beam size (up to 4x the base beam size).
	maxResults := max(
		searchSet.MaxResults, vi.options.QualitySamples, idxCtx.options.BaseBeamSize*4)
	subSearchSet := &idxCtx.tempSubSearchSet
	*subSearchSet = SearchSet{MaxResults: maxResults}
	idxCtx.tempResults[0] = SearchResult{ChildKey: ChildKey{PartitionKey: RootKey}}
	searchLevel, err := vi.searchChildPartitions(ctx, idxCtx, subSearchSet, idxCtx.tempResults[:1])
	if err != nil {
		return err
	}

	if searchLevel < idxCtx.level {
		// This should only happen when inserting into the root.
		if searchLevel != idxCtx.level-1 {
			panic(errors.AssertionFailedf("caller passed invalid level %d", idxCtx.level))
		}
		if idxCtx.options.ReturnVectors {
			panic(errors.AssertionFailedf("ReturnVectors=true not supported for this case"))
		}
		searchSet.Add(&SearchResult{
			ChildKey: ChildKey{PartitionKey: RootKey},
		})
		return nil
	}

	for {
		results := subSearchSet.PopResults()
		if len(results) == 0 && searchLevel > LeafLevel {
			// This should never happen, as it means that interior partition(s)
			// have no children. The vector deletion logic should prevent that.
			panic(errors.AssertionFailedf(
				"interior partition(s) on level %d has no children", searchLevel))
		}

		var zscore float64
		if searchLevel > LeafLevel {
			// Compute the Z-score of the candidate list if there are enough
			// samples. Otherwise, use the default Z-score of 0.
			if len(results) >= vi.options.QualitySamples {
				for i := range vi.options.QualitySamples {
					idxCtx.tempQualitySamples[i] = float64(results[i].QuerySquaredDistance)
				}
				samples := idxCtx.tempQualitySamples[:vi.options.QualitySamples]
				zscore = vi.stats.ReportSearch(searchLevel, samples, idxCtx.options.UpdateStats)
			}
		}

		if searchLevel <= idxCtx.level {
			// We've reached the end of the search.
			if searchLevel != idxCtx.level {
				// This indicates index corruption, since each lower level should
				// be one less than its parent level.
				panic(errors.AssertionFailedf("somehow skipped to level %d when searching for level %d",
					searchLevel, idxCtx.level))
			}

			// Aggregate all stats from searching lower levels of the tree.
			searchSet.Stats.Add(&subSearchSet.Stats)

			if !idxCtx.options.SkipRerank || idxCtx.options.ReturnVectors {
				// Re-rank search results with full vectors.
				searchSet.Stats.FullVectorCount += len(results)
				results, err = vi.rerankSearchResults(ctx, idxCtx, results)
				if err != nil {
					return err
				}
			}
			searchSet.AddAll(results)
			break
		}

		// Calculate beam size for searching next level.
		beamSize := idxCtx.options.BaseBeamSize
		if beamSize == 0 {
			beamSize = vi.options.BaseBeamSize
		}

		if !vi.options.DisableAdaptiveSearch {
			// Look at variance in result distances to calculate the beam size for
			// the next level. The less variance there is, the larger the beam size.
			// The intuition is that the closer the distances are to one another, the
			// more densely packed are the vectors, and the more partitions they're
			// likely to be spread across.
			tempBeamSize := float64(beamSize) * math.Pow(2, -zscore)
			tempBeamSize = max(min(tempBeamSize, float64(beamSize)*2), float64(beamSize)/2)

			if searchLevel > LeafLevel+1 {
				// Use progressively smaller beam size for higher levels, since
				// each contains exponentially fewer partitions.
				tempBeamSize /= math.Pow(2, float64(searchLevel-(LeafLevel+1)))
			}

			beamSize = int(math.Ceil(tempBeamSize))
		}
		beamSize = max(beamSize, 1)

		searchLevel--
		if searchLevel == idxCtx.level {
			// Searching the last level, so return enough search results to:
			// 1. Ensure that the number of results requested by the caller is
			//    respected.
			// 2. Ensure there are enough samples for re-ranking to work well, even
			//    if there are deleted vectors.
			if !vi.options.DisableErrorBounds {
				subSearchSet.MaxResults = int(math.Ceil(float64(searchSet.MaxResults) * DeletedMultiplier))
				subSearchSet.MaxExtraResults = subSearchSet.MaxResults * RerankMultiplier
			} else {
				subSearchSet.MaxResults = searchSet.MaxResults * RerankMultiplier / 2
				subSearchSet.MaxExtraResults = 0
			}

			if searchLevel > LeafLevel {
				// Ensure there are enough results for calculating stats.
				subSearchSet.MaxResults = max(subSearchSet.MaxResults, vi.options.QualitySamples)
			}
		}

		// Search up to beamSize child partitions. The results are in sorted order,
		// since we always sort non-leaf levels above, and this must be a non-leaf
		// level (leaf-level partitions do not have children).
		results = results[:min(beamSize, len(results))]
		_, err = vi.searchChildPartitions(ctx, idxCtx, subSearchSet, results)
		if errors.Is(err, ErrRestartOperation) {
			return vi.searchHelper(ctx, idxCtx, searchSet)
		} else if err != nil {
			return err
		}
	}

	return nil
}

// searchChildPartitions searches for nearest neighbors to the query vector in
// the set of partitions referenced by the given search results. It adds the
// closest matches to the given search set.
func (vi *Index) searchChildPartitions(
	ctx context.Context, idxCtx *Context, searchSet *SearchSet, parentResults SearchResults,
) (level Level, err error) {
	idxCtx.tempToSearch = ensureSliceLen(idxCtx.tempToSearch, len(parentResults))
	for i := range parentResults {
		// If this is an Insert or SearchForInsert operation, then do not scan
		// leaf vectors. Insert operations never need leaf vectors and scanning
		// them only increases the contention footprint. This is only needed when
		// we're searching the root partition, as we do not know its level until
		// we read its metadata record. By contrast, we know whether a non-root
		// partition is a leaf partition because we know the level of its parent.
		partitionKey := parentResults[i].ChildKey.PartitionKey
		idxCtx.tempToSearch[i] = PartitionToSearch{
			Key:                partitionKey,
			ExcludeLeafVectors: idxCtx.forInsert && partitionKey == RootKey,
		}
	}

	level, err = idxCtx.txn.SearchPartitions(
		ctx, idxCtx.treeKey, idxCtx.tempToSearch, idxCtx.randomized, searchSet)
	if err != nil {
		return 0, err
	}

	for i := range parentResults {
		count := idxCtx.tempToSearch[i].Count
		searchSet.Stats.SearchedPartition(level, count)

		// If one of the searched partitions has only 1 vector remaining, do not
		// return that vector when "ignoreLonelyVector" is true.
		partitionKey := parentResults[i].ChildKey.PartitionKey
		if idxCtx.ignoreLonelyVector && idxCtx.level == level && count == 1 {
			searchSet.RemoveByParent(partitionKey)
		}

		// If searching for vector to delete, skip partitions that are in a state
		// that does not allow add and remove operations.
		if idxCtx.forDelete && idxCtx.level == level {
			if !idxCtx.tempToSearch[i].StateDetails.State.AllowAddOrRemove() {
				searchSet.RemoveByParent(partitionKey)
			}
		}

		// Enqueue background fixup if a split or merge operation needs to be
		// started or continued after stalling.
		state := idxCtx.tempToSearch[i].StateDetails
		needSplit := count > vi.options.MaxPartitionSize
		needSplit = needSplit || state.MaybeSplitStalled(vi.options.StalledOpTimeout())
		needMerge := count < vi.options.MinPartitionSize && partitionKey != RootKey
		needMerge = needMerge || state.MaybeMergeStalled(vi.options.StalledOpTimeout())
		if needSplit {
			vi.fixups.AddSplit(ctx, idxCtx.treeKey,
				parentResults[i].ParentPartitionKey, partitionKey, false /* singleStep */)
		} else if needMerge {
			vi.fixups.AddMerge(ctx, idxCtx.treeKey,
				parentResults[i].ParentPartitionKey, partitionKey, false /* singleStep */)
		}

		// If the root partition is in a non-ready state, then we need to check
		// whether its target partitions also need to be searched. Newly inserted
		// vectors can get forwarded to target partitions during a split, and we
		// need to make sure we can find them.
		if partitionKey != RootKey || state.State == ReadyState {
			continue
		}

		// This is the root partition in a non-ready state. Check whether target
		// positions also need to be searched. This can happen when the root
		// partition is in the middle of a split.
		if len(parentResults) != 1 {
			return InvalidLevel, errors.AssertionFailedf(
				"expected exactly one root partition to search")
		}

		idxCtx.tempResults[0] = SearchResult{
			ParentPartitionKey: RootKey,
			ChildKey:           ChildKey{PartitionKey: state.Target1},
		}
		idxCtx.tempResults[1] = SearchResult{
			ParentPartitionKey: RootKey,
			ChildKey:           ChildKey{PartitionKey: state.Target2},
		}

		if state.State == DrainingForSplitState {
			// In the DrainingForSplit state, the target partitions are still at
			// the same level as the root partition, so merge their contents into
			// the search set (which will remove any duplicates).
			targetLevel, err := vi.searchChildPartitions(ctx, idxCtx, searchSet, idxCtx.tempResults[:2])
			if err != nil {
				return 0, err
			}
			if targetLevel != level {
				return 0, errors.AssertionFailedf(
					"root partition has level %d, but target partitions have level %d", level, targetLevel)
			}
		} else if state.State == AddingLevelState {
			// In the AddingLevel state, the target partitions should be treated as
			// children of the root partition. Add their partition keys to the search
			// set.
			// NOTE: QuerySquaredDistance is zero for both partitions. This should
			// only impact accuracy if beam size is < 4. If we decide to handle
			// this edge case, we can use GetFullVectors to fetch the sub-partition
			// centroids in order to compute distances.
			searchSet.Add(&idxCtx.tempResults[0])
			searchSet.Add(&idxCtx.tempResults[1])
		}
	}

	return level, nil
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

		// Exclude deleted vectors from results.
		if candidates[i].Vector == nil {
			// Vector was deleted, so add fixup to delete it.
			vi.fixups.AddDeleteVector(
				ctx, candidates[i].ParentPartitionKey, candidates[i].ChildKey.KeyBytes)

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

// ensureSliceLen returns a slice of the given length and generic type. If the
// existing slice has enough capacity, that slice is returned after adjusting
// its length. Otherwise, a new, larger slice is allocated.
func ensureSliceLen[T any](s []T, l int) []T {
	if cap(s) < l {
		return make([]T, l, max(l*3/2, 16))
	}
	return s[:l]
}
