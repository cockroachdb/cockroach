// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
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

// VectorIndexOptions specifies options that control how the index will be
// built, as well as default options for how it will be searched. A given search
// operation can specify SearchOptions to override the default behavior.
type VectorIndexOptions struct {
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
}

// SearchOptions specifies options that apply to a particular search operation
// over the vector index.
type SearchOptions struct {
	// BaseBeamSize is the default number of child partitions that will be
	// searched at each level. Adaptive search will automatically decrease or
	// increase this value as the search proceeds. If zero, then it defaults to
	// the BaseBeamSize value from VectorIndexOptions.
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

// searchContext contains per-thread state needed during index search
// operations. Fields in the context are set at the beginning of an index
// operation and passed down the call stack.
type searchContext struct {
	Ctx       context.Context
	Workspace internal.Workspace
	Txn       vecstore.Txn
	Options   SearchOptions

	// Level of the tree from which search results are returned. For the Search
	// operation, this is always LeafLevel, but inserts and splits/merges can
	// search at intermediate levels of the tree.
	Level vecstore.Level

	// Original is the original, full-size vector that was passed to the top-level
	// method on VectorIndex.
	Original vector.T

	// Randomized is the original vector after it has been randomized by applying
	// a random orthogonal transformation (ROT).
	Randomized vector.T

	tempSearchSet       vecstore.SearchSet
	tempResults         [1]vecstore.SearchResult
	tempQualitySamples  [MaxQualitySamples]float64
	tempKeys            []vecstore.PartitionKey
	tempCounts          []int
	tempVectorsWithKeys []vecstore.VectorWithKey
}

// VectorIndex implements the C-SPANN algorithm, which adapts Microsoft’s SPANN
// and SPFresh algorithms to work well with CockroachDB’s unique distributed
// architecture. This enables CockroachDB to efficiently answer approximate
// nearest neighbor (ANN) queries with high accuracy, low latency, and fresh
// results, with millions or even billions of indexed vectors. In a departure
// from SPANN (and similar to Google's ScaNN), C-SPANN packs hundreds of vectors
// into each partition, and indexes partitions using a K-means tree.
//
// VectorIndex is thread-safe. There should typically be only one VectorIndex
// instance in the process for each index.
type VectorIndex struct {
	// options specify how the index will be built and searched, by default.
	options VectorIndexOptions
	// store is the interface with the component that transactionally stores
	// partitions and vectors.
	store vecstore.Store
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

// NewVectorIndex constructs a new vector index instance. Typically, only one
// VectorIndex instance should be created for each index in the process.
//
// NOTE: It's important that the index is always initialized with the same seed,
// first at the time of creation and then every time it's used.
//
// NOTE: If "stopper" is not nil, then the vector index will start a background
// goroutine to process index fixups. When the index is no longer needed, the
// caller must call Close to shut down the background goroutine.
func NewVectorIndex(
	ctx context.Context,
	store vecstore.Store,
	quantizer quantize.Quantizer,
	seed int64,
	options *VectorIndexOptions,
	stopper *stop.Stopper,
) (*VectorIndex, error) {
	vi := &VectorIndex{
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

	if err := vi.stats.Init(ctx, store); err != nil {
		return nil, err
	}

	return vi, nil
}

// Store returns the underlying vector store for the index.
func (vi *VectorIndex) Store() vecstore.Store {
	return vi.store
}

// Fixups returns the background fixup processor for the index.
func (vi *VectorIndex) Fixups() *FixupProcessor {
	return &vi.fixups
}

// Options returns the options that specify how the index should be built and
// searched.
func (vi *VectorIndex) Options() VectorIndexOptions {
	return vi.options
}

// FormatStats returns index statistics as a formatted string.
func (vi *VectorIndex) FormatStats() string {
	return vi.stats.Format()
}

// Close shuts down any background fixup workers. While this also happens when
// the Init context is closed or the stopper is quiesced, this method can be
// used to perform the shutdown independently of those mechanisms.
func (vi *VectorIndex) Close() {
	vi.fixups.cancel()
}

// Insert adds a new vector with the given primary key to the index. This is
// called within the scope of a transaction so that the index does not appear to
// change during the insert.
//
// NOTE: This can result in two vectors with the same primary key being inserted
// into the index. To minimize this possibility, callers should call Delete
// before Insert when a vector is updated. Even then, it's not guaranteed that
// Delete will find the old vector. Vector index methods handle this rare case
// by checking for duplicates when returning search results.
func (vi *VectorIndex) Insert(
	ctx context.Context, txn vecstore.Txn, vector vector.T, key vecstore.PrimaryKey,
) error {
	// Potentially throttle insert operation if background work is falling behind.
	if err := vi.fixups.DelayInsertOrDelete(ctx); err != nil {
		return err
	}

	var parentSearchCtx searchContext
	vi.setupInsertContext(ctx, txn, vector, &parentSearchCtx)

	// Randomize the vector.
	tempRandomized := parentSearchCtx.Workspace.AllocVector(vi.quantizer.GetDims())
	defer parentSearchCtx.Workspace.FreeVector(tempRandomized)
	parentSearchCtx.Randomized = vi.randomizeVector(vector, tempRandomized)

	// Insert the vector into the secondary index.
	childKey := vecstore.ChildKey{PrimaryKey: key}
	return vi.insertHelper(&parentSearchCtx, childKey)
}

// Delete attempts to remove a vector from the index, given its value and
// primary key. This is called within the scope of a transaction so that the
// index does not appear to change during the delete.
//
// NOTE: Delete may not be able to locate the vector in the index, meaning a
// "dangling vector" reference will be left in the tree. Vector index methods
// handle this rare case by checking for duplicates when returning search
// results.
func (vi *VectorIndex) Delete(
	ctx context.Context, txn vecstore.Txn, vector vector.T, key vecstore.PrimaryKey,
) error {
	// Potentially throttle delete operation if background work is falling behind.
	if err := vi.fixups.DelayInsertOrDelete(ctx); err != nil {
		return err
	}

	// Search for the vector in the index.
	searchCtx := searchContext{
		Txn:      txn,
		Original: vector,
		Level:    vecstore.LeafLevel,
		Options: SearchOptions{
			SkipRerank:  vi.options.DisableErrorBounds,
			UpdateStats: true,
		},
	}
	searchCtx.Ctx = internal.WithWorkspace(ctx, &searchCtx.Workspace)

	// Randomize the vector.
	tempRandomized := searchCtx.Workspace.AllocVector(vi.quantizer.GetDims())
	defer searchCtx.Workspace.FreeVector(tempRandomized)
	searchCtx.Randomized = vi.randomizeVector(vector, tempRandomized)

	searchSet := vecstore.SearchSet{MaxResults: 1, MatchKey: key}

	// Search with the base beam size. If that fails to find the vector, try again
	// with a larger beam size, in order to minimize the chance of dangling
	// vector references in the index.
	baseBeamSize := max(vi.options.BaseBeamSize, 1)
	for {
		searchCtx.Options.BaseBeamSize = baseBeamSize

		err := vi.searchHelper(&searchCtx, &searchSet)
		if err != nil {
			return err
		}
		results := searchSet.PopUnsortedResults()
		if len(results) == 0 {
			// Retry search with significantly higher beam size.
			if baseBeamSize == vi.options.BaseBeamSize {
				baseBeamSize *= 8
				continue
			}
			return nil
		}

		// Remove the vector from its partition in the store.
		_, err = vi.removeFromPartition(ctx, txn, results[0].ParentPartitionKey, results[0].ChildKey)
		if errors.Is(err, vecstore.ErrRestartOperation) {
			// If store requested the operation be retried, then re-run the
			// search and delete.
			continue
		}
		return err
	}
}

// Search finds vectors in the index that are closest to the given query vector
// and returns them in the search set. Set searchSet.MaxResults to limit the
// number of results. This is called within the scope of a transaction so that
// the index does not appear to change during the search.
func (vi *VectorIndex) Search(
	ctx context.Context,
	txn vecstore.Txn,
	queryVector vector.T,
	searchSet *vecstore.SearchSet,
	options SearchOptions,
) error {
	searchCtx := searchContext{
		Txn:      txn,
		Original: queryVector,
		Level:    vecstore.LeafLevel,
		Options:  options,
	}
	searchCtx.Ctx = internal.WithWorkspace(ctx, &searchCtx.Workspace)

	// Randomize the vector.
	tempRandomized := searchCtx.Workspace.AllocVector(vi.quantizer.GetDims())
	defer searchCtx.Workspace.FreeVector(tempRandomized)
	searchCtx.Randomized = vi.randomizeVector(queryVector, tempRandomized)

	return vi.searchHelper(&searchCtx, searchSet)
}

// SearchForInsert finds the best partition in which to insert the given vector.
// It always returns a single search result containing the key of that
// partition, as well as the centroid of the partition (in the Vector field).
// This is useful for callers that directly insert KV rows rather than using
// this library to do it.
func (vi *VectorIndex) SearchForInsert(
	ctx context.Context, txn vecstore.Txn, vector vector.T,
) (*vecstore.SearchResult, error) {
	// Potentially throttle operation if background work is falling behind.
	if err := vi.fixups.DelayInsertOrDelete(ctx); err != nil {
		return nil, err
	}

	var parentSearchCtx searchContext
	vi.setupInsertContext(ctx, txn, vector, &parentSearchCtx)

	// Randomize the vector.
	tempRandomized := parentSearchCtx.Workspace.AllocVector(vi.quantizer.GetDims())
	defer parentSearchCtx.Workspace.FreeVector(tempRandomized)
	parentSearchCtx.Randomized = vi.randomizeVector(vector, tempRandomized)

	result, err := vi.searchForInsertHelper(&parentSearchCtx)
	if err != nil {
		return nil, err
	}

	// Now fetch the centroid of the insert partition. This has the side effect
	// of checking the size of the partition, in case it's over-sized.
	partitionKey := result.ChildKey.PartitionKey
	metadata, err := txn.GetPartitionMetadata(ctx, partitionKey, true /* forUpdate */)
	if err != nil {
		return nil, err
	}
	if metadata.Count > vi.options.MaxPartitionSize {
		vi.fixups.AddSplit(ctx, result.ParentPartitionKey, partitionKey)
	}

	result.Vector = metadata.Centroid
	return result, nil
}

// SuspendFixups suspends background fixup processing until ProcessFixups is
// explicitly called. It is used for testing.
func (vi *VectorIndex) SuspendFixups() {
	vi.fixups.Suspend()
}

// ProcessFixups waits until all pending fixups have been processed by
// background workers. It is used for testing.
func (vi *VectorIndex) ProcessFixups() {
	vi.fixups.Process()
}

// ForceSplit enqueues a split fixup. It is used for testing.
func (vi *VectorIndex) ForceSplit(
	ctx context.Context, parentPartitionKey vecstore.PartitionKey, partitionKey vecstore.PartitionKey,
) {
	vi.fixups.AddSplit(ctx, parentPartitionKey, partitionKey)
}

// ForceMerge enqueues a merge fixup. It is used for testing.
func (vi *VectorIndex) ForceMerge(
	ctx context.Context, parentPartitionKey vecstore.PartitionKey, partitionKey vecstore.PartitionKey,
) {
	vi.fixups.AddMerge(ctx, parentPartitionKey, partitionKey)
}

// setupInsertContext sets up the given search context for an insert operation.
// Before performing an insert, we need to search for the best partition with
// the closest centroid to the query vector. The partition in which to insert
// the vector is at the parent of of the leaf level.
func (vi *VectorIndex) setupInsertContext(
	ctx context.Context, txn vecstore.Txn, vector vector.T, parentSearchCtx *searchContext,
) {
	// Perform the search using quantized vectors rather than full vectors (i.e.
	// skip reranking).
	*parentSearchCtx = searchContext{
		Txn:      txn,
		Original: vector,
		Level:    vecstore.SecondLevel,
		Options: SearchOptions{
			BaseBeamSize: vi.options.BaseBeamSize,
			SkipRerank:   vi.options.DisableErrorBounds,
			UpdateStats:  true,
		},
	}
	parentSearchCtx.Ctx = internal.WithWorkspace(ctx, &parentSearchCtx.Workspace)
}

// insertHelper looks for the best partition in which to add the vector and then
// adds the vector to that partition. This is an internal helper method that can
// be used by callers once they have set up a search context.
func (vi *VectorIndex) insertHelper(
	parentSearchCtx *searchContext, childKey vecstore.ChildKey,
) error {
	result, err := vi.searchForInsertHelper(parentSearchCtx)
	if err != nil {
		return err
	}
	parentPartitionKey := result.ParentPartitionKey
	partitionKey := result.ChildKey.PartitionKey
	err = vi.addToPartition(parentSearchCtx.Ctx, parentSearchCtx.Txn, parentPartitionKey,
		partitionKey, parentSearchCtx.Randomized, childKey)
	if errors.Is(err, vecstore.ErrRestartOperation) {
		return vi.insertHelper(parentSearchCtx, childKey)
	}
	return err
}

// searchForInsertHelper searches for the best partition in which to add a
// vector and returns that as exactly one search result (never nil).
func (vi *VectorIndex) searchForInsertHelper(
	parentSearchCtx *searchContext,
) (*vecstore.SearchResult, error) {
	parentSearchCtx.tempSearchSet = vecstore.SearchSet{MaxResults: 1}
	err := vi.searchHelper(parentSearchCtx, &parentSearchCtx.tempSearchSet)
	if err != nil {
		return nil, err
	}
	results := parentSearchCtx.tempSearchSet.PopUnsortedResults()
	if len(results) != 1 {
		return nil, errors.AssertionFailedf(
			"SearchForInsert should return exactly one result, got %d", len(results))
	}
	return &results[0], err
}

// addToPartition calls the store to add the given vector to an existing
// partition. If this causes the partition to exceed its maximum size, a split
// fixup will be enqueued.
func (vi *VectorIndex) addToPartition(
	ctx context.Context,
	txn vecstore.Txn,
	parentPartitionKey vecstore.PartitionKey,
	partitionKey vecstore.PartitionKey,
	vector vector.T,
	childKey vecstore.ChildKey,
) error {
	metadata, err := txn.AddToPartition(ctx, partitionKey, vector, childKey)
	if err != nil {
		return errors.Wrapf(err, "adding vector to partition %d", partitionKey)
	}
	if metadata.Count > vi.options.MaxPartitionSize {
		vi.fixups.AddSplit(ctx, parentPartitionKey, partitionKey)
	}
	return vi.stats.OnAddOrRemoveVector(ctx)
}

// removeFromPartition calls the store to remove a vector, by its key, from an
// existing partition.
func (vi *VectorIndex) removeFromPartition(
	ctx context.Context,
	txn vecstore.Txn,
	partitionKey vecstore.PartitionKey,
	childKey vecstore.ChildKey,
) (metadata vecstore.PartitionMetadata, err error) {
	metadata, err = txn.RemoveFromPartition(ctx, partitionKey, childKey)
	if err != nil {
		return vecstore.PartitionMetadata{},
			errors.Wrapf(err, "removing vector from partition %d", partitionKey)
	}
	if err := vi.stats.OnAddOrRemoveVector(ctx); err != nil {
		return vecstore.PartitionMetadata{}, err
	}
	return metadata, nil
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
func (vi *VectorIndex) searchHelper(searchCtx *searchContext, searchSet *vecstore.SearchSet) error {
	// Return enough search results to:
	// 1. Ensure that the number of results requested by the caller is respected.
	// 2. Ensure that there are enough samples for calculating stats.
	// 3. Ensure that there are enough results for adaptive querying to dynamically
	//    expand the beam size (up to 4x the base beam size).
	maxResults := max(
		searchSet.MaxResults, vi.options.QualitySamples, searchCtx.Options.BaseBeamSize*4)
	subSearchSet := vecstore.SearchSet{MaxResults: maxResults}
	searchCtx.tempResults[0] = vecstore.SearchResult{
		ChildKey: vecstore.ChildKey{PartitionKey: vecstore.RootKey}}
	searchLevel, err := vi.searchChildPartitions(searchCtx, &subSearchSet, searchCtx.tempResults[:])
	if err != nil {
		return err
	}

	if searchLevel < searchCtx.Level {
		// This should only happen when inserting into the root.
		if searchLevel != searchCtx.Level-1 {
			panic(errors.AssertionFailedf("caller passed invalid level %d", searchCtx.Level))
		}
		if searchCtx.Options.ReturnVectors {
			panic(errors.AssertionFailedf("ReturnVectors=true not supported for this case"))
		}
		searchSet.Add(&vecstore.SearchResult{
			ChildKey: vecstore.ChildKey{PartitionKey: vecstore.RootKey},
		})
		return nil
	}

	for {
		results := subSearchSet.PopUnsortedResults()
		if len(results) == 0 && searchLevel > vecstore.LeafLevel {
			// This should never happen, as it means that interior partition(s)
			// have no children. The vector deletion logic should prevent that.
			panic(errors.AssertionFailedf(
				"interior partition(s) on level %d has no children", searchLevel))
		}

		var zscore float64
		if searchLevel > vecstore.LeafLevel {
			// Results need to be sorted in order to calculate their "spread". This
			// also sorts them for determining which partitions to search next.
			results.Sort()

			// Compute the Z-score of the candidate list if there are enough
			// samples. Otherwise, use the default Z-score of 0.
			if len(results) >= vi.options.QualitySamples {
				for i := 0; i < vi.options.QualitySamples; i++ {
					searchCtx.tempQualitySamples[i] = float64(results[i].QuerySquaredDistance)
				}
				samples := searchCtx.tempQualitySamples[:vi.options.QualitySamples]
				zscore = vi.stats.ReportSearch(searchLevel, samples, searchCtx.Options.UpdateStats)
			}
		}

		if searchLevel <= searchCtx.Level {
			// We've reached the end of the search.
			if searchLevel != searchCtx.Level {
				// This indicates index corruption, since each lower level should
				// be one less than its parent level.
				panic(errors.AssertionFailedf("somehow skipped to level %d when searching for level %d",
					searchLevel, searchCtx.Level))
			}

			// Aggregate all stats from searching lower levels of the tree.
			searchSet.Stats.Add(&subSearchSet.Stats)

			results = vi.pruneDuplicates(results)
			if !searchCtx.Options.SkipRerank || searchCtx.Options.ReturnVectors {
				// Re-rank search results with full vectors.
				searchSet.Stats.FullVectorCount += len(results)
				results, err = vi.rerankSearchResults(searchCtx, results)
				if err != nil {
					return err
				}
			}
			searchSet.AddAll(results)
			break
		}

		// Calculate beam size for searching next level.
		beamSize := searchCtx.Options.BaseBeamSize
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

			if searchLevel > vecstore.LeafLevel+1 {
				// Use progressively smaller beam size for higher levels, since
				// each contains exponentially fewer partitions.
				tempBeamSize /= math.Pow(2, float64(searchLevel-(vecstore.LeafLevel+1)))
			}

			beamSize = int(math.Ceil(tempBeamSize))
		}
		beamSize = max(beamSize, 1)

		searchLevel--
		if searchLevel == searchCtx.Level {
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

			if searchLevel > vecstore.LeafLevel {
				// Ensure there are enough results for calculating stats.
				subSearchSet.MaxResults = max(subSearchSet.MaxResults, vi.options.QualitySamples)
			}
		}

		// Search up to beamSize child partitions. The results are in sorted order,
		// since we always sort non-leaf levels above, and this must be a non-leaf
		// level (leaf-level partitions do not have children).
		results = results[:min(beamSize, len(results))]
		_, err = vi.searchChildPartitions(searchCtx, &subSearchSet, results)
		if errors.Is(err, vecstore.ErrRestartOperation) {
			return vi.searchHelper(searchCtx, searchSet)
		} else if err != nil {
			return err
		}
	}

	return nil
}

// searchChildPartitions searches for nearest neighbors to the query vector in
// the set of partitions referenced by the given search results. It adds the
// closest matches to the given search set.
func (vi *VectorIndex) searchChildPartitions(
	searchCtx *searchContext, searchSet *vecstore.SearchSet, parentResults vecstore.SearchResults,
) (level vecstore.Level, err error) {
	searchCtx.tempKeys = ensureSliceLen(searchCtx.tempKeys, len(parentResults))
	for i := range parentResults {
		searchCtx.tempKeys[i] = parentResults[i].ChildKey.PartitionKey
	}

	searchCtx.tempCounts = ensureSliceLen(searchCtx.tempCounts, len(parentResults))
	level, err = searchCtx.Txn.SearchPartitions(
		searchCtx.Ctx, searchCtx.tempKeys, searchCtx.Randomized, searchSet, searchCtx.tempCounts)
	if err != nil {
		return 0, err
	}

	for i := range parentResults {
		count := searchCtx.tempCounts[i]
		searchSet.Stats.SearchedPartition(level, count)

		partitionKey := parentResults[i].ChildKey.PartitionKey
		if count < vi.options.MinPartitionSize && partitionKey != vecstore.RootKey {
			vi.fixups.AddMerge(
				searchCtx.Ctx, parentResults[i].ParentPartitionKey, partitionKey)
		} else if count > vi.options.MaxPartitionSize {
			vi.fixups.AddSplit(
				searchCtx.Ctx, parentResults[i].ParentPartitionKey, partitionKey)
		}
	}

	return level, nil
}

// pruneDuplicates removes candidates with duplicate child keys. This is rare,
// but it can happen when a vector updated in the primary index cannot be
// located in the secondary index.
// NOTE: This logic can remove the "wrong" duplicate, with a quantized distance
// that doesn't correspond to the true distance. However, this has no impact as
// long as we rerank candidates using the original full-size vectors. Even if
// we're not reranking, the impact of this should be minimal, since duplicates
// are so rare and there's already quite a bit of inaccuracy when not reranking.
func (vi *VectorIndex) pruneDuplicates(candidates []vecstore.SearchResult) []vecstore.SearchResult {
	if len(candidates) <= 1 {
		// No possibility of duplicates.
		return candidates
	}

	if candidates[0].ChildKey.PrimaryKey == nil {
		// Only leaf partitions can have duplicates.
		return candidates
	}

	dups := make(map[string]bool, len(candidates))
	for i := 0; i < len(candidates); i++ {
		key := candidates[i].ChildKey.PrimaryKey
		if _, ok := dups[string(key)]; ok {
			// Found duplicate, so remove it by replacing it with the last
			// candidate.
			candidates[i] = candidates[len(candidates)-1]
			candidates = candidates[:len(candidates)-1]
			i--
			continue
		}
		dups[string(key)] = true
	}
	return candidates
}

// rerankSearchResults updates the given set of candidates with their exact
// distances from the query vector. It does this by fetching the original full
// size vectors from the store, in order to re-rank the top candidates for
// extra search result accuracy.
func (vi *VectorIndex) rerankSearchResults(
	searchCtx *searchContext, candidates []vecstore.SearchResult,
) ([]vecstore.SearchResult, error) {
	if len(candidates) == 0 {
		return candidates, nil
	}

	// Fetch the full vectors from the store.
	candidates, err := vi.getRerankVectors(searchCtx, candidates)
	if err != nil {
		return candidates, err
	}

	queryVector := searchCtx.Randomized
	if searchCtx.Level == vecstore.LeafLevel {
		// Leaf vectors haven't been randomized, so compare with the original query
		// vector if available, or un-randomize the randomized vector. The original
		// vector is not available in some cases where split/merge needs to move
		// vectors between partitions.
		if searchCtx.Original != nil {
			queryVector = searchCtx.Original
		} else {
			queryVector = searchCtx.Workspace.AllocVector(vi.quantizer.GetDims())
			defer searchCtx.Workspace.FreeVector(queryVector)
			vi.unRandomizeVector(searchCtx.Randomized, queryVector)
		}
	}

	// Compute exact distances for the vectors.
	for i := range candidates {
		candidate := &candidates[i]
		candidate.QuerySquaredDistance = num32.L2SquaredDistance(candidate.Vector, queryVector)
		candidate.ErrorBound = 0
	}

	return candidates, nil
}

// getRerankVectors updates the given search candidates with the original full
// size vectors from the store. If a candidate's vector has been deleted from
// the primary index, that candidate is removed from the list of candidates
// that's returned.
func (vi *VectorIndex) getRerankVectors(
	searchCtx *searchContext, candidates []vecstore.SearchResult,
) ([]vecstore.SearchResult, error) {
	// Prepare vector references.
	searchCtx.tempVectorsWithKeys = ensureSliceLen(searchCtx.tempVectorsWithKeys, len(candidates))
	for i := 0; i < len(candidates); i++ {
		searchCtx.tempVectorsWithKeys[i].Key = candidates[i].ChildKey
	}

	// The store is expected to fetch the vectors in parallel.
	err := searchCtx.Txn.GetFullVectors(searchCtx.Ctx, searchCtx.tempVectorsWithKeys)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(candidates); i++ {
		candidates[i].Vector = searchCtx.tempVectorsWithKeys[i].Vector

		// Exclude deleted vectors from results.
		if candidates[i].Vector == nil {
			// Vector was deleted, so add fixup to delete it.
			vi.fixups.AddDeleteVector(
				searchCtx.Ctx, candidates[i].ParentPartitionKey, candidates[i].ChildKey.PrimaryKey)

			// Move the last candidate to the current position and reduce size
			// of slice by one.
			searchCtx.tempVectorsWithKeys[i] = searchCtx.tempVectorsWithKeys[len(candidates)-1]
			candidates[i] = candidates[len(candidates)-1]
			candidates = candidates[:len(candidates)-1]
			i--
		}
	}

	return candidates, nil
}

// randomizeVector performs a random orthogonal transformation (ROT) on the
// "original" vector and writes it to the "randomized" vector. The caller is
// responsible for allocating the randomized vector with length equal to the
// index's dimensions.
//
// Randomizing vectors distributes skew more evenly across dimensions and
// across vectors in a set. Distance and angle between any two vectors
// remains unchanged, as long as the same ROT is applied to both.
func (vi *VectorIndex) randomizeVector(original vector.T, randomized vector.T) vector.T {
	return num32.MulMatrixByVector(&vi.rot, original, randomized, num32.NoTranspose)
}

// unRandomizeVector inverts the random orthogonal transformation performed by
// randomizeVector, in order to recover the original vector from its randomized
// form. The caller is responsible for allocating the original vector with
// length equal to the index's dimensions.
func (vi *VectorIndex) unRandomizeVector(randomized vector.T, original vector.T) vector.T {
	return num32.MulMatrixByVector(&vi.rot, randomized, original, num32.Transpose)
}

// FormatOptions modifies the behavior of the Format method.
type FormatOptions struct {
	// PrimaryKeyStrings, if true, indicates that primary key bytes should be
	// interpreted as strings. This is used for testing scenarios.
	PrimaryKeyStrings bool
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
func (vi *VectorIndex) Format(
	ctx context.Context, txn vecstore.Txn, options FormatOptions,
) (str string, err error) {
	ctx = internal.WithWorkspace(ctx, &internal.Workspace{})

	// Write formatted bytes to this buffer.
	var buf bytes.Buffer

	writePrimaryKey := func(key vecstore.PrimaryKey) {
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

	var helper func(partitionKey vecstore.PartitionKey, parentPrefix string, childPrefix string) error
	helper = func(partitionKey vecstore.PartitionKey, parentPrefix string, childPrefix string) error {
		partition, err := txn.GetPartition(ctx, partitionKey)
		if err != nil {
			return err
		}
		// Get centroid for the partition and un-randomize it so that it displays
		// the original vector.
		random := partition.Centroid()
		original := make(vector.T, len(random))
		vi.unRandomizeVector(random, original)
		buf.WriteString(parentPrefix)
		buf.WriteString("• ")
		buf.WriteString(strconv.FormatInt(int64(partitionKey), 10))
		buf.WriteByte(' ')
		writeVector(&buf, original, 4)
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

			if partition.Level() == vecstore.LeafLevel {
				refs := []vecstore.VectorWithKey{{Key: childKey}}
				if err = txn.GetFullVectors(ctx, refs); err != nil {
					return err
				}
				buf.WriteString(parentPrefix)
				buf.WriteString("• ")
				writePrimaryKey(childKey.PrimaryKey)
				if refs[0].Vector != nil {
					buf.WriteByte(' ')
					writeVector(&buf, refs[0].Vector, 4)
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

	if err = helper(vecstore.RootKey, "", ""); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// formatFloat formats each number to the specified number of decimal places,
// removing unnecessary zeros. Don't print negative zero, since this causes
// diffs when running on Linux vs. Mac.
func formatFloat(value float32, prec int) string {
	s := strconv.FormatFloat(float64(value), 'f', prec, 32)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	if s == "-0" {
		return "0"
	}
	return s
}

// writeVector formats the given vector like "(1, 2, ..., 9, 10)" and writes it
// to the given buffer, using the specified precision.
func writeVector(buf *bytes.Buffer, vector vector.T, prec int) {
	buf.WriteString("(")
	if len(vector) > 4 {
		// Show first 2 numbers, '...', and last 2 numbers.
		buf.WriteString(formatFloat(vector[0], prec))
		buf.WriteString(", ")
		buf.WriteString(formatFloat(vector[1], prec))
		buf.WriteString(", ..., ")
		buf.WriteString(formatFloat(vector[len(vector)-2], prec))
		buf.WriteString(", ")
		buf.WriteString(formatFloat(vector[len(vector)-1], prec))
	} else {
		// Show all numbers if there are 4 or fewer.
		for i, val := range vector {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(formatFloat(val, prec))
		}
	}
	buf.WriteString(")")
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
