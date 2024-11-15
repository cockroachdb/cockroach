// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// RerankMultiplier is multiplied by MaxResults to calculate the maximum number
// of search results that will be reranked with the original full-size vectors.
const RerankMultiplier = 10

// DeletedMultiplier increases the number of results that will be reranked, in
// order to account for vectors that may have been deleted in the primary index.
const DeletedMultiplier = 1.2

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
	// Seed is used to initialize a deterministic random number generator for
	// testing purposes. If this is zero, then the global random number generator
	// is used intead.
	Seed int64
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
	// in search results.
	ReturnVectors bool
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

	// Randomized is the original vector after being processed by the quantizer's
	// RandomizeVector method. It can have a different number of dimensions than
	// Original and different values in those dimensions.
	Randomized vector.T

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
	fixups fixupProcessor
}

// NewVectorIndex constructs a new vector index instance. Typically, only one
// VectorIndex instance should be created for each index in the process.
func NewVectorIndex(
	ctx context.Context,
	store vecstore.Store,
	quantizer quantize.Quantizer,
	options *VectorIndexOptions,
) (*VectorIndex, error) {
	vi := &VectorIndex{
		options:       *options,
		store:         store,
		rootQuantizer: quantize.NewUnQuantizer(quantizer.GetRandomDims()),
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

	vi.fixups.Init(vi, options.Seed)

	return vi, nil
}

// CreateRoot creates an empty root partition in the store. This should only be
// called once when the index is first created.
func (vi *VectorIndex) CreateRoot(ctx context.Context, txn vecstore.Txn) error {
	// Use the UnQuantizer because vectors in the root are not quantized.
	dims := vi.rootQuantizer.GetRandomDims()
	vectors := vector.MakeSet(dims)
	rootQuantizedSet := vi.rootQuantizer.Quantize(ctx, &vectors)
	rootPartition := vecstore.NewPartition(
		vi.rootQuantizer, rootQuantizedSet, []vecstore.ChildKey{}, vecstore.LeafLevel)
	return vi.store.SetRootPartition(ctx, txn, rootPartition)
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
	// Search for the leaf partition with the closest centroid to the query
	// vector.
	parentSearchCtx := searchContext{
		Txn:      txn,
		Original: vector,
		Level:    vecstore.LeafLevel + 1,
		Options: SearchOptions{
			BaseBeamSize: vi.options.BaseBeamSize,
			SkipRerank:   vi.options.DisableErrorBounds,
		},
	}
	parentSearchCtx.Ctx = internal.WithWorkspace(ctx, &parentSearchCtx.Workspace)

	// Randomize the vector if required by the quantizer.
	tempRandomized := parentSearchCtx.Workspace.AllocVector(vi.quantizer.GetRandomDims())
	defer parentSearchCtx.Workspace.FreeVector(tempRandomized)
	vi.quantizer.RandomizeVector(ctx, vector, tempRandomized, false /* invert */)
	parentSearchCtx.Randomized = tempRandomized

	// Insert the vector into the secondary index.
	childKey := vecstore.ChildKey{PrimaryKey: key}
	return vi.insertHelper(&parentSearchCtx, childKey, true /* allowRetry */)
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
	// Search with the base beam size. If that fails to find the vector, try again
	// with a larger beam size, in order to minimize the chance of dangling
	// vector references in the index.
	baseBeamSize := vi.options.BaseBeamSize
	for {
		// Search for the vector in the index.
		searchCtx := searchContext{
			Txn:      txn,
			Original: vector,
			Level:    vecstore.LeafLevel,
			Options: SearchOptions{
				BaseBeamSize: baseBeamSize,
				SkipRerank:   vi.options.DisableErrorBounds,
			},
		}
		searchCtx.Ctx = internal.WithWorkspace(ctx, &searchCtx.Workspace)

		// Randomize the vector if required by the quantizer.
		tempRandomized := searchCtx.Workspace.AllocVector(vi.quantizer.GetRandomDims())
		defer searchCtx.Workspace.FreeVector(tempRandomized)
		vi.quantizer.RandomizeVector(ctx, vector, tempRandomized, false /* invert */)
		searchCtx.Randomized = tempRandomized

		searchSet := vecstore.SearchSet{MaxResults: 1, MatchKey: key}
		err := vi.searchHelper(&searchCtx, &searchSet, true /* allowRetry */)
		if err != nil {
			return err
		}
		results := searchSet.PopResults()
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

	// Randomize the vector if required by the quantizer.
	tempRandomized := searchCtx.Workspace.AllocVector(vi.quantizer.GetRandomDims())
	defer searchCtx.Workspace.FreeVector(tempRandomized)
	vi.quantizer.RandomizeVector(ctx, queryVector, tempRandomized, false /* invert */)
	searchCtx.Randomized = tempRandomized

	return vi.searchHelper(&searchCtx, searchSet, true /* allowRetry */)
}

// insertHelper looks for the best partition in which to add the vector and then
// adds the vector to that partition.
func (vi *VectorIndex) insertHelper(
	parentSearchCtx *searchContext, childKey vecstore.ChildKey, allowRetry bool,
) error {
	// The partition in which to insert the vector is at the parent level
	// (level+1). Return enough results to have good candidates for inserting
	// the vector into another partition.
	searchSet := vecstore.SearchSet{MaxResults: 1}
	err := vi.searchHelper(parentSearchCtx, &searchSet, allowRetry)
	if err != nil {
		return err
	}
	results := searchSet.PopResults()
	parentPartitionKey := results[0].ParentPartitionKey
	partitionKey := results[0].ChildKey.PartitionKey
	_, err = vi.addToPartition(parentSearchCtx.Ctx, parentSearchCtx.Txn, parentPartitionKey,
		partitionKey, parentSearchCtx.Randomized, childKey)
	if errors.Is(err, vecstore.ErrPartitionNotFound) {
		// Retry the insert after root partition cache invalidation.
		if !allowRetry {
			// This indicates index corruption, since it should only require a
			// single retry to handle the case where the root partition is stale.
			// There should be no other cases that a partition cannot be found.
			panic(errors.AssertionFailedf("partition cannot be found even though root is not stale"))
		}
		return vi.insertHelper(parentSearchCtx, childKey, false /* allowRetry */)
	} else if err != nil {
		return err
	}

	return nil
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
) (int, error) {
	count, err := vi.store.AddToPartition(ctx, txn, partitionKey, vector, childKey)
	if err != nil {
		return 0, err
	}
	if count > vi.options.MaxPartitionSize {
		vi.fixups.AddSplit(ctx, parentPartitionKey, partitionKey)
	}
	return count, nil
}

// removeFromPartition calls the store to remove a vector, by its key, from an
// existing partition.
func (vi *VectorIndex) removeFromPartition(
	ctx context.Context,
	txn vecstore.Txn,
	partitionKey vecstore.PartitionKey,
	childKey vecstore.ChildKey,
) (int, error) {
	return vi.store.RemoveFromPartition(ctx, txn, partitionKey, childKey)
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
func (vi *VectorIndex) searchHelper(
	searchCtx *searchContext, searchSet *vecstore.SearchSet, allowRetry bool,
) error {
	// Return enough search results to:
	// 1. Ensure that the number of results requested by the caller is respected.
	// 2. Ensure that there are enough samples for calculating stats.
	// 3. Ensure that there are enough results for adaptive querying to dynamically
	//    expand the beam size (up to 4x the base beam size).
	maxResults := max(
		searchSet.MaxResults, vi.options.QualitySamples, searchCtx.Options.BaseBeamSize*4)
	subSearchSet := vecstore.SearchSet{MaxResults: maxResults}
	searchCtx.tempKeys = ensureSliceLen(searchCtx.tempKeys, 1)
	searchCtx.tempKeys[0] = vecstore.RootKey
	searchLevel, err := vi.searchChildPartitions(searchCtx, &subSearchSet, searchCtx.tempKeys)
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
		if len(results) == 0 {
			// This should never happen, as it means that interior partition(s)
			// have no children. The vector deletion logic should prevent that.
			panic(errors.AssertionFailedf(
				"interior partition(s) on level %d has no children", searchLevel))
		}

		var zscore float64
		if searchLevel > vecstore.LeafLevel {
			// Compute the z-score of the candidate results list.
			// TODO(andyk): Track z-score stats.
			zscore = 0
		}

		if searchLevel <= searchCtx.Level {
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
			tempBeamSize = max(min(tempBeamSize, float64(beamSize)*4), float64(beamSize)/2)

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
		}

		// Search up to beamSize child partitions.
		results.Sort()
		keyCount := min(beamSize, len(results))
		searchCtx.tempKeys = ensureSliceLen(searchCtx.tempKeys, keyCount)
		for i := 0; i < keyCount; i++ {
			searchCtx.tempKeys[i] = results[i].ChildKey.PartitionKey
		}

		_, err = vi.searchChildPartitions(searchCtx, &subSearchSet, searchCtx.tempKeys)
		if errors.Is(err, vecstore.ErrPartitionNotFound) {
			// The cached root partition must be stale, so retry the search.
			if !allowRetry {
				// This indicates index corruption, since it should only require
				// a single retry to handle the case where the root partition is
				// stale. There should be no other cases that a partition cannot
				// be found.
				panic(errors.AssertionFailedf("partition cannot be found even though root is not stale"))
			}
			return vi.searchHelper(searchCtx, searchSet, false /* allowRetry */)
		} else if err != nil {
			return err
		}
	}

	return nil
}

// searchChildPartitions searches the set of requested partitions for the query
// vector and adds the closest matches to the given search set.
func (vi *VectorIndex) searchChildPartitions(
	searchCtx *searchContext, searchSet *vecstore.SearchSet, partitionKeys []vecstore.PartitionKey,
) (level vecstore.Level, err error) {
	searchCtx.tempCounts = ensureSliceLen(searchCtx.tempCounts, len(partitionKeys))
	level, err = vi.store.SearchPartitions(
		searchCtx.Ctx, searchCtx.Txn, partitionKeys, searchCtx.Randomized,
		searchSet, searchCtx.tempCounts)
	if err != nil {
		return 0, err
	}

	for i := 0; i < len(searchCtx.tempCounts); i++ {
		count := searchCtx.tempCounts[i]
		searchSet.Stats.SearchedPartition(level, count)
		// TODO(andyk): Enqueue a split/merge fixup for the partition.
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
			queryVector = searchCtx.Workspace.AllocVector(vi.quantizer.GetOriginalDims())
			defer searchCtx.Workspace.FreeVector(queryVector)
			vi.quantizer.RandomizeVector(
				searchCtx.Ctx, searchCtx.Randomized, queryVector, true /* invert */)
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
	err := vi.store.GetFullVectors(searchCtx.Ctx, searchCtx.Txn, searchCtx.tempVectorsWithKeys)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(candidates); i++ {
		candidates[i].Vector = searchCtx.tempVectorsWithKeys[i].Vector

		// Exclude deleted vectors from results.
		if candidates[i].Vector == nil {
			// Vector was deleted, so add fixup to delete it.
			// TODO(andyk): Enqueue a delete of a vector.

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
	var buf bytes.Buffer

	// Format each number to 4 decimal places, removing unnecessary trailing
	// zeros.
	formatFloat := func(value float32) string {
		s := strconv.FormatFloat(float64(value), 'f', 4, 32)
		if strings.Contains(s, ".") {
			s = strings.TrimRight(s, "0")
			s = strings.TrimRight(s, ".")
		}
		return s
	}

	writeVector := func(vector vector.T) {
		buf.WriteByte('(')
		if len(vector) > 4 {
			// Show first 2 numbers, '...', and last 2 numbers.
			buf.WriteString(formatFloat(vector[0]))
			buf.WriteString(", ")
			buf.WriteString(formatFloat(vector[1]))
			buf.WriteString(", ..., ")
			buf.WriteString(formatFloat(vector[len(vector)-2]))
			buf.WriteString(", ")
			buf.WriteString(formatFloat(vector[len(vector)-1]))
		} else {
			// Show all numbers if there are 4 or fewer.
			for i, val := range vector {
				if i != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(formatFloat(val))
			}
		}
		buf.WriteByte(')')
	}

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
		partition, err := vi.store.GetPartition(ctx, txn, partitionKey)
		if err != nil {
			return err
		}
		// Get centroid for the partition and un-randomize it so that it displays
		// the original vector.
		random := partition.Centroid()
		original := make(vector.T, len(random))
		vi.quantizer.RandomizeVector(ctx, original, random, true /* invert */)
		buf.WriteString(parentPrefix)
		buf.WriteString("• ")
		buf.WriteString(strconv.FormatInt(int64(partitionKey), 10))
		buf.WriteByte(' ')
		writeVector(original)
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
				if err = vi.store.GetFullVectors(ctx, txn, refs); err != nil {
					return err
				}
				buf.WriteString(parentPrefix)
				buf.WriteString("• ")
				writePrimaryKey(childKey.PrimaryKey)
				if refs[0].Vector != nil {
					buf.WriteByte(' ')
					writeVector(refs[0].Vector)
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

// ensureSliceLen returns a slice of the given length and generic type. If the
// existing slice has enough capacity, that slice is returned after adjusting
// its length. Otherwise, a new, larger slice is allocated.
func ensureSliceLen[T any](s []T, l int) []T {
	if cap(s) < l {
		return make([]T, l, max(l*3/2, 16))
	}
	return s[:l]
}
