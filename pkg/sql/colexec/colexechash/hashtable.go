// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexechash

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// HashTableBuildMode represents different modes in which the HashTable can be
// built.
type HashTableBuildMode int

const (
	// HashTableFullBuildMode is the mode where HashTable buffers all input
	// tuples and populates First and Next arrays for each hash bucket.
	HashTableFullBuildMode HashTableBuildMode = iota

	// HashTableDistinctBuildMode is the mode where HashTable only buffers
	// distinct tuples and discards the duplicates. In this mode the hash table
	// actually stores only the equality columns, so the columns with positions
	// not present in keyCols will remain zero-capacity vectors in Vals.
	HashTableDistinctBuildMode
)

// HashTableProbeMode represents different modes of probing the HashTable.
type HashTableProbeMode int

const (
	// HashTableDefaultProbeMode is the default probing mode of the HashTable.
	HashTableDefaultProbeMode HashTableProbeMode = iota

	// HashTableDeletingProbeMode is the mode of probing the HashTable in which
	// it "deletes" the tuples from itself once they are matched against
	// probing tuples.
	//
	// For example, if we have a HashTable consisting of tuples {1, 1}, {1, 2},
	// {2, 3}, and the probing tuples are {1, 4}, {1, 5}, {1, 6}, then we get
	// the following when probing on the first column only:
	//   {1, 4} -> {1, 1}   | HashTable = {1, 2}, {2, 3}
	//   {1, 5} -> {1, 2}   | HashTable = {2, 3}
	//   {1, 6} -> no match | HashTable = {2, 3}
	// Note that the output of such probing is not fully deterministic when
	// tuples contain non-equality columns.
	HashTableDeletingProbeMode
)

// keyID encodes the ordinal of the corresponding tuple.
//
// For each tuple with the ordinal 'i' (the ordinal among all tuples in the
// hash table or within a single probing batch), keyID is calculated as:
//
//	keyID = i + 1.
//
// keyID of 0 is reserved to indicate the end of the hash chain.
type keyID = uint32

// hashChains describes the partitioning of a set of tuples into singly-linked
// lists ("buckets") where all tuples in a list have the same hash.
//
// In order to iterate over the list corresponding to a particular hash value
// 'bucket', the keyID of the head of the list can be looked up using
// First[bucket], and the following keyIDs of the tuples in the list can be
// found using Next[keyID] traversal. Whenever keyID of 0 is encountered, the
// end of the list has been reached.
type hashChains struct {
	// First stores the first keyID of the tuple that resides in each bucket.
	// The tuple is the head of the corresponding hash chain.
	//
	// The length of this slice is equal to the number of buckets used in the
	// hash table at the moment.
	First []keyID

	// Next is a densely-packed list that stores the keyID of the next tuple in
	// the hash chain, where an ID of 0 is reserved to represent the end of the
	// chain.
	//
	// The length of this slice is equal to the number of tuples stored in the
	// hash table at the moment plus one (Next[0] is unused).
	Next []keyID
}

// hashTableProbeBuffer stores the information related to the probing batch.
//
// From the high level, this struct facilitates checking whether tuples from the
// probing batch are equal to "candidate" tuples (those that have the same
// hash). In other words, hashTableProbeBuffer helps the hash table to determine
// whether there is a hash collision or an actual equality match.
//
// Given that this struct is tied to the probing batch, all slices are limited
// by the length of the batch (in some cases plus one) in size.
type hashTableProbeBuffer struct {
	// When the hash table is used by the hash aggregator or the unordered
	// distinct, an implicit hash table is built on the probing batch itself,
	// and it is stored in this hashChains object. The goal is to reuse the
	// facilities of the hash table to partition all tuples in the batch into
	// equality buckets (i.e. into lists where all tuples are equal on the hash
	// columns).
	//
	// Not used by the hash joiner.
	hashChains

	// limitedSlicesAreAccountedFor indicates whether we have already
	// accounted for the memory used by the slices below.
	limitedSlicesAreAccountedFor bool

	///////////////////////////////////////////////////////////////
	// Slices below are allocated dynamically but are limited by //
	// HashTable.maxProbingBatchLength in size.                  //
	///////////////////////////////////////////////////////////////

	// ToCheck stores the indices of tuples from the probing batch for which we
	// still want to keep traversing the hash chain trying to find equality
	// matches.
	//
	// For example, if the probing batch has 4 tuples, then initially ToCheck
	// will be [0, 1, 2, 3]. Say, tuples with ordinals 0 and 3 are determined to
	// definitely not have any matches (e.g. because the corresponding hash
	// chains are empty) while tuples with ordinals 1 and 2 have hash
	// collisions against the current "candidates", then ToCheck will be updated
	// to [1, 2].
	ToCheck []uint32

	// ToCheckID stores the keyIDs of the current "candidate" matches for the
	// tuples from the probing batch. Concretely, ToCheckID[i] is the keyID of
	// the tuple in the hash table which we are currently comparing with the ith
	// tuple of the probing batch. i is included in ToCheck. The result of the
	// comparison is stored in 'differs' and/or 'foundNull'.
	//
	// On the first iteration:
	//   ToCheckID[i] = First[hash[i]]
	// (i.e. we're comparing the ith probing tuple against the head of the hash
	// chain). For the next iteration of probing, new values of ToCheckID are
	// calculated as
	//   ToCheckID[i] = Next[ToCheckID[i]].
	// Whenever ToCheckID[i] becomes 0, there are no more matches for the ith
	// probing tuple, and the ith tuple is no longer included into ToCheck.
	ToCheckID []keyID

	// differs stores whether the probing tuple included in ToCheck differs
	// from the corresponding "candidate" tuple specified in ToCheckID.
	differs []bool

	// foundNull stores whether the probing tuple contains a NULL value in the
	// key. Populated when allowNullEquality=false since a single NULL makes the
	// key distinct from all other possible "candidates".
	foundNull []bool

	// HeadID stores the keyID of the tuple that has an equality match with the
	// tuple at any given index from the probing batch. Unlike First where we
	// might have a hash collision, HeadID stores the actual equality matches.
	//
	// All three users of the hash table use HeadID differently.
	//
	// In the hash joiner, HeadID is used only when the hash table might contain
	// duplicate values (this is not the case when the equality columns form a
	// key). The hash table describes the partitioning of the build (right) side
	// table, and the probing batch comes from the left side. During the probing
	// phase, if a match is found for the probing tuple i, then HeadID[i] is set
	// to the keyID of that match, then the HashTable.Same slice is used to
	// traverse all equality matches for the given probing tuple.
	//
	// In the hash aggregator, then the hash table can describe the partitioning
	// of the probing batch (via ProbeScratch) or of already found grouping
	// buckets (via BuildScratch), depending on the phase of the algorithm. See
	// an extensive comment on hashAggregator.onlineAgg for more details.
	//
	// The unordered distinct uses HeadID in the following manner:
	// - in the first step of the probing phase (when duplicates are being
	// removed from the probing batch), HeadID[i] is set to the first tuple that
	// matches ith tuple. Since ith tuple always matches itself (and possibly
	// others), HeadID values will never be zero in this step;
	// - in the second step of the probing phase (once the batch only contains
	// unique tuples), the non-zero HeadID value is set for a particular tuple
	// once the tuple is determined to not have duplicates with any tuples
	// already in the hash table.
	// See a comment on DistinctBuild for an example.
	HeadID []keyID

	// HashBuffer stores the hash values of each tuple in the probing batch. It
	// will be dynamically updated when the HashTable is built in distinct mode.
	HashBuffer []uint32
}

// HashTable is a structure used by the hash joiner, the hash aggregator, and
// the unordered distinct in order to perform the equality checks and
// de-duplication.
//
// When used by the hash joiner, the whole build (right) side table is inserted
// into the hash table. Later, the left side table is read one batch at a time,
// and that batch is used to probe against the hash table to find matches. On a
// single probing iteration at most one match is found for each tuple in the
// probing batch. In order to find all matches, for each probing tuple the
// corresponding hash chain will be fully traversed. For more details see the
// comment on colexecjoin.hashJoiner.
//
// When used by the hash aggregator and the unordered distinct, the hash table
// is built in an incremental fashion. One batch is read from the input, then
// using the hash table's facilities the batch is divided into "equality"
// buckets. Next, the de-duplicated batch is probed against the hash table to
// find possible matches for each tuple. For more details see the comments on
// hashAggregator.onlineAgg and DistinctBuild.
type HashTable struct {
	allocator             *colmem.Allocator
	maxProbingBatchLength int

	// unlimitedSlicesNumUint32AccountedFor stores the number of uint32 from
	// the unlimited slices that we have already accounted for.
	unlimitedSlicesNumUint32AccountedFor int64

	// BuildScratch contains the hash chains among tuples of the hash table
	// (those stored in Vals).
	BuildScratch hashChains

	// ProbeScratch contains the scratch buffers that provide the facilities of
	// the hash table (like equality checks) for the probing batch.
	ProbeScratch hashTableProbeBuffer

	// Keys is a scratch space that stores the equality columns of the tuples
	// currently being probed.
	Keys []*coldata.Vec

	// Same is a densely-packed list that stores the keyID of the next key in
	// the hash table that has the same value as the current key. The HeadID of
	// the key is the first key of that value found in the next linked list.
	// This field will be lazily populated by the prober.
	//
	// Same is only used when the HashTable contains non-distinct keys (in
	// HashTableFullBuildMode mode) and is probed via HashTableDefaultProbeMode
	// mode.
	Same []keyID
	// Visited represents whether each of the corresponding keys have been
	// touched by the prober.
	//
	// Visited is only used by the hash joiner when the HashTable contains
	// non-distinct keys or when performing set-operation joins.
	Visited []bool

	// Vals stores columns of the build source that are specified in colsToStore
	// in the constructor. The ID of a tuple at any index of Vals is index + 1.
	Vals *colexecutils.AppendOnlyBufferedBatch
	// keyCols stores the indices of Vals which are used for equality
	// comparison.
	keyCols []uint32

	// numBuckets returns the number of buckets the HashTable employs at the
	// moment. This number increases as more tuples are added into the hash
	// table.
	numBuckets uint32
	// loadFactor determines the average number of tuples per bucket exceeding
	// of which will trigger resizing the hash table.
	loadFactor float64

	// allowNullEquality determines if NULL keys should be treated as equal to
	// each other.
	allowNullEquality bool

	datumAlloc    tree.DatumAlloc
	cancelChecker colexecutils.CancelChecker

	BuildMode HashTableBuildMode
	probeMode HashTableProbeMode
}

var _ colexecop.Resetter = &HashTable{}

// NewHashTable returns a new HashTable.
//
// - allocator must be the allocator that is owned by the hash table and not
// shared with any other components.
//
// - maxProbingBatchLength indicates the maximum size of the probing batch.
//
// - loadFactor determines the average number of tuples per bucket which, if
// exceeded, will trigger resizing the hash table. This number can have a
// noticeable effect on the performance, so every user of the hash table should
// choose the number that works well for the corresponding use case. 1.0 could
// be used as the initial default value, and most likely the best value will be
// in [0.1, 10.0] range.
//
// - initialNumHashBuckets determines the number of buckets allocated initially.
// When the current load factor of the hash table exceeds the loadFactor, the
// hash table is resized by doubling the number of buckets. The user of the hash
// table should choose this number based on the amount of its other allocations,
// but it is likely should be in [8, coldata.BatchSize()] range.
// The thinking process for choosing coldata.BatchSize() could be roughly as
// follows:
// - on one hand, if we make several other allocations that have to be at
// least coldata.BatchSize() in size, then we don't win much in the case of
// the input with small number of tuples;
// - on the other hand, if we start out with a larger number, we won't be
// using the vast of majority of the buckets on the input with small number
// of tuples (a downside) while not gaining much in the case of the input
// with large number of tuples.
func NewHashTable(
	ctx context.Context,
	allocator *colmem.Allocator,
	maxProbingBatchLength int,
	loadFactor float64,
	initialNumHashBuckets uint32,
	sourceTypes []*types.T,
	keyCols []uint32,
	allowNullEquality bool,
	buildMode HashTableBuildMode,
	probeMode HashTableProbeMode,
) *HashTable {
	if !allowNullEquality && probeMode == HashTableDeletingProbeMode {
		// At the moment, we don't have a use case for such behavior, so let's
		// assert that it is not requested.
		colexecerror.InternalError(errors.AssertionFailedf("HashTableDeletingProbeMode is supported only when null equality is allowed"))
	}
	// Note that we don't perform memory accounting of the internal memory here
	// and delay it till buildFromBufferedTuples in order to appease *-disk
	// logic test configs (our disk-spilling infrastructure doesn't know how to
	// fallback to disk when a memory limit is hit in the constructor methods
	// of the operators or in Init() implementations).

	// colsToStore indicates the positions of columns to actually store in the
	// hash table depending on the build mode:
	// - all columns are stored in the full build mode
	// - only columns with indices in keyCols are stored in the distinct build
	// mode (columns with other indices will remain zero-capacity vectors in
	// Vals).
	var colsToStore []int
	switch buildMode {
	case HashTableFullBuildMode:
		colsToStore = make([]int, len(sourceTypes))
		for i := range colsToStore {
			colsToStore[i] = i
		}
	case HashTableDistinctBuildMode:
		colsToStore = make([]int, len(keyCols))
		for i := range colsToStore {
			colsToStore[i] = int(keyCols[i])
		}
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unknown HashTableBuildMode %d", buildMode))
	}
	ht := &HashTable{
		allocator:             allocator,
		maxProbingBatchLength: maxProbingBatchLength,
		BuildScratch: hashChains{
			First: make([]keyID, initialNumHashBuckets),
		},
		Keys:              make([]*coldata.Vec, len(keyCols)),
		Vals:              colexecutils.NewAppendOnlyBufferedBatch(allocator, sourceTypes, colsToStore),
		keyCols:           keyCols,
		numBuckets:        initialNumHashBuckets,
		loadFactor:        loadFactor,
		allowNullEquality: allowNullEquality,
		BuildMode:         buildMode,
		probeMode:         probeMode,
	}

	if buildMode == HashTableDistinctBuildMode {
		ht.ProbeScratch.First = make([]keyID, initialNumHashBuckets)
		// ht.BuildScratch.Next will be populated dynamically by appending to
		// it, but we need to make sure that the special keyID=0 (which
		// indicates the end of the hash chain) is always present.
		ht.BuildScratch.Next = []keyID{0}
	}

	ht.cancelChecker.Init(ctx)
	return ht
}

// shouldResize returns whether the hash table storing numTuples should be
// resized in order to not exceed the load factor given the current number of
// buckets.
func (ht *HashTable) shouldResize(numTuples int) bool {
	return float64(numTuples)/float64(ht.numBuckets) > ht.loadFactor
}

// probeBufferInternalMaxMemUsed returns the maximum memory used by the slices
// of hashTableProbeBuffer that are limited by maxProbingBatchLength in size.
func probeBufferInternalMaxMemUsed(maxProbingBatchLength int) int64 {
	// probeBufferInternalMaxMemUsed accounts for:
	// - five uint32 slices:
	//   - hashTableProbeBuffer.hashChains.Next
	//   - hashTableProbeBuffer.ToCheck
	//   - hashTableProbeBuffer.ToCheckID
	//   - hashTableProbeBuffer.HeadID
	//   - hashTableProbeBuffer.HashBuffer
	// - two bool slices:
	//   - hashTableProbeBuffer.differs
	//   - hashTableProbeBuffer.foundNull.
	return memsize.Uint32*int64(5*maxProbingBatchLength) + memsize.Bool*int64(2*maxProbingBatchLength)
}

// accountForLimitedSlices checks whether we have already accounted for the
// memory used by the slices that are limited by maxProbingBatchLength in size
// and adjusts the allocator accordingly if we haven't.
func (p *hashTableProbeBuffer) accountForLimitedSlices(
	allocator *colmem.Allocator, maxProbingBatchLength int,
) {
	if p.limitedSlicesAreAccountedFor {
		return
	}
	allocator.AdjustMemoryUsage(probeBufferInternalMaxMemUsed(maxProbingBatchLength))
	p.limitedSlicesAreAccountedFor = true
}

func (ht *HashTable) unlimitedSlicesCapacity() int64 {
	// Note that if ht.ProbeScratch.First is nil, it'll have zero capacity.
	return int64(cap(ht.BuildScratch.First) + cap(ht.ProbeScratch.First) + cap(ht.BuildScratch.Next))
}

// buildFromBufferedTuples builds the hash table from already buffered tuples
// in ht.Vals. It'll determine the appropriate number of buckets that satisfy
// the target load factor.
func (ht *HashTable) buildFromBufferedTuples() {
	for ht.shouldResize(ht.Vals.Length()) {
		ht.numBuckets *= 2
	}
	// Account for memory used by the internal auxiliary slices that are limited
	// in size.
	ht.ProbeScratch.accountForLimitedSlices(ht.allocator, ht.maxProbingBatchLength)
	// Figure out the minimum capacities of the unlimited slices before actually
	// allocating then.
	needCapacity := int64(ht.numBuckets) + int64(ht.Vals.Length()+1) // ht.BuildScratch.First + ht.BuildScratch.Next
	if ht.ProbeScratch.First != nil {
		needCapacity += int64(ht.numBuckets) // ht.ProbeScratch.First
	}
	if needCapacity > ht.unlimitedSlicesCapacity() {
		// We'll need to allocate larger slices then we currently have, so
		// perform the memory accounting for the anticipated memory usage. Note
		// that it might not be precise, so we'll reconcile after the
		// allocations below.
		ht.allocator.AdjustMemoryUsage(memsize.Uint32 * (needCapacity - ht.unlimitedSlicesNumUint32AccountedFor))
		ht.unlimitedSlicesNumUint32AccountedFor = needCapacity
	}
	// Perform the actual build.
	ht.buildFromBufferedTuplesNoAccounting()
	// Now ensure that the accounting is precise (cap's of the slices might
	// exceed len's that we've accounted for).
	ht.allocator.AdjustMemoryUsageAfterAllocation(memsize.Uint32 * (ht.unlimitedSlicesCapacity() - ht.unlimitedSlicesNumUint32AccountedFor))
	ht.unlimitedSlicesNumUint32AccountedFor = ht.unlimitedSlicesCapacity()
}

// buildFromBufferedTuples builds the hash table from already buffered tuples in
// ht.Vals according to the current number of buckets. No memory accounting is
// performed, so this function is guaranteed to not throw a memory error.
func (ht *HashTable) buildFromBufferedTuplesNoAccounting() {
	ht.BuildScratch.First = colexecutils.MaybeAllocateUint32Array(ht.BuildScratch.First, int(ht.numBuckets))
	if ht.ProbeScratch.First != nil {
		ht.ProbeScratch.First = colexecutils.MaybeAllocateUint32Array(ht.ProbeScratch.First, int(ht.numBuckets))
	}
	// ht.BuildScratch.Next is used to store the computed hash value of each key.
	ht.BuildScratch.Next = colexecutils.MaybeAllocateUint32Array(ht.BuildScratch.Next, ht.Vals.Length()+1)

	for i, keyCol := range ht.keyCols {
		ht.Keys[i] = ht.Vals.ColVec(int(keyCol))
	}
	ht.ComputeBuckets(ht.BuildScratch.Next[1:], ht.Keys, ht.Vals.Length(), nil /* sel */)
	ht.buildNextChains(ht.BuildScratch.First, ht.BuildScratch.Next, 1 /* offset */, uint32(ht.Vals.Length()))
}

// FullBuild executes the entirety of the hash table build phase using the input
// as the build source. The input is entirely consumed in the process. Note that
// the hash table is assumed to operate in HashTableFullBuildMode.
func (ht *HashTable) FullBuild(input colexecop.Operator) {
	if ht.BuildMode != HashTableFullBuildMode {
		colexecerror.InternalError(errors.AssertionFailedf(
			"HashTable.FullBuild is called in unexpected build mode %d", ht.BuildMode,
		))
	}
	// We're using the hash table with the full build mode in which we will
	// fully buffer all tuples from the input first and only then we'll build
	// the hash table. Such approach allows us to compute the desired number of
	// hash buckets for the target load factor (this is done in
	// buildFromBufferedTuples()).
	for {
		batch := input.Next()
		if batch.Length() == 0 {
			break
		}
		ht.Vals.AppendTuples(batch, 0 /* startIdx */, batch.Length())
	}
	ht.buildFromBufferedTuples()
}

// DistinctBuild appends all distinct tuples from batch to the hash table. Note
// that the hash table is assumed to operate in HashTableDistinctBuildMode.
// batch is updated to include only the distinct tuples.
//
// This is achieved by first removing duplicates within the batch itself and
// then probing the de-duplicated batch against the hash table (which contains
// already emitted distinct tuples). Duplicates of the emitted tuples are also
// removed from the batch, and if there are any tuples left, all of them are
// distinct and, thus, are appended into the hash table and outputted.
//
// Let's go through an example of how this function works: our input stream
// contains the following tuples:
//
//	{-6}, {-6}, {-7}, {-5}, {-8}, {-5}, {-5}, {-8}.
//
// (Note that negative values are chosen in order to visually distinguish them
// from the IDs that we'll be working with below.)
// We will use coldata.BatchSize() == 4 and let's assume that we will use a
// hash function
//
//	h(-5) = 1, h(-6) = 1, h(-7) = 0, h(-8) = 0
//
// with two buckets in the hash table.
//
// I. we get a batch [-6, -6, -7, -5].
//  1. ComputeHashAndBuildChains:
//     a) compute hash buckets:
//     HashBuffer = [1, 1, 0, 1]
//     ProbeScratch.Next = [reserved, 1, 1, 0, 1]
//     b) build 'Next' chains between hash buckets:
//     ProbeScratch.First = [3, 1] (length of First == # of hash buckets)
//     ProbeScratch.Next  = [reserved, 2, 4, 0, 0]
//     (Note that we have a hash collision in the bucket with hash 1.)
//  2. RemoveDuplicates within the batch:
//  1. first iteration in FindBuckets:
//     a) all 4 tuples included to be checked against heads of their hash
//     chains:
//     ToCheck = [0, 1, 2, 3]
//     ToCheckID = [1, 1, 3, 1]
//     b) after performing the equality check using CheckProbeForDistinct,
//     tuples 0, 1, 2 are found to be equal to the heads of their hash
//     chains while tuple 3 (-5) has a hash collision with tuple 0 (-6),
//     so it is kept for another iteration:
//     ToCheck = [3]
//     ToCheckID = [x, x, x, 2]
//     HeadID  = [1, 1, 3, x]
//  2. second iteration in FindBuckets finds that tuple 3 (-5) again has a
//     hash collision with tuple 1 (-6), so it is kept for another
//     iteration:
//     ToCheck = [3]
//     ToCheckID = [x, x, x, 4]
//     HeadID  = [1, 1, 3, x]
//  3. third iteration finds a match for tuple (the tuple itself), no more
//     tuples to check, so the iterations stop:
//     ToCheck = []
//     HeadID  = [1, 1, 3, 4]
//  4. the duplicates are represented by having the same HeadID values, and
//     all duplicates are removed in updateSel:
//     batch = [-6, -6, -7, -5]
//     length = 3, sel = [0, 2, 3]
//     Notably, HashBuffer is compacted accordingly:
//     HashBuffer = [1, 0, 1]
//  3. The hash table is empty, so RemoveDuplicates against the hash table is
//     skipped.
//  4. All 3 tuples still present in the batch are distinct, they are appended
//     to the hash table and will be emitted to the output:
//     Vals = [-6, -7, -5]
//     BuildScratch.First = [2, 1]
//     BuildScratch.Next  = [reserved, 3, 0, 0]
//     We have fully processed the first batch.
//
// II. we get a batch [-8, -5, -5, -8].
//  1. ComputeHashAndBuildChains:
//     a) compute hash buckets:
//     HashBuffer = [0, 1, 1, 0]
//     ProbeScratch.Next = [reserved, 0, 1, 1, 0]
//     b) build 'Next' chains between hash buckets:
//     ProbeScratch.First = [1, 2]
//     ProbeScratch.Next  = [reserved, 4, 3, 0, 0]
//  2. RemoveDuplicates within the batch:
//  1. first iteration in FindBuckets:
//     a) all 4 tuples included to be checked against heads of their hash
//     chains:
//     ToCheck = [0, 1, 2, 3]
//     ToCheckID = [1, 2, 2, 1]
//     b) after performing the equality check using CheckProbeForDistinct,
//     all tuples are found to be equal to the heads of their hash
//     chains, no more tuples to check, so the iterations stop:
//     ToCheck = []
//     HeadID  = [1, 2, 2, 1]
//  2. the duplicates are represented by having the same HeadID values, and
//     all duplicates are removed in updateSel:
//     batch = [-8, -5, -5, -8]
//     length = 2, sel = [0, 1]
//     Notably, HashBuffer is compacted accordingly:
//     HashBuffer = [0, 1]
//  3. RemoveDuplicates against the hash table:
//  1. first iteration in FindBuckets:
//     a) both tuples included to be checked against heads of their hash
//     chains of the hash table (meaning BuildScratch.First and
//     BuildScratch.Next are used to populate ToCheckID values):
//     ToCheck = [0, 1]
//     ToCheckID = [2, 1]
//     b) after performing the equality check using CheckBuildForDistinct,
//     both tuples are found to have hash collisions (-8 with -7 and -5
//     with -6), so both are kept for another iteration:
//     ToCheck = [0, 1]
//     ToCheckID = [0, 2]
//  2. second iteration in FindBuckets finds that tuple 1 (-5) has a match
//     whereas tuple 0 (-8) is distinct (because its ToCheckID is 0), no
//     more tuples to check:
//     ToCheck = []
//     HeadID  = [1, 0]
//  3. duplicates are represented by having HeadID value of 0, so the batch
//     is updated to only include tuple -8:
//     batch = [-8, -5, -5, -8]
//     length = 1, sel = [0]
//     HashBuffer = [0]
//  4. The single tuple still present in the batch is distinct, it is appended
//     to the hash table and will be emitted to the output:
//     Vals = [-6, -7, -5, -8]
//     BuildScratch.First = [2, 1]
//     BuildScratch.Next  = [reserved, 3, 4, 0, 0]
//     We have fully processed the second batch and the input as a whole.
//
// NOTE: b *must* be a non-zero length batch.
func (ht *HashTable) DistinctBuild(batch coldata.Batch) {
	if ht.BuildMode != HashTableDistinctBuildMode {
		colexecerror.InternalError(errors.AssertionFailedf(
			"HashTable.DistinctBuild is called in unexpected build mode %d", ht.BuildMode,
		))
	}
	ht.ComputeHashAndBuildChains(batch)
	ht.RemoveDuplicates(
		batch, ht.Keys, ht.ProbeScratch.First, ht.ProbeScratch.Next,
		ht.CheckProbeForDistinct, true, /* probingAgainstItself */
	)
	// We only check duplicates when there is at least one buffered tuple.
	if ht.Vals.Length() > 0 {
		ht.RemoveDuplicates(
			batch, ht.Keys, ht.BuildScratch.First, ht.BuildScratch.Next,
			ht.CheckBuildForDistinct, false, /* probingAgainstItself */
		)
	}
	if batch.Length() > 0 {
		ht.AppendAllDistinct(batch)
	}
}

// ComputeHashAndBuildChains computes the hash codes of the tuples in batch and
// then builds 'First' chains between those tuples. The goal is to separate all
// tuples in batch into singly linked lists containing only tuples with the
// same hash code. Those 'Next' chains are stored in ht.ProbeScratch.Next.
func (ht *HashTable) ComputeHashAndBuildChains(batch coldata.Batch) {
	srcVecs := batch.ColVecs()
	for i, keyCol := range ht.keyCols {
		ht.Keys[i] = srcVecs[keyCol]
	}

	batchLength := batch.Length()
	if cap(ht.ProbeScratch.Next) < batchLength+1 {
		ht.ProbeScratch.Next = make([]keyID, batchLength+1)
	}
	ht.ComputeBuckets(ht.ProbeScratch.Next[1:batchLength+1], ht.Keys, batchLength, batch.Selection())
	ht.ProbeScratch.HashBuffer = append(ht.ProbeScratch.HashBuffer[:0], ht.ProbeScratch.Next[1:batchLength+1]...)

	// We need to zero out 'First' buffer for all hash codes present in
	// HashBuffer, and there are two possible approaches that we choose from
	// based on a heuristic - we can either iterate over all hash codes and
	// zero out only the relevant elements (beneficial when 'First' buffer is
	// at least batchLength in size) or zero out the whole 'First' buffer
	// (beneficial otherwise).
	if batchLength < len(ht.ProbeScratch.First) {
		for _, hash := range ht.ProbeScratch.HashBuffer[:batchLength] {
			ht.ProbeScratch.First[hash] = 0
		}
	} else {
		for n := 0; n < len(ht.ProbeScratch.First); n += copy(ht.ProbeScratch.First[n:], colexecutils.ZeroUint32Column) {
		}
	}

	ht.buildNextChains(ht.ProbeScratch.First, ht.ProbeScratch.Next, 1 /* offset */, uint32(batchLength))
}

// RemoveDuplicates updates the selection vector of the batch to only include
// distinct tuples when probing against a hash table specified by 'first' and
// 'next' vectors as well as 'duplicatesChecker'.
// NOTE: *first* and *next* vectors should be properly populated.
func (ht *HashTable) RemoveDuplicates(
	batch coldata.Batch,
	keyCols []*coldata.Vec,
	first, next []keyID,
	duplicatesChecker func([]*coldata.Vec, uint32, []int) uint32,
	probingAgainstItself bool,
) {
	ht.FindBuckets(
		batch, keyCols, first, next, duplicatesChecker,
		false /* zeroHeadIDForDistinctTuple */, probingAgainstItself,
	)
	ht.updateSel(batch)
}

// AppendAllDistinct appends all tuples from batch to the hash table. It
// assumes that all tuples are distinct and that ht.ProbeScratch.HashBuffer
// contains the hash codes for all of them.
// NOTE: batch must be of non-zero length.
func (ht *HashTable) AppendAllDistinct(batch coldata.Batch) {
	numBuffered := uint32(ht.Vals.Length())
	ht.Vals.AppendTuples(batch, 0 /* startIdx */, batch.Length())
	ht.BuildScratch.Next = append(ht.BuildScratch.Next, ht.ProbeScratch.HashBuffer[:batch.Length()]...)
	ht.buildNextChains(ht.BuildScratch.First, ht.BuildScratch.Next, numBuffered+1, uint32(batch.Length()))
	if ht.shouldResize(ht.Vals.Length()) {
		ht.buildFromBufferedTuples()
	}
}

// RepairAfterDistinctBuild rebuilds the hash table populated via DistinctBuild
// in the case a memory error was thrown.
func (ht *HashTable) RepairAfterDistinctBuild() {
	// Note that we don't try to be smart and "repair" the already built hash
	// table in which only the most recently added tuples might need to be
	// "built". We do it this way because the memory error could have occurred
	// in several spots making it harder to reason about what are the
	// "consistent" tuples and what are those that need to be repaired. This is
	// a minor performance hit, but it is done only once throughout the lifetime
	// of the unordered distinct when it just spilled to disk, so the regression
	// is ok.
	ht.buildFromBufferedTuplesNoAccounting()
}

// checkCols performs a column by column checkCol on the key columns.
func (ht *HashTable) checkCols(probeVecs []*coldata.Vec, nToCheck uint32, probeSel []int) {
	switch ht.probeMode {
	case HashTableDefaultProbeMode:
		for i, keyCol := range ht.keyCols {
			ht.checkCol(probeVecs[i], ht.Vals.ColVec(int(keyCol)), i, nToCheck, probeSel)
		}
	case HashTableDeletingProbeMode:
		for i, keyCol := range ht.keyCols {
			ht.checkColDeleting(probeVecs[i], ht.Vals.ColVec(int(keyCol)), i, nToCheck, probeSel)
		}
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unsupported hash table probe mode: %d", ht.probeMode))
	}
}

// checkColsForDistinctTuples performs a column by column check to find distinct
// tuples in the probe table that are not present in the build table.
// NOTE: It assumes that probeSel has already been populated and it is not nil.
func (ht *HashTable) checkColsForDistinctTuples(
	probeVecs []*coldata.Vec, nToCheck uint32, probeSel []int,
) {
	buildVecs := ht.Vals.ColVecs()
	for i := range ht.keyCols {
		probeVec := probeVecs[i]
		buildVec := buildVecs[ht.keyCols[i]]

		ht.checkColForDistinctTuples(probeVec, buildVec, nToCheck, probeSel)
	}
}

// ComputeBuckets computes the hash value of each key and stores the result in
// buckets.
func (ht *HashTable) ComputeBuckets(buckets []uint32, keys []*coldata.Vec, nKeys int, sel []int) {
	if nKeys == 0 {
		// No work to do - avoid doing the loops below.
		return
	}

	initHash(buckets, nKeys, DefaultInitHashValue)

	// Check if we received more tuples than the current allocation size and
	// increase it if so (limiting it by coldata.BatchSize()).
	if nKeys > ht.datumAlloc.DefaultAllocSize && ht.datumAlloc.DefaultAllocSize < coldata.BatchSize() {
		ht.datumAlloc.DefaultAllocSize = nKeys
		if ht.datumAlloc.DefaultAllocSize > coldata.BatchSize() {
			ht.datumAlloc.DefaultAllocSize = coldata.BatchSize()
		}
	}

	for i := range ht.keyCols {
		rehash(buckets, keys[i], nKeys, sel, ht.cancelChecker, &ht.datumAlloc)
	}

	finalizeHash(buckets, nKeys, ht.numBuckets)
}

// buildNextChains builds the hash map from the computed hash values.
func (ht *HashTable) buildNextChains(first, next []keyID, offset, batchSize uint32) {
	// The loop direction here is reversed to ensure that when we are building the
	// next chain for the probe table, the keyID in each equality chain inside
	// `next` is strictly in ascending order. This is crucial to ensure that when
	// built in distinct mode, hash table will emit the first distinct tuple it
	// encounters. When the next chain is built for build side, this invariant no
	// longer holds for the equality chains inside `next`. This is ok however for
	// the build side since all tuple that buffered on build side are already
	// distinct, therefore we can be sure that when we emit a tuple, there cannot
	// potentially be other tuples with the same key.
	for id := offset + batchSize - 1; id >= offset; id-- {
		// keyID is stored into corresponding hash bucket at the front of the next
		// chain.
		hash := next[id]
		firstKeyID := first[hash]
		// This is to ensure that `first` always points to the tuple with smallest
		// keyID in each equality chain. firstKeyID==0 means it is the first tuple
		// that we have encountered with the given hash value.
		if firstKeyID == 0 || id < firstKeyID {
			next[id] = firstKeyID
			first[hash] = id
		} else {
			next[id] = next[firstKeyID]
			next[firstKeyID] = id
		}
	}
	ht.cancelChecker.CheckEveryCall()
}

// SetupLimitedSlices ensures that HeadID, differs, foundNull, ToCheckID, and
// ToCheck are of the desired length and are set up for probing.
// Note that if the old ToCheckID or ToCheck slices have enough capacity, they
// are *not* zeroed out.
func (p *hashTableProbeBuffer) SetupLimitedSlices(length int) {
	p.HeadID = colexecutils.MaybeAllocateLimitedUint32Array(p.HeadID, length)
	p.differs = colexecutils.MaybeAllocateLimitedBoolArray(p.differs, length)
	p.foundNull = colexecutils.MaybeAllocateLimitedBoolArray(p.foundNull, length)
	// Note that we don't use maybeAllocate* methods below because ToCheckID and
	// ToCheck don't need to be zeroed out when reused.
	if cap(p.ToCheckID) < length {
		p.ToCheckID = make([]keyID, length)
	} else {
		p.ToCheckID = p.ToCheckID[:length]
	}
	if cap(p.ToCheck) < length {
		p.ToCheck = make([]uint32, length)
	} else {
		p.ToCheck = p.ToCheck[:length]
	}
}

// CheckBuildForDistinct finds all tuples in probeVecs that are *not* present in
// buffered tuples stored in ht.Vals. It stores the probeVecs's distinct tuples'
// keyIDs in HeadID buffer.
// NOTE: It assumes that probeVecs does not contain any duplicates itself.
// NOTE: It assumes that probeSel has already been populated and it is not nil.
// NOTE: It assumes that nToCheck is positive.
func (ht *HashTable) CheckBuildForDistinct(
	probeVecs []*coldata.Vec, nToCheck uint32, probeSel []int,
) uint32 {
	if probeSel == nil {
		colexecerror.InternalError(errors.AssertionFailedf("invalid selection vector"))
	}
	ht.checkColsForDistinctTuples(probeVecs, nToCheck, probeSel)
	nDiffers := uint32(0)
	toCheckSlice := ht.ProbeScratch.ToCheck
	_ = toCheckSlice[nToCheck-1]
	for toCheckPos := uint32(0); toCheckPos < nToCheck && nDiffers < nToCheck; toCheckPos++ {
		//gcassert:bce
		toCheck := toCheckSlice[toCheckPos]
		if ht.ProbeScratch.foundNull[toCheck] {
			// foundNull is only set to true when allowNullEquality is false, so
			// since this tuple has a NULL value, it's distinct from all others.
			ht.ProbeScratch.foundNull[toCheck] = false
			// Calculated using the convention: keyID = keys.indexOf(key) + 1.
			ht.ProbeScratch.HeadID[toCheck] = toCheck + 1
		} else if ht.ProbeScratch.differs[toCheck] {
			// Continue probing in this next chain for the probe key.
			ht.ProbeScratch.differs[toCheck] = false
			//gcassert:bce
			toCheckSlice[nDiffers] = toCheck
			nDiffers++
		}
	}
	return nDiffers
}

// CheckBuildForAggregation finds all tuples in probeVecs that *are* present in
// buffered tuples stored in ht.Vals. For each present tuple at position i it
// stores keyID of its duplicate in HeadID[i], for all non-present tuples the
// corresponding HeadID value is left unchanged.
// NOTE: It assumes that probeVecs does not contain any duplicates itself.
// NOTE: It assumes that probeSel has already been populated and it is not nil.
// NOTE: It assumes that nToCheck is positive.
func (ht *HashTable) CheckBuildForAggregation(
	probeVecs []*coldata.Vec, nToCheck uint32, probeSel []int,
) uint32 {
	if probeSel == nil {
		colexecerror.InternalError(errors.AssertionFailedf("invalid selection vector"))
	}
	if !ht.allowNullEquality {
		colexecerror.InternalError(errors.AssertionFailedf("NULL equality is assumed to be allowed in CheckBuildForAggregation"))
	}
	ht.checkColsForDistinctTuples(probeVecs, nToCheck, probeSel)
	nDiffers := uint32(0)
	toCheckSlice := ht.ProbeScratch.ToCheck
	_ = toCheckSlice[nToCheck-1]
	for toCheckPos := uint32(0); toCheckPos < nToCheck && nDiffers < nToCheck; toCheckPos++ {
		//gcassert:bce
		toCheck := toCheckSlice[toCheckPos]
		if ht.ProbeScratch.differs[toCheck] {
			// We have a hash collision, so we need to continue probing against
			// the next tuples in the hash chain.
			ht.ProbeScratch.differs[toCheck] = false
			//gcassert:bce
			toCheckSlice[nDiffers] = toCheck
			nDiffers++
		} else {
			// This tuple has a duplicate in the hash table, so we remember
			// keyID of that duplicate.
			ht.ProbeScratch.HeadID[toCheck] = ht.ProbeScratch.ToCheckID[toCheck]
		}
	}
	return nDiffers
}

// DistinctCheck determines if the current key in the ToCheckID bucket matches the
// equality column key. If there is a match, then the key is removed from
// ToCheck. If the bucket has reached the end, the key is rejected. The ToCheck
// list is reconstructed to only hold the indices of the keyCols keys that have
// not been found. The new length of ToCheck is returned by this function.
func (ht *HashTable) DistinctCheck(nToCheck uint32, probeSel []int) uint32 {
	ht.checkCols(ht.Keys, nToCheck, probeSel)
	// Select the indices that differ and put them into ToCheck.
	nDiffers := uint32(0)
	toCheckSlice := ht.ProbeScratch.ToCheck
	_ = toCheckSlice[nToCheck-1]
	for toCheckPos := uint32(0); toCheckPos < nToCheck && nDiffers < nToCheck; toCheckPos++ {
		//gcassert:bce
		toCheck := toCheckSlice[toCheckPos]
		if ht.ProbeScratch.foundNull[toCheck] {
			// We found a NULL value in the equality column with
			// allowNullEquality=false, so we know for sure this probing tuple
			// won't ever get a match.
			ht.ProbeScratch.ToCheckID[toCheck] = 0
		} else if ht.ProbeScratch.differs[toCheck] {
			ht.ProbeScratch.differs[toCheck] = false
			//gcassert:bce
			toCheckSlice[nDiffers] = toCheck
			nDiffers++
		}
	}
	return nDiffers
}

// Reset resets the HashTable for reuse.
// NOTE: memory that already has been allocated for ht.Vals is *not* released.
// However, resetting the length of ht.Vals to zero doesn't confuse the
// allocator - it is smart enough to look at the capacities of the allocated
// vectors, and the capacities would stay the same until an actual new
// allocation is needed, and at that time the allocator will update the memory
// account accordingly.
func (ht *HashTable) Reset(_ context.Context) {
	for n := 0; n < len(ht.BuildScratch.First); n += copy(ht.BuildScratch.First[n:], colexecutils.ZeroUint32Column) {
	}
	ht.Vals.ResetInternalBatch()
	// ht.ProbeScratch.Next, ht.Same and ht.Visited are reset separately before
	// they are used (these slices are not used in all of the code paths).
	// ht.ProbeScratch.HeadID, ht.ProbeScratch.differs, and
	// ht.ProbeScratch.foundNull are reset before they are used (these slices
	// are limited in size and dynamically allocated).
	// ht.ProbeScratch.ToCheckID and ht.ProbeScratch.ToCheck don't need to be
	// reset because they are populated manually every time before checking the
	// columns.
	if ht.BuildMode == HashTableDistinctBuildMode {
		// In "distinct" build mode, ht.BuildScratch.Next is populated
		// iteratively, whenever we find tuples that we haven't seen before. In
		// order to reuse the same underlying memory we need to slice up that
		// slice (note that keyID=0 is reserved for the end of all hash chains,
		// so we make the length 1).
		ht.BuildScratch.Next = ht.BuildScratch.Next[:1]
	}
}

// Release releases all of the slices used by the hash table as well as the
// corresponding memory reservations.
//
// The hash table becomes unusable, so any method calls after Release() will
// result in an undefined behavior.
//
// Note that this happens to implement the execreleasable.Releasable interface,
// but this method serves a different purpose (namely we want to release the
// hash table mid-execution, when the spilling to disk occurs), so the hash
// table is not added to the slice of objects that are released on the flow
// cleanup.
func (ht *HashTable) Release() {
	ht.allocator.ReleaseAll()
	*ht = HashTable{}
}
