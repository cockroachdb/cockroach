// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const (
	// The number of random candidates to select from a larger list of possible
	// candidates. Because the allocator heuristics are being run on every node it
	// is actually not desirable to set this value higher. Doing so can lead to
	// situations where the allocator determistically selects the "best" node for a
	// decision and all of the nodes pile on allocations to that node. See "power
	// of two random choices":
	// https://brooker.co.za/blog/2012/01/17/two-random.html and
	// https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf.
	allocatorRandomCount = 2

	// maxFractionUsedThreshold: if the fraction used of a store descriptor
	// capacity is greater than this value, it will never be used as a rebalance
	// or allocate target and we will actively try to move replicas off of it.
	maxFractionUsedThreshold = 0.95

	// rebalanceToMaxFractionUsedThreshold: if the fraction used of a store
	// descriptor capacity is greater than this value, it will never be used as a
	// rebalance target. This is important for providing a buffer between fully
	// healthy stores and full stores (as determined by
	// maxFractionUsedThreshold).  Without such a buffer, replicas could
	// hypothetically ping pong back and forth between two nodes, making one full
	// and then the other.
	rebalanceToMaxFractionUsedThreshold = 0.925
)

// EnableStatsBasedRebalancing controls whether range rebalancing takes
// additional variables such as write load and disk usage into account.
// If disabled, rebalancing is done purely based on replica count.
var EnableStatsBasedRebalancing = settings.RegisterBoolSetting(
	"kv.allocator.stat_based_rebalancing.enabled",
	"set to enable rebalancing of range replicas based on write load and disk usage",
	false,
)

// rangeRebalanceThreshold is the minimum ratio of a store's range count to
// the mean range count at which that store is considered overfull or underfull
// of ranges.
var rangeRebalanceThreshold = settings.RegisterNonNegativeFloatSetting(
	"kv.allocator.range_rebalance_threshold",
	"minimum fraction away from the mean a store's range count can be before it is considered overfull or underfull",
	0.05,
)

// statRebalanceThreshold is the same as rangeRebalanceThreshold, but for
// statistics other than range count. This should be larger than
// rangeRebalanceThreshold because certain stats (like keys written per second)
// are inherently less stable and thus we need to be a little more forgiving to
// avoid thrashing.
//
// Note that there isn't a ton of science behind this number, but setting it
// to .05 and .1 were shown to cause some instability in clusters without load
// on them.
//
// TODO(a-robinson): Should disk usage be held to a higher standard than this?
var statRebalanceThreshold = settings.RegisterNonNegativeFloatSetting(
	"kv.allocator.stat_rebalance_threshold",
	"minimum fraction away from the mean a store's stats (like disk usage or writes per second) can be before it is considered overfull or underfull",
	0.20,
)

type scorerOptions struct {
	deterministic                bool
	statsBasedRebalancingEnabled bool
	rangeRebalanceThreshold      float64
	statRebalanceThreshold       float64
}

type balanceDimensions struct {
	ranges rangeCountStatus
	bytes  float64
	writes float64
}

func (bd *balanceDimensions) totalScore() float64 {
	return float64(bd.ranges) + bd.bytes + bd.writes
}

func (bd balanceDimensions) String() string {
	return fmt.Sprintf("%.2f(ranges=%d, bytes=%.2f, writes=%.2f)",
		bd.totalScore(), int(bd.ranges), bd.bytes, bd.writes)
}

func (bd balanceDimensions) compactString(options scorerOptions) string {
	if !options.statsBasedRebalancingEnabled {
		return fmt.Sprintf("%d", bd.ranges)
	}
	return bd.String()
}

// candidate store for allocation.
type candidate struct {
	store          roachpb.StoreDescriptor
	valid          bool
	fullDisk       bool
	necessary      bool
	diversityScore float64
	convergesScore int
	balanceScore   balanceDimensions
	rangeCount     int
	details        string
}

func (c candidate) String() string {
	str := fmt.Sprintf("s%d, valid:%t, fulldisk:%t, necessary:%t, diversity:%.2f, converges:%d, "+
		"balance:%s, rangeCount:%d, logicalBytes:%s, writesPerSecond:%.2f",
		c.store.StoreID, c.valid, c.fullDisk, c.necessary, c.diversityScore, c.convergesScore,
		c.balanceScore, c.rangeCount, humanizeutil.IBytes(c.store.Capacity.LogicalBytes),
		c.store.Capacity.WritesPerSecond)
	if c.details != "" {
		return fmt.Sprintf("%s, details:(%s)", str, c.details)
	}
	return str
}

func (c candidate) compactString(options scorerOptions) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "s%d", c.store.StoreID)
	if !c.valid {
		fmt.Fprintf(&buf, ", valid:%t", c.valid)
	}
	if c.fullDisk {
		fmt.Fprintf(&buf, ", fullDisk:%t", c.fullDisk)
	}
	if c.necessary {
		fmt.Fprintf(&buf, ", necessary:%t", c.necessary)
	}
	if c.diversityScore != 0 {
		fmt.Fprintf(&buf, ", diversity:%.2f", c.diversityScore)
	}
	fmt.Fprintf(&buf, ", converges:%d, balance:%s, rangeCount:%d",
		c.convergesScore, c.balanceScore.compactString(options), c.rangeCount)
	if options.statsBasedRebalancingEnabled {
		fmt.Fprintf(&buf, ", logicalBytes:%s, writesPerSecond:%.2f",
			humanizeutil.IBytes(c.store.Capacity.LogicalBytes), c.store.Capacity.WritesPerSecond)
	}
	if c.details != "" {
		fmt.Fprintf(&buf, ", details:(%s)", c.details)
	}
	return buf.String()
}

// less returns true if o is a better fit for some range than c is.
func (c candidate) less(o candidate) bool {
	return c.compare(o) < 0
}

// compare is analogous to strcmp in C or string::compare in C++ -- it returns
// a positive result if c is a better fit for the range than o, 0 if they're
// equivalent, or a negative result if o is a better fit than c. The magnitude
// of the result reflects some rough idea of how much better the better
// candidate is.
func (c candidate) compare(o candidate) float64 {
	if !o.valid {
		return 6
	}
	if !c.valid {
		return -6
	}
	if o.fullDisk {
		return 5
	}
	if c.fullDisk {
		return -5
	}
	if c.necessary != o.necessary {
		if c.necessary {
			return 4
		}
		return -4
	}
	if c.diversityScore != o.diversityScore {
		if c.diversityScore > o.diversityScore {
			return 3
		}
		return -3
	}
	if c.convergesScore != o.convergesScore {
		if c.convergesScore > o.convergesScore {
			return 2 + float64(c.convergesScore-o.convergesScore)/10.0
		}
		return -(2 + float64(o.convergesScore-c.convergesScore)/10.0)
	}
	if c.balanceScore.totalScore() != o.balanceScore.totalScore() {
		if c.balanceScore.totalScore() > o.balanceScore.totalScore() {
			return 1 + (c.balanceScore.totalScore()-o.balanceScore.totalScore())/10.0
		}
		return -(1 + (o.balanceScore.totalScore()-c.balanceScore.totalScore())/10.0)
	}
	// Sometimes we compare partially-filled in candidates, e.g. those with
	// diversity scores filled in but not balance scores or range counts. This
	// avoids returning NaN in such cases.
	if c.rangeCount == 0 && o.rangeCount == 0 {
		return 0
	}
	if c.rangeCount < o.rangeCount {
		return float64(o.rangeCount-c.rangeCount) / float64(o.rangeCount)
	}
	return -float64(c.rangeCount-o.rangeCount) / float64(c.rangeCount)
}

type candidateList []candidate

func (cl candidateList) String() string {
	if len(cl) == 0 {
		return "[]"
	}
	var buffer bytes.Buffer
	buffer.WriteRune('[')
	for _, c := range cl {
		buffer.WriteRune('\n')
		buffer.WriteString(c.String())
	}
	buffer.WriteRune(']')
	return buffer.String()
}

func (cl candidateList) compactString(options scorerOptions) string {
	if len(cl) == 0 {
		return "[]"
	}
	var buffer bytes.Buffer
	buffer.WriteRune('[')
	for _, c := range cl {
		buffer.WriteRune('\n')
		buffer.WriteString(c.compactString(options))
	}
	buffer.WriteRune(']')
	return buffer.String()
}

// byScore implements sort.Interface to sort by scores.
type byScore candidateList

var _ sort.Interface = byScore(nil)

func (c byScore) Len() int           { return len(c) }
func (c byScore) Less(i, j int) bool { return c[i].less(c[j]) }
func (c byScore) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

// byScoreAndID implements sort.Interface to sort by scores and ids.
type byScoreAndID candidateList

var _ sort.Interface = byScoreAndID(nil)

func (c byScoreAndID) Len() int { return len(c) }
func (c byScoreAndID) Less(i, j int) bool {
	if c[i].diversityScore == c[j].diversityScore &&
		c[i].convergesScore == c[j].convergesScore &&
		c[i].balanceScore.totalScore() == c[j].balanceScore.totalScore() &&
		c[i].rangeCount == c[j].rangeCount &&
		c[i].necessary == c[j].necessary &&
		c[i].fullDisk == c[j].fullDisk &&
		c[i].valid == c[j].valid {
		return c[i].store.StoreID < c[j].store.StoreID
	}
	return c[i].less(c[j])
}
func (c byScoreAndID) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

// onlyValidAndNotFull returns all the elements in a sorted (by score reversed)
// candidate list that are valid and not nearly full.
func (cl candidateList) onlyValidAndNotFull() candidateList {
	for i := len(cl) - 1; i >= 0; i-- {
		if cl[i].valid && !cl[i].fullDisk {
			return cl[:i+1]
		}
	}
	return candidateList{}
}

// best returns all the elements in a sorted (by score reversed) candidate list
// that share the highest constraint score and are valid.
func (cl candidateList) best() candidateList {
	cl = cl.onlyValidAndNotFull()
	if len(cl) <= 1 {
		return cl
	}
	for i := 1; i < len(cl); i++ {
		if cl[i].necessary == cl[0].necessary &&
			cl[i].diversityScore == cl[0].diversityScore &&
			cl[i].convergesScore == cl[0].convergesScore {
			continue
		}
		return cl[:i]
	}
	return cl
}

// worst returns all the elements in a sorted (by score reversed) candidate
// list that share the lowest constraint score.
func (cl candidateList) worst() candidateList {
	if len(cl) <= 1 {
		return cl
	}
	// Are there invalid candidates? If so, pick those.
	if !cl[len(cl)-1].valid {
		for i := len(cl) - 2; i >= 0; i-- {
			if cl[i].valid {
				return cl[i+1:]
			}
		}
	}
	// Are there candidates with a nearly full disk? If so, pick those.
	if cl[len(cl)-1].fullDisk {
		for i := len(cl) - 2; i >= 0; i-- {
			if !cl[i].fullDisk {
				return cl[i+1:]
			}
		}
	}
	// Find the worst constraint/locality/converges values.
	for i := len(cl) - 2; i >= 0; i-- {
		if cl[i].necessary == cl[len(cl)-1].necessary &&
			cl[i].diversityScore == cl[len(cl)-1].diversityScore &&
			cl[i].convergesScore == cl[len(cl)-1].convergesScore {
			continue
		}
		return cl[i+1:]
	}
	return cl
}

// betterThan returns all elements from a sorted (by score reversed) candidate
// list that have a higher score than the candidate
func (cl candidateList) betterThan(c candidate) candidateList {
	for i := 0; i < len(cl); i++ {
		if !c.less(cl[i]) {
			return cl[:i]
		}
	}
	return cl
}

// selectGood randomly chooses a good candidate store from a sorted (by score
// reversed) candidate list using the provided random generator.
func (cl candidateList) selectGood(randGen allocatorRand) *candidate {
	cl = cl.best()
	if len(cl) == 0 {
		return nil
	}
	if len(cl) == 1 {
		return &cl[0]
	}
	randGen.Lock()
	order := randGen.Perm(len(cl))
	randGen.Unlock()
	best := &cl[order[0]]
	for i := 1; i < allocatorRandomCount; i++ {
		if best.less(cl[order[i]]) {
			best = &cl[order[i]]
		}
	}
	return best
}

// selectBad randomly chooses a bad candidate store from a sorted (by score
// reversed) candidate list using the provided random generator.
func (cl candidateList) selectBad(randGen allocatorRand) *candidate {
	cl = cl.worst()
	if len(cl) == 0 {
		return nil
	}
	if len(cl) == 1 {
		return &cl[0]
	}
	randGen.Lock()
	order := randGen.Perm(len(cl))
	randGen.Unlock()
	worst := &cl[order[0]]
	for i := 1; i < allocatorRandomCount; i++ {
		if cl[order[i]].less(*worst) {
			worst = &cl[order[i]]
		}
	}
	return worst
}

// removeCandidate remove the specified candidate from candidateList.
func (cl candidateList) removeCandidate(c candidate) candidateList {
	for i := 0; i < len(cl); i++ {
		if cl[i].store.StoreID == c.store.StoreID {
			cl = append(cl[:i], cl[i+1:]...)
			break
		}
	}
	return cl
}

// allocateCandidates creates a candidate list of all stores that can be used
// for allocating a new replica ordered from the best to the worst. Only
// stores that meet the criteria are included in the list.
func allocateCandidates(
	sl StoreList,
	constraints analyzedConstraints,
	existing []roachpb.ReplicaDescriptor,
	rangeInfo RangeInfo,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
	options scorerOptions,
) candidateList {
	var candidates candidateList
	for _, s := range sl.stores {
		if storeHasReplica(s.StoreID, existing) {
			continue
		}
		constraintsOK, necessary := allocateConstraintsCheck(s, constraints)
		if !constraintsOK {
			continue
		}
		if !maxCapacityCheck(s) {
			continue
		}
		diversityScore := diversityAllocateScore(s, existingNodeLocalities)
		balanceScore := balanceScore(sl, s.Capacity, rangeInfo, options)
		candidates = append(candidates, candidate{
			store:          s,
			valid:          constraintsOK,
			necessary:      necessary,
			diversityScore: diversityScore,
			balanceScore:   balanceScore,
			rangeCount:     int(s.Capacity.RangeCount),
		})
	}
	if options.deterministic {
		sort.Sort(sort.Reverse(byScoreAndID(candidates)))
	} else {
		sort.Sort(sort.Reverse(byScore(candidates)))
	}
	return candidates
}

// removeCandidates creates a candidate list of all existing replicas' stores
// ordered from least qualified for removal to most qualified. Stores that are
// marked as not valid, are in violation of a required criteria.
func removeCandidates(
	sl StoreList,
	constraints analyzedConstraints,
	rangeInfo RangeInfo,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
	options scorerOptions,
) candidateList {
	var candidates candidateList
	for _, s := range sl.stores {
		constraintsOK, necessary := removeConstraintsCheck(s, constraints)
		if !constraintsOK {
			candidates = append(candidates, candidate{
				store:     s,
				valid:     false,
				necessary: necessary,
				details:   "constraint check fail",
			})
			continue
		}
		diversityScore := diversityRemovalScore(s.Node.NodeID, existingNodeLocalities)
		balanceScore := balanceScore(sl, s.Capacity, rangeInfo, options)
		var convergesScore int
		if !rebalanceFromConvergesOnMean(sl, s.Capacity, rangeInfo, options) {
			// If removing this candidate replica does not converge the store
			// stats to their means, we make it less attractive for removal by
			// adding 1 to the constraint score. Note that when selecting a
			// candidate for removal the candidates with the lowest scores are
			// more likely to be removed.
			convergesScore = 1
		}
		candidates = append(candidates, candidate{
			store:          s,
			valid:          constraintsOK,
			necessary:      necessary,
			fullDisk:       !maxCapacityCheck(s),
			diversityScore: diversityScore,
			convergesScore: convergesScore,
			balanceScore:   balanceScore,
			rangeCount:     int(s.Capacity.RangeCount),
		})
	}
	if options.deterministic {
		sort.Sort(sort.Reverse(byScoreAndID(candidates)))
	} else {
		sort.Sort(sort.Reverse(byScore(candidates)))
	}
	return candidates
}

type rebalanceOptions struct {
	existingCandidates candidateList
	candidates         candidateList
}

// rebalanceCandidates creates two candidate lists. The first contains all
// existing replica's stores, ordered from least qualified for rebalancing to
// most qualified. The second list is of all potential stores that could be
// used as rebalancing receivers, ordered from best to worst.
func rebalanceCandidates(
	ctx context.Context,
	allStores StoreList,
	constraints analyzedConstraints,
	rangeInfo RangeInfo,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
	localityLookupFn func(roachpb.NodeID) string,
	options scorerOptions,
) []rebalanceOptions {
	// 1. Determine whether existing replicas are valid and/or necessary.
	type existingStore struct {
		cand        candidate
		localityStr string
	}
	existingStores := make(map[roachpb.StoreID]existingStore)
	var needRebalanceFrom bool
	curDiversityScore := rangeDiversityScore(existingNodeLocalities)
	for _, store := range allStores.stores {
		for _, repl := range rangeInfo.Desc.Replicas {
			if store.StoreID != repl.StoreID {
				continue
			}
			valid, necessary := removeConstraintsCheck(store, constraints)
			fullDisk := !maxCapacityCheck(store)
			if !valid {
				if !needRebalanceFrom {
					log.VEventf(ctx, 2, "s%d: should-rebalance(invalid): locality:%q",
						store.StoreID, store.Node.Locality)
				}
				needRebalanceFrom = true
			}
			if fullDisk {
				if !needRebalanceFrom {
					log.VEventf(ctx, 2, "s%d: should-rebalance(full-disk): capacity:%q",
						store.StoreID, store.Capacity)
				}
				needRebalanceFrom = true
			}
			existingStores[store.StoreID] = existingStore{
				cand: candidate{
					store:          store,
					valid:          valid,
					necessary:      necessary,
					fullDisk:       fullDisk,
					diversityScore: curDiversityScore,
				},
				localityStr: localityLookupFn(store.Node.NodeID),
			}
		}
	}

	// 2. For each store, determine the stores that would be the best
	// replacements on the basis of constraints, disk fullness, and diversity.
	// Only the best should be included when computing balanceScores, since it
	// isn't fair to compare the fullness of stores in a valid/necessary/diverse
	// locality to those in an invalid/unnecessary/nondiverse locality (see
	// #20751).  Along the way, determine whether rebalance is needed to improve
	// the range along these critical dimensions.
	//
	// This creates groups of stores that are valid to compare with each other.
	// For example, if a range has a replica in localities A, B, and C, it's ok
	// to compare other stores in locality A with the existing store in locality
	// A, but would be bad for diversity if we were to compare them to the
	// existing stores in localities B and C (see #20751 for more background).
	//
	// NOTE: We can't just do this once per localityStr because constraints can
	// also include node Attributes or store Attributes. We could try to group
	// stores by attributes as well, but it's simplest to just run this for each
	// store.
	type comparableStoreList struct {
		existing   []roachpb.StoreDescriptor
		sl         StoreList
		candidates candidateList
	}
	var comparableStores []comparableStoreList
	var needRebalanceTo bool
	for _, existing := range existingStores {
		// If this store is equivalent in both Locality and Node/Store Attributes to
		// some other existing store, then we can treat them the same. We have to
		// include Node/Store Attributes because they affect constraints.
		var matchedOtherExisting bool
		for i, stores := range comparableStores {
			if sameLocalityAndAttrs(stores.existing[0], existing.cand.store) {
				comparableStores[i].existing = append(comparableStores[i].existing, existing.cand.store)
				matchedOtherExisting = true
				break
			}
		}
		if matchedOtherExisting {
			continue
		}
		var comparableCands candidateList
		for _, store := range allStores.stores {
			constraintsOK, necessary := rebalanceFromConstraintsCheck(
				store, existing.cand.store.StoreID, constraints)
			maxCapacityOK := maxCapacityCheck(store)
			diversityScore := diversityRebalanceFromScore(
				store, existing.cand.store.Node.NodeID, existingNodeLocalities)
			cand := candidate{
				store:          store,
				valid:          constraintsOK,
				necessary:      necessary,
				fullDisk:       !maxCapacityOK,
				diversityScore: diversityScore,
			}
			if !cand.less(existing.cand) {
				comparableCands = append(comparableCands, cand)
				if !needRebalanceFrom && !needRebalanceTo && existing.cand.less(cand) {
					needRebalanceTo = true
					log.VEventf(ctx, 2, "s%d: should-rebalance(necessary/diversity=s%d): oldNecessary:%t, newNecessary:%t, oldDiversity:%f, newDiversity:%f, locality:%q",
						existing.cand.store.StoreID, store.StoreID, existing.cand.necessary, cand.necessary,
						existing.cand.diversityScore, cand.diversityScore, store.Node.Locality)
				}
			}
		}
		if options.deterministic {
			sort.Sort(sort.Reverse(byScoreAndID(comparableCands)))
		} else {
			sort.Sort(sort.Reverse(byScore(comparableCands)))
		}
		bestCands := comparableCands.best()
		bestStores := make([]roachpb.StoreDescriptor, len(bestCands))
		for i := range bestCands {
			bestStores[i] = bestCands[i].store
		}
		comparableStores = append(comparableStores, comparableStoreList{
			existing:   []roachpb.StoreDescriptor{existing.cand.store},
			sl:         makeStoreList(bestStores),
			candidates: bestCands,
		})
	}

	// 3. Decide whether we should try to rebalance. Note that for each existing
	// store, we only compare its fullness stats to the stats of "comparable"
	// stores, i.e. those stores that at least as valid, necessary, and diverse
	// as the existing store.
	needRebalance := needRebalanceFrom || needRebalanceTo
	var shouldRebalanceCheck bool
	if !needRebalance {
		for _, existing := range existingStores {
			var sl StoreList
		outer:
			for _, comparable := range comparableStores {
				for _, existingCand := range comparable.existing {
					if existing.cand.store.StoreID == existingCand.StoreID {
						sl = comparable.sl
						break outer
					}
				}
			}
			// TODO(a-robinson): Some moderate refactoring could extract this logic out
			// into the loop below, avoiding duplicate balanceScore calculations.
			if shouldRebalance(ctx, existing.cand.store, sl, rangeInfo, options) {
				shouldRebalanceCheck = true
				break
			}
		}
	}
	if !needRebalance && !shouldRebalanceCheck {
		return nil
	}

	// 4. Create sets of rebalance options, i.e. groups of candidate stores and
	// the existing replicas that they could legally replace in the range.  We
	// have to make a separate set of these for each group of comparableStores.
	results := make([]rebalanceOptions, 0, len(comparableStores))
	for _, comparable := range comparableStores {
		var existingCandidates candidateList
		var candidates candidateList
		for _, existingDesc := range comparable.existing {
			existing, ok := existingStores[existingDesc.StoreID]
			if !ok {
				log.Errorf(ctx, "BUG: missing candidate for existing store %+v; stores: %+v",
					existingDesc, existingStores)
				continue
			}
			if !existing.cand.valid {
				existing.cand.details = "constraint check fail"
				existingCandidates = append(existingCandidates, existing.cand)
				continue
			}
			balanceScore := balanceScore(comparable.sl, existing.cand.store.Capacity, rangeInfo, options)
			var convergesScore int
			if !rebalanceFromConvergesOnMean(comparable.sl, existing.cand.store.Capacity, rangeInfo, options) {
				// Similarly to in removeCandidates, any replica whose removal
				// would not converge the range stats to their means is given a
				// constraint score boost of 1 to make it less attractive for
				// removal.
				convergesScore = 1
			}
			existing.cand.convergesScore = convergesScore
			existing.cand.balanceScore = balanceScore
			existing.cand.rangeCount = int(existing.cand.store.Capacity.RangeCount)
			existingCandidates = append(existingCandidates, existing.cand)
		}

		for _, cand := range comparable.candidates {
			// We handled the possible candidates for removal above. Don't process
			// anymore here.
			if _, ok := existingStores[cand.store.StoreID]; ok {
				continue
			}
			// We already computed valid, necessary, fullDisk, and diversityScore
			// above, but recompute fullDisk using special rebalanceTo logic for
			// rebalance candidates.
			s := cand.store
			cand.fullDisk = !rebalanceToMaxCapacityCheck(s)
			cand.balanceScore = balanceScore(comparable.sl, s.Capacity, rangeInfo, options)
			if rebalanceToConvergesOnMean(comparable.sl, s.Capacity, rangeInfo, options) {
				// This is the counterpart of !rebalanceFromConvergesOnMean from
				// the existing candidates. Candidates whose addition would
				// converge towards the range count mean are promoted.
				cand.convergesScore = 1
			} else if !needRebalance {
				// Only consider this candidate if we must rebalance due to constraint,
				// disk fullness, or diversity reasons.
				log.VEventf(ctx, 3, "not considering %+v as a candidate for range %+v: score=%s storeList=%+v",
					s, rangeInfo, cand.balanceScore, comparable.sl)
				continue
			}
			cand.rangeCount = int(s.Capacity.RangeCount)
			candidates = append(candidates, cand)
		}

		if len(existingCandidates) == 0 || len(candidates) == 0 {
			continue
		}

		if options.deterministic {
			sort.Sort(sort.Reverse(byScoreAndID(existingCandidates)))
			sort.Sort(sort.Reverse(byScoreAndID(candidates)))
		} else {
			sort.Sort(sort.Reverse(byScore(existingCandidates)))
			sort.Sort(sort.Reverse(byScore(candidates)))
		}

		// Only return candidates better than the worst existing replica.
		improvementCandidates := candidates.betterThan(existingCandidates[len(existingCandidates)-1])
		if len(improvementCandidates) == 0 {
			continue
		}
		results = append(results, rebalanceOptions{
			existingCandidates: existingCandidates,
			candidates:         improvementCandidates,
		})
		log.VEventf(ctx, 5, "rebalance candidates #%d: %s\nexisting replicas: %s",
			len(results), results[len(results)-1].candidates, results[len(results)-1].existingCandidates)
	}

	return results
}

// bestRebalanceTarget returns the best target to try to rebalance to out of
// the provided options, and removes it from the relevant candidate list.
// Also returns the existing replicas that the chosen candidate was compared to.
// Returns nil if there are no more targets worth rebalancing to.
func bestRebalanceTarget(
	randGen allocatorRand, options []rebalanceOptions,
) (*candidate, candidateList) {
	bestIdx := -1
	var bestTarget *candidate
	var replaces candidate
	for i, option := range options {
		if len(option.candidates) == 0 {
			continue
		}
		target := option.candidates.selectGood(randGen)
		if target == nil {
			continue
		}
		existing := option.existingCandidates[len(option.existingCandidates)-1]
		if betterRebalanceTarget(target, &existing, bestTarget, &replaces) == target {
			bestIdx = i
			bestTarget = target
			replaces = existing
		}
	}
	if bestIdx == -1 {
		return nil, nil
	}
	// Copy the selected target out of the candidates slice before modifying
	// the slice. Without this, the returned pointer likely will be pointing
	// to a different candidate than intended due to movement within the slice.
	copiedTarget := *bestTarget
	options[bestIdx].candidates = options[bestIdx].candidates.removeCandidate(copiedTarget)
	return &copiedTarget, options[bestIdx].existingCandidates
}

// betterRebalanceTarget returns whichever of target1 or target2 is a larger
// improvement over its corresponding existing replica that it will be
// replacing in the range.
func betterRebalanceTarget(target1, existing1, target2, existing2 *candidate) *candidate {
	if target2 == nil {
		return target1
	}
	// Try to pick whichever target is a larger improvement over the replica that
	// they'll replace.
	comp1 := target1.compare(*existing1)
	comp2 := target2.compare(*existing2)
	if comp1 > comp2 {
		return target1
	}
	if comp1 < comp2 {
		return target2
	}
	// If the two targets are equally better than their corresponding existing
	// replicas, just return whichever target is better.
	if target1.less(*target2) {
		return target2
	}
	return target1
}

// shouldRebalance returns whether the specified store is a candidate for
// having a replica removed from it given the candidate store list.
func shouldRebalance(
	ctx context.Context,
	store roachpb.StoreDescriptor,
	sl StoreList,
	rangeInfo RangeInfo,
	options scorerOptions,
) bool {
	if !options.statsBasedRebalancingEnabled {
		return shouldRebalanceNoStats(ctx, store, sl, options)
	}

	// Rebalance if this store is full enough that the range is a bad fit.
	score := balanceScore(sl, store.Capacity, rangeInfo, options)
	if rangeIsBadFit(score) {
		log.VEventf(ctx, 2,
			"s%d: should-rebalance(bad-fit): balanceScore=%s, capacity=(%v), rangeInfo=%+v, "+
				"(meanRangeCount=%.1f, meanDiskUsage=%s, meanWritesPerSecond=%.2f), ",
			store.StoreID, score, store.Capacity, rangeInfo,
			sl.candidateRanges.mean, humanizeutil.IBytes(int64(sl.candidateLogicalBytes.mean)),
			sl.candidateWritesPerSecond.mean)
		return true
	}

	// Rebalance if there exists another store that is very in need of the
	// range and this store is a somewhat bad match for it.
	if rangeIsPoorFit(score) {
		for _, desc := range sl.stores {
			otherScore := balanceScore(sl, desc.Capacity, rangeInfo, options)
			if !rangeIsGoodFit(otherScore) {
				log.VEventf(ctx, 5,
					"s%d is not a good enough fit to replace s%d: balanceScore=%s, capacity=(%v), rangeInfo=%+v, "+
						"(meanRangeCount=%.1f, meanDiskUsage=%s, meanWritesPerSecond=%.2f), ",
					desc.StoreID, store.StoreID, otherScore, desc.Capacity, rangeInfo,
					sl.candidateRanges.mean, humanizeutil.IBytes(int64(sl.candidateLogicalBytes.mean)),
					sl.candidateWritesPerSecond.mean)
				continue
			}
			if storeHasReplica(desc.StoreID, rangeInfo.Desc.Replicas) {
				continue
			}
			log.VEventf(ctx, 2,
				"s%d: should-rebalance(better-fit=s%d): balanceScore=%s, capacity=(%v), rangeInfo=%+v, "+
					"otherScore=%s, otherCapacity=(%v), "+
					"(meanRangeCount=%.1f, meanDiskUsage=%s, meanWritesPerSecond=%.2f), ",
				store.StoreID, desc.StoreID, score, store.Capacity, rangeInfo,
				otherScore, desc.Capacity, sl.candidateRanges.mean,
				humanizeutil.IBytes(int64(sl.candidateLogicalBytes.mean)), sl.candidateWritesPerSecond.mean)
			return true
		}
	}

	// If we reached this point, we're happy with the range where it is.
	log.VEventf(ctx, 3,
		"s%d: should-not-rebalance: balanceScore=%s, capacity=(%v), rangeInfo=%+v, "+
			"(meanRangeCount=%.1f, meanDiskUsage=%s, meanWritesPerSecond=%.2f), ",
		store.StoreID, score, store.Capacity, rangeInfo, sl.candidateRanges.mean,
		humanizeutil.IBytes(int64(sl.candidateLogicalBytes.mean)), sl.candidateWritesPerSecond.mean)
	return false
}

// shouldRebalance implements the decision of whether to rebalance for the case
// when stats-based rebalancing is disabled and decisions should thus be
// made based only on range counts.
func shouldRebalanceNoStats(
	ctx context.Context, store roachpb.StoreDescriptor, sl StoreList, options scorerOptions,
) bool {
	overfullThreshold := int32(math.Ceil(overfullRangeThreshold(options, sl.candidateRanges.mean)))
	if store.Capacity.RangeCount > overfullThreshold {
		log.VEventf(ctx, 2,
			"s%d: should-rebalance(ranges-overfull): rangeCount=%d, mean=%.2f, overfull-threshold=%d",
			store.StoreID, store.Capacity.RangeCount, sl.candidateRanges.mean, overfullThreshold)
		return true
	}

	if float64(store.Capacity.RangeCount) > sl.candidateRanges.mean {
		underfullThreshold := int32(math.Floor(underfullRangeThreshold(options, sl.candidateRanges.mean)))
		for _, desc := range sl.stores {
			if desc.Capacity.RangeCount < underfullThreshold {
				log.VEventf(ctx, 2,
					"s%d: should-rebalance(better-fit-ranges=s%d): rangeCount=%d, otherRangeCount=%d, "+
						"mean=%.2f, underfull-threshold=%d",
					store.StoreID, desc.StoreID, store.Capacity.RangeCount, desc.Capacity.RangeCount,
					sl.candidateRanges.mean, underfullThreshold)
				return true
			}
		}
	}

	// If we reached this point, we're happy with the range where it is.
	return false
}

// storeHasReplica returns true if the provided NodeID contains an entry in
// the provided list of existing replicas.
func storeHasReplica(storeID roachpb.StoreID, existing []roachpb.ReplicaDescriptor) bool {
	for _, r := range existing {
		if r.StoreID == storeID {
			return true
		}
	}
	return false
}

func sameLocalityAndAttrs(s1, s2 roachpb.StoreDescriptor) bool {
	if !s1.Node.Locality.Equals(s2.Node.Locality) {
		return false
	}
	if !s1.Node.Attrs.Equals(s2.Node.Attrs) {
		return false
	}
	if !s1.Attrs.Equals(s2.Attrs) {
		return false
	}
	return true
}

// storeHasConstraint returns whether a store's attributes or node's locality
// matches the key value pair in the constraint.
func storeHasConstraint(store roachpb.StoreDescriptor, c config.Constraint) bool {
	if c.Key == "" {
		for _, attrs := range []roachpb.Attributes{store.Attrs, store.Node.Attrs} {
			for _, attr := range attrs.Attrs {
				if attr == c.Value {
					return true
				}
			}
		}
	} else {
		for _, tier := range store.Node.Locality.Tiers {
			if c.Key == tier.Key && c.Value == tier.Value {
				return true
			}
		}
	}
	return false
}

type analyzedConstraints struct {
	constraints []config.Constraints
	// True if the per-replica constraints don't fully cover all the desired
	// replicas in the range (sum(constraints.NumReplicas) < zone.NumReplicas).
	// In such cases, we allow replicas that don't match any of the per-replica
	// constraints, but never mark them as necessary.
	unconstrainedReplicas bool
	// For each set of constraints in the above slice, track which StoreIDs
	// satisfy them. This field is unused if there are no constraints.
	satisfiedBy [][]roachpb.StoreID
	// Maps from StoreID to the indices in the constraints slice of which
	// constraints the store satisfies. This field is unused if there are no
	// constraints.
	satisfies map[roachpb.StoreID][]int
}

// analyzeConstraints processes the zone config constraints that apply to a
// range along with the current replicas for a range, spitting back out
// information about which constraints are satisfied by which replicas and
// which replicas satisfy which constraints, aiding in allocation decisions.
func analyzeConstraints(
	ctx context.Context,
	getStoreDescFn func(roachpb.StoreID) (roachpb.StoreDescriptor, bool),
	existing []roachpb.ReplicaDescriptor,
	zone config.ZoneConfig,
) analyzedConstraints {
	result := analyzedConstraints{
		constraints: zone.Constraints,
	}

	if len(zone.Constraints) > 0 {
		result.satisfiedBy = make([][]roachpb.StoreID, len(zone.Constraints))
		result.satisfies = make(map[roachpb.StoreID][]int)
	}

	var constrainedReplicas int32
	for i, subConstraints := range zone.Constraints {
		constrainedReplicas += subConstraints.NumReplicas
		for _, repl := range existing {
			// If for some reason we don't have the store descriptor (which shouldn't
			// happen once a node is hooked into gossip), trust that it's valid. This
			// is a much more stable failure state than frantically moving everything
			// off such a node.
			store, ok := getStoreDescFn(repl.StoreID)
			if !ok || subConstraintsCheck(store, subConstraints.Constraints) {
				result.satisfiedBy[i] = append(result.satisfiedBy[i], store.StoreID)
				result.satisfies[store.StoreID] = append(result.satisfies[store.StoreID], i)
			}
		}
	}
	if constrainedReplicas > 0 && constrainedReplicas < zone.NumReplicas {
		result.unconstrainedReplicas = true
	}
	return result
}

// allocateConstraintsCheck checks the potential allocation target store
// against all the constraints. If it matches a constraint at all, it's valid.
// If it matches a constraint that is not already fully satisfied by existing
// replicas, then it's necessary.
//
// NB: This assumes that the sum of all constraints.NumReplicas is equal to
// configured number of replicas for the range, or that there's just one set of
// constraints with NumReplicas set to 0. This is meant to be enforced in the
// config package.
func allocateConstraintsCheck(
	store roachpb.StoreDescriptor, analyzed analyzedConstraints,
) (valid bool, necessary bool) {
	// All stores are valid when there are no constraints.
	if len(analyzed.constraints) == 0 {
		return true, false
	}

	for i, constraints := range analyzed.constraints {
		if constraintsOK := subConstraintsCheck(store, constraints.Constraints); constraintsOK {
			valid = true
			matchingStores := analyzed.satisfiedBy[i]
			if len(matchingStores) < int(constraints.NumReplicas) {
				return true, true
			}
		}
	}

	if analyzed.unconstrainedReplicas {
		valid = true
	}

	return valid, false
}

// removeConstraintsCheck checks the existing store against the analyzed
// constraints, determining whether it's valid (matches some constraint) and
// necessary (matches some constraint that no other existing replica matches).
// The difference between this and allocateConstraintsCheck is that this is to
// be used on an existing replica of the range, not a potential addition.
func removeConstraintsCheck(
	store roachpb.StoreDescriptor, analyzed analyzedConstraints,
) (valid bool, necessary bool) {
	// All stores are valid when there are no constraints.
	if len(analyzed.constraints) == 0 {
		return true, false
	}

	// The store satisfies none of the constraints, and the zone is not configured
	// to desire more replicas than constraints have been specified for.
	if len(analyzed.satisfies[store.StoreID]) == 0 && !analyzed.unconstrainedReplicas {
		return false, false
	}

	// Check if the store matches a constraint that isn't overly satisfied.
	// If so, then keeping it around is necessary to ensure that constraint stays
	// fully satisfied.
	for _, constraintIdx := range analyzed.satisfies[store.StoreID] {
		if len(analyzed.satisfiedBy[constraintIdx]) <= int(analyzed.constraints[constraintIdx].NumReplicas) {
			return true, true
		}
	}

	// If neither of the above is true, then the store is valid but nonessential.
	// NOTE: We could be more precise here by trying to find the least essential
	// existing replica and only considering that one nonessential, but this is
	// sufficient to avoid violating constraints.
	return true, false
}

// rebalanceConstraintsCheck checks the potential rebalance target store
// against the analyzed constraints, determining whether it's valid whether it
// will be necessary if fromStoreID (an existing replica) is removed from the
// range.
func rebalanceFromConstraintsCheck(
	store roachpb.StoreDescriptor, fromStoreID roachpb.StoreID, analyzed analyzedConstraints,
) (valid bool, necessary bool) {
	// All stores are valid when there are no constraints.
	if len(analyzed.constraints) == 0 {
		return true, false
	}

	// Check the store against all the constraints. If it matches a constraint at
	// all, it's valid. If it matches a constraint that is not already fully
	// satisfied by existing replicas or that is only fully satisfied because of
	// fromStoreID, then it's necessary.
	//
	// NB: This assumes that the sum of all constraints.NumReplicas is equal to
	// configured number of replicas for the range, or that there's just one set
	// of constraints with NumReplicas set to 0. This is meant to be enforced in
	// the config package.
	for i, constraints := range analyzed.constraints {
		if constraintsOK := subConstraintsCheck(store, constraints.Constraints); constraintsOK {
			valid = true
			matchingStores := analyzed.satisfiedBy[i]
			if len(matchingStores) < int(constraints.NumReplicas) ||
				(len(matchingStores) == int(constraints.NumReplicas) &&
					containsStore(analyzed.satisfiedBy[i], fromStoreID)) {
				return true, true
			}
		}
	}

	if analyzed.unconstrainedReplicas {
		valid = true
	}

	return valid, false
}

// containsStore returns true if the list of StoreIDs contains the target.
func containsStore(stores []roachpb.StoreID, target roachpb.StoreID) bool {
	for _, storeID := range stores {
		if storeID == target {
			return true
		}
	}
	return false
}

// constraintsCheck returns true iff the provided store would be a valid in a
// range with the provided constraints.
func constraintsCheck(store roachpb.StoreDescriptor, constraints []config.Constraints) bool {
	if len(constraints) == 0 {
		return true
	}

	for _, subConstraints := range constraints {
		if constraintsOK := subConstraintsCheck(store, subConstraints.Constraints); constraintsOK {
			return true
		}
	}
	return false
}

// subConstraintsCheck checks a store against a single set of constraints (out
// of the possibly numerous sets that apply to a range), returning true iff the
// store matches the constraints.
func subConstraintsCheck(store roachpb.StoreDescriptor, constraints []config.Constraint) bool {
	for _, constraint := range constraints {
		hasConstraint := storeHasConstraint(store, constraint)
		switch {
		case constraint.Type == config.Constraint_REQUIRED && !hasConstraint:
			return false
		case constraint.Type == config.Constraint_PROHIBITED && hasConstraint:
			return false
		}
	}
	return true
}

// rangeDiversityScore returns a value between 0 and 1 based on how diverse the
// given range is. A higher score means the range is more diverse.
// All below diversity-scoring methods should in theory be implemented by
// calling into this one, but they aren't to avoid allocations.
func rangeDiversityScore(existingNodeLocalities map[roachpb.NodeID]roachpb.Locality) float64 {
	var sumScore float64
	var numSamples int
	for n1, l1 := range existingNodeLocalities {
		for n2, l2 := range existingNodeLocalities {
			// Only compare pairs of replicas where s2 > s1 to avoid computing the
			// diversity score between each pair of localities twice.
			if n2 <= n1 {
				continue
			}
			sumScore += l1.DiversityScore(l2)
			numSamples++
		}
	}
	if numSamples == 0 {
		return roachpb.MaxDiversityScore
	}
	return sumScore / float64(numSamples)
}

// diversityAllocateScore returns a value between 0 and 1 based on how
// desirable it would be to add a replica to store. A higher score means the
// store is a better fit.
func diversityAllocateScore(
	store roachpb.StoreDescriptor, existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
) float64 {
	var sumScore float64
	var numSamples int
	// We don't need to calculate the overall diversityScore for the range, just
	// how well the new store would fit, because for any store that we might
	// consider adding the pairwise average diversity of the existing replicas
	// is the same.
	for _, locality := range existingNodeLocalities {
		newScore := store.Node.Locality.DiversityScore(locality)
		sumScore += newScore
		numSamples++
	}
	// If the range has no replicas, any node would be a perfect fit.
	if numSamples == 0 {
		return roachpb.MaxDiversityScore
	}
	return sumScore / float64(numSamples)
}

// diversityRemovalScore returns a value between 0 and 1 based on how desirable
// it would be to remove a node's replica of a range.  A higher score indicates
// that the node is a better fit (i.e. keeping it around is good for diversity).
func diversityRemovalScore(
	nodeID roachpb.NodeID, existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
) float64 {
	var sumScore float64
	var numSamples int
	locality := existingNodeLocalities[nodeID]
	// We don't need to calculate the overall diversityScore for the range, because the original overall diversityScore
	// of this range is always the same.
	for otherNodeID, otherLocality := range existingNodeLocalities {
		if otherNodeID == nodeID {
			continue
		}
		newScore := locality.DiversityScore(otherLocality)
		sumScore += newScore
		numSamples++
	}
	if numSamples == 0 {
		return roachpb.MaxDiversityScore
	}
	return sumScore / float64(numSamples)
}

// diversityRebalanceScore returns a value between 0 and 1 based on how
// desirable it would be to rebalance away from an existing store to the target
// store. Because the store to be removed isn't specified, this assumes that
// the worst-fitting store from a diversity perspective will be removed. A
// higher score indicates that the provided store is a better fit for the
// range.
func diversityRebalanceScore(
	store roachpb.StoreDescriptor, existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
) float64 {
	if len(existingNodeLocalities) == 0 {
		return roachpb.MaxDiversityScore
	}
	var maxScore float64
	// For every existing node, calculate what the diversity score would be if we
	// remove that node's replica to replace it with one on the provided store.
	for removedNodeID := range existingNodeLocalities {
		score := diversityRebalanceFromScore(store, removedNodeID, existingNodeLocalities)
		if score > maxScore {
			maxScore = score
		}
	}
	return maxScore
}

// diversityRebalanceFromScore returns a value between 0 and 1 based on how
// desirable it would be to rebalance away from the specified node and to
// the specified store. This is the same as diversityRebalanceScore, but for
// the case where there's a particular replica we want to consider removing.
// A higher score indicates that the provided store is a better fit for the
// range.
func diversityRebalanceFromScore(
	store roachpb.StoreDescriptor,
	fromNodeID roachpb.NodeID,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
) float64 {
	// Compute the pairwise diversity score of all replicas that will exist
	// after adding store and removing fromNodeID.
	var sumScore float64
	var numSamples int
	for nodeID, locality := range existingNodeLocalities {
		if nodeID == fromNodeID {
			continue
		}
		newScore := store.Node.Locality.DiversityScore(locality)
		sumScore += newScore
		numSamples++
		for otherNodeID, otherLocality := range existingNodeLocalities {
			// Only compare pairs of replicas where otherNodeID > nodeID to avoid
			// computing the diversity score between each pair of localities twice.
			if otherNodeID <= nodeID || otherNodeID == fromNodeID {
				continue
			}
			newScore := locality.DiversityScore(otherLocality)
			sumScore += newScore
			numSamples++
		}
	}
	if numSamples == 0 {
		return roachpb.MaxDiversityScore
	}
	return sumScore / float64(numSamples)
}

type rangeCountStatus int

const (
	overfull  rangeCountStatus = -1
	balanced  rangeCountStatus = 0
	underfull rangeCountStatus = 1
)

func oppositeStatus(rcs rangeCountStatus) rangeCountStatus {
	return -rcs
}

// balanceScore returns an arbitrarily scaled score where higher scores are for
// stores where the range is a better fit based on various balance factors
// like range count, disk usage, and QPS.
func balanceScore(
	sl StoreList, sc roachpb.StoreCapacity, rangeInfo RangeInfo, options scorerOptions,
) balanceDimensions {
	var dimensions balanceDimensions
	if float64(sc.RangeCount) > overfullRangeThreshold(options, sl.candidateRanges.mean) {
		dimensions.ranges = overfull
	} else if float64(sc.RangeCount) < underfullRangeThreshold(options, sl.candidateRanges.mean) {
		dimensions.ranges = underfull
	} else {
		dimensions.ranges = balanced
	}
	if options.statsBasedRebalancingEnabled {
		dimensions.bytes = balanceContribution(
			options,
			dimensions.ranges,
			sl.candidateLogicalBytes.mean,
			float64(sc.LogicalBytes),
			sc.BytesPerReplica,
			float64(rangeInfo.LogicalBytes))
		dimensions.writes = balanceContribution(
			options,
			dimensions.ranges,
			sl.candidateWritesPerSecond.mean,
			sc.WritesPerSecond,
			sc.WritesPerReplica,
			rangeInfo.WritesPerSecond)
	}
	return dimensions
}

// balanceContribution generates a single dimension's contribution to a range's
// balanceScore, where larger values mean a store is a better fit for a given
// range.
func balanceContribution(
	options scorerOptions,
	rcs rangeCountStatus,
	mean float64,
	storeVal float64,
	percentiles roachpb.Percentiles,
	rangeVal float64,
) float64 {
	if storeVal > overfullStatThreshold(options, mean) {
		return percentileScore(rcs, percentiles, rangeVal)
	} else if storeVal < underfullStatThreshold(options, mean) {
		// To ensure that we behave symmetrically when underfull compared to
		// when we're overfull, inverse both the rangeCountStatus and the
		// result returned by percentileScore. This makes it so that being
		// overfull on ranges and on the given dimension behaves symmetrically to
		// being underfull on ranges and the given dimension (and ditto for
		// overfull on ranges and underfull on a dimension, etc.).
		return -percentileScore(oppositeStatus(rcs), percentiles, rangeVal)
	}
	return 0
}

// percentileScore returns a score for how desirable it is to put a range
// onto a particular store given the assumption that the store is overfull
// along a particular dimension. Takes as parameters:
// * How the number of ranges on the store compares to the norm
// * The distribution of values in the store for the dimension
// * The range's value for the dimension
// A higher score means that the range is a better fit for the store.
func percentileScore(
	rcs rangeCountStatus, percentiles roachpb.Percentiles, rangeVal float64,
) float64 {
	// Note that there is not any great research behind these values. If they're
	// causing thrashing or a bad imbalance, rethink them and modify them as
	// appropriate.
	if rcs == balanced {
		// If the range count is balanced, we should prefer ranges that are
		// very small on this particular dimension to try to rebalance this dimension
		// without messing up the replica counts.
		if rangeVal < percentiles.P10 {
			return 1
		} else if rangeVal < percentiles.P25 {
			return 0.5
		} else if rangeVal > percentiles.P90 {
			return -1
		} else if rangeVal > percentiles.P75 {
			return -0.5
		}
		// else rangeVal >= percentiles.P25 && rangeVal <= percentiles.P75
		// It may be better to return more than 0 here, since taking on an
		// average range isn't necessarily bad, but for now let's see how this works.
		return 0
	} else if rcs == overfull {
		// If this store has too many ranges, we're ok with moving any range that's
		// at least somewhat sizable in this dimension, since we want to reduce both
		// the range count and this metric. Moving extreme outliers may be less
		// desirable, though, so favor very heavy ranges slightly less and disfavor
		// very light ranges.
		//
		// Note that we can't truly disfavor large ranges, since that prevents us
		// from rebalancing nonempty ranges to empty stores (since all nonempty
		// ranges will be greater than an empty store's P90).
		if rangeVal > percentiles.P90 {
			return -0.5
		} else if rangeVal >= percentiles.P25 {
			return -1
		} else if rangeVal >= percentiles.P10 {
			return 0
		}
		// else rangeVal < percentiles.P10
		return 0.5
	} else if rcs == underfull {
		// If this store has too few ranges but is overloaded on some other
		// dimension, we need to prioritize moving away replicas that are
		// high in that dimension and accepting replicas that are low in it.
		if rangeVal < percentiles.P10 {
			return 1
		} else if rangeVal < percentiles.P25 {
			return 0.5
		} else if rangeVal > percentiles.P90 {
			return -1
		} else if rangeVal > percentiles.P75 {
			return -0.5
		}
		// else rangeVal >= percentiles.P25 && rangeVal <= percentiles.P75
		return 0
	}
	panic(fmt.Sprintf("reached unreachable code: %+v; %+v; %+v", rcs, percentiles, rangeVal))
}

func rangeIsGoodFit(bd balanceDimensions) bool {
	// A score greater than 1 means that more than one dimension improves
	// without being canceled out by the third, since each dimension can only
	// contribute a value from [-1,1] to the score.
	return bd.totalScore() > 1
}

func rangeIsBadFit(bd balanceDimensions) bool {
	// This is the same logic as for rangeIsGoodFit, just reversed.
	return bd.totalScore() < -1
}

func rangeIsPoorFit(bd balanceDimensions) bool {
	// A score less than -0.5 isn't a great fit for a range, since the
	// bad dimensions outweigh the good by at least one entire dimension.
	return bd.totalScore() < -0.5
}

func overfullRangeThreshold(options scorerOptions, mean float64) float64 {
	if !options.statsBasedRebalancingEnabled {
		return mean * (1 + options.rangeRebalanceThreshold)
	}
	return math.Max(mean*(1+options.rangeRebalanceThreshold), mean+5)
}

func underfullRangeThreshold(options scorerOptions, mean float64) float64 {
	if !options.statsBasedRebalancingEnabled {
		return mean * (1 - options.rangeRebalanceThreshold)
	}
	return math.Min(mean*(1-options.rangeRebalanceThreshold), mean-5)
}

func overfullStatThreshold(options scorerOptions, mean float64) float64 {
	return mean * (1 + options.statRebalanceThreshold)
}

func underfullStatThreshold(options scorerOptions, mean float64) float64 {
	return mean * (1 - options.statRebalanceThreshold)
}

func rebalanceFromConvergesOnMean(
	sl StoreList, sc roachpb.StoreCapacity, rangeInfo RangeInfo, options scorerOptions,
) bool {
	return rebalanceConvergesOnMean(
		sl,
		sc,
		sc.RangeCount-1,
		sc.LogicalBytes-rangeInfo.LogicalBytes,
		sc.WritesPerSecond-rangeInfo.WritesPerSecond,
		options)
}

func rebalanceToConvergesOnMean(
	sl StoreList, sc roachpb.StoreCapacity, rangeInfo RangeInfo, options scorerOptions,
) bool {
	return rebalanceConvergesOnMean(
		sl,
		sc,
		sc.RangeCount+1,
		sc.LogicalBytes+rangeInfo.LogicalBytes,
		sc.WritesPerSecond+rangeInfo.WritesPerSecond,
		options)
}

func rebalanceConvergesOnMean(
	sl StoreList,
	sc roachpb.StoreCapacity,
	newRangeCount int32,
	newLogicalBytes int64,
	newWritesPerSecond float64,
	options scorerOptions,
) bool {
	if !options.statsBasedRebalancingEnabled {
		return convergesOnMean(float64(sc.RangeCount), float64(newRangeCount), sl.candidateRanges.mean)
	}

	// Note that we check both converges and diverges. If we always decremented
	// convergeCount when something didn't converge, ranges with stats equal to 0
	// would almost never converge (and thus almost never get rebalanced).
	var convergeCount int
	if convergesOnMean(float64(sc.RangeCount), float64(newRangeCount), sl.candidateRanges.mean) {
		convergeCount++
	} else if divergesFromMean(float64(sc.RangeCount), float64(newRangeCount), sl.candidateRanges.mean) {
		convergeCount--
	}
	if convergesOnMean(float64(sc.LogicalBytes), float64(newLogicalBytes), sl.candidateLogicalBytes.mean) {
		convergeCount++
	} else if divergesFromMean(float64(sc.LogicalBytes), float64(newLogicalBytes), sl.candidateLogicalBytes.mean) {
		convergeCount--
	}
	if convergesOnMean(sc.WritesPerSecond, newWritesPerSecond, sl.candidateWritesPerSecond.mean) {
		convergeCount++
	} else if divergesFromMean(sc.WritesPerSecond, newWritesPerSecond, sl.candidateWritesPerSecond.mean) {
		convergeCount--
	}
	return convergeCount > 0
}

func convergesOnMean(oldVal, newVal, mean float64) bool {
	return math.Abs(newVal-mean) < math.Abs(oldVal-mean)
}

func divergesFromMean(oldVal, newVal, mean float64) bool {
	return math.Abs(newVal-mean) > math.Abs(oldVal-mean)
}

// maxCapacityCheck returns true if the store has room for a new replica.
func maxCapacityCheck(store roachpb.StoreDescriptor) bool {
	return store.Capacity.FractionUsed() < maxFractionUsedThreshold
}

// rebalanceToMaxCapacityCheck returns true if the store has enough room to
// accept a rebalance. The bar for this is stricter than for whether a store
// has enough room to accept a necessary replica (i.e. via AllocateCandidates).
func rebalanceToMaxCapacityCheck(store roachpb.StoreDescriptor) bool {
	return store.Capacity.FractionUsed() < rebalanceToMaxFractionUsedThreshold
}
