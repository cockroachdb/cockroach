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

// candidate store for allocation.
type candidate struct {
	store           roachpb.StoreDescriptor
	valid           bool
	constraintScore float64
	convergesScore  int
	balanceScore    balanceDimensions
	rangeCount      int
	details         string
}

func (c candidate) String() string {
	return fmt.Sprintf("s%d, valid:%t, constraint:%.2f, converges:%d, balance:%s, rangeCount:%d, "+
		"logicalBytes:%s, writesPerSecond:%.2f, details:(%s)",
		c.store.StoreID, c.valid, c.constraintScore, c.convergesScore, c.balanceScore, c.rangeCount,
		humanizeutil.IBytes(c.store.Capacity.LogicalBytes), c.store.Capacity.WritesPerSecond, c.details)
}

// less returns true if o is a better fit for some range than c is.
func (c candidate) less(o candidate) bool {
	if !o.valid {
		return false
	}
	if !c.valid {
		return true
	}
	if c.constraintScore != o.constraintScore {
		return c.constraintScore < o.constraintScore
	}
	if c.convergesScore != o.convergesScore {
		return c.convergesScore < o.convergesScore
	}
	if c.balanceScore.totalScore() != o.balanceScore.totalScore() {
		return c.balanceScore.totalScore() < o.balanceScore.totalScore()
	}
	return c.rangeCount > o.rangeCount
}

// worthRebalancingTo returns true if o is enough of a better fit for some
// range than c is that it's worth rebalancing from c to o.
func (c candidate) worthRebalancingTo(o candidate, options scorerOptions) bool {
	if !o.valid {
		return false
	}
	if !c.valid {
		return true
	}
	if c.constraintScore != o.constraintScore {
		return c.constraintScore < o.constraintScore
	}
	if c.convergesScore != o.convergesScore {
		return c.convergesScore < o.convergesScore
	}
	// You might intuitively think that we should require o's balanceScore to
	// be considerably higher than c's balanceScore, but that will effectively
	// rule out rebalancing in clusters where one locality is much larger or
	// smaller than the others, since all the stores in that locality will tend
	// to have either a maximal or minimal balanceScore.
	if c.balanceScore.totalScore() != o.balanceScore.totalScore() {
		return c.balanceScore.totalScore() < o.balanceScore.totalScore()
	}
	// Instead, just require a gap between their number of ranges. This isn't
	// great, particularly for stats-based rebalancing, but it only breaks
	// balanceScore ties and it's a workable stop-gap on the way to something
	// like #20751.
	avgRangeCount := float64(c.rangeCount+o.rangeCount) / 2.0
	// Use an overfullThreshold that is at least a couple replicas larger than
	// the average of the two, to ensure that we don't keep rebalancing back
	// and forth between nodes that only differ by one or two replicas.
	overfullThreshold := math.Max(overfullRangeThreshold(options, avgRangeCount), avgRangeCount+1.5)
	return float64(c.rangeCount) > overfullThreshold
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
	if c[i].constraintScore == c[j].constraintScore &&
		c[i].convergesScore == c[j].convergesScore &&
		c[i].balanceScore.totalScore() == c[j].balanceScore.totalScore() &&
		c[i].rangeCount == c[j].rangeCount &&
		c[i].valid == c[j].valid {
		return c[i].store.StoreID < c[j].store.StoreID
	}
	return c[i].less(c[j])
}
func (c byScoreAndID) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

// onlyValid returns all the elements in a sorted (by score reversed) candidate
// list that are valid.
func (cl candidateList) onlyValid() candidateList {
	for i := len(cl) - 1; i >= 0; i-- {
		if cl[i].valid {
			return cl[:i+1]
		}
	}
	return candidateList{}
}

// best returns all the elements in a sorted (by score reversed) candidate list
// that share the highest constraint score and are valid.
func (cl candidateList) best() candidateList {
	cl = cl.onlyValid()
	if len(cl) <= 1 {
		return cl
	}
	for i := 1; i < len(cl); i++ {
		if cl[i].constraintScore < cl[0].constraintScore ||
			(cl[i].constraintScore == cl[len(cl)-1].constraintScore &&
				cl[i].convergesScore < cl[len(cl)-1].convergesScore) {
			return cl[:i]
		}
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
	// Find the worst constraint values.
	for i := len(cl) - 2; i >= 0; i-- {
		if cl[i].constraintScore > cl[len(cl)-1].constraintScore ||
			(cl[i].constraintScore == cl[len(cl)-1].constraintScore &&
				cl[i].convergesScore > cl[len(cl)-1].convergesScore) {
			return cl[i+1:]
		}
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
	if len(cl) == 0 {
		return nil
	}
	cl = cl.best()
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
	if len(cl) == 0 {
		return nil
	}
	cl = cl.worst()
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
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	rangeInfo RangeInfo,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
	options scorerOptions,
) candidateList {
	var candidates candidateList
	for _, s := range sl.stores {
		if !preexistingReplicaCheck(s.Node.NodeID, existing) {
			continue
		}
		constraintsOk, preferredMatched := constraintCheck(s, constraints)
		if !constraintsOk {
			continue
		}
		if !maxCapacityCheck(s) {
			continue
		}
		diversityScore := diversityAllocateScore(s, existingNodeLocalities)
		balanceScore := balanceScore(sl, s.Capacity, rangeInfo, options)
		candidates = append(candidates, candidate{
			store:           s,
			valid:           true,
			constraintScore: diversityScore + float64(preferredMatched),
			balanceScore:    balanceScore,
			rangeCount:      int(s.Capacity.RangeCount),
			details:         fmt.Sprintf("diversity=%.2f, preferred=%d", diversityScore, preferredMatched),
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
	constraints config.Constraints,
	rangeInfo RangeInfo,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
	options scorerOptions,
) candidateList {
	var candidates candidateList
	for _, s := range sl.stores {
		constraintsOk, preferredMatched := constraintCheck(s, constraints)
		if !constraintsOk {
			candidates = append(candidates, candidate{
				store:   s,
				valid:   false,
				details: "constraint check fail",
			})
			continue
		}
		if !maxCapacityCheck(s) {
			candidates = append(candidates, candidate{
				store:   s,
				valid:   false,
				details: "max capacity check fail",
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
			store:           s,
			valid:           true,
			constraintScore: diversityScore + float64(preferredMatched),
			convergesScore:  convergesScore,
			balanceScore:    balanceScore,
			rangeCount:      int(s.Capacity.RangeCount),
			details:         fmt.Sprintf("diversity=%.2f, preferred=%d", diversityScore, preferredMatched),
		})
	}
	if options.deterministic {
		sort.Sort(sort.Reverse(byScoreAndID(candidates)))
	} else {
		sort.Sort(sort.Reverse(byScore(candidates)))
	}
	return candidates
}

// rebalanceCandidates creates two candidate lists. The first contains all
// existing replica's stores, ordered from least qualified for rebalancing to
// most qualified. The second list is of all potential stores that could be
// used as rebalancing receivers, ordered from best to worst.
func rebalanceCandidates(
	ctx context.Context,
	sl StoreList,
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	rangeInfo RangeInfo,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
	options scorerOptions,
) (candidateList, candidateList) {
	// Load the exiting storesIDs into a map to eliminate having to loop
	// through the existing descriptors more than once.
	existingStoreIDs := make(map[roachpb.StoreID]struct{})
	for _, repl := range existing {
		existingStoreIDs[repl.StoreID] = struct{}{}
	}

	// Go through all the stores and find all that match the constraints so that
	// we can have accurate stats for rebalance calculations.
	var constraintsOkStoreDescriptors []roachpb.StoreDescriptor

	type constraintInfo struct {
		ok      bool
		matched int
	}
	storeInfos := make(map[roachpb.StoreID]constraintInfo)
	var rebalanceConstraintsCheck bool
	for _, s := range sl.stores {
		constraintsOk, preferredMatched := constraintCheck(s, constraints)
		storeInfos[s.StoreID] = constraintInfo{ok: constraintsOk, matched: preferredMatched}
		_, exists := existingStoreIDs[s.StoreID]
		if constraintsOk {
			constraintsOkStoreDescriptors = append(constraintsOkStoreDescriptors, s)
		} else if exists {
			rebalanceConstraintsCheck = true
			log.VEventf(ctx, 2, "must rebalance from s%d due to constraint check", s.StoreID)
		}
	}

	constraintsOkStoreList := makeStoreList(constraintsOkStoreDescriptors)
	var shouldRebalanceCheck bool
	if !rebalanceConstraintsCheck {
		for _, store := range sl.stores {
			if _, ok := existingStoreIDs[store.StoreID]; ok {
				if shouldRebalance(ctx, store, constraintsOkStoreList, rangeInfo, existingNodeLocalities, options) {
					shouldRebalanceCheck = true
					break
				}
			}
		}
	}

	// Only rebalance away if the constraints don't match or shouldRebalance
	// indicated that we should consider moving the range away from one of its
	// existing stores.
	if !rebalanceConstraintsCheck && !shouldRebalanceCheck {
		return nil, nil
	}

	var existingCandidates candidateList
	var candidates candidateList
	for _, s := range sl.stores {
		storeInfo := storeInfos[s.StoreID]
		maxCapacityOK := maxCapacityCheck(s)
		if _, ok := existingStoreIDs[s.StoreID]; ok {
			if !storeInfo.ok {
				existingCandidates = append(existingCandidates, candidate{
					store:   s,
					valid:   false,
					details: "constraint check fail",
				})
				continue
			}
			if !maxCapacityOK {
				existingCandidates = append(existingCandidates, candidate{
					store:   s,
					valid:   false,
					details: "max capacity check fail",
				})
				continue
			}
			diversityScore := diversityRemovalScore(s.Node.NodeID, existingNodeLocalities)
			balanceScore := balanceScore(sl, s.Capacity, rangeInfo, options)
			var convergesScore int
			if !rebalanceFromConvergesOnMean(sl, s.Capacity, rangeInfo, options) {
				// Similarly to in removeCandidates, any replica whose removal
				// would not converge the range stats to their means is given a
				// constraint score boost of 1 to make it less attractive for
				// removal.
				convergesScore = 1
			}
			existingCandidates = append(existingCandidates, candidate{
				store:           s,
				valid:           true,
				constraintScore: diversityScore + float64(storeInfo.matched),
				convergesScore:  convergesScore,
				balanceScore:    balanceScore,
				rangeCount:      int(s.Capacity.RangeCount),
				details:         fmt.Sprintf("diversity=%.2f, preferred=%d", diversityScore, storeInfo.matched),
			})
		} else {
			if !storeInfo.ok || !maxCapacityOK {
				continue
			}
			balanceScore := balanceScore(sl, s.Capacity, rangeInfo, options)
			var convergesScore int
			if rebalanceToConvergesOnMean(sl, s.Capacity, rangeInfo, options) {
				// This is the counterpart of !rebalanceFromConvergesOnMean from
				// the existing candidates. Candidates whose addition would
				// converge towards the range count mean are promoted.
				convergesScore = 1
			} else if !rebalanceConstraintsCheck {
				// Only consider this candidate if we must rebalance due to a
				// constraint check requirements.
				log.VEventf(ctx, 3, "not considering %+v as a candidate for range %+v: score=%s storeList=%+v",
					s, rangeInfo, balanceScore, sl)
				continue
			}
			diversityScore := diversityRebalanceScore(s, existingNodeLocalities)
			candidates = append(candidates, candidate{
				store:           s,
				valid:           true,
				constraintScore: diversityScore + float64(storeInfo.matched),
				convergesScore:  convergesScore,
				balanceScore:    balanceScore,
				rangeCount:      int(s.Capacity.RangeCount),
				details:         fmt.Sprintf("diversity=%.2f, preferred=%d", diversityScore, storeInfo.matched),
			})
		}
	}

	if options.deterministic {
		sort.Sort(sort.Reverse(byScoreAndID(existingCandidates)))
		sort.Sort(sort.Reverse(byScoreAndID(candidates)))
	} else {
		sort.Sort(sort.Reverse(byScore(existingCandidates)))
		sort.Sort(sort.Reverse(byScore(candidates)))
	}

	return existingCandidates, candidates
}

// shouldRebalance returns whether the specified store is a candidate for
// having a replica removed from it given the candidate store list.
func shouldRebalance(
	ctx context.Context,
	store roachpb.StoreDescriptor,
	sl StoreList,
	rangeInfo RangeInfo,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
	options scorerOptions,
) bool {
	// Rebalance if this store is too full.
	if store.Capacity.FractionUsed() >= maxFractionUsedThreshold {
		log.VEventf(ctx, 2, "s%d: should-rebalance(disk-full): fraction-used=%.2f, capacity=(%v)",
			store.StoreID, store.Capacity.FractionUsed(), store.Capacity)
		return true
	}

	diversityScore := rangeDiversityScore(existingNodeLocalities)
	for _, desc := range sl.stores {
		if !preexistingReplicaCheck(desc.Node.NodeID, rangeInfo.Desc.Replicas) {
			continue
		}
		otherScore := diversityRebalanceFromScore(desc, store.Node.NodeID, existingNodeLocalities)
		if otherScore > diversityScore {
			log.VEventf(ctx, 2,
				"s%d: should-rebalance(better-diversity=s%d): diversityScore=%.5f, otherScore=%.5f",
				store.StoreID, desc.StoreID, diversityScore, otherScore)
			return true
		}
	}

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
			if !preexistingReplicaCheck(desc.Node.NodeID, rangeInfo.Desc.Replicas) {
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

// preexistingReplicaCheck returns true if no existing replica is present on
// the candidate's node.
func preexistingReplicaCheck(nodeID roachpb.NodeID, existing []roachpb.ReplicaDescriptor) bool {
	for _, r := range existing {
		if r.NodeID == nodeID {
			return false
		}
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

// constraintCheck returns true iff all required and prohibited constraints are
// satisfied. Stores with attributes or localities that match the most positive
// constraints return higher scores.
func constraintCheck(store roachpb.StoreDescriptor, constraints config.Constraints) (bool, int) {
	if len(constraints.Constraints) == 0 {
		return true, 0
	}
	positive := 0
	for _, constraint := range constraints.Constraints {
		hasConstraint := storeHasConstraint(store, constraint)
		switch {
		case constraint.Type == config.Constraint_REQUIRED && !hasConstraint:
			return false, 0
		case constraint.Type == config.Constraint_PROHIBITED && hasConstraint:
			return false, 0
		case (constraint.Type == config.Constraint_POSITIVE && hasConstraint):
			positive++
		}
	}
	return true, positive
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
