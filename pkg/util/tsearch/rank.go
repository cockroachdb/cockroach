// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tsearch

import (
	"math"
	"sort"
	"strings"
)

// defaultWeights is the default list of weights corresponding to the tsvector
// lexeme weights D, C, B, and A.
var defaultWeights = [4]float32{0.1, 0.2, 0.4, 1.0}

// Bitmask for the normalization integer. These define different ranking
// behaviors. They're defined in Postgres in tsrank.c.
// 0, the default, ignores the document length.
// 1 devides the rank by 1 + the logarithm of the document length.
// 2 divides the rank by the document length.
// 4 divides the rank by the mean harmonic distance between extents.
//
//	NOTE: This is only implemented by ts_rank_cd, which is currently not
//	implemented by CockroachDB. This constant is left for consistency with
//	the original PostgreSQL source code.
//
// 8 divides the rank by the number of unique words in document.
// 16 divides the rank by 1 + the logarithm of the number of unique words in document.
// 32 divides the rank by itself + 1.
type rankBehavior int

const (
	// rankNoNorm is the default. It ignores the document length.
	rankNoNorm rankBehavior = 0x0
	// rankNormLoglength divides the rank by 1 + the logarithm of the document length.
	rankNormLoglength = 0x01
	// rankNormLength divides the rank by the document length.
	rankNormLength = 0x02
	// rankNormExtdist divides the rank by the mean harmonic distance between extents.
	// Note, this is only implemented by ts_rank_cd, which is not currently implemented
	// by CockroachDB. The constant is kept for consistency with Postgres.
	rankNormExtdist = 0x04
	// rankNormUniq divides the rank by the number of unique words in document.
	rankNormUniq = 0x08
	// rankNormLoguniq divides the rank by 1 + the logarithm of the number of unique words in document.
	rankNormLoguniq = 0x10
	// rankNormRdivrplus1 divides the rank by itself + 1.
	rankNormRdivrplus1 = 0x20
)

// Defeat the unused linter.
var _ = rankNoNorm
var _ = rankNormExtdist

// cntLen returns the count of represented lexemes in a tsvector, including
// the number of repeated lexemes in the vector.
func cntLen(v TSVector) int {
	var ret int
	for i := range v {
		posLen := len(v[i].positions)
		if posLen > 0 {
			ret += posLen
		} else {
			ret += 1
		}
	}
	return ret
}

// Rank implements the ts_rank functionality, which ranks a tsvector against a
// tsquery. The weights parameter is a list of weights corresponding to the
// tsvector lexeme weights D, C, B, and A. The method parameter is a bitmask
// defining different ranking behaviors, defined in the rankBehavior type
// above in this file. The default ranking behavior is 0, which doesn't perform
// any normalization based on the document length.
//
// N.B.: this function is directly translated from the calc_rank function in
// tsrank.c, which contains almost no comments. As of this time, I am unable
// to sufficiently explain how this ranker works, but I'm confident that the
// implementation is at least compatible with Postgres.
// https://github.com/postgres/postgres/blob/765f5df726918bcdcfd16bcc5418e48663d1dd59/src/backend/utils/adt/tsrank.c#L357
func Rank(weights []float32, v TSVector, q TSQuery, method int) (float32, error) {
	w := defaultWeights
	if weights != nil {
		copy(w[:4], weights[:4])
	}
	if len(v) == 0 || q.root == nil {
		return 0, nil
	}
	var res float32
	if q.root.op == and || q.root.op == followedby {
		res = rankAnd(w, v, q)
	} else {
		res = rankOr(w, v, q)
	}
	if res < 0 {
		// This constant is taken from the Postgres source code, unfortunately I
		// don't understand its meaning.
		res = 1e-20
	}
	if method&rankNormLoglength > 0 {
		res /= float32(math.Log(float64(cntLen(v)+1)) / math.Log(2.0))
	}

	if method&rankNormLength > 0 {
		l := cntLen(v)
		if l > 0 {
			res /= float32(l)
		}
	}
	// rankNormExtDist is not applicable - it's only used for ts_rank_cd.

	if method&rankNormUniq > 0 {
		res /= float32(len(v))
	}

	if method&rankNormLoguniq > 0 {
		res /= float32(math.Log(float64(len(v)+1)) / math.Log(2.0))
	}

	if method&rankNormRdivrplus1 > 0 {
		res /= res + 1
	}

	return res, nil
}

func sortAndDistinctQueryTerms(q TSQuery) []*tsNode {
	// Extract all leaf nodes from the query tree.
	leafNodes := make([]*tsNode, 0)
	var extractTerms func(q *tsNode)
	extractTerms = func(q *tsNode) {
		if q == nil {
			return
		}
		if q.op != invalid {
			extractTerms(q.l)
			extractTerms(q.r)
		} else {
			leafNodes = append(leafNodes, q)
		}
	}
	extractTerms(q.root)
	// Sort the terms.
	sort.Slice(leafNodes, func(i, j int) bool {
		return leafNodes[i].term.lexeme < leafNodes[j].term.lexeme
	})
	// Then distinct: (wouldn't it be nice if Go had generics?)
	lastUniqueIdx := 0
	for j := 1; j < len(leafNodes); j++ {
		if leafNodes[j].term.lexeme != leafNodes[lastUniqueIdx].term.lexeme {
			// We found a unique entry, at index i. The last unique entry in the array
			// was at lastUniqueIdx, so set the entry after that one to our new unique
			// entry, and bump lastUniqueIdx for the next loop iteration.
			lastUniqueIdx++
			leafNodes[lastUniqueIdx] = leafNodes[j]
		}
	}
	leafNodes = leafNodes[:lastUniqueIdx+1]
	return leafNodes
}

// findRankMatches finds all matches for a given query term in a tsvector,
// regardless of the expected query weight.
// query is the term being matched. v is the tsvector being searched.
// matches is a slice of matches to append to, to save on allocations as this
// function is called in a loop.
func findRankMatches(query *tsNode, v TSVector, matches [][]tsPosition) [][]tsPosition {
	target := query.term.lexeme
	i := sort.Search(len(v), func(i int) bool {
		return v[i].lexeme >= target
	})
	if i >= len(v) {
		return matches
	}
	if query.term.isPrefixMatch() {
		for j := i; j < len(v); j++ {
			t := v[j]
			if !strings.HasPrefix(t.lexeme, target) {
				break
			}
			matches = append(matches, t.positions)
		}
	} else if v[i].lexeme == target {
		matches = append(matches, v[i].positions)
	}
	return matches
}

// rankOr computes the rank for a query with an OR operator at its root.
// It takes the same parameters as TSRank.
func rankOr(weights [4]float32, v TSVector, q TSQuery) float32 {
	queryLeaves := sortAndDistinctQueryTerms(q)
	var matches = make([][]tsPosition, 0)
	var res float32
	for i := range queryLeaves {
		matches = matches[:0]
		matches = findRankMatches(queryLeaves[i], v, matches)
		if len(matches) == 0 {
			continue
		}
		resj := float32(0.0)
		wjm := float32(-1.0)
		jm := 0
		for _, innerMatches := range matches {
			for j, pos := range innerMatches {
				termWeight := pos.weight.val()
				weight := weights[termWeight]
				resj = resj + weight/float32((j+1)*(j+1))
				if weight > wjm {
					wjm = weight
					jm = j
				}
			}
		}
		// Explanation from Postgres tsrank.c:
		// limit (sum(1/i^2),i=1,inf) = pi^2/6
		// resj = sum(wi/i^2),i=1,noccurence,
		// wi - should be sorted desc,
		// don't sort for now, just choose maximum weight. This should be corrected
		// Oleg Bartunov
		res = res + (wjm+resj-wjm/float32((jm+1)*(jm+1)))/1.64493406685
	}
	if len(queryLeaves) > 0 {
		res /= float32(len(queryLeaves))
	}
	return res
}

// rankAnd computes the rank for a query with an AND or followed-by operator at
// its root. It takes the same parameters as TSRank.
func rankAnd(weights [4]float32, v TSVector, q TSQuery) float32 {
	queryLeaves := sortAndDistinctQueryTerms(q)
	if len(queryLeaves) < 2 {
		return rankOr(weights, v, q)
	}
	pos := make([][]tsPosition, len(queryLeaves))
	res := float32(-1)
	var matches = make([][]tsPosition, 0)
	for i := range queryLeaves {
		matches = matches[:0]
		matches = findRankMatches(queryLeaves[i], v, matches)
		for _, innerMatches := range matches {
			pos[i] = innerMatches
			// Loop back through the earlier position matches
			for k := 0; k < i; k++ {
				if pos[k] == nil {
					continue
				}
				for l := range pos[i] {
					// For each of the earlier matches
					for p := range pos[k] {
						dist := int(pos[i][l].position) - int(pos[k][p].position)
						if dist < 0 {
							dist = -dist
						}
						if dist != 0 {
							curw := float32(math.Sqrt(float64(weights[pos[i][l].weight.val()] * weights[pos[k][p].weight.val()] * wordDistance(dist))))
							if res < 0 {
								res = curw
							} else {
								res = 1.0 - (1.0-res)*(1.0-curw)
							}
						}
					}
				}
			}
		}
	}
	return res
}

// Returns a weight of a word collocation. See Postgres tsrank.c.
func wordDistance(dist int) float32 {
	if dist > 100 {
		return 1e-30
	}
	return float32(1.0 / (1.005 + 0.05*math.Exp(float64(float32(dist)/1.5-2))))
}
