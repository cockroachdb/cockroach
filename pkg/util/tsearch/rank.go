// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
type rankBehavior int

const (
	RANK_NO_NORM         rankBehavior = 0x0
	RANK_NORM_LOGLENGTH               = 0x01
	RANK_NORM_LENGTH                  = 0x02
	RANK_NORM_EXTDIST                 = 0x04
	RANK_NORM_UNIQ                    = 0x08
	RANK_NORM_LOGUNIQ                 = 0x10
	RANK_NORM_RDIVRPLUS1              = 0x20
	DEF_NORM_METHOD                   = RANK_NO_NORM
)

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
// defining different ranking behaviors, defined in the rankBehavior type. See
// the Postgres documentation for more details.
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
		res = 1e-20
	}
	if method&RANK_NORM_LOGLENGTH > 0 {
		res /= float32(math.Log(float64(cntLen(v)+1)) / math.Log(2.0))
	}

	if method&RANK_NORM_LENGTH > 0 {
		l := cntLen(v)
		if l > 0 {
			res /= float32(l)
		}
	}
	// RANK_NORM_EXTDIST not applicable

	if method&RANK_NORM_UNIQ > 0 {
		res /= float32(len(v))
	}

	if method&RANK_NORM_LOGUNIQ > 0 {
		res /= float32(math.Log(float64(len(v)+1)) / math.Log(2.0))
	}

	if method&RANK_NORM_RDIVRPLUS1 > 0 {
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

func findRankMatches(
	query *tsNode, v TSVector, target string, matches [][]tsPosition,
) [][]tsPosition {
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

func rankOr(weights [4]float32, v TSVector, q TSQuery) float32 {
	queryLeaves := sortAndDistinctQueryTerms(q)
	var matches = make([][]tsPosition, 0)
	var res float32
	for i := range queryLeaves {
		matches = matches[:0]
		matches = findRankMatches(queryLeaves[i], v, queryLeaves[i].term.lexeme, matches)
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
		matches = findRankMatches(queryLeaves[i], v, queryLeaves[i].term.lexeme, matches)
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
