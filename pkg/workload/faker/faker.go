// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package faker

import "golang.org/x/exp/rand"

// This is a rough go port of https://github.com/joke2k/faker.

// Faker returns a generator for random data of various types: names, addresses,
// "lorem ipsum" placeholder text. Faker is safe for concurrent use.
type Faker struct {
	addressFaker
	loremFaker
	nameFaker
}

// NewFaker returns a new Faker instance. This causes a good amount of
// allocations, so it's best to reuse these when possible.
func NewFaker() Faker {
	f := Faker{
		loremFaker: newLoremFaker(),
		nameFaker:  newNameFaker(),
	}
	f.addressFaker = newAddressFaker(f.nameFaker)
	return f
}

type weightedEntry struct {
	weight float64
	entry  interface{}
}

type weightedEntries struct {
	entries     []weightedEntry
	totalWeight float64
}

func makeWeightedEntries(entriesAndWeights ...interface{}) *weightedEntries {
	we := make([]weightedEntry, 0, len(entriesAndWeights)/2)
	var totalWeight float64
	for idx := 0; idx < len(entriesAndWeights); idx += 2 {
		e, w := entriesAndWeights[idx], entriesAndWeights[idx+1].(float64)
		we = append(we, weightedEntry{weight: w, entry: e})
		totalWeight += w
	}
	return &weightedEntries{entries: we, totalWeight: totalWeight}
}

func (e *weightedEntries) Rand(rng *rand.Rand) interface{} {
	rngWeight := rng.Float64() * e.totalWeight
	var w float64
	for i := range e.entries {
		w += e.entries[i].weight
		if w > rngWeight {
			return e.entries[i].entry
		}
	}
	panic(`unreachable`)
}

func randInt(rng *rand.Rand, minInclusive, maxInclusive int) int {
	return rng.Intn(maxInclusive-minInclusive+1) + minInclusive
}
