// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package faker

import (
	"sort"
	"sync"

	"golang.org/x/exp/rand"
)

// This is a rough go port of https://github.com/joke2k/faker.

var fakerOnce sync.Once
var faker Faker

// GetFaker lazily initializes a singleton Faker and returns it.
func GetFaker() Faker {
	fakerOnce.Do(func() {
		faker = newFaker()
	})
	return faker
}

// Faker returns a generator for random data of various types: names, addresses,
// "lorem ipsum" placeholder text. Faker is safe for concurrent use.
type Faker struct {
	addressFaker
	loremFaker
	nameFaker
}

func newFaker() Faker {
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
	sort.Slice(we, func(i, j int) bool { return we[i].weight < we[j].weight })
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
