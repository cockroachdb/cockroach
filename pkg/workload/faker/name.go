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

import (
	"fmt"

	"golang.org/x/exp/rand"
)

type nameFaker struct {
	formatsFemale, formatsMale     *weightedEntries
	firstNameFemale, firstNameMale *weightedEntries
	lastName                       *weightedEntries
	prefixFemale, prefixMale       *weightedEntries
	suffixFemale, suffixMale       *weightedEntries
}

// Name returns a random en_US person name.
func (f *nameFaker) Name(rng *rand.Rand) string {
	if rng.Intn(2) == 0 {
		return f.formatsFemale.Rand(rng).(func(rng *rand.Rand) string)(rng)
	}
	return f.formatsMale.Rand(rng).(func(rng *rand.Rand) string)(rng)
}

func newNameFaker() nameFaker {
	f := nameFaker{}
	f.formatsFemale = makeWeightedEntries(
		func(rng *rand.Rand) string {
			return fmt.Sprintf(`%s %s`, f.firstNameFemale.Rand(rng), f.lastName.Rand(rng))
		}, 0.97,
		func(rng *rand.Rand) string {
			return fmt.Sprintf(`%s %s %s`, f.prefixFemale.Rand(rng), f.firstNameFemale.Rand(rng), f.lastName.Rand(rng))
		}, 0.015,
		func(rng *rand.Rand) string {
			return fmt.Sprintf(`%s %s %s`, f.firstNameFemale.Rand(rng), f.lastName.Rand(rng), f.suffixFemale.Rand(rng))
		}, 0.02,
		func(rng *rand.Rand) string {
			return fmt.Sprintf(`%s %s %s %s`, f.prefixFemale.Rand(rng), f.firstNameFemale.Rand(rng), f.lastName.Rand(rng), f.suffixFemale.Rand(rng))
		}, 0.005,
	)

	f.formatsMale = makeWeightedEntries(
		func(rng *rand.Rand) string {
			return fmt.Sprintf(`%s %s`, f.firstNameMale.Rand(rng), f.lastName.Rand(rng))
		}, 0.97,
		func(rng *rand.Rand) string {
			return fmt.Sprintf(`%s %s %s`, f.prefixMale.Rand(rng), f.firstNameMale.Rand(rng), f.lastName.Rand(rng))
		}, 0.015,
		func(rng *rand.Rand) string {
			return fmt.Sprintf(`%s %s %s`, f.firstNameMale.Rand(rng), f.lastName.Rand(rng), f.suffixMale.Rand(rng))
		}, 0.02,
		func(rng *rand.Rand) string {
			return fmt.Sprintf(`%s %s %s %s`, f.prefixMale.Rand(rng), f.firstNameMale.Rand(rng), f.lastName.Rand(rng), f.suffixMale.Rand(rng))
		}, 0.005,
	)

	f.firstNameFemale = firstNameFemale()
	f.firstNameMale = firstNameMale()
	f.lastName = lastName()
	f.prefixFemale = makeWeightedEntries(
		`Mrs.`, 0.5,
		`Ms.`, 0.1,
		`Miss`, 0.1,
		`Dr.`, 0.3,
	)
	f.prefixMale = makeWeightedEntries(
		`Mr.`, 0.7,
		`Dr.`, 0.3,
	)
	f.suffixFemale = makeWeightedEntries(
		`MD`, 0.5,
		`DDS`, 0.3,
		`PhD`, 0.1,
		`DVM`, 0.2,
	)
	f.suffixMale = makeWeightedEntries(
		`Jr.`, 0.2,
		`II`, 0.05,
		`III`, 0.03,
		`IV`, 0.015,
		`V`, 0.005,
		`MD`, 0.3,
		`DDS`, 0.2,
		`PhD`, 0.1,
		`DVM`, 0.1,
	)
	return f
}
