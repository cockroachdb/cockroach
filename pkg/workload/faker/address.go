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
	"strconv"

	"golang.org/x/exp/rand"
)

type addressFaker struct {
	streetAddress *weightedEntries
	streetSuffix  *weightedEntries

	name nameFaker
}

// StreetAddress returns a random en_US street address.
func (f *addressFaker) StreetAddress(rng *rand.Rand) string {
	return f.streetAddress.Rand(rng).(func(rng *rand.Rand) string)(rng)
}

func (f *addressFaker) buildingNumber(rng *rand.Rand) string {
	return strconv.Itoa(randInt(rng, 1000, 99999))
}

func (f *addressFaker) streetName(rng *rand.Rand) string {
	return fmt.Sprintf(`%s %s`, f.firstOrLastName(rng), f.streetSuffix.Rand(rng))
}

func (f *addressFaker) firstOrLastName(rng *rand.Rand) string {
	switch rng.Intn(3) {
	case 0:
		return f.name.firstNameFemale.Rand(rng).(string)
	case 1:
		return f.name.firstNameMale.Rand(rng).(string)
	case 2:
		return f.name.lastName.Rand(rng).(string)
	}
	panic(`unreachable`)
}

func secondaryAddress(rng *rand.Rand) string {
	switch rng.Intn(2) {
	case 0:
		return fmt.Sprintf(`Apt. %d`, rng.Intn(100))
	case 1:
		return fmt.Sprintf(`Suite %d`, rng.Intn(100))
	}
	panic(`unreachable`)
}

func newAddressFaker(name nameFaker) addressFaker {
	f := addressFaker{name: name}
	f.streetSuffix = streetSuffix()
	f.streetAddress = makeWeightedEntries(
		func(rng *rand.Rand) string {
			return fmt.Sprintf(`%s %s`, f.buildingNumber(rng), f.streetName(rng))
		}, 0.5,
		func(rng *rand.Rand) string {
			return fmt.Sprintf(`%s %s %s`,
				f.buildingNumber(rng), f.streetName(rng), secondaryAddress(rng))
		}, 0.5,
	)
	return f
}
