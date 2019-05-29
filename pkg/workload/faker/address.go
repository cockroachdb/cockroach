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
		return fmt.Sprintf(`Apt. %d`, rand.Intn(100))
	case 1:
		return fmt.Sprintf(`Suite %d`, rand.Intn(100))
	}
	panic(`unreachable`)
}

func newAddressFaker(name nameFaker) addressFaker {
	f := addressFaker{name: name}
	f.streetSuffix = makeWeightedEntries(
		`Alley`, 1.0,
		`Avenue`, 1.0,
		`Branch`, 1.0,
		`Bridge`, 1.0,
		`Brook`, 1.0,
		`Brooks`, 1.0,
		`Burg`, 1.0,
		`Burgs`, 1.0,
		`Bypass`, 1.0,
		`Camp`, 1.0,
		`Canyon`, 1.0,
		`Cape`, 1.0,
		`Causeway`, 1.0,
		`Center`, 1.0,
		`Centers`, 1.0,
		`Circle`, 1.0,
		`Circles`, 1.0,
		`Cliff`, 1.0,
		`Cliffs`, 1.0,
		`Club`, 1.0,
		`Common`, 1.0,
		`Corner`, 1.0,
		`Corners`, 1.0,
		`Course`, 1.0,
		`Court`, 1.0,
		`Courts`, 1.0,
		`Cove`, 1.0,
		`Coves`, 1.0,
		`Creek`, 1.0,
		`Crescent`, 1.0,
		`Crest`, 1.0,
		`Crossing`, 1.0,
		`Crossroad`, 1.0,
		`Curve`, 1.0,
		`Dale`, 1.0,
		`Dam`, 1.0,
		`Divide`, 1.0,
		`Drive`, 1.0,
		`Drive`, 1.0,
		`Drives`, 1.0,
		`Estate`, 1.0,
		`Estates`, 1.0,
		`Expressway`, 1.0,
		`Extension`, 1.0,
		`Extensions`, 1.0,
		`Fall`, 1.0,
		`Falls`, 1.0,
		`Ferry`, 1.0,
		`Field`, 1.0,
		`Fields`, 1.0,
		`Flat`, 1.0,
		`Flats`, 1.0,
		`Ford`, 1.0,
		`Fords`, 1.0,
		`Forest`, 1.0,
		`Forge`, 1.0,
		`Forges`, 1.0,
		`Fork`, 1.0,
		`Forks`, 1.0,
		`Fort`, 1.0,
		`Freeway`, 1.0,
		`Garden`, 1.0,
		`Gardens`, 1.0,
		`Gateway`, 1.0,
		`Glen`, 1.0,
		`Glens`, 1.0,
		`Green`, 1.0,
		`Greens`, 1.0,
		`Grove`, 1.0,
		`Groves`, 1.0,
		`Harbor`, 1.0,
		`Harbors`, 1.0,
		`Haven`, 1.0,
		`Heights`, 1.0,
		`Highway`, 1.0,
		`Hill`, 1.0,
		`Hills`, 1.0,
		`Hollow`, 1.0,
		`Inlet`, 1.0,
		`Inlet`, 1.0,
		`Island`, 1.0,
		`Island`, 1.0,
		`Islands`, 1.0,
		`Islands`, 1.0,
		`Isle`, 1.0,
		`Isle`, 1.0,
		`Junction`, 1.0,
		`Junctions`, 1.0,
		`Key`, 1.0,
		`Keys`, 1.0,
		`Knoll`, 1.0,
		`Knolls`, 1.0,
		`Lake`, 1.0,
		`Lakes`, 1.0,
		`Land`, 1.0,
		`Landing`, 1.0,
		`Lane`, 1.0,
		`Light`, 1.0,
		`Lights`, 1.0,
		`Loaf`, 1.0,
		`Lock`, 1.0,
		`Locks`, 1.0,
		`Locks`, 1.0,
		`Lodge`, 1.0,
		`Lodge`, 1.0,
		`Loop`, 1.0,
		`Mall`, 1.0,
		`Manor`, 1.0,
		`Manors`, 1.0,
		`Meadow`, 1.0,
		`Meadows`, 1.0,
		`Mews`, 1.0,
		`Mill`, 1.0,
		`Mills`, 1.0,
		`Mission`, 1.0,
		`Mission`, 1.0,
		`Motorway`, 1.0,
		`Mount`, 1.0,
		`Mountain`, 1.0,
		`Mountain`, 1.0,
		`Mountains`, 1.0,
		`Mountains`, 1.0,
		`Neck`, 1.0,
		`Orchard`, 1.0,
		`Oval`, 1.0,
		`Overpass`, 1.0,
		`Park`, 1.0,
		`Parks`, 1.0,
		`Parkway`, 1.0,
		`Parkways`, 1.0,
		`Pass`, 1.0,
		`Passage`, 1.0,
		`Path`, 1.0,
		`Pike`, 1.0,
		`Pine`, 1.0,
		`Pines`, 1.0,
		`Place`, 1.0,
		`Plain`, 1.0,
		`Plains`, 1.0,
		`Plains`, 1.0,
		`Plaza`, 1.0,
		`Plaza`, 1.0,
		`Point`, 1.0,
		`Points`, 1.0,
		`Port`, 1.0,
		`Port`, 1.0,
		`Ports`, 1.0,
		`Ports`, 1.0,
		`Prairie`, 1.0,
		`Prairie`, 1.0,
		`Radial`, 1.0,
		`Ramp`, 1.0,
		`Ranch`, 1.0,
		`Rapid`, 1.0,
		`Rapids`, 1.0,
		`Rest`, 1.0,
		`Ridge`, 1.0,
		`Ridges`, 1.0,
		`River`, 1.0,
		`Road`, 1.0,
		`Road`, 1.0,
		`Roads`, 1.0,
		`Roads`, 1.0,
		`Route`, 1.0,
		`Row`, 1.0,
		`Rue`, 1.0,
		`Run`, 1.0,
		`Shoal`, 1.0,
		`Shoals`, 1.0,
		`Shore`, 1.0,
		`Shores`, 1.0,
		`Skyway`, 1.0,
		`Spring`, 1.0,
		`Springs`, 1.0,
		`Springs`, 1.0,
		`Spur`, 1.0,
		`Spurs`, 1.0,
		`Square`, 1.0,
		`Square`, 1.0,
		`Squares`, 1.0,
		`Squares`, 1.0,
		`Station`, 1.0,
		`Station`, 1.0,
		`Stravenue`, 1.0,
		`Stravenue`, 1.0,
		`Stream`, 1.0,
		`Stream`, 1.0,
		`Street`, 1.0,
		`Street`, 1.0,
		`Streets`, 1.0,
		`Summit`, 1.0,
		`Summit`, 1.0,
		`Terrace`, 1.0,
		`Throughway`, 1.0,
		`Trace`, 1.0,
		`Track`, 1.0,
		`Trafficway`, 1.0,
		`Trail`, 1.0,
		`Trail`, 1.0,
		`Tunnel`, 1.0,
		`Tunnel`, 1.0,
		`Turnpike`, 1.0,
		`Turnpike`, 1.0,
		`Underpass`, 1.0,
		`Union`, 1.0,
		`Unions`, 1.0,
		`Valley`, 1.0,
		`Valleys`, 1.0,
		`Via`, 1.0,
		`Viaduct`, 1.0,
		`View`, 1.0,
		`Views`, 1.0,
		`Village`, 1.0,
		`Village`, 1.0,
		`Villages`, 1.0,
		`Ville`, 1.0,
		`Vista`, 1.0,
		`Vista`, 1.0,
		`Walk`, 1.0,
		`Walks`, 1.0,
		`Wall`, 1.0,
		`Way`, 1.0,
		`Ways`, 1.0,
		`Well`, 1.0,
		`Wells`, 1.0,
	)
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
