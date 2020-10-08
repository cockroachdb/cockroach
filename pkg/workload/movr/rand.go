// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package movr

import (
	"encoding/json"

	"golang.org/x/exp/rand"
)

const numerals = `1234567890`

var vehicleTypes = [...]string{`skateboard`, `bike`, `scooter`}
var vehicleColors = [...]string{`red`, `yellow`, `blue`, `green`, `black`}
var bikeBrands = [...]string{
	`Merida`, `Fuji`, `Cervelo`, `Pinarello`, `Santa Cruz`, `Kona`, `Schwinn`}

func randString(rng *rand.Rand, length int, alphabet string) string {
	buf := make([]byte, length)
	for i := range buf {
		buf[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(buf)
}

func randCreditCard(rng *rand.Rand) string {
	return randString(rng, 10, numerals)
}

func randVehicleType(rng *rand.Rand) string {
	return vehicleTypes[rng.Intn(len(vehicleTypes))]
}

func randVehicleStatus(rng *rand.Rand) string {
	r := rng.Intn(100)
	switch {
	case r < 40:
		return `available`
	case r < 95:
		return `in_use`
	default:
		return `lost`
	}
}

func randLatLong(rng *rand.Rand) (float64, float64) {
	lat, long := float64(-180+rng.Intn(360)), float64(-90+rng.Intn(180))
	return lat, long
}

func randCity(rng *rand.Rand) string {
	idx := rng.Int31n(int32(len(cities)))
	return cities[idx].city
}

func randVehicleMetadata(rng *rand.Rand, vehicleType string) string {
	m := map[string]string{
		`color`: vehicleColors[rng.Intn(len(vehicleColors))],
	}
	switch vehicleType {
	case `bike`:
		m[`brand`] = bikeBrands[rng.Intn(len(bikeBrands))]
	}
	j, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(j)
}
