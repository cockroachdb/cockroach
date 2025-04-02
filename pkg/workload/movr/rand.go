// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package movr

import (
	"encoding/json"
	"math/rand/v2"
)

const numerals = `1234567890`

var vehicleTypes = [...]string{`skateboard`, `bike`, `scooter`}
var vehicleColors = [...]string{`red`, `yellow`, `blue`, `green`, `black`}
var bikeBrands = [...]string{
	`Merida`, `Fuji`, `Cervelo`, `Pinarello`, `Santa Cruz`, `Kona`, `Schwinn`}

func randString(rng *rand.Rand, length int, alphabet string) string {
	buf := make([]byte, length)
	for i := range buf {
		buf[i] = alphabet[rng.IntN(len(alphabet))]
	}
	return string(buf)
}

func randCreditCard(rng *rand.Rand) string {
	return randString(rng, 10, numerals)
}

func randVehicleType(rng *rand.Rand) string {
	return vehicleTypes[rng.IntN(len(vehicleTypes))]
}

func randVehicleStatus(rng *rand.Rand) string {
	r := rng.IntN(100)
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
	lat, long := float64(-180+rng.IntN(360)), float64(-90+rng.IntN(180))
	return lat, long
}

func randCity(rng *rand.Rand) string {
	idx := rng.Int32N(int32(len(cities)))
	return cities[idx].city
}

func randVehicleMetadata(rng *rand.Rand, vehicleType string) string {
	m := map[string]string{
		`color`: vehicleColors[rng.IntN(len(vehicleColors))],
	}
	switch vehicleType {
	case `bike`:
		m[`brand`] = bikeBrands[rng.IntN(len(bikeBrands))]
	}
	j, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(j)
}
