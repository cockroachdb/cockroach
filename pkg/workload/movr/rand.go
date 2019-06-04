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

package movr

import (
	"encoding/json"
	"strings"

	"golang.org/x/exp/rand"
)

const alpha = `abcdefghijklmnopqrstuvwxyz`
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

func randWord(rng *rand.Rand) string {
	return randString(rng, 7, alpha)
}

func randParagraph(rng *rand.Rand) string {
	words := make([]string, rng.Intn(100))
	for i := range words {
		words[i] = randWord(rng)
	}
	return strings.Join(words, ` `)
}

func randName(rng *rand.Rand) string {
	return randString(rng, 7, alpha) + ` ` + randString(rng, 10, alpha)
}

func randAddress(rng *rand.Rand) string {
	return randString(rng, 20, alpha)
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
