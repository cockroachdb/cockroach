// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
