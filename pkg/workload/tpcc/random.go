// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package tpcc

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
const numbers = "1234567890"
const aChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

var cLastTokens = [...]string{
	"BAR", "OUGHT", "ABLE", "PRI", "PRES",
	"ESE", "ANTI", "CALLY", "ATION", "EING"}

// cLoad is the value of C at load time. See 2.1.6.1.
// It's used for the non-uniform random generator.
var cLoad int

// cCustomerID is the value of C for the customer id generator. 2.1.6.
var cCustomerID int

// cCustomerID is the value of C for the item id generator. 2.1.6.
var cItemID int

func init() {
	rand.Seed(timeutil.Now().UnixNano())
	cLoad = rand.Intn(256)
	cItemID = rand.Intn(1024)
	cCustomerID = rand.Intn(8192)
}

func randStringFromAlphabet(rng *rand.Rand, minLen, maxLen int, alphabet string) string {
	size := maxLen
	if maxLen-minLen != 0 {
		size = randInt(rng, minLen, maxLen)
	}
	if size == 0 {
		return ""
	}

	b := make([]byte, size)
	for i := range b {
		b[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(b)
}

// randAString generates a random alphanumeric string of length between min and
// max inclusive. See 4.3.2.2.
func randAString(rng *rand.Rand, min, max int) string {
	return randStringFromAlphabet(rng, min, max, aChars)
}

// randOriginalString generates a random a-string[26..50] with 10% chance of
// containing the string "ORIGINAL" somewhere in the middle of the string.
// See 4.3.3.1.
func randOriginalString(rng *rand.Rand) string {
	if rng.Intn(9) == 0 {
		l := randInt(rng, 26, 50)
		off := randInt(rng, 0, l-8)
		return randAString(rng, off, off) + originalString + randAString(rng, l-off-8, l-off-8)
	}
	return randAString(rng, 26, 50)
}

// randNString generates a random numeric string of length between min anx max
// inclusive. See 4.3.2.2.
func randNString(rng *rand.Rand, min, max int) string {
	return randStringFromAlphabet(rng, min, max, numbers)
}

// randState produces a random US state. (spec just says 2 letters)
func randState(rng *rand.Rand) string {
	return randStringFromAlphabet(rng, 2, 2, letters)
}

// randZip produces a random "zip code" - a 4-digit number plus the constant
// "11111". See 4.3.2.7.
func randZip(rng *rand.Rand) string {
	return randNString(rng, 4, 4) + "11111"
}

// randTax produces a random tax between [0.0000..0.2000]
// See 2.1.5.
func randTax(rng *rand.Rand) float64 {
	return float64(randInt(rng, 0, 2000)) / float64(10000.0)
}

// randInt returns a number within [min, max] inclusive.
// See 2.1.4.
func randInt(rng *rand.Rand, min, max int) int {
	return rng.Intn(max-min+1) + min
}

// See 4.3.2.3.
func randCLastSyllables(n int) string {
	result := ""
	for i := 0; i < 3; i++ {
		result = cLastTokens[n%10] + result
		n /= 10
	}
	return result
}

// See 4.3.2.3.
func randCLast(rng *rand.Rand) string {
	return randCLastSyllables(((rng.Intn(256) | rng.Intn(1000)) + cLoad) % 1000)
}

// Return a non-uniform random customer ID. See 2.1.6.
func randCustomerID(rng *rand.Rand) int {
	return ((rng.Intn(1024) | (rng.Intn(3000) + 1) + cCustomerID) % 3000) + 1
}

// Return a non-uniform random item ID. See 2.1.6.
func randItemID(rng *rand.Rand) int {
	return ((rng.Intn(8190) | (rng.Intn(100000) + 1) + cItemID) % 100000) + 1
}
