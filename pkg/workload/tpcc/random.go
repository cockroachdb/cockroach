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
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/exp/rand"
)

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
	rand.Seed(uint64(timeutil.Now().UnixNano()))
	cLoad = rand.Intn(256)
	cItemID = rand.Intn(1024)
	cCustomerID = rand.Intn(8192)
}

func randStringFromAlphabet(
	rng *rand.Rand,
	a *bufalloc.ByteAllocator,
	minLen, maxLen int,
	randStringFn func(rand.Source, []byte),
) []byte {
	size := maxLen
	if maxLen-minLen != 0 {
		size = int(randInt(rng, minLen, maxLen))
	}
	if size == 0 {
		return nil
	}

	var b []byte
	*a, b = a.Alloc(size, 0 /* extraCap */)
	// TODO(dan): According to the benchmark, it's faster to pass a
	// *rand.PCGSource here than it is to pass a *rand.Rand. I tried doing the
	// plumbing and didn't see a difference in BenchmarkInitTPCC, but I'm not
	// convinced that I didn't mess something up.
	//
	// name                      old time/op    new time/op    delta
	// RandStringFast/letters-8    86.2ns ± 2%    74.9ns ± 0%  -13.17%  (p=0.008 n=5+5)
	// RandStringFast/numbers-8    86.8ns ± 7%    74.2ns ± 1%  -14.50%  (p=0.008 n=5+5)
	// RandStringFast/aChars-8      101ns ± 2%      86ns ± 1%  -15.15%  (p=0.008 n=5+5)
	//
	// name                      old speed      new speed      delta
	// RandStringFast/letters-8   302MB/s ± 2%   347MB/s ± 0%  +15.08%  (p=0.008 n=5+5)
	// RandStringFast/numbers-8   300MB/s ± 7%   350MB/s ± 1%  +16.81%  (p=0.008 n=5+5)
	// RandStringFast/aChars-8    256MB/s ± 2%   303MB/s ± 1%  +18.42%  (p=0.008 n=5+5)
	randStringFn(rng, b)
	return b
}

// randAString generates a random alphanumeric string of length between min and
// max inclusive. See 4.3.2.2.
func randAString(rng *rand.Rand, a *bufalloc.ByteAllocator, min, max int) []byte {
	return randStringFromAlphabet(rng, a, min, max, randStringAChars)
}

// randOriginalString generates a random a-string[26..50] with 10% chance of
// containing the string "ORIGINAL" somewhere in the middle of the string.
// See 4.3.3.1.
func randOriginalString(rng *rand.Rand, a *bufalloc.ByteAllocator) []byte {
	if rng.Intn(9) == 0 {
		l := int(randInt(rng, 26, 50))
		off := int(randInt(rng, 0, l-8))
		return []byte(string(randAString(rng, a, off, off)) + originalString + string(randAString(rng, a, l-off-8, l-off-8)))
	}
	return randAString(rng, a, 26, 50)
}

// randNString generates a random numeric string of length between min and max
// inclusive. See 4.3.2.2.
func randNString(rng *rand.Rand, a *bufalloc.ByteAllocator, min, max int) []byte {
	return randStringFromAlphabet(rng, a, min, max, randStringNumbers)
}

// randState produces a random US state. (spec just says 2 letters)
func randState(rng *rand.Rand, a *bufalloc.ByteAllocator) []byte {
	return randStringFromAlphabet(rng, a, 2, 2, randStringLetters)
}

// randZip produces a random "zip code" - a 4-digit number plus the constant
// "11111". See 4.3.2.7.
func randZip(rng *rand.Rand, a *bufalloc.ByteAllocator) []byte {
	return []byte(string(randNString(rng, a, 4, 4)) + "11111")
}

// randTax produces a random tax between [0.0000..0.2000]
// See 2.1.5.
func randTax(rng *rand.Rand) float64 {
	return float64(randInt(rng, 0, 2000)) / float64(10000.0)
}

// randInt returns a number within [min, max] inclusive.
// See 2.1.4.
func randInt(rng *rand.Rand, min, max int) int64 {
	return int64(rng.Intn(max-min+1) + min)
}

// See 4.3.2.3.
func randCLastSyllables(n int) []byte {
	result := ""
	for i := 0; i < 3; i++ {
		result = cLastTokens[n%10] + result
		n /= 10
	}
	return []byte(result)
}

// See 4.3.2.3.
func randCLast(rng *rand.Rand) []byte {
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

// NOTE: The following are intentionally duplicated. They're a very hot path in
// restoring a TPCC fixture and hardcoding alphabet, len(alphabet), and
// charsPerRand seems to trigger some compiler optimizations that don't happen
// if those things are params. Don't modify these without consulting
// BenchmarkRandStringFast and BenchmarkInitTPCC.

func randStringLetters(rng rand.Source, buf []byte) {
	const letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	const lettersLen = uint64(len(letters))
	const lettersCharsPerRand = uint64(13) // floor(log(math.MaxUint64)/log(lettersLen))

	var r, charsLeft uint64
	for i := 0; i < len(buf); i++ {
		if charsLeft == 0 {
			r = rng.Uint64()
			charsLeft = lettersCharsPerRand
		}
		buf[i] = letters[r%lettersLen]
		r = r / lettersLen
		charsLeft--
	}
}

func randStringNumbers(rng rand.Source, buf []byte) {
	const numbers = "1234567890"
	const numbersLen = uint64(len(numbers))
	const numbersCharsPerRand = uint64(19) // floor(log(math.MaxUint64)/log(numbersLen))

	var r, charsLeft uint64
	for i := 0; i < len(buf); i++ {
		if charsLeft == 0 {
			r = rng.Uint64()
			charsLeft = numbersCharsPerRand
		}
		buf[i] = numbers[r%numbersLen]
		r = r / numbersLen
		charsLeft--
	}
}

func randStringAChars(rng rand.Source, buf []byte) {
	const aChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	const aCharsLen = uint64(len(aChars))
	const aCharsCharsPerRand = uint64(10) // floor(log(math.MaxUint64)/log(aCharsLen))

	var r, charsLeft uint64
	for i := 0; i < len(buf); i++ {
		if charsLeft == 0 {
			r = rng.Uint64()
			charsLeft = aCharsCharsPerRand
		}
		buf[i] = aChars[r%aCharsLen]
		r = r / aCharsLen
		charsLeft--
	}
}
