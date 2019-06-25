// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"golang.org/x/exp/rand"
)

var cLastTokens = [...]string{
	"BAR", "OUGHT", "ABLE", "PRI", "PRES",
	"ESE", "ANTI", "CALLY", "ATION", "EING"}

func (w *tpcc) initNonUniformRandomConstants() {
	rng := rand.New(rand.NewSource(w.seed))
	w.cLoad = rng.Intn(256)
	w.cItemID = rng.Intn(1024)
	w.cCustomerID = rng.Intn(8192)
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
		var buf []byte
		*a, buf = a.Alloc(l, 0 /* extraCap */)
		copy(buf[:off], randAString(rng, a, off, off))
		copy(buf[off:off+8], originalString)
		copy(buf[off+8:], randAString(rng, a, l-off-8, l-off-8))
		return buf
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
	var buf []byte
	*a, buf = a.Alloc(9, 0 /* extraCap */)
	copy(buf[:4], randNString(rng, a, 4, 4))
	copy(buf[4:], `11111`)
	return buf
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

// randCLastSyllables returns a customer last name string generated according to
// the table in 4.3.2.3. Given a number between 0 and 999, each of the three
// syllables is determined by the corresponding digit in the three digit
// representation of the number. For example, the number 371 generates the name
// PRICALLYOUGHT, and the number 40 generates the name BARPRESBAR.
func randCLastSyllables(n int, a *bufalloc.ByteAllocator) []byte {
	const scratchLen = 3 * 5 // 3 entries from cLastTokens * max len of an entry
	var buf []byte
	*a, buf = a.Alloc(scratchLen, 0 /* extraCap */)
	buf = buf[:0]
	buf = append(buf, cLastTokens[n/100]...)
	n = n % 100
	buf = append(buf, cLastTokens[n/10]...)
	n = n % 10
	buf = append(buf, cLastTokens[n]...)
	return buf
}

// See 4.3.2.3.
func (w *tpcc) randCLast(rng *rand.Rand, a *bufalloc.ByteAllocator) []byte {
	return randCLastSyllables(((rng.Intn(256)|rng.Intn(1000))+w.cLoad)%1000, a)
}

// Return a non-uniform random customer ID. See 2.1.6.
func (w *tpcc) randCustomerID(rng *rand.Rand) int {
	return ((rng.Intn(1024) | (rng.Intn(3000) + 1) + w.cCustomerID) % 3000) + 1
}

// Return a non-uniform random item ID. See 2.1.6.
func (w *tpcc) randItemID(rng *rand.Rand) int {
	return ((rng.Intn(8190) | (rng.Intn(100000) + 1) + w.cItemID) % 100000) + 1
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
