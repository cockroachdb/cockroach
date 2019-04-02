// Copyright 2017 The Cockroach Authors.
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

package tpcc

import (
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
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

const precomputedLength = 10000
const aCharsAlphabet = `abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890`
const lettersAlphabet = `ABCDEFGHIJKLMNOPQRSTUVWXYZ`
const numbersAlphabet = `1234567890`

// Special case for the default tpcc seed of 0. Using the default seed means
// that at most one of each of these is ever made and initialized. Using
// non-default seeds will share them somewhat, but intentionally lets them get
// gc'd after use.
var aCharsInitSeed0 = workloadimpl.PrecomputedRandInit(
	rand.New(rand.NewSource(0)), precomputedLength, aCharsAlphabet)
var lettersInitSeed0 = workloadimpl.PrecomputedRandInit(
	rand.New(rand.NewSource(0)), precomputedLength, lettersAlphabet)
var numbersInitSeed0 = workloadimpl.PrecomputedRandInit(
	rand.New(rand.NewSource(0)), precomputedLength, numbersAlphabet)

type tpccRand struct {
	*rand.Rand

	aChars, letters, numbers                   workloadimpl.PrecomputedRand
	aCharsOffset, lettersOffset, numbersOffset int
}

func randStringFromAlphabet(
	rng *rand.Rand,
	a *bufalloc.ByteAllocator,
	minLen, maxLen int,
	pr workloadimpl.PrecomputedRand,
	prOffset *int,
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
	*prOffset = pr.FillBytes(*prOffset, b)
	return b
}

// randAString generates a random alphanumeric string of length between min and
// max inclusive. See 4.3.2.2.
func randAString(rng *tpccRand, a *bufalloc.ByteAllocator, min, max int) []byte {
	// For speed, this is done using precomputed random data, which is explictly
	// allowed by the spec for initial data only. See 4.3.2.1.
	return randStringFromAlphabet(rng.Rand, a, min, max, rng.aChars, &rng.aCharsOffset)
}

// randNString generates a random numeric string of length between min and max
// inclusive. See 4.3.2.2.
func randNString(rng *tpccRand, a *bufalloc.ByteAllocator, min, max int) []byte {
	// For speed, this is done using precomputed random data, which is explictly
	// allowed by the spec for initial data only. See 4.3.2.1.
	return randStringFromAlphabet(rng.Rand, a, min, max, rng.numbers, &rng.numbersOffset)
}

// randState produces a random US state. (spec just says 2 letters)
func randState(rng *tpccRand, a *bufalloc.ByteAllocator) []byte {
	// For speed, this is done using precomputed random data, which is explictly
	// allowed by the spec for initial data only. See 4.3.2.1.
	return randStringFromAlphabet(rng.Rand, a, 2, 2, rng.letters, &rng.lettersOffset)
}

// randOriginalString generates a random a-string[26..50] with 10% chance of
// containing the string "ORIGINAL" somewhere in the middle of the string.
// See 4.3.3.1.
func randOriginalString(rng *tpccRand, a *bufalloc.ByteAllocator) []byte {
	if rng.Rand.Intn(9) == 0 {
		l := int(randInt(rng.Rand, 26, 50))
		off := int(randInt(rng.Rand, 0, l-8))
		var buf []byte
		*a, buf = a.Alloc(l, 0 /* extraCap */)
		copy(buf[:off], randAString(rng, a, off, off))
		copy(buf[off:off+8], originalString)
		copy(buf[off+8:], randAString(rng, a, l-off-8, l-off-8))
		return buf
	}
	return randAString(rng, a, 26, 50)
}

// randZip produces a random "zip code" - a 4-digit number plus the constant
// "11111". See 4.3.2.7.
func randZip(rng *tpccRand, a *bufalloc.ByteAllocator) []byte {
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
func randCLast(rng *rand.Rand, a *bufalloc.ByteAllocator) []byte {
	return randCLastSyllables(((rng.Intn(256)|rng.Intn(1000))+cLoad)%1000, a)
}

// Return a non-uniform random customer ID. See 2.1.6.
func randCustomerID(rng *rand.Rand) int {
	return ((rng.Intn(1024) | (rng.Intn(3000) + 1) + cCustomerID) % 3000) + 1
}

// Return a non-uniform random item ID. See 2.1.6.
func randItemID(rng *rand.Rand) int {
	return ((rng.Intn(8190) | (rng.Intn(100000) + 1) + cItemID) % 100000) + 1
}
