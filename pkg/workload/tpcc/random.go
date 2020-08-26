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
	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
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

const precomputedLength = 10000
const aCharsAlphabet = `abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890`
const lettersAlphabet = `ABCDEFGHIJKLMNOPQRSTUVWXYZ`
const numbersAlphabet = `1234567890`

type tpccRand struct {
	*rand.Rand

	aChars, letters, numbers workloadimpl.PrecomputedRand
}

type aCharsOffset int
type lettersOffset int
type numbersOffset int

func randStringFromAlphabet(
	rng *rand.Rand,
	scratch []byte,
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

	b := scratch[:size]
	*prOffset = pr.FillBytes(*prOffset, b)
	return b
}

// randAStringInitialDataOnly generates a random alphanumeric string of length
// between min and max inclusive. It uses a set of pregenerated random data,
// which the spec allows only for initial data. See 4.3.2.2.
//
// For speed, this is done using precomputed random data, which is explicitly
// allowed by the spec for initial data only. See 4.3.2.1.
func randAStringInitialDataOnly(
	rng *tpccRand, ao *aCharsOffset, scratch []byte, min, max int,
) []byte {
	return randStringFromAlphabet(rng.Rand, scratch, min, max, rng.aChars, (*int)(ao))
}

// randNStringInitialDataOnly generates a random numeric string of length
// between min and max inclusive. See 4.3.2.2.
//
// For speed, this is done using precomputed random data, which is explicitly
// allowed by the spec for initial data only. See 4.3.2.1.
func randNStringInitialDataOnly(
	rng *tpccRand, no *numbersOffset, scratch []byte, min, max int,
) []byte {
	return randStringFromAlphabet(rng.Rand, scratch, min, max, rng.numbers, (*int)(no))
}

// randStateInitialDataOnly produces a random US state. (spec just says 2
// letters)
//
// For speed, this is done using precomputed random data, which is explicitly
// allowed by the spec for initial data only. See 4.3.2.1.
func randStateInitialDataOnly(rng *tpccRand, lo *lettersOffset, scratch []byte) []byte {
	return randStringFromAlphabet(rng.Rand, scratch, 2, 2, rng.letters, (*int)(lo))
}

// randOriginalStringInitialDataOnly generates a random a-string[26..50] with
// 10% chance of containing the string "ORIGINAL" somewhere in the middle of the
// string. See 4.3.3.1.
//
// For speed, this is done using precomputed random data, which is explicitly
// allowed by the spec for initial data only. See 4.3.2.1.
func randOriginalStringInitialDataOnly(rng *tpccRand, ao *aCharsOffset, scratch []byte) []byte {
	if rng.Rand.Intn(9) == 0 {
		l := int(randInt(rng.Rand, 26, 50))
		off := int(randInt(rng.Rand, 0, l-8))
		buf := scratch[:l]
		_ = randAStringInitialDataOnly(rng, ao, buf[:off:off], off, off)
		copy(buf[off:off+8], originalString)
		_ = randAStringInitialDataOnly(rng, ao, buf[off+8:], l-off-8, l-off-8)
		return buf
	}
	return randAStringInitialDataOnly(rng, ao, scratch, 26, 50)
}

// randZip produces a random "zip code" - a 4-digit number plus the constant
// "11111". See 4.3.2.7.
//
// For speed, this is done using precomputed random data, which is explicitly
// allowed by the spec for initial data only. See 4.3.2.1.
func randZipInitialDataOnly(rng *tpccRand, no *numbersOffset, scratch []byte) []byte {
	buf := scratch[:9]
	_ = randNStringInitialDataOnly(rng, no, buf[:4:4], 4, 4)
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

const maxCLastLength = 3 * 5 // 3 entries from cLastTokens * max len of an entry

// randCLastSyllables returns a customer last name string generated according to
// the table in 4.3.2.3. Given a number between 0 and 999, each of the three
// syllables is determined by the corresponding digit in the three digit
// representation of the number. For example, the number 371 generates the name
// PRICALLYOUGHT, and the number 40 generates the name BARPRESBAR.
func randCLastSyllables(n int, scratch []byte) []byte {
	buf := scratch[:0]
	buf = append(buf, cLastTokens[n/100]...)
	n = n % 100
	buf = append(buf, cLastTokens[n/10]...)
	n = n % 10
	buf = append(buf, cLastTokens[n]...)
	return buf
}

// See 4.3.2.3.
func (w *tpcc) randCLast(rng *rand.Rand, scratch []byte) []byte {
	return randCLastSyllables(((rng.Intn(256)|rng.Intn(1000))+w.cLoad)%1000, scratch)
}

// Return a non-uniform random customer ID. See 2.1.6.
func (w *tpcc) randCustomerID(rng *rand.Rand) int {
	return ((rng.Intn(1024) | (rng.Intn(3000) + 1) + w.cCustomerID) % 3000) + 1
}

// Return a non-uniform random item ID. See 2.1.6.
func (w *tpcc) randItemID(rng *rand.Rand) int {
	return ((rng.Intn(8190) | (rng.Intn(100000) + 1) + w.cItemID) % 100000) + 1
}
