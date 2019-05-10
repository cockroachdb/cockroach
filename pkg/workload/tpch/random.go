// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpch

import (
	"bytes"
	"strconv"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/workload/faker"
	"golang.org/x/exp/rand"
)

const alphanumericLen64 = `abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890, `

// randInt returns a random value between x and y inclusively, with a mean of
// (x+y)/2. See 4.2.2.3.
func randInt(rng *rand.Rand, x, y int) int {
	return rng.Intn(y-x+1) + x
}

func randFloat(rng *rand.Rand, x, y, shift int) float32 {
	return float32(randInt(rng, x, y)) / float32(shift)
}

type textPool interface {
	// 4.2.2.10:
	// The term text string[min, max] represents a substring of a 300 MB string
	// populated according to the pseudo text grammar defined in Clause 4.2.2.14.
	// The length of the substring is a random number between min and max
	// inclusive. The substring offset is randomly chosen.
	//
	// randString implementations must be threadsafe.
	randString(rng *rand.Rand, minLen, maxLen int) []byte
}

type fakeTextPool struct {
	seed uint64
	once struct {
		sync.Once
		buf []byte
	}
}

// randString implements textPool with a cheaper simulation of the 300 MB
// string. It's not to spec both because it's shorter and also because it's not
// generated according to the pseudo text grammar.
func (p *fakeTextPool) randString(rng *rand.Rand, minLen, maxLen int) []byte {
	const fakeTextPoolSize = 1 << 20 // 1 MiB
	p.once.Do(func() {
		bufRng := rand.New(rand.NewSource(p.seed))
		f := faker.NewFaker()
		// This loop generates random paragraphs and adds them until the length is
		// >= fakeTextPoolSize. Add some extra capacity so that we don't allocate
		// and copy on the paragraph that goes over.
		buf := bytes.NewBuffer(make([]byte, 0, fakeTextPoolSize+1024))
		for buf.Len() < fakeTextPoolSize {
			buf.WriteString(f.Paragraph(bufRng))
			buf.WriteString(` `)
		}
		p.once.buf = buf.Bytes()[:fakeTextPoolSize:fakeTextPoolSize]
	})
	start := rng.Intn(len(p.once.buf) - maxLen)
	end := start + rng.Intn(maxLen-minLen) + minLen
	return p.once.buf[start:end]
}

// randVString returns "a string comprised of randomly generated alphanumeric
// characters within a character set of at least 64 symbols. The length of the
// string is a random value between min and max inclusive". See 4.2.2.7.
func randVString(rng *rand.Rand, a *bufalloc.ByteAllocator, minLen, maxLen int) []byte {
	var buf []byte
	*a, buf = a.Alloc(randInt(rng, minLen, maxLen), 0)
	for i := range buf {
		buf[i] = alphanumericLen64[rng.Intn(len(alphanumericLen64))]
	}
	return buf
}

// randPhone returns a phone number generated according to 4.2.2.9.
func randPhone(rng *rand.Rand, a *bufalloc.ByteAllocator, nationKey int16) []byte {
	var buf []byte
	*a, buf = a.Alloc(15, 0)
	buf = buf[:0]

	countryCode := nationKey + 10
	localNumber1 := randInt(rng, 100, 999)
	localNumber2 := randInt(rng, 100, 999)
	localNumber3 := randInt(rng, 1000, 9999)
	buf = strconv.AppendInt(buf, int64(countryCode), 10)
	buf = append(buf, '-')
	buf = strconv.AppendInt(buf, int64(localNumber1), 10)
	buf = append(buf, '-')
	buf = strconv.AppendInt(buf, int64(localNumber2), 10)
	buf = append(buf, '-')
	buf = strconv.AppendInt(buf, int64(localNumber3), 10)
	return buf
}

var randPartNames = [...]string{
	"almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
	"blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
	"cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick",
	"floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
	"hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
	"magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
	"navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder",
	"puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell",
	"sienna", "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato",
	"turquoise", "violet", "wheat", "white", "yellow",
}

const maxPartNameLen = 10
const nPartNames = 5

// randPartName concatenates 5 random unique strings from randPartNames, separated
// by spaces.
func randPartName(rng *rand.Rand, namePerm []int, a *bufalloc.ByteAllocator) []byte {
	// do nPartNames iterations of rand.Perm, to get a random 5-subset of the
	// indexes into randPartNames.
	for i := 0; i < nPartNames; i++ {
		j := rng.Intn(i + 1)
		namePerm[i] = namePerm[j]
		namePerm[j] = i
	}
	var buf []byte
	*a, buf = a.Alloc(maxPartNameLen*nPartNames+nPartNames, 0)
	buf = buf[:0]
	for i := 0; i < nPartNames; i++ {
		if i != 0 {
			buf = append(buf, byte(' '))
		}
		buf = append(buf, randPartNames[namePerm[i]]...)
	}
	return buf
}

const manufacturerString = "Manufacturer#"

func randMfgr(rng *rand.Rand, a *bufalloc.ByteAllocator) (byte, []byte) {
	var buf []byte
	*a, buf = a.Alloc(len(manufacturerString)+1, 0)

	copy(buf, manufacturerString)
	m := byte(rng.Intn(5) + '1')
	buf[len(buf)-1] = m
	return m, buf
}

const brandString = "Brand#"

func randBrand(rng *rand.Rand, a *bufalloc.ByteAllocator, m byte) []byte {
	var buf []byte
	*a, buf = a.Alloc(len(brandString)+2, 0)

	copy(buf, brandString)
	n := byte(rng.Intn(5) + '1')
	buf[len(buf)-2] = m
	buf[len(buf)-1] = n
	return buf
}

const clerkString = "Clerk#"

func randClerk(rng *rand.Rand, a *bufalloc.ByteAllocator, scaleFactor int) []byte {
	var buf []byte
	*a, buf = a.Alloc(len(clerkString)+9, 0)
	copy(buf, clerkString)
	ninePaddedInt(buf[len(clerkString):], int64(randInt(rng, 1, scaleFactor*1000)))
	return buf
}

const supplierString = "Supplier#"

func supplierName(a *bufalloc.ByteAllocator, suppKey int64) []byte {
	var buf []byte
	*a, buf = a.Alloc(len(supplierString)+9, 0)
	copy(buf, supplierString)
	ninePaddedInt(buf[len(supplierString):], suppKey)
	return buf
}

const customerString = "Customer#"

func customerName(a *bufalloc.ByteAllocator, custKey int64) []byte {
	var buf []byte
	*a, buf = a.Alloc(len(customerString)+9, 0)
	copy(buf, customerString)
	ninePaddedInt(buf[len(customerString):], custKey)
	return buf
}

const ninePadding = `000000000`

func ninePaddedInt(buf []byte, x int64) {
	buf = buf[:len(ninePadding)]
	intLen := len(strconv.AppendInt(buf[:0], x, 10))
	numZeros := len(ninePadding) - intLen
	copy(buf[numZeros:], buf[:intLen])
	copy(buf[:numZeros], ninePadding[:numZeros])
}

func randSyllables(
	rng *rand.Rand, a *bufalloc.ByteAllocator, maxLen int, syllables [][]string,
) []byte {
	var buf []byte
	*a, buf = a.Alloc(maxLen, 0)
	buf = buf[:0]

	for i, syl := range syllables {
		if i != 0 {
			buf = append(buf, ' ')
			buf = append(buf, syl[rng.Intn(len(syl))]...)
		}
	}
	return buf
}

var typeSyllables = [][]string{
	{"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"},
	{"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"},
	{"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"},
}

const maxTypeLen = 25

func randType(rng *rand.Rand, a *bufalloc.ByteAllocator) []byte {
	return randSyllables(rng, a, maxTypeLen, typeSyllables)
}

var containerSyllables = [][]string{
	{"SM", "MED", "JUMBO", "WRAP"},
	{"BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"},
}

const maxContainerLen = 10

func randContainer(rng *rand.Rand, a *bufalloc.ByteAllocator) []byte {
	return randSyllables(rng, a, maxContainerLen, containerSyllables)
}

var segments = []string{
	"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD",
}

func randSegment(rng *rand.Rand) []byte {
	return encoding.UnsafeConvertStringToBytes(segments[rng.Intn(len(segments))])
}

var priorities = []string{
	"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED",
}

func randPriority(rng *rand.Rand) []byte {
	return encoding.UnsafeConvertStringToBytes(priorities[rng.Intn(len(priorities))])
}

var instructions = []string{
	"DELIVER IN PERSON",
	"COLLECT COD", "NONE",
	"TAKE BACK RETURN",
}

func randInstruction(rng *rand.Rand) []byte {
	return encoding.UnsafeConvertStringToBytes(instructions[rng.Intn(len(instructions))])
}

var modes = []string{
	"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB",
}

func randMode(rng *rand.Rand) []byte {
	return []byte(modes[rng.Intn(len(modes))])
}
