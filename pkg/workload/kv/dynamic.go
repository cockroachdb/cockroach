// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// dynamicGenerator provides abstractions for generating keys with a distribution that
// may change as a function of time. Logically, dynamic maintains a mapping
// between slots, for which a static distribution such as zipfian or hash may
// generate keys for. Slots are like virtual keys, each slot holds a mapping to
// a physical key in the cockroach keyspace. The manipulation of this mapping
// based on triggers such as elapsed duration or cumulative keys written is
// what makes the distribution dynamic.
//
// The components of the dynamic key generator are listed below.
//
// (1) slots maintains the mapping between slots to keys. It has a simple api,
// requesting a key for a slot will return the key held within the slot. It
// also takes a function that may map over the slots either uniformly
// (virtually, not actually mapping over any slots) or partially (actually
// re-arranging the slots).
//
// (2) distribution is the underlying static distribution. It has one function,
// which is key(width), which must return a key in the range [0, width], with
// some probability distribution. It has no requirements that the key has been
// written or read previously.
//
// (3) dynGen controls when to remmap slots and holds (1) slots and a (2)
// distribution. It uses these two as well as the triggers and mapping function
// that is applied when the trigger predicate is true. It has one method,
// key(time), which is called with the current time and returns the next key.
//
// (4) dynamicGenerator implements the keyGenerator interface. It maintains two
// (3) dynGens, for reading and writing. This has the implication that the read
// generator and write generator are independent and may produce keys with
// different dynamic distributions. The interesting methods that it implements
// are readKey(), which returns a key for reading and writeKey(), which returns
// a key to be written.
//
// Logically, the sequence of calls from a readKey() or writeKey() on (4) is
// the same:
//
//  i) dynamicGenerator.(write|read)Key
//    ii) dynGen.key
//      iii) dynGen.shouldRemap
//         a) remap slots
//      iv) slot = distribution.key
//      v) key = slots.key(slot)
//  vi) return key
type dynamicGenerator struct {
	random           *rand.Rand
	read, write      dynamicDistribution
	now              time.Time
	testingBlockTime bool
}

func newDynamicGenerator(
	random *rand.Rand, read dynamicDistribution, write dynamicDistribution,
) *dynamicGenerator {
	return &dynamicGenerator{
		random: random,
		read:   read,
		write:  write,
		now:    timeutil.Now(),
	}
}

func (d *dynamicGenerator) writeKey() int64 {
	d.maybeUpdateNow()
	return d.write.key(d.now)
}

func (d *dynamicGenerator) readKey() int64 {
	d.maybeUpdateNow()
	return d.read.key(d.now)
}

func (d *dynamicGenerator) rand() *rand.Rand {
	return d.random
}

func (d *dynamicGenerator) sequence() int64 {
	return 0
}

func (d *dynamicGenerator) maybeUpdateNow() {
	if !d.testingBlockTime {
		d.now = timeutil.Now()
	}
}

// movingHotKeyGenerator returns a key generator that generates write keys in a
// sequential pattern. Read keys occur on the last k most recently written
// keys. An example is shown below, where the y axis is the key space in
// ascending order, the x axis is time in ascending order. The units displayed
// on the graph represent the QPS on the key k at time t.
//
//      k
//
//      |_ _ _ _ 5 3 1
//      |_ _ _ 5 3 1 _
// keys |_ _ 5 3 1 _ _
//      |_ 5 3 1 _ _ _
//      |5 3 1 _ _ _ _
//      +-------------- t
//            time
func movingHotKeyGenerator(seq *sequence, random *rand.Rand) *dynamicGenerator {
	if random == nil {
		random = rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	}

	cycleLength := seq.config.cycleLength
	readPercent := seq.config.readPercent
	readSkew := seq.config.readSkew

	fmt.Printf("read-percent %d, read-skew %d, cycle-length %d\n", readPercent, readSkew, cycleLength)

	writePercent := 100 - readPercent

	if writePercent == 0 {
		writePercent = 1
	}

	readWidth := int64(math.Ceil(float64(readPercent) / float64(writePercent)))
	writeWidth := int64(1)

	readMapTrigger := keyDeltaDecider(readWidth)
	writeMapTrigger := keyDeltaDecider(writeWidth)

	mapFn := func(s slots) {
		s.uniformMap(rotateFn(1))
	}

	// NB: In the moving hotkey, the write generator only writes one key at a
	// time, sequentially - therefore the underlying static distribution is
	// irrelevant. Pick sequential as it is the cheapest.
	readStaticDistribution := newDistribution(seq.config, random)
	writeStaticDistribution := &sequentialD{}

	readDistribution := newDynGen(readSkew*readWidth, cycleLength, readMapTrigger, mapFn, readStaticDistribution, true)
	writeDistribution := newDynGen(writeWidth, cycleLength, writeMapTrigger, mapFn, writeStaticDistribution, true)

	// We start writing at a key that is equal to the read width, offsetting
	// the writes to the right of the keyspace. Reads will occur in the range
	// [head - readWidth * readSkew, head].
	writeDistribution.slots.uniformMap(rotateFn(readWidth * readSkew))

	// reads should be rotated to have
	// slot 0 -> head, slot 1 -> head-1, ..., slot n -> head-n
	// currently
	// slot 0 -> head - n, slot 1 ->  head - n + 1, ..., slot n -> head
	//
	// width: same
	// map same
	//
	// initial map:
	// rank [5,4,3,2,1] -> [1,2,3,4,5]
	// slot [1,2,3,4,5] -> [1,2,3,4,5]
	//
	// key -> width - slot + offset
	// 5 -> 1 ... 6 -> 1
	// 4 -> 2 ... 5 -> 2
	// 3 -> 3 ... 4 -> 3
	// 2 -> 4 ... 3 -> 4
	// 1 -> 5 ... 2 -> 5
	// f(slot) = width - slot + offset

	return newDynamicGenerator(random, readDistribution, writeDistribution)
}

// periodicKeyGenerator returns a key generator that generates  keys in a
// periodic pattern. Moving between the periods at the interval given. An
// example is shown below, where the y axis is the key space in ascending
// order, the x axis is time in  ascending order. The units displayed on the
// graph represent the QPS on the key k at time t.
//
//      k
//
//      |_ _ _ _ 3 3 3
//      |_ _ _ _ 3 3 3
// keys |_ _ _ _ _ _ _
//      |3 3 3 _ _ _ _
//      |3 3 3 _ _ _ _
//      +------------ t
//            time
func periodicKeyGenerator(seq *sequence, random *rand.Rand) *dynamicGenerator {
	if random == nil {
		random = rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	}

	randoms := randomDupe(random, 2)
	writeRandom := randoms[0]
	readRandom := randoms[1]

	interval, err := time.ParseDuration(seq.config.dynamicInterval)
	if err != nil {
		return nil
	}

	cycleLength := seq.config.cycleLength
	width := cycleLength / 4

	mapTrigger := keyIntervalDecider(interval)

	mapFn := func(s slots) {
		// TODO(kvoli): support continuous fn for smoothing. e.g.
		//     k
		//
		//     |1 _ 1 2 3 2 1
		//     |1 _ 1 2 3 2 1
		//     |2 3 2 1 _ 1 2
		//keys |_ _ _ _ _ _ _
		//     |2 3 2 1 _ 1 2
		//     +-------------- t
		//           time
		s.uniformMap(rotateFn(cycleLength / 2))
	}

	readStaticDistribution := newDistribution(seq.config, readRandom)
	writeStaticDistribution := newDistribution(seq.config, writeRandom)

	readDistribution := newDynGen(width, cycleLength, mapTrigger, mapFn, readStaticDistribution, true)
	writeDistribution := newDynGen(width, cycleLength, mapTrigger, mapFn, writeStaticDistribution, true)

	return newDynamicGenerator(random, readDistribution, writeDistribution)
}

// shuffleGenerator returns a key generator that generates keys in a shuffled
// mapping of the underling distribution. The shuffling occurs at the interval
// given. Keys belong to a sequence of size k, where the sequences are
// shuffled. A sequence of size 1 implies that individual keys are shuffled. An
// example is shown below, where the y axis is the key space in ascending
// order, the x axis is time in  ascending order. The units displayed on the
// graph represent the QPS on the key k at time t.
//
//     k
//
//     |_ _ 1 1 3 3 _
//     |_ _ 2 2 _ _ 1
//keys |1 1 3 3 _ _ 2
//     |2 2 _ _ 1 1 3
//     |3 3 _ _ 2 2 _
//     +-------------- t
//           time
func shuffleGenerator(seq *sequence, random *rand.Rand) *dynamicGenerator {
	if random == nil {
		random = rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	}

	width := seq.config.cycleLength
	chunk := seq.config.shuffleChunk

	randoms := randomDupe(random, 4)
	writeRandom := randoms[0]
	readRandom := randoms[1]
	writeMapRandom := randoms[2]
	readMapRandom := randoms[3]

	interval, err := time.ParseDuration(seq.config.dynamicInterval)
	if err != nil {
		return nil
	}

	mapTrigger := keyIntervalDecider(interval)

	readMapFn := func(s slots) {
		s.partialMap(shuffleFn(readMapRandom, int(chunk)))
	}

	writeMapFn := func(s slots) {
		s.partialMap(shuffleFn(writeMapRandom, int(chunk)))
	}

	readStaticDistribution := newDistribution(seq.config, readRandom)
	writeStaticDistribution := newDistribution(seq.config, writeRandom)

	readDistribution := newDynGen(width, width, mapTrigger, readMapFn, readStaticDistribution, false)
	writeDistribution := newDynGen(width, width, mapTrigger, writeMapFn, writeStaticDistribution, false)

	// Create the slots by forcing the lazy initialization using an identityFn.
	writeDistribution.slots.partialMap(identityFn())
	readDistribution.slots.partialMap(identityFn())

	return newDynamicGenerator(random, readDistribution, writeDistribution)
}

// uniformMapFn is a mapping function that edits the slots:key assignment over
// every slot.
type uniformMapFn func(int64) int64

// rotateFn returns a uniformMapFn that rotates the slots by the rotation value
// given.
func rotateFn(rotation int64) uniformMapFn {
	return func(existing int64) int64 {
		return existing + rotation
	}
}

// partialMapFn is a mapping function that edits the slot:key assignment over
// 0-many slots.
type partialMapFn func([]int64) []int64

// shuffleFn returns a partialMapFn that shuffles chunks of the slot array. For
// example, if the underlying slot array were [4,1,3,2] and it were shuffled
// with chunk size 2, the possible resuls are: [4,1,3,2], [3,2,4,1].
func shuffleFn(rand *rand.Rand, chunk int) partialMapFn {
	return func(s []int64) []int64 {
		nSlots := len(s)
		newSlots := make([]int64, nSlots)

		// Sanity check that we don't have a negative or zero chunk size.
		if chunk < 1 {
			chunk = 1
		}

		nChunks := int(math.Ceil(float64(nSlots) / float64(chunk)))
		permArray := rand.Perm(nChunks)

		for i := 0; i < nChunks; i++ {
			for j := 0; j < chunk && i*chunk+j < nSlots; j++ {
				from := permArray[i]*chunk + j
				to := i*chunk + j
				newSlots[to] = s[from]
			}
		}
		return newSlots
	}
}

// identityFn returns a paritalMapFn that returns the same array that it is
// given..
func identityFn() partialMapFn {
	return func(s []int64) []int64 {
		return s
	}
}

// slots provides methods to apply partial and uniform mapping to keys. Keys
// are held in slots, which may be rearranged to change the resulting location
// of a key. Initially, slot 0 holds key 0, slot 1 holds key 1... slot n holds
// key n.
type slots interface {
	// uniformMap applies a function to all slots.
	uniformMap(uniformMapFn)
	// partialMap applies a function to a subset of underlying slots.
	partialMap(partialMapFn)
	// key returns the key held at the slot passed in.
	key(slot int64) int64
	// enforceUniform removes any individual slot mapping. All future calls to
	// partialMap will have no effect.
	enforceUniform()
}

// slotMap is an implementation of the slots interface.
type slotMap struct {
	slots        []int64
	uniformDelta int64
	nSlots       int64
	noSlot       bool
}

func (sm *slotMap) uniformMap(f uniformMapFn) {
	sm.uniformDelta = f(sm.uniformDelta)
}

func (sm *slotMap) partialMap(f partialMapFn) {
	// Disallow partial mapping when no slot has been declared.
	if sm.noSlot {
		return
	}

	// Lazily initialize the slot array when a partial map is applied for the
	// first time.
	if len(sm.slots) < int(sm.nSlots) {
		sm.slots = make([]int64, sm.nSlots)
		for i := range sm.slots {
			sm.slots[i] = int64(i)
		}
	}

	sm.slots = f(sm.slots)
}

func (sm *slotMap) key(slot int64) int64 {
	var slotKey int64
	slot = slot % sm.nSlots
	if sm.noSlot {
		slotKey = slot
	} else {
		slotKey = sm.slots[slot]
	}

	return (slotKey + sm.uniformDelta) % sm.nSlots
}

func (sm *slotMap) enforceUniform() { sm.noSlot = true }

type distribution interface {
	key(width int64) int64
}

func newDistribution(config *kv, random *rand.Rand) distribution {
	if config.sequential {
		return &sequentialD{}
	}
	if config.zipfian {
		return &zipfD{rand: random}
	}
	return &uniformD{rand: random}
}

type zipfD struct {
	rand *rand.Rand
}

func (z *zipfD) key(width int64) int64 {
	zipf := newZipf(1.1, 1, uint64(width))
	return width - int64(zipf.Uint64(z.rand))
}

type uniformD struct {
	rand *rand.Rand
}

func (u *uniformD) key(width int64) int64 {
	return u.rand.Int63n(width)
}

type sequentialD struct {
	seq int64
}

func (s *sequentialD) key(width int64) int64 {
	s.seq++
	return s.seq % width
}

type dynamicDistribution interface {
	key(time.Time) int64
	// TODO(kvoli): This is ugly, figure out a way to drop this from the iface.
	setLastMap(t time.Time)
}

type remapDeciderFn func(generated int64, lastRemap time.Time, now time.Time) (bool, time.Time, int64)

func keyDeltaDecider(delta int64) remapDeciderFn {
	return func(generated int64, lastRemap time.Time, now time.Time) (bool, time.Time, int64) {
		if generated < delta {
			return false, time.Time{}, 0
		}
		return true, now, generated - delta
	}
}

func keyIntervalDecider(interval time.Duration) remapDeciderFn {
	return func(generated int64, lastRemap time.Time, now time.Time) (bool, time.Time, int64) {
		if now.Before(lastRemap.Add(interval)) {
			return false, time.Time{}, 0
		}
		return true, now, generated
	}
}

// dynamicDist generates contains a distribution and generates keys for that
// distribution.
type dynamicDist struct {
	width     int64
	nSlots    int64
	generated int64

	shouldRemap remapDeciderFn
	lastRemap   time.Time

	distribution distribution
	slots        slots
	f            func(slots)
}

func newDynGen(
	width, nSlots int64,
	shouldRemap remapDeciderFn,
	f func(slots),
	distribution distribution,
	noSlot bool,
) *dynamicDist {
	return &dynamicDist{
		width:        width,
		nSlots:       nSlots,
		shouldRemap:  shouldRemap,
		f:            f,
		distribution: distribution,
		lastRemap:    timeutil.Now(),
		slots:        &slotMap{slots: []int64{}, noSlot: nSlots < 1 || noSlot, nSlots: nSlots},
	}
}

func (d *dynamicDist) key(t time.Time) int64 {
	for {
		should, newTime, newGenerated := d.shouldRemap(d.generated, d.lastRemap, t)
		if !should {
			break
		}
		d.lastRemap = newTime
		d.generated = newGenerated

		d.f(d.slots)
	}

	// Collect a distribution key, from the underlying distribution function.
	distributionSlot := d.distribution.key(d.width)

	// Map the slot to a physical key.
	slotKey := d.slots.key(distributionSlot)

	d.generated++

	return slotKey
}

func (d *dynamicDist) setLastMap(t time.Time) {
	d.lastRemap = t
}

// randomDupe takes a random instance that has been initialized and using it's
// current state, returns n random instances that are guaraanteed to have the
// same deterministic state.
func randomDupe(random *rand.Rand, n int) []*rand.Rand {
	seed := random.Int63()

	randoms := make([]*rand.Rand, n)
	for i := range randoms {
		randoms[i] = rand.New(rand.NewSource(seed))
	}
	return randoms
}
