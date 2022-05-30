// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workload

import (
	"container/list"
	"math/rand"
	"time"
)

// LoadEvent represent a key access that generates load against the database.
// TODO(kvoli): The single key interface is expensive when parsed. Consider
// pre-aggregating load events into batches to ammortize this cost.
type LoadEvent struct {
	IsWrite bool
	Size    int64
	Key     int64
}

// Generator generates a workload where each op contains: key,
// op type (e.g., read/write), size.
type Generator interface {
	// GetNext returns a LoadEvent which happens before or at maxTime, if exists.
	GetNext(maxTime time.Time) (done bool, event LoadEvent)
}

// RandomGenerator generates random operations within some limits.
type RandomGenerator struct {
	seed           int64
	keyGenerator   KeyGenerator
	rand           *rand.Rand
	lastRun        time.Time
	rollsPerSecond float64
	readRatio      float64
	maxSize        int
	minSize        int
	opBuffer       list.List
}

// NewRandomGenerator returns a generator that generates random operations
// within some limits.
func NewRandomGenerator(
	start time.Time,
	seed int64,
	keyGenerator KeyGenerator,
	rate float64,
	readRatio float64,
	maxSize int,
	minSize int,
) Generator {
	return newRandomGenerator(start, seed, keyGenerator, rate, readRatio, maxSize, minSize)
}

// newRandomGenerator returns a generator that generates random operations
// within some limits.
func newRandomGenerator(
	start time.Time,
	seed int64,
	keyGenerator KeyGenerator,
	rate float64,
	readRatio float64,
	maxSize int,
	minSize int,
) *RandomGenerator {
	return &RandomGenerator{
		seed:           seed,
		keyGenerator:   keyGenerator,
		rand:           keyGenerator.rand(),
		lastRun:        start,
		rollsPerSecond: rate,
		readRatio:      readRatio,
		maxSize:        maxSize,
		minSize:        minSize,
	}
}

// GetNext is part of the WorkloadGenerator interface.
func (rwg *RandomGenerator) GetNext(maxTime time.Time) (done bool, event LoadEvent) {
	rwg.maybeUpdateBuffer(maxTime)
	if next := rwg.opBuffer.Front(); next != nil {
		rwg.opBuffer.Remove(next)
		return false, next.Value.(LoadEvent)
	}
	return true, LoadEvent{}
}

// maybeUpdateBuffer checks the elapsed duration since last generating
// operations and the maxTime passed in. If the duration multiplied by the rate
// of operations per second is greater than or equal to 1, the operation buffer
// is updated with new generated operations.
func (rwg *RandomGenerator) maybeUpdateBuffer(maxTime time.Time) {
	elapsed := maxTime.Sub(rwg.lastRun).Seconds()
	count := int(elapsed * rwg.rollsPerSecond)
	// Do not attempt to generate additional load events if the elapsed
	// duration is not sufficiently large. If we did, this would bump the last
	// run to maxTime and we may end up in a cycle where no events are ever
	// generated if the rate of load events is less than the interval at which
	// this function is called.
	if count < 1 {
		return
	}

	// Here we skew slightly towards writes to take the difference in rounding.
	reads := int(float64(count) * rwg.readRatio)
	writes := count - reads
	for read := 0; read < reads; read++ {
		rwg.opBuffer.PushBack(
			LoadEvent{
				Size:    int64(rwg.rand.Intn(rwg.maxSize-rwg.minSize+1) + rwg.minSize),
				IsWrite: false,
				Key:     rwg.keyGenerator.readKey(),
			})
	}
	for write := 0; write < writes; write++ {
		rwg.opBuffer.PushBack(
			LoadEvent{
				Size:    int64(rwg.rand.Intn(rwg.maxSize-rwg.minSize+1) + rwg.minSize),
				IsWrite: true,
				Key:     rwg.keyGenerator.writeKey(),
			})
	}
	rwg.lastRun = maxTime
}

// KeyGenerator generates read and write keys.
type KeyGenerator interface {
	writeKey() int64
	readKey() int64
	rand() *rand.Rand
}

// uniformGenerator generates keys with a uniform distribution. Note that keys
// do not necessarily need to be written before they may have a read issued
// against them.
type uniformGenerator struct {
	cycle  int64
	random *rand.Rand
}

// NewUniformKeyGen returns a key generator that generates keys with a
// uniform distribution.
func NewUniformKeyGen(cycle int64, rand *rand.Rand) KeyGenerator {
	return &uniformGenerator{
		cycle:  cycle,
		random: rand,
	}
}

func (g *uniformGenerator) writeKey() int64 {
	return g.random.Int63n(g.cycle)
}

func (g *uniformGenerator) readKey() int64 {
	return g.random.Int63n(g.cycle)
}

func (g *uniformGenerator) rand() *rand.Rand {
	return g.random
}

// zipfianGenerator generates keys with a power-rank distribution. Note that keys
// do not necessarily need to be written before they may have a read issued
// against them.
type zipfianGenerator struct {
	cycle  int64
	random *rand.Rand
	zipf   *rand.Zipf
}

// NewZipfianKeyGen returns a key generator that generates reads and writes
// following a Zipfian distribution. Where few keys are relatively frequent,
// whilst the others are infrequently accessed. The generator generates values
// k ∈ [0, cycle] such that P(k) is proportional to (v + k) ** (-s).
// Requirements: cycle > 0, s > 1, and v >= 1
func NewZipfianKeyGen(cycle int64, s float64, v float64, random *rand.Rand) KeyGenerator {
	return &zipfianGenerator{
		cycle:  cycle,
		random: random,
		zipf:   rand.NewZipf(random, s, v, uint64(cycle)),
	}
}

func (g *zipfianGenerator) writeKey() int64 {
	return int64(g.zipf.Uint64())
}

func (g *zipfianGenerator) readKey() int64 {
	return int64(g.zipf.Uint64())
}

func (g *zipfianGenerator) rand() *rand.Rand {
	return g.random
}
