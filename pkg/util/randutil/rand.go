// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package randutil

import (
	crypto_rand "crypto/rand"
	"encoding/binary"
	"fmt"
	"log" // Don't bring cockroach/util/log into this low-level package.
	"math/rand"
	"runtime"
	"strings"
	_ "unsafe" // required by go:linkname

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// globalSeed contains a pseudo random seed that should only be used in tests.
var globalSeed int64

// rng is a random number generator used to generate seeds for test random
// number generators.
var rng *rand.Rand

// lastTestName is the function name of the last test we have seen.
var lastTestName string

// mtx protects rng and lastTestName.
var mtx syncutil.Mutex

// Initializes the global random seed. This value can be specified via an
// environment variable COCKROACH_RANDOM_SEED=x.
func init() {
	globalSeed = envutil.EnvOrDefaultInt64("COCKROACH_RANDOM_SEED", NewPseudoSeed())
	rng = rand.New(rand.NewSource(globalSeed))
}

// NewPseudoSeed generates a seed from crypto/rand.
func NewPseudoSeed() int64 {
	var seed int64
	err := binary.Read(crypto_rand.Reader, binary.LittleEndian, &seed)
	if err != nil {
		panic(fmt.Sprintf("could not read from crypto/rand: %s", err))
	}
	return seed
}

// NewPseudoRand returns an instance of math/rand.Rand seeded from the
// environment variable COCKROACH_RANDOM_SEED.  If that variable is not set,
// crypto/rand is used to generate a seed. The seed is also returned so we can
// easily and cheaply generate unique streams of numbers. The created object is
// not safe for concurrent access.
func NewPseudoRand() (*rand.Rand, int64) {
	seed := envutil.EnvOrDefaultInt64("COCKROACH_RANDOM_SEED", NewPseudoSeed())
	return rand.New(rand.NewSource(seed)), seed
}

// NewTestRand returns an instance of math/rand.Rand seeded from rng, which is
// seeded with the global seed. If the caller is a test with a different
// path-qualified name than the previous caller, rng is reseeded from the global
// seed. This rand.Rand is useful in testing to produce deterministic,
// reproducible behavior.
func NewTestRand() (*rand.Rand, int64) {
	mtx.Lock()
	defer mtx.Unlock()
	fxn := getTestName()
	if fxn != "" && lastTestName != fxn {
		// Re-seed rng (the source of seeds for test random number generators) with
		// the global seed so that individual tests are reproducible using the
		// random seed.
		lastTestName = fxn
		rng = rand.New(rand.NewSource(globalSeed))
	}
	seed := rng.Int63()
	return rand.New(rand.NewSource(seed)), seed
}

// NewTestRandWithSeed returns an instance of math/rand.Rand, similar to
// NewTestRand, but with the seed specified.
func NewTestRandWithSeed(seed int64) *rand.Rand {
	mtx.Lock()
	defer mtx.Unlock()
	fxn := getTestName()
	if fxn != "" && lastTestName != fxn {
		lastTestName = fxn
	}
	return rand.New(rand.NewSource(seed))
}

// RandIntInRange returns a value in [min, max)
func RandIntInRange(r *rand.Rand, min, max int) int {
	return min + r.Intn(max-min)
}

// RandInt63InRange returns a value in [min, max)
func RandInt63InRange(r *rand.Rand, min, max int64) int64 {
	return min + r.Int63n(max-min)
}

var randLetters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandBytes returns a byte slice of the given length with random
// data.
func RandBytes(r *rand.Rand, size int) []byte {
	if size <= 0 {
		return nil
	}

	arr := make([]byte, size)
	for i := 0; i < len(arr); i++ {
		arr[i] = randLetters[r.Intn(len(randLetters))]
	}
	return arr
}

// FastUint32 returns a lock free uint32 value. Compared to rand.Uint32, this
// implementation scales. We're using the go runtime's implementation through a
// linker trick.
//
//go:linkname FastUint32 runtime.fastrand
func FastUint32() uint32

// FastInt63 returns a non-negative pseudo-random 63-bit integer as an int64.
// Compared to rand.Int63(), this implementation scales.
func FastInt63() int64 {
	x, y := FastUint32(), FastUint32() // 32-bit halves
	u := uint64(x)<<32 ^ uint64(y)
	i := int64(u >> 1) // clear sign bit
	return i
}

// ReadTestdataBytes reads random bytes, but then nudges them into printable
// ASCII, *reducing their randomness* to make them a little friendlier for
// humans using them as testdata.
func ReadTestdataBytes(r *rand.Rand, arr []byte) {
	_, _ = r.Read(arr)
	for i := range arr {
		arr[i] = arr[i] & 0x7F // mask out non-ascii
		if arr[i] < ' ' {      // Nudge the control chars up, into the letters.
			arr[i] += 'A'
		}
	}
}

// SeedForTests seeds the random number generator and prints the seed
// value used. This function should be called from TestMain; individual tests
// should not touch the seed of the global random number generator.
func SeedForTests() {
	rand.Seed(globalSeed)
	log.Printf("random seed: %v", globalSeed)
}

// getTestName returns the calling test function name, returning an empty string
// if not found. The number of calls up the call stack is limited.
func getTestName() string {
	pcs := make([]uintptr, 10)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		fxn := frame.Function
		if strings.Contains(fxn, ".Test") {
			return fxn
		}
		if !more {
			break
		}
	}
	return ""
}
