// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randutil

import (
	crypto_rand "crypto/rand"
	"encoding/binary"
	"fmt"
	"log" // Don't bring cockroach/util/log into this low-level package.
	"math/rand"
	"runtime"
	"strings"
	"time"
	_ "unsafe" // required by go:linkname

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// lockedSource is a thread safe math/rand.Source. See math/rand/rand.go.
type lockedSource struct {
	mu  syncutil.Mutex
	src rand.Source64
}

// NewLockedSource creates random source protected by mutex.
func NewLockedSource(seed int64) rand.Source {
	return &lockedSource{
		src: rand.NewSource(seed).(rand.Source64),
	}
}

func (rng *lockedSource) Int63() (n int64) {
	rng.mu.Lock()
	defer rng.mu.Unlock()
	n = rng.src.Int63()
	return
}

func (rng *lockedSource) Uint64() (n uint64) {
	rng.mu.Lock()
	defer rng.mu.Unlock()
	n = rng.src.Uint64()
	return
}

func (rng *lockedSource) Seed(seed int64) {
	rng.mu.Lock()
	defer rng.mu.Unlock()
	rng.src.Seed(seed)
}

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

// Same as NewPseudoRand, but the returned Rand is using thread safe underlying source.
func NewLockedPseudoRand() (*rand.Rand, int64) {
	seed := envutil.EnvOrDefaultInt64("COCKROACH_RANDOM_SEED", NewPseudoSeed())
	return rand.New(NewLockedSource(seed)), seed
}

// NewPseudoRandWithGlobalSeed returns an instance of math/rand.Rand, which is
// seeded with the global seed.
// It's _not_ intended to be called directly from a test; use NewTestRand for that.
// Instead, this function is useful for seeding other random number generators, on which the tests
// may depend; e.g., metamorphic constants.
// N.B. unlike NewTestRand, this function _never_ reseeds rng.
func NewPseudoRandWithGlobalSeed() (*rand.Rand, int64) {
	return rand.New(rand.NewSource(globalSeed)), globalSeed
}

// NewTestRand returns an instance of math/rand.Rand seeded from rng, which is
// seeded with the global seed. If the caller is a test with a different
// path-qualified name than the previous caller, rng is reseeded from the global
// seed. This rand.Rand is useful in testing to produce deterministic,
// reproducible behavior.
func NewTestRand() (*rand.Rand, int64) {
	return newTestRandImpl(rand.NewSource)
}

// NewLockedTestRand is identical to NewTestRand but returned rand.Rand is using
// thread safe underlying source.
func NewLockedTestRand() (*rand.Rand, int64) {
	return newTestRandImpl(NewLockedSource)
}

func newTestRandImpl(f func(int64) rand.Source) (*rand.Rand, int64) {
	mtx.Lock()
	defer mtx.Unlock()
	fxn := getTestName()
	if fxn != "" && lastTestName != fxn {
		// Re-seed rng (the source of seeds for test random number generators) with
		// the global seed so that individual tests are reproducible using the
		// random seed.
		lastTestName = fxn
		rng = rand.New(f(globalSeed))
	}
	seed := rng.Int63()
	return rand.New(f(seed)), seed
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

// RandUint64n generates a 64-bit random number in [0, n) range.
// Note: n == 0 means n is math.MaxUint64 + 1
func RandUint64n(r *rand.Rand, n uint64) uint64 {
	if n == 0 {
		return r.Uint64()
	}
	// If n is less than 64 bits, delegate to 63 bit version.
	if n < (1 << 63) {
		return uint64(r.Int63n(int64(n)))
	}
	v := r.Uint64()
	for v > n {
		v = r.Uint64()
	}
	return v
}

// RandDuration returns a random duration in [0, max).
func RandDuration(r *rand.Rand, max time.Duration) time.Duration {
	return time.Duration(r.Int63n(int64(max)))
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

// PrintableKeyAlphabet to use with random string generation to produce strings
// that doesn't need to be escaped when found as a part of a key and is
// generally human printable.
const PrintableKeyAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// RandString generates a random string of the desired length from the
// input alphabet. It is useful when you want to generate keys that would
// be printable without further escaping if alphabet is restricted to
// alphanumeric chars.
func RandString(rng *rand.Rand, length int, alphabet string) string {
	runes := []rune(alphabet)
	var buf strings.Builder
	buf.Grow(length)
	for i := 0; i < length; i++ {
		buf.WriteRune(runes[rng.Intn(len(runes))])
	}
	return buf.String()
}

// SeedForTests seeds the random number generator and prints the seed
// value used. This function should be called from TestMain; individual tests
// should not touch the seed of the global random number generator.
func SeedForTests() {
	//lint:ignore SA1019 deprecated
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
