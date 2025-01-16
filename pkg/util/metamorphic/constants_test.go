// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metamorphic

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestMetamorphicEligible sanity checks that any test code should be eligible
// to have metamorphic variables enabled.
func TestMetamorphicEligible(t *testing.T) {
	require.True(t, metamorphicEligible())
}

// TestMetamorphicFromOverride checks that overrides are used.
func TestMetamorphicFromOverride(t *testing.T) {
	setOverridesFromString("val2=7")
	var (
		_ = ConstantWithTestRange("val1", 1, 1, 100)
		v = ConstantWithTestRange("val2", 2, 1, 100)
		_ = ConstantWithTestRange("val3", 3, 1, 100)
	)
	require.Equal(t, 7, v)
}

// TestMetamorphicRngSeed checks that `constants.rngSeed` corresponds to `randutil.globalSeed`
func TestMetamorphicRngSeed(t *testing.T) {
	// Make sure metamorphic testing is enabled.
	require.True(t, metamorphicEligible())
	require.False(t, disableMetamorphicTesting)
	// N.B. We can't access `randutil.globalSeed` directly.
	// Instead, the call below forces NewTestRand to reseed its iternal RNG with the globalSeed.
	// The returned seed is _not_ the globalSeed, but one (application of) `Int63` away.
	testRng, testSeed := randutil.NewTestRand()
	// Thus, if rngSeed == globalSeed, it implies r(rngSeed).Int63() == testSeed.
	// N.B. The converse doesn't necessarily hold, as two different seeds could yield the same PRNG sequence. However,
	// it's extremely improbable. Hence, this assertion should suffice.
	expectedSeed := rand.New(rand.NewSource(rngSeed)).Int63()
	require.Equal(t, expectedSeed, testSeed)
	// On the off-chance there is a collision, let's do 100 iterations. The probability of two different seeds yielding
	// the same sequence of length 101 is infinitesimally small.
	r := rand.New(rand.NewSource(expectedSeed))
	for i := 0; i < 100; i++ {
		require.Equal(t, r.Int63(), testRng.Int63())
	}
}
