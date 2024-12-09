// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metamorphic

import (
	"testing"

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
