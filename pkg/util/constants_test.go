// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMetamorphicEligible sanity checks that any test code should be eligible
// to have metamorphic variables enabled.
func TestMetamorphicEligible(t *testing.T) {
	require.True(t, metamorphicEligible())
}
