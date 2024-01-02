// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
