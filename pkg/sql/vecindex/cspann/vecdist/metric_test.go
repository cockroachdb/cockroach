// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecdist

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

func TestMetric(t *testing.T) {
	require.Equal(t, float32(10), Measure(L2Squared, vector.T{1, 2}, vector.T{4, 3}))
	require.Equal(t, float32(-10), Measure(InnerProduct, vector.T{1, 2}, vector.T{4, 3}))
	require.Equal(t, float32(-9), Measure(Cosine, vector.T{1, 2}, vector.T{4, 3}))
}
