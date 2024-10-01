// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVectorLoadString(t *testing.T) {
	require.Equal(t, "(queries-per-second=1.0 cpu-per-second=1ms)", Vector{1, 1000000}.String())
}
