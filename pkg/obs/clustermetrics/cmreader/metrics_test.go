// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmreader

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRegistryReader(t *testing.T) {
	rr := NewRegistryReader()
	r, ok := rr.(*registry)
	require.True(t, ok)
	require.NotNil(t, r)
}
