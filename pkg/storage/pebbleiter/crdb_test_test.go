// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pebbleiter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPebbleIterWrapped(t *testing.T) {
	// Sanity-check: make sure CrdbTestBuild is set. This should be true for
	// any test.
	require.NotNil(t, MaybeWrap(nil))
}
