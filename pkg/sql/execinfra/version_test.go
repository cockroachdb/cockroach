// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfra

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestVersionNotBumped is a sanity check that we won't ever bump DistSQL
// version in backwards-incompatible way.
func TestVersionNotBumped(t *testing.T) {
	defer leaktest.AfterTest(t)()

	require.Equal(t, 71, int(Version))            // DO NOT ADJUST
	require.Equal(t, 71, int(MinAcceptedVersion)) // DO NOT ADJUST
}
