// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
