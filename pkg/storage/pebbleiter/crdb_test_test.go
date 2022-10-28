// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
