// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package load

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVectorLoadString(t *testing.T) {
	require.Equal(t, "(queries-per-second=1.0 cpu-per-second=1ms)", Vector{1, 1000000}.String())
}
