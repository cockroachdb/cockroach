// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
)

func TestVolatilitySet(t *testing.T) {
	var v VolatilitySet

	check := func(str string, isLeakProof, hasStable, hasVolatile bool) {
		t.Helper()

		require.Equal(t, v.String(), str)
		require.Equal(t, v.IsLeakProof(), isLeakProof)
		require.Equal(t, v.HasStable(), hasStable)
		require.Equal(t, v.HasVolatile(), hasVolatile)
	}
	check("leak-proof", true, false, false)

	v.Add(tree.VolatilityLeakProof)
	check("leak-proof", true, false, false)

	v.AddImmutable()
	check("immutable", false, false, false)

	v.AddStable()
	check("stable", false, true, false)

	v.AddVolatile()
	check("stable+volatile", false, true, true)

	v = 0
	v.AddVolatile()
	check("volatile", false, false, true)

	var w VolatilitySet
	w.AddImmutable()
	v.UnionWith(w)
	check("volatile", false, false, true)

	w.AddStable()
	v.UnionWith(w)
	check("stable+volatile", false, true, true)
}
