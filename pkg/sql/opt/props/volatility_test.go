// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package props

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/stretchr/testify/require"
)

func TestVolatilitySet(t *testing.T) {
	var v VolatilitySet

	check := func(str string, isLeakproof, hasStable, hasVolatile bool) {
		t.Helper()

		require.Equal(t, v.String(), str)
		require.Equal(t, v.IsLeakproof(), isLeakproof)
		require.Equal(t, v.HasStable(), hasStable)
		require.Equal(t, v.HasVolatile(), hasVolatile)
	}
	check("leakproof", true, false, false)

	v.Add(volatility.Leakproof)
	check("leakproof", true, false, false)

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
