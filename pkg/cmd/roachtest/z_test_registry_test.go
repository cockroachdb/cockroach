// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestMakeTestRegistry(t *testing.T) {
	testutils.RunTrueAndFalse(t, "preferSSD", func(t *testing.T, preferSSD bool) {
		r, err := makeTestRegistry(spec.AWS, "foo", "zone123", preferSSD)
		require.NoError(t, err)
		require.Equal(t, preferSSD, r.preferSSD)
		require.Equal(t, "zone123", r.zones)
		require.Equal(t, "foo", r.instanceType)
		require.Equal(t, spec.AWS, r.cloud)

		s := r.MakeClusterSpec(100, spec.Geo(), spec.Zones("zone99"), spec.CPU(12), spec.PreferSSD())
		require.EqualValues(t, 100, s.NodeCount)
		require.Equal(t, "foo", s.InstanceType)
		require.True(t, s.Geo)
		require.Equal(t, "zone99", s.Zones)
		require.EqualValues(t, 12, s.CPUs)
		require.True(t, s.PreferLocalSSD)
	})

}
