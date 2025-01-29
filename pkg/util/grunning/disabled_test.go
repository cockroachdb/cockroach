// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// See grunning.Supported() for an explanation behind this build tag.
//
//go:build !bazel

package grunning_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/stretchr/testify/require"
)

func TestDisabled(t *testing.T) {
	require.False(t, grunning.Supported())
	require.Zero(t, grunning.Time())
	require.Zero(t, grunning.Difference(grunning.Time(), grunning.Time()))
	require.Zero(t, grunning.Elapsed(grunning.Time(), grunning.Time()))
}
